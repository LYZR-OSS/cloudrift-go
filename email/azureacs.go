package email

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

const acsAPIVersion = "2023-03-31"

// acsTokenScope is the Entra scope for ACS data-plane access.
var acsTokenScope = []string{"https://communication.azure.com/.default"}

// AzureACSBackend is the Azure Communication Services email backend.
//
// There is no official Go SDK for ACS email, so this backend talks to the ACS
// REST API directly. Two auth modes are supported, chosen by NewAzureACS:
//   - access key (from ConnectionString or Endpoint) → HMAC-SHA256 request
//     signing
//   - Entra (service principal / managed identity) → Bearer token
type AzureACSBackend struct {
	endpoint    string
	defaultFrom string
	httpClient  *http.Client

	// Access-key auth.
	accessKey []byte // decoded HMAC key; nil when using token auth

	// Token auth.
	cred azcore.TokenCredential // nil when using access-key auth
}

var _ Backend = (*AzureACSBackend)(nil)

// NewAzureACS constructs an ACS email backend. Auth is inferred from Config:
//   - ConnectionString set → access-key signing (endpoint + key parsed from it)
//   - ClientSecret set → service principal (TenantID + ClientID + ClientSecret)
//   - otherwise → managed identity (Endpoint required)
func NewAzureACS(cfg Config) (*AzureACSBackend, error) {
	b := &AzureACSBackend{
		endpoint:    cfg.Endpoint,
		defaultFrom: cfg.DefaultFrom,
		httpClient:  http.DefaultClient,
	}

	if cfg.ConnectionString != "" {
		endpoint, key, err := parseACSConnectionString(cfg.ConnectionString)
		if err != nil {
			return nil, err
		}
		b.endpoint = endpoint
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil {
			return nil, fmt.Errorf("%w: ACS access key is not valid base64: %w", core.ErrEmail, err)
		}
		b.accessKey = decoded
		return b, nil
	}

	if b.endpoint == "" {
		return nil, fmt.Errorf("%w: ACS requires Endpoint or ConnectionString", core.ErrEmail)
	}

	var cred azcore.TokenCredential
	var err error
	if cfg.ClientSecret != "" {
		cred, err = azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
	} else {
		var miOpts *azidentity.ManagedIdentityCredentialOptions
		if cfg.ClientID != "" {
			miOpts = &azidentity.ManagedIdentityCredentialOptions{ID: azidentity.ClientID(cfg.ClientID)}
		}
		cred, err = azidentity.NewManagedIdentityCredential(miOpts)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrEmail, err)
	}
	b.cred = cred
	return b, nil
}

// acsMessage is the JSON request body for POST /emails:send.
type acsMessage struct {
	SenderAddress string            `json:"senderAddress"`
	Content       acsContent        `json:"content"`
	Recipients    acsRecipients     `json:"recipients"`
	Attachments   []acsAttachment   `json:"attachments,omitempty"`
	ReplyTo       []acsAddress      `json:"replyTo,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
}

type acsContent struct {
	Subject   string `json:"subject"`
	PlainText string `json:"plainText,omitempty"`
	HTML      string `json:"html,omitempty"`
}

type acsRecipients struct {
	To  []acsAddress `json:"to"`
	Cc  []acsAddress `json:"cc,omitempty"`
	Bcc []acsAddress `json:"bcc,omitempty"`
}

type acsAddress struct {
	Address string `json:"address"`
}

type acsAttachment struct {
	Name            string `json:"name"`
	ContentType     string `json:"contentType"`
	ContentInBase64 string `json:"contentInBase64"`
}

func addresses(addrs []string) []acsAddress {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]acsAddress, len(addrs))
	for i, a := range addrs {
		out[i] = acsAddress{Address: a}
	}
	return out
}

func (b *AzureACSBackend) Send(ctx context.Context, msg EmailMessage) (string, error) {
	sender, err := resolveSender(msg, b.defaultFrom)
	if err != nil {
		return "", err
	}

	body := acsMessage{
		SenderAddress: sender,
		Content:       acsContent{Subject: msg.Subject, PlainText: msg.BodyText, HTML: msg.BodyHTML},
		Recipients: acsRecipients{
			To:  addresses(msg.To),
			Cc:  addresses(msg.Cc),
			Bcc: addresses(msg.Bcc),
		},
		ReplyTo: addresses(msg.ReplyTo),
		Headers: msg.Headers,
	}
	for _, att := range msg.Attachments {
		body.Attachments = append(body.Attachments, acsAttachment{
			Name:            att.Filename,
			ContentType:     att.contentType(),
			ContentInBase64: base64.StdEncoding.EncodeToString(att.Content),
		})
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("%w: marshaling ACS request: %w", core.ErrEmailSend, err)
	}

	endpoint := strings.TrimRight(b.endpoint, "/")
	reqURL := fmt.Sprintf("%s/emails:send?api-version=%s", endpoint, acsAPIVersion)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("%w: %w", core.ErrEmailSend, err)
	}
	req.Header.Set("Content-Type", "application/json")

	if err := b.authorize(ctx, req, payload); err != nil {
		return "", err
	}

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: %w", core.ErrEmailSend, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", mapACSErr(resp.StatusCode, string(respBody))
	}

	// The send is async (typically 202). Prefer the JSON operation id, then
	// the request id header.
	var parsed struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(respBody, &parsed); err == nil && parsed.ID != "" {
		return parsed.ID, nil
	}
	if id := resp.Header.Get("x-ms-request-id"); id != "" {
		return id, nil
	}
	if loc := resp.Header.Get("Operation-Location"); loc != "" {
		return loc, nil
	}
	return "", nil
}

func (b *AzureACSBackend) SendBatch(ctx context.Context, msgs []EmailMessage) ([]string, error) {
	return sendBatchLoop(ctx, b, msgs)
}

func (b *AzureACSBackend) HealthCheck(ctx context.Context) bool {
	// Validate that the configured auth path can produce a credential. A full
	// reachability probe would require sending an email, so we keep it cheap.
	if b.cred != nil {
		_, err := b.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: acsTokenScope})
		return err == nil
	}
	return b.endpoint != "" && len(b.accessKey) > 0
}

// Close is a no-op: ACS uses the shared HTTP transport managed by Go.
func (b *AzureACSBackend) Close(ctx context.Context) error { return nil }

// authorize attaches the appropriate auth header to req for the given body.
func (b *AzureACSBackend) authorize(ctx context.Context, req *http.Request, body []byte) error {
	if b.accessKey != nil {
		signACSRequest(req, body, b.accessKey)
		return nil
	}
	token, err := b.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: acsTokenScope})
	if err != nil {
		return fmt.Errorf("%w: acquiring ACS token: %w", core.ErrEmailSend, err)
	}
	req.Header.Set("Authorization", "Bearer "+token.Token)
	return nil
}

// signACSRequest signs req with the ACS HMAC-SHA256 scheme in place.
func signACSRequest(req *http.Request, body []byte, key []byte) {
	now := time.Now().UTC().Format(http.TimeFormat) // RFC1123 GMT
	contentHash := acsContentHash(body)

	pathAndQuery := req.URL.Path
	if req.URL.RawQuery != "" {
		pathAndQuery += "?" + req.URL.RawQuery
	}
	host := req.URL.Host

	stringToSign := strings.Join([]string{
		req.Method,
		pathAndQuery,
		now + ";" + host + ";" + contentHash,
	}, "\n")

	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	req.Header.Set("x-ms-date", now)
	req.Header.Set("Date", now)
	req.Header.Set("x-ms-content-sha256", contentHash)
	req.Header.Set("Authorization",
		"HMAC-SHA256 SignedHeaders=x-ms-date;host;x-ms-content-sha256&Signature="+signature)
}

// acsContentHash returns base64(SHA256(body)).
func acsContentHash(body []byte) string {
	sum := sha256.Sum256(body)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// parseACSConnectionString extracts the endpoint and base64 access key from a
// connection string of the form "endpoint=https://...;accesskey=BASE64KEY".
func parseACSConnectionString(cs string) (endpoint, accessKey string, err error) {
	for _, part := range strings.Split(cs, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "endpoint":
			endpoint = strings.TrimSpace(v)
		case "accesskey":
			accessKey = strings.TrimSpace(v)
		}
	}
	if endpoint == "" || accessKey == "" {
		return "", "", fmt.Errorf("%w: ACS connection string must contain endpoint= and accesskey=", core.ErrEmail)
	}
	if _, perr := url.Parse(endpoint); perr != nil {
		return "", "", fmt.Errorf("%w: invalid ACS endpoint: %w", core.ErrEmail, perr)
	}
	return endpoint, accessKey, nil
}

func mapACSErr(status int, body string) error {
	switch status {
	case http.StatusTooManyRequests:
		return fmt.Errorf("%w: ACS %d: %s", core.ErrEmailThrottled, status, body)
	case http.StatusBadRequest, http.StatusForbidden:
		low := strings.ToLower(body)
		if strings.Contains(low, "sender") || strings.Contains(low, "domain") {
			return fmt.Errorf("%w: ACS %d: %s", core.ErrSenderUnverified, status, body)
		}
		if strings.Contains(low, "recipient") {
			return fmt.Errorf("%w: ACS %d: %s", core.ErrRecipientRejected, status, body)
		}
	}
	return fmt.Errorf("%w: ACS %d: %s", core.ErrEmailSend, status, body)
}
