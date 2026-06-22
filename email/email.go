// Package email provides a provider-neutral interface over transactional
// email backends: AWS SES (SESv2), Azure Communication Services (ACS) email,
// and raw SMTP.
//
// Construct a backend once at service startup via New (or NewSES /
// NewAzureACS / NewSMTP) and reuse it — backends hold long-lived clients.
// Release sockets at shutdown with Close.
//
// Build an EmailMessage and pass it to Send. From falls back to the backend's
// DefaultFrom when empty; at least one of BodyText / BodyHTML must be set.
package email

import (
	"context"
	"fmt"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// Attachment is an email attachment.
//
// Content is the raw payload bytes. ContentType is used directly in the MIME /
// provider request — pick the right one ("application/pdf", "image/png", ...)
// so the recipient's mail client renders it correctly. An empty ContentType
// defaults to "application/octet-stream".
type Attachment struct {
	Filename    string
	Content     []byte
	ContentType string
}

// EmailMessage is an outbound email.
//
// From falls back to the backend's DefaultFrom when empty. At least one of
// BodyText / BodyHTML must be set.
type EmailMessage struct {
	To          []string
	Subject     string
	BodyText    string
	BodyHTML    string
	From        string
	Cc          []string
	Bcc         []string
	ReplyTo     []string
	Attachments []Attachment
	Headers     map[string]string
}

// Backend is the provider-neutral transactional email interface.
type Backend interface {
	// Send sends a single email and returns the provider message ID.
	Send(ctx context.Context, msg EmailMessage) (string, error)
	// SendBatch sends multiple emails and returns their message IDs.
	SendBatch(ctx context.Context, msgs []EmailMessage) ([]string, error)
	// HealthCheck returns true if the email backend is reachable (never errors).
	HealthCheck(ctx context.Context) bool
	// Close releases the underlying client and sockets.
	Close(ctx context.Context) error
}

// Config carries the union of provider options. Only the fields relevant to
// the chosen provider are read; the factory routes to the appropriate auth
// method based on which credential fields are set.
type Config struct {
	// DefaultFrom is the sender used when EmailMessage.From is empty.
	DefaultFrom string

	// AWS SES (SESv2).
	Region             string // default "us-east-1"
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string
	ProfileName        string
	EndpointURL        string // custom endpoint (LocalStack, ...)

	// Azure Communication Services email.
	ConnectionString string // "endpoint=https://...;accesskey=BASE64KEY"
	Endpoint         string // https://<resource>.communication.azure.com
	TenantID         string
	ClientID         string // service principal app ID, or user-assigned MI client ID
	ClientSecret     string

	// SMTP.
	Host     string
	Port     int    // default depends on Mode (587 starttls, 465 tls, 25 plaintext)
	Username string
	Password string
	Mode     string // "starttls" (default), "tls", "plaintext"
}

// New instantiates an email backend.
//
// provider is "ses", "azure_acs", or "smtp". The auth method is inferred from
// which credential fields are set, exactly as in the Python library:
//
//	New(ctx, "ses", Config{Region: "us-east-1", DefaultFrom: "noreply@example.com"})            // IAM role / env
//	New(ctx, "ses", Config{AWSAccessKeyID: "...", AWSSecretAccessKey: "...", DefaultFrom: "..."})
//	New(ctx, "azure_acs", Config{ConnectionString: "endpoint=https://...;accesskey=...", DefaultFrom: "..."})
//	New(ctx, "azure_acs", Config{Endpoint: "https://...communication.azure.com", DefaultFrom: "..."}) // managed identity
//	New(ctx, "smtp", Config{Host: "smtp.sendgrid.net", Username: "apikey", Password: "...", DefaultFrom: "..."})
//	New(ctx, "smtp", Config{Mode: "tls", Host: "smtp.example.com", Port: 465, Username: "u", Password: "p", DefaultFrom: "..."})
func New(ctx context.Context, provider string, cfg Config) (Backend, error) {
	switch provider {
	case "ses":
		return NewSES(ctx, cfg)
	case "azure_acs":
		return NewAzureACS(cfg)
	case "smtp":
		return NewSMTP(cfg)
	}
	return nil, fmt.Errorf("%w: unknown email provider %q (choose 'ses', 'azure_acs', or 'smtp')",
		core.ErrEmail, provider)
}

// contentType returns the attachment's content type, defaulting to
// "application/octet-stream" when unset.
func (a Attachment) contentType() string {
	if a.ContentType == "" {
		return "application/octet-stream"
	}
	return a.ContentType
}

// resolveSender returns msg.From, falling back to defaultFrom, and validates
// that a sender and at least one body part are present.
func resolveSender(msg EmailMessage, defaultFrom string) (string, error) {
	sender := msg.From
	if sender == "" {
		sender = defaultFrom
	}
	if sender == "" {
		return "", fmt.Errorf("%w: no sender address: set EmailMessage.From or Config.DefaultFrom",
			core.ErrEmail)
	}
	if msg.BodyText == "" && msg.BodyHTML == "" {
		return "", fmt.Errorf("%w: Send requires BodyText and/or BodyHTML", core.ErrEmail)
	}
	return sender, nil
}

// sendBatchLoop is the default SendBatch implementation: it loops Send.
func sendBatchLoop(ctx context.Context, b Backend, msgs []EmailMessage) ([]string, error) {
	ids := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		id, err := b.Send(ctx, msg)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
