// Package messaging provides a provider-neutral interface over point-to-point
// cloud queues: AWS SQS and Azure Service Bus.
//
// Construct a backend once at service startup via New (or NewSQS /
// NewAzureServiceBus) and reuse it — backends hold long-lived clients.
// Release sockets at shutdown with Close.
package messaging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// Message is a received queue message.
type Message struct {
	ID string
	// Body is the raw message payload bytes (binary-safe). Use Message.JSON()
	// to decode a JSON object body.
	Body []byte
	// ReceiptHandle acknowledges the message via Delete or DeadLetter.
	ReceiptHandle string
	// Attributes are the message's user attributes — SQS MessageAttributes /
	// Azure Service Bus ApplicationProperties — stringified.
	Attributes map[string]string
}

// JSON decodes the message body as a JSON object. Convenience for the common
// case where the body is JSON (e.g. produced via SendJSON).
func (m Message) JSON() (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal(m.Body, &out); err != nil {
		return nil, fmt.Errorf("%w: decoding message body: %w", core.ErrMessaging, err)
	}
	return out, nil
}

// OutgoingMessage is one message for SendBatch: raw body bytes plus optional
// user attributes.
type OutgoingMessage struct {
	Body       []byte
	Attributes map[string]string
}

// Backend is the provider-neutral queue interface. Bodies are raw bytes
// (binary-safe) so any payload — JSON, OTLP protobuf, base64 — round-trips
// unchanged. Use SendJSON / Message.JSON for the JSON-object convenience path.
type Backend interface {
	// Send sends a raw message body with optional user attributes (SQS
	// MessageAttributes / Service Bus ApplicationProperties). delay postpones
	// visibility. Returns the message ID.
	Send(ctx context.Context, body []byte, attributes map[string]string, delay time.Duration) (string, error)
	// SendBatch sends multiple messages. Returns their message IDs.
	SendBatch(ctx context.Context, messages []OutgoingMessage) ([]string, error)
	// Receive fetches up to maxMessages, long-polling up to waitTime.
	Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]Message, error)
	// Delete acknowledges a message by its receipt handle.
	Delete(ctx context.Context, receiptHandle string) error
	// DeadLetter moves a received message to the dead-letter queue and
	// acknowledges it. Azure Service Bus implements this natively; SQS has no
	// per-message dead-letter API, so the backend emulates it by re-sending
	// the body to the configured DLQ and deleting the original.
	DeadLetter(ctx context.Context, receiptHandle, reason string) error
	// GetQueueDepth returns the approximate number of waiting messages. This
	// is an estimate: cloud queues report it asynchronously and it may lag
	// in-flight (received-but-not-yet-deleted) messages.
	GetQueueDepth(ctx context.Context) (int64, error)
	// Purge deletes all messages in the queue.
	Purge(ctx context.Context) error
	// HealthCheck returns true if the queue is reachable (never errors).
	HealthCheck(ctx context.Context) bool
	// Close releases the underlying client and sockets.
	Close(ctx context.Context) error
}

// Config carries the union of provider options. Only the fields relevant to
// the chosen provider are read; the factory routes to the appropriate auth
// method based on which credential fields are set.
type Config struct {
	// AWS SQS.
	QueueURL           string
	Region             string // default "us-east-1"
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string
	ProfileName        string
	EndpointURL        string // custom endpoint (LocalStack, ...)
	// RoleARN, when set, is assumed via STS on top of the base credentials
	// (static keys / profile / default chain). ExternalID is passed in the
	// AssumeRole call when set. Enables cross-account SQS access.
	RoleARN    string
	ExternalID string
	// DLQURL is the dead-letter queue URL for SQS DeadLetter emulation. If
	// empty it is resolved lazily from the source queue's RedrivePolicy.
	DLQURL string

	// Azure Service Bus.
	QueueName               string
	ConnectionString        string
	FullyQualifiedNamespace string // <ns>.servicebus.windows.net
	TenantID                string
	ClientID                string // service principal app ID, or user-assigned MI client ID
	ClientSecret            string
}

// New instantiates a messaging backend.
//
// provider is "sqs" or "azure_bus". The auth method is inferred from which
// credential fields are set, exactly as in the Python library:
//
//	New(ctx, "sqs", Config{QueueURL: "https://sqs...", Region: "us-east-1"})  // IAM role / env
//	New(ctx, "sqs", Config{QueueURL: "...", AWSAccessKeyID: "...", AWSSecretAccessKey: "..."})
//	New(ctx, "azure_bus", Config{ConnectionString: "...", QueueName: "q"})
//	New(ctx, "azure_bus", Config{FullyQualifiedNamespace: "ns.servicebus.windows.net",
//	    QueueName: "q"})                                                      // managed identity
func New(ctx context.Context, provider string, cfg Config) (Backend, error) {
	switch provider {
	case "sqs":
		return NewSQS(ctx, cfg)
	case "azure_bus":
		return NewAzureServiceBus(cfg)
	}
	return nil, fmt.Errorf("%w: unknown messaging provider %q (choose 'sqs' or 'azure_bus')",
		core.ErrMessaging, provider)
}

// SendJSON is a thin convenience over Send for the common case of a JSON-object
// payload: it marshals message and sends it with the given attributes/delay.
func SendJSON(ctx context.Context, b Backend, message map[string]any, attributes map[string]string, delay time.Duration) (string, error) {
	body, err := json.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("%w: %w", core.ErrMessageSend, err)
	}
	return b.Send(ctx, body, attributes, delay)
}

// newID returns a random RFC 4122 v4 UUID string (used where the provider SDK
// does not assign client-side message IDs).
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	dst := make([]byte, 36)
	hex.Encode(dst, b[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], b[10:])
	return string(dst)
}
