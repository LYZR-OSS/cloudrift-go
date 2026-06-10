// Package pubsub provides a provider-neutral interface over topic-based
// pub/sub backends: AWS SNS and Azure Event Grid.
//
// Unlike package messaging (point-to-point queues), pub/sub backends fan out
// messages to multiple subscribers via topics.
package pubsub

import (
	"context"
	"fmt"

	"github.com/lyzr-ai/cloudrift-go/core"
)

// BatchMessage is one entry of PublishBatch.
type BatchMessage struct {
	Message    string
	Attributes map[string]string
}

// Backend is the provider-neutral pub/sub interface.
type Backend interface {
	// Publish publishes a message to a topic (an SNS topic ARN or an Event
	// Grid event source). Returns the message ID.
	Publish(ctx context.Context, topic, message string, attributes map[string]string) (string, error)
	// PublishBatch publishes multiple messages to a topic. Returns their IDs.
	PublishBatch(ctx context.Context, topic string, messages []BatchMessage) ([]string, error)
	// HealthCheck returns true if the pub/sub backend is reachable (never errors).
	HealthCheck(ctx context.Context) bool
	// Close releases the underlying client and sockets.
	Close(ctx context.Context) error
}

// Config carries the union of provider options. Only the fields relevant to
// the chosen provider are read; the factory routes to the appropriate auth
// method based on which credential fields are set.
type Config struct {
	// AWS SNS.
	Region             string // default "us-east-1"
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string
	ProfileName        string
	EndpointURL        string // custom endpoint (LocalStack, ...)

	// Azure Event Grid.
	Endpoint     string // topic endpoint URL
	AccessKey    string
	TenantID     string
	ClientID     string // service principal app ID, or user-assigned MI client ID
	ClientSecret string
}

// New instantiates a pub/sub backend.
//
// provider is "sns" or "azure_eventgrid". The auth method is inferred from
// which credential fields are set, exactly as in the Python library:
//
//	New(ctx, "sns", Config{Region: "us-east-1"})  // IAM role / env
//	New(ctx, "sns", Config{AWSAccessKeyID: "...", AWSSecretAccessKey: "..."})
//	New(ctx, "azure_eventgrid", Config{Endpoint: "https://...", AccessKey: "..."})
//	New(ctx, "azure_eventgrid", Config{Endpoint: "...", TenantID: "...",
//	    ClientID: "...", ClientSecret: "..."})
func New(ctx context.Context, provider string, cfg Config) (Backend, error) {
	switch provider {
	case "sns":
		return NewSNS(ctx, cfg)
	case "azure_eventgrid":
		return NewAzureEventGrid(cfg)
	}
	return nil, fmt.Errorf("%w: unknown pubsub provider %q (choose 'sns' or 'azure_eventgrid')",
		core.ErrPubSub, provider)
}
