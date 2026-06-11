package pubsub

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/messaging"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventgrid/publisher"

	"github.com/NeuralgoLyzr/cloudrift-go/core"
)

// AzureEventGridBackend is the Azure Event Grid pub/sub backend. It publishes
// CloudEvent messages to an Event Grid topic endpoint.
//
// Authentication is chosen by NewAzureEventGrid based on which Config fields
// are set: access key, service principal (ClientSecret), or managed identity.
type AzureEventGridBackend struct {
	client *publisher.Client
}

var _ Backend = (*AzureEventGridBackend)(nil)

// NewAzureEventGrid constructs an Event Grid backend for cfg.Endpoint.
func NewAzureEventGrid(cfg Config) (*AzureEventGridBackend, error) {
	var client *publisher.Client
	var err error
	switch {
	case cfg.AccessKey != "":
		client, err = publisher.NewClientWithSharedKeyCredential(
			cfg.Endpoint, azcore.NewKeyCredential(cfg.AccessKey), nil)

	case cfg.ClientSecret != "":
		var cred *azidentity.ClientSecretCredential
		cred, err = azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
		if err == nil {
			client, err = publisher.NewClient(cfg.Endpoint, cred, nil)
		}

	default: // managed identity (system or user-assigned via ClientID)
		var miOpts *azidentity.ManagedIdentityCredentialOptions
		if cfg.ClientID != "" {
			miOpts = &azidentity.ManagedIdentityCredentialOptions{ID: azidentity.ClientID(cfg.ClientID)}
		}
		var cred *azidentity.ManagedIdentityCredential
		cred, err = azidentity.NewManagedIdentityCredential(miOpts)
		if err == nil {
			client, err = publisher.NewClient(cfg.Endpoint, cred, nil)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrPubSub, err)
	}
	return &AzureEventGridBackend{client: client}, nil
}

func (b *AzureEventGridBackend) Publish(ctx context.Context, topic, message string, attributes map[string]string) (string, error) {
	event, err := newCloudEvent(topic, message, attributes)
	if err != nil {
		return "", err
	}
	if _, err := b.client.PublishCloudEvents(ctx, []messaging.CloudEvent{event}, nil); err != nil {
		return "", mapEGErr(err, topic)
	}
	return event.ID, nil
}

func (b *AzureEventGridBackend) PublishBatch(ctx context.Context, topic string, messages []BatchMessage) ([]string, error) {
	events := make([]messaging.CloudEvent, 0, len(messages))
	ids := make([]string, 0, len(messages))
	for _, msg := range messages {
		event, err := newCloudEvent(topic, msg.Message, msg.Attributes)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
		ids = append(ids, event.ID)
	}
	if _, err := b.client.PublishCloudEvents(ctx, events, nil); err != nil {
		return nil, mapEGErr(err, topic)
	}
	return ids, nil
}

// HealthCheck is best-effort: Event Grid has no lightweight ping.
func (b *AzureEventGridBackend) HealthCheck(ctx context.Context) bool { return true }

// Close is a no-op: the publisher client shares an HTTP transport managed by Go.
func (b *AzureEventGridBackend) Close(ctx context.Context) error { return nil }

func newCloudEvent(topic, message string, attributes map[string]string) (messaging.CloudEvent, error) {
	var opts *messaging.CloudEventOptions
	if len(attributes) > 0 {
		ext := make(map[string]any, len(attributes))
		for k, v := range attributes {
			ext[k] = v
		}
		opts = &messaging.CloudEventOptions{Extensions: ext}
	}
	event, err := messaging.NewCloudEvent(topic, "cloudrift.event", message, opts)
	if err != nil {
		return messaging.CloudEvent{}, fmt.Errorf("%w: %w", core.ErrPublish, err)
	}
	return event, nil
}

func mapEGErr(err error, topic string) error {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		switch re.StatusCode {
		case 404:
			return fmt.Errorf("%w: %s: %w", core.ErrTopicNotFound, topic, err)
		case 403:
			return fmt.Errorf("%w: access denied for topic %s: %w", core.ErrPubSub, topic, err)
		}
	}
	return fmt.Errorf("%w: %w", core.ErrPublish, err)
}
