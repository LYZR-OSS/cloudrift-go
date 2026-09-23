package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubapi "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// GCPPubSubBackend is the Google Cloud Pub/Sub messaging backend.
//
// Pub/Sub splits what SQS and Service Bus call a queue into two resources:
// messages are published to a topic and received from a pull subscription. The
// backend therefore takes both, and each is required only by the direction
// that uses it — a send-only producer sets just Topic and never opens a
// subscriber channel; a consumer sets Subscription. Calling the other half
// without it returns core.ErrMessaging.
//
// Operations Pub/Sub cannot honor fail loudly rather than differing silently:
// a Send delay and GetQueueDepth return core.ErrNotImplemented, and DeadLetter
// is emulated (publish to DeadLetterTopic, then ack) as on SQS.
type GCPPubSubBackend struct {
	topic           string // full resource names; "" when not configured
	subscription    string
	deadLetterTopic string
	publisher       *pubsubapi.TopicAdminClient        // nil without Topic or DeadLetterTopic
	subscriber      *pubsubapi.SubscriptionAdminClient // nil without Subscription

	mu sync.Mutex
	// pending maps ack ID → raw message body, retained between Receive and
	// Delete/DeadLetter so emulated dead-lettering can re-publish the original
	// payload (same approach as the SQS backend).
	pending map[string][]byte
}

var _ Backend = (*GCPPubSubBackend)(nil)

// noWaitPullTimeout bounds a Receive with no waitTime. Pub/Sub has no
// short-poll mode (ReturnImmediately is deprecated), so without a bound a
// non-polling Receive would block on the Pull RPC's 60s default timeout.
const noWaitPullTimeout = 3 * time.Second

// cloudPlatformScope is requested for metadata-server tokens (PreferMetadata).
// GCP authorizes each call against IAM, so the coarse scope is the documented
// default for service accounts.
const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

var (
	errNoTopic        = fmt.Errorf("%w: no Topic configured; set Config.Topic to send messages", core.ErrMessaging)
	errNoSubscription = fmt.Errorf("%w: no Subscription configured; set Config.Subscription to receive messages", core.ErrMessaging)
)

// NewGCPPubSub constructs a Pub/Sub backend. Topic, Subscription, and
// DeadLetterTopic take a bare ID (resolved against cfg.Project) or a full
// projects/<p>/topics/<t> resource name.
//
// Identity routing mirrors the Python factory: ServiceAccountJSON >
// ServiceAccountFile > PreferMetadata > Application Default Credentials (GKE
// Workload Identity, Cloud Run, or `gcloud auth application-default login`).
// The underlying clients honor PUBSUB_EMULATOR_HOST.
func NewGCPPubSub(ctx context.Context, cfg Config) (*GCPPubSubBackend, error) {
	if cfg.Project == "" {
		return nil, fmt.Errorf("%w: gcp_pubsub requires Project", core.ErrMessaging)
	}
	if cfg.Topic == "" && cfg.Subscription == "" {
		return nil, fmt.Errorf("%w: gcp_pubsub needs a Topic (to send), a Subscription (to receive), or both",
			core.ErrMessaging)
	}
	opts, err := gcpClientOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrMessaging, err)
	}
	b := &GCPPubSubBackend{pending: make(map[string][]byte)}
	if cfg.Topic != "" {
		b.topic = gcpResource(cfg.Project, "topics", cfg.Topic)
	}
	if cfg.Subscription != "" {
		b.subscription = gcpResource(cfg.Project, "subscriptions", cfg.Subscription)
	}
	if cfg.DeadLetterTopic != "" {
		b.deadLetterTopic = gcpResource(cfg.Project, "topics", cfg.DeadLetterTopic)
	}
	if b.topic != "" || b.deadLetterTopic != "" {
		if b.publisher, err = pubsubapi.NewTopicAdminClient(ctx, opts...); err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrMessaging, err)
		}
	}
	if b.subscription != "" {
		if b.subscriber, err = pubsubapi.NewSubscriptionAdminClient(ctx, opts...); err != nil {
			_ = b.Close(ctx)
			return nil, fmt.Errorf("%w: %w", core.ErrMessaging, err)
		}
	}
	return b, nil
}

// gcpClientOptions resolves the identity the Pub/Sub clients authenticate as
// (the Go counterpart of cloudrift Python's core/gcp_credentials.py). nil
// means Application Default Credentials.
func gcpClientOptions(cfg Config) ([]option.ClientOption, error) {
	switch {
	case cfg.ServiceAccountFile != "" && cfg.ServiceAccountJSON != "":
		return nil, errors.New("set ServiceAccountFile or ServiceAccountJSON, not both")
	case cfg.PreferMetadata && (cfg.ServiceAccountFile != "" || cfg.ServiceAccountJSON != ""):
		return nil, errors.New("PreferMetadata reads the attached service account from the metadata server; " +
			"it cannot be combined with an explicit service account")
	case cfg.ServiceAccountJSON != "":
		return []option.ClientOption{option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(cfg.ServiceAccountJSON))}, nil
	case cfg.ServiceAccountFile != "":
		return []option.ClientOption{option.WithAuthCredentialsFile(option.ServiceAccount, cfg.ServiceAccountFile)}, nil
	case cfg.PreferMetadata:
		// ADC checks GOOGLE_APPLICATION_CREDENTIALS first, so a stray key path
		// left in the environment would silently shadow the workload's attached
		// identity; going straight to the metadata server rules that out.
		return []option.ClientOption{option.WithTokenSource(google.ComputeTokenSource("", cloudPlatformScope))}, nil
	}
	return nil, nil
}

// gcpResource returns name as a full Pub/Sub resource name, resolving a bare
// ID against project.
func gcpResource(project, collection, name string) string {
	if strings.HasPrefix(name, "projects/") {
		return name
	}
	return "projects/" + project + "/" + collection + "/" + name
}

// Send publishes body to the topic. Pub/Sub has no per-message delivery delay,
// so a positive delay returns core.ErrNotImplemented rather than delivering
// early.
func (b *GCPPubSubBackend) Send(ctx context.Context, body []byte, attributes map[string]string, delay time.Duration) (string, error) {
	if delay > 0 {
		return "", fmt.Errorf("%w: Pub/Sub has no per-message delivery delay; schedule the send yourself or use Cloud Tasks",
			core.ErrNotImplemented)
	}
	ids, err := b.publish(ctx, []*pubsubpb.PubsubMessage{{Data: body, Attributes: attributes}})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

// SendBatch publishes all messages in a single Publish request.
func (b *GCPPubSubBackend) SendBatch(ctx context.Context, messages []OutgoingMessage) ([]string, error) {
	if len(messages) == 0 {
		return nil, nil // Pub/Sub rejects an empty Publish
	}
	msgs := make([]*pubsubpb.PubsubMessage, len(messages))
	for i, m := range messages {
		msgs[i] = &pubsubpb.PubsubMessage{Data: m.Body, Attributes: m.Attributes}
	}
	return b.publish(ctx, msgs)
}

func (b *GCPPubSubBackend) publish(ctx context.Context, msgs []*pubsubpb.PubsubMessage) ([]string, error) {
	if b.topic == "" {
		return nil, errNoTopic
	}
	resp, err := b.publisher.Publish(ctx, &pubsubpb.PublishRequest{Topic: b.topic, Messages: msgs})
	if err != nil {
		return nil, mapPubSubErr(err, b.topic, core.ErrMessageSend)
	}
	return resp.MessageIds, nil
}

// Receive pulls up to maxMessages from the subscription. Pub/Sub has no
// long-poll parameter, so waitTime bounds the Pull RPC instead; the deadline
// elapsing with nothing to return yields an empty slice, as an idle SQS long
// poll does.
func (b *GCPPubSubBackend) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]Message, error) {
	if b.subscription == "" {
		return nil, errNoSubscription
	}
	if waitTime <= 0 {
		waitTime = noWaitPullTimeout
	}
	pctx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()
	resp, err := b.subscriber.Pull(pctx, &pubsubpb.PullRequest{
		Subscription: b.subscription,
		MaxMessages:  int32(maxMessages),
	})
	if err != nil {
		// Surfaces as the context error while gax is between retries, as a
		// gRPC status once the RPC itself times out.
		if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
			return []Message{}, nil
		}
		return nil, mapPubSubErr(err, b.subscription, core.ErrMessaging)
	}
	messages := make([]Message, 0, len(resp.ReceivedMessages))
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, rm := range resp.ReceivedMessages {
		m := rm.GetMessage()
		b.pending[rm.AckId] = m.GetData()
		messages = append(messages, Message{
			ID:            m.GetMessageId(),
			Body:          m.GetData(),
			ReceiptHandle: rm.AckId,
			Attributes:    m.GetAttributes(),
		})
	}
	return messages, nil
}

// Delete acknowledges a message by its ack ID.
func (b *GCPPubSubBackend) Delete(ctx context.Context, receiptHandle string) error {
	if b.subscription == "" {
		return errNoSubscription
	}
	err := b.subscriber.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: b.subscription,
		AckIds:       []string{receiptHandle},
	})
	b.mu.Lock()
	delete(b.pending, receiptHandle)
	b.mu.Unlock()
	if err != nil {
		return mapPubSubErr(err, b.subscription, core.ErrMessaging)
	}
	return nil
}

// DeadLetter publishes the message body to DeadLetterTopic, then acks the
// original — the same emulation as SQS, with the same caveat: the two calls
// are not transactional, so a crash between them can leave the message in both
// places. Pub/Sub's native dead-letter policy is delivery-attempt based with
// no per-message API, and the configured dead-letter topic cannot be read back
// off a subscription without the admin API, so DeadLetterTopic must be set.
func (b *GCPPubSubBackend) DeadLetter(ctx context.Context, receiptHandle, reason string) error {
	b.mu.Lock()
	body, ok := b.pending[receiptHandle]
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: no pending message for receipt handle %q; call Receive first and use the returned ReceiptHandle",
			core.ErrMessaging, receiptHandle)
	}
	if b.deadLetterTopic == "" {
		return fmt.Errorf("%w: no DeadLetterTopic configured; set Config.DeadLetterTopic, or give the subscription "+
			"a dead-letter policy and let Pub/Sub move the message after max delivery attempts", core.ErrMessaging)
	}
	if _, err := b.publisher.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: b.deadLetterTopic,
		Messages: []*pubsubpb.PubsubMessage{
			{Data: body, Attributes: map[string]string{"DeadLetterReason": reason}},
		},
	}); err != nil {
		return mapPubSubErr(err, b.deadLetterTopic, core.ErrMessaging)
	}
	if err := b.subscriber.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: b.subscription,
		AckIds:       []string{receiptHandle},
	}); err != nil {
		return mapPubSubErr(err, b.subscription, core.ErrMessaging)
	}
	b.mu.Lock()
	delete(b.pending, receiptHandle)
	b.mu.Unlock()
	return nil
}

// GetQueueDepth is not available on Pub/Sub: backlog size is the Cloud
// Monitoring metric subscription/num_undelivered_messages, not a data-plane
// call, and reading it would need the Monitoring API and
// roles/monitoring.viewer.
func (b *GCPPubSubBackend) GetQueueDepth(ctx context.Context) (int64, error) {
	return 0, fmt.Errorf("%w: Pub/Sub does not expose queue depth on the data plane; "+
		"read the Cloud Monitoring metric subscription/num_undelivered_messages", core.ErrNotImplemented)
}

// Purge seeks the subscription to now, which acknowledges everything published
// before this moment — Pub/Sub has no purge call.
func (b *GCPPubSubBackend) Purge(ctx context.Context) error {
	if b.subscription == "" {
		return errNoSubscription
	}
	if _, err := b.subscriber.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: b.subscription,
		Target:       &pubsubpb.SeekRequest_Time{Time: timestamppb.Now()},
	}); err != nil {
		return mapPubSubErr(err, b.subscription, core.ErrMessaging)
	}
	b.mu.Lock()
	clear(b.pending)
	b.mu.Unlock()
	return nil
}

// HealthCheck asks IAM whether the caller holds what each configured direction
// needs: pubsub.topics.publish on the topic, pubsub.subscriptions.consume on
// the subscription. TestIamPermissions itself needs no permission, so this
// works for an identity holding only roles/pubsub.publisher or
// roles/pubsub.subscriber — GetTopic/GetSubscription would need a read
// permission neither role grants, and report such a service unhealthy while it
// works fine. A missing resource or permission reports unhealthy.
func (b *GCPPubSubBackend) HealthCheck(ctx context.Context) bool {
	if b.topic != "" {
		resp, err := b.publisher.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
			Resource:    b.topic,
			Permissions: []string{"pubsub.topics.publish"},
		})
		if err != nil || len(resp.Permissions) == 0 {
			return false
		}
	}
	if b.subscription != "" {
		resp, err := b.subscriber.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
			Resource:    b.subscription,
			Permissions: []string{"pubsub.subscriptions.consume"},
		})
		if err != nil || len(resp.Permissions) == 0 {
			return false
		}
	}
	return true
}

// Close releases the gRPC connections.
func (b *GCPPubSubBackend) Close(ctx context.Context) error {
	b.mu.Lock()
	clear(b.pending)
	b.mu.Unlock()
	var errs []error
	if b.publisher != nil {
		errs = append(errs, b.publisher.Close())
	}
	if b.subscriber != nil {
		errs = append(errs, b.subscriber.Close())
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: %w", core.ErrMessaging, err)
	}
	return nil
}

// mapPubSubErr translates a gRPC status into the cloudrift hierarchy by code,
// as the Python backend maps by exception type.
func mapPubSubErr(err error, resource string, base error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %s: %w", core.ErrQueueNotFound, resource, err)
	}
	return fmt.Errorf("%w: %s: %w", base, resource, err)
}
