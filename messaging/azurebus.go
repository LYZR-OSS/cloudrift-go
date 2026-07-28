package messaging

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// AzureServiceBusBackend is the Azure Service Bus messaging backend.
//
// A single *azservicebus.Client (one AMQP connection) is reused for the
// lifetime of the backend. Each Receive opens a receiver that stays open
// until every message it delivered has been settled via Delete or DeadLetter
// (Service Bus settles messages through the receiver's lock token, so the
// receiver must outlive its in-flight messages).
type AzureServiceBusBackend struct {
	queueName        string
	client           *azservicebus.Client
	connectionString string
	namespace        string
	cred             azcore.TokenCredential

	mu sync.Mutex
	// pending maps lock token (hex) → the receiver + message needed to settle it.
	pending map[string]*sbPending
	// receiverTokens tracks the outstanding lock tokens per receiver so the
	// receiver can be closed once all its messages are settled.
	receiverTokens map[*azservicebus.Receiver]map[string]struct{}
}

type sbPending struct {
	receiver *azservicebus.Receiver
	message  *azservicebus.ReceivedMessage
}

var _ Backend = (*AzureServiceBusBackend)(nil)

// NewAzureServiceBus constructs a Service Bus backend. Routing mirrors the
// Python factory: ConnectionString > ClientSecret (service principal) >
// managed identity (with FullyQualifiedNamespace).
func NewAzureServiceBus(cfg Config) (*AzureServiceBusBackend, error) {
	b := &AzureServiceBusBackend{
		queueName:      cfg.QueueName,
		pending:        make(map[string]*sbPending),
		receiverTokens: make(map[*azservicebus.Receiver]map[string]struct{}),
	}
	switch {
	case cfg.ConnectionString != "":
		client, err := azservicebus.NewClientFromConnectionString(cfg.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrMessaging, err)
		}
		b.client = client
		b.connectionString = cfg.ConnectionString

	case cfg.FullyQualifiedNamespace == "":
		return nil, fmt.Errorf("%w: provide either ConnectionString or FullyQualifiedNamespace",
			core.ErrMessaging)

	case cfg.ClientSecret != "":
		cred, err := azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrMessaging, err)
		}
		client, err := azservicebus.NewClient(cfg.FullyQualifiedNamespace, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrMessaging, err)
		}
		b.client, b.namespace, b.cred = client, cfg.FullyQualifiedNamespace, cred

	default: // workload identity → managed identity → az CLI (user-assigned via ClientID)
		cred, err := core.NewAzureCredential(cfg.ClientID)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrMessaging, err)
		}
		client, err := azservicebus.NewClient(cfg.FullyQualifiedNamespace, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrMessaging, err)
		}
		b.client, b.namespace, b.cred = client, cfg.FullyQualifiedNamespace, cred
	}
	return b, nil
}

func (b *AzureServiceBusBackend) Send(ctx context.Context, message map[string]any, delay time.Duration) (string, error) {
	body, err := json.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("%w: %w", core.ErrMessageSend, err)
	}
	sender, err := b.client.NewSender(b.queueName, nil)
	if err != nil {
		return "", b.mapErr(err, core.ErrMessageSend)
	}
	defer sender.Close(ctx)

	id := newID()
	msg := &azservicebus.Message{Body: body, MessageID: &id}
	if delay > 0 {
		t := time.Now().UTC().Add(delay)
		msg.ScheduledEnqueueTime = &t
	}
	if err := sender.SendMessage(ctx, msg, nil); err != nil {
		return "", b.mapErr(err, core.ErrMessageSend)
	}
	return id, nil
}

func (b *AzureServiceBusBackend) SendBatch(ctx context.Context, messages []map[string]any) ([]string, error) {
	sender, err := b.client.NewSender(b.queueName, nil)
	if err != nil {
		return nil, b.mapErr(err, core.ErrMessageSend)
	}
	defer sender.Close(ctx)

	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		return nil, b.mapErr(err, core.ErrMessageSend)
	}
	ids := make([]string, 0, len(messages))
	for _, m := range messages {
		body, err := json.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrMessageSend, err)
		}
		id := newID()
		ids = append(ids, id)
		if err := batch.AddMessage(&azservicebus.Message{Body: body, MessageID: &id}, nil); err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrMessageSend, err)
		}
	}
	if err := sender.SendMessageBatch(ctx, batch, nil); err != nil {
		return nil, b.mapErr(err, core.ErrMessageSend)
	}
	return ids, nil
}

func (b *AzureServiceBusBackend) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]Message, error) {
	receiver, err := b.client.NewReceiverForQueue(b.queueName, nil)
	if err != nil {
		return nil, b.mapErr(err, core.ErrMessaging)
	}

	rctx := ctx
	var cancel context.CancelFunc
	if waitTime > 0 {
		rctx, cancel = context.WithTimeout(ctx, waitTime)
		defer cancel()
	}
	raw, err := receiver.ReceiveMessages(rctx, maxMessages, nil)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		_ = receiver.Close(ctx)
		return nil, b.mapErr(err, core.ErrMessaging)
	}
	if len(raw) == 0 {
		_ = receiver.Close(ctx)
		return []Message{}, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	tokens := make(map[string]struct{}, len(raw))
	messages := make([]Message, 0, len(raw))
	for _, m := range raw {
		token := hex.EncodeToString(m.LockToken[:])
		b.pending[token] = &sbPending{receiver: receiver, message: m}
		tokens[token] = struct{}{}

		var body map[string]any
		if err := json.Unmarshal(m.Body, &body); err != nil {
			return nil, fmt.Errorf("%w: decoding message body: %w", core.ErrMessaging, err)
		}
		attrs := map[string]string{}
		if m.SequenceNumber != nil {
			attrs["sequence_number"] = fmt.Sprint(*m.SequenceNumber)
		}
		if m.EnqueuedTime != nil {
			attrs["enqueued_time"] = m.EnqueuedTime.String()
		}
		messages = append(messages, Message{
			ID:            m.MessageID,
			Body:          body,
			ReceiptHandle: token,
			Attributes:    attrs,
		})
	}
	b.receiverTokens[receiver] = tokens
	return messages, nil
}

// settle resolves a receipt handle to its pending entry, applies fn (complete
// or dead-letter), and closes the receiver once all its messages are settled.
func (b *AzureServiceBusBackend) settle(
	ctx context.Context,
	receiptHandle string,
	fn func(*azservicebus.Receiver, *azservicebus.ReceivedMessage) error,
) error {
	b.mu.Lock()
	entry, ok := b.pending[receiptHandle]
	if ok {
		delete(b.pending, receiptHandle)
	}
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: no pending message for receipt handle %q; call Receive first and use the returned ReceiptHandle",
			core.ErrMessaging, receiptHandle)
	}

	settleErr := fn(entry.receiver, entry.message)

	b.mu.Lock()
	if tokens, ok := b.receiverTokens[entry.receiver]; ok {
		delete(tokens, receiptHandle)
		if len(tokens) == 0 {
			delete(b.receiverTokens, entry.receiver)
			defer entry.receiver.Close(ctx) //nolint:errcheck // best-effort close after unlock
		}
	}
	b.mu.Unlock()

	if settleErr != nil {
		return b.mapErr(settleErr, core.ErrMessaging)
	}
	return nil
}

func (b *AzureServiceBusBackend) Delete(ctx context.Context, receiptHandle string) error {
	return b.settle(ctx, receiptHandle, func(r *azservicebus.Receiver, m *azservicebus.ReceivedMessage) error {
		return r.CompleteMessage(ctx, m, nil)
	})
}

func (b *AzureServiceBusBackend) DeadLetter(ctx context.Context, receiptHandle, reason string) error {
	return b.settle(ctx, receiptHandle, func(r *azservicebus.Receiver, m *azservicebus.ReceivedMessage) error {
		return r.DeadLetterMessage(ctx, m, &azservicebus.DeadLetterOptions{
			Reason:           &reason,
			ErrorDescription: &reason,
		})
	})
}

// GetQueueDepth queries the management plane for the active message count.
func (b *AzureServiceBusBackend) GetQueueDepth(ctx context.Context) (int64, error) {
	var adminClient *admin.Client
	var err error
	if b.connectionString != "" {
		adminClient, err = admin.NewClientFromConnectionString(b.connectionString, nil)
	} else {
		adminClient, err = admin.NewClient(b.namespace, b.cred, nil)
	}
	if err != nil {
		return 0, b.mapErr(err, core.ErrMessaging)
	}
	resp, err := adminClient.GetQueueRuntimeProperties(ctx, b.queueName, nil)
	if err != nil {
		return 0, b.mapErr(err, core.ErrMessaging)
	}
	if resp == nil {
		return 0, fmt.Errorf("%w: %s", core.ErrQueueNotFound, b.queueName)
	}
	return int64(resp.ActiveMessageCount), nil
}

// Purge drains the queue by receiving and completing messages until empty.
func (b *AzureServiceBusBackend) Purge(ctx context.Context) error {
	receiver, err := b.client.NewReceiverForQueue(b.queueName, nil)
	if err != nil {
		return b.mapErr(err, core.ErrMessaging)
	}
	defer receiver.Close(ctx)
	for {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msgs, err := receiver.ReceiveMessages(rctx, 100, nil)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return b.mapErr(err, core.ErrMessaging)
		}
		if len(msgs) == 0 {
			return nil
		}
		for _, m := range msgs {
			if err := receiver.CompleteMessage(ctx, m, nil); err != nil {
				return b.mapErr(err, core.ErrMessaging)
			}
		}
	}
}

// HealthCheck validates queue connectivity by opening and closing a sender.
func (b *AzureServiceBusBackend) HealthCheck(ctx context.Context) bool {
	sender, err := b.client.NewSender(b.queueName, nil)
	if err != nil {
		return false
	}
	return sender.Close(ctx) == nil
}

func (b *AzureServiceBusBackend) Close(ctx context.Context) error {
	b.mu.Lock()
	receivers := make([]*azservicebus.Receiver, 0, len(b.receiverTokens))
	for r := range b.receiverTokens {
		receivers = append(receivers, r)
	}
	b.receiverTokens = make(map[*azservicebus.Receiver]map[string]struct{})
	b.pending = make(map[string]*sbPending)
	b.mu.Unlock()

	for _, r := range receivers {
		_ = r.Close(ctx)
	}
	if err := b.client.Close(ctx); err != nil {
		return fmt.Errorf("%w: %w", core.ErrMessaging, err)
	}
	return nil
}

func (b *AzureServiceBusBackend) mapErr(err error, base error) error {
	var re *azcore.ResponseError
	if errors.As(err, &re) && re.StatusCode == 404 {
		return fmt.Errorf("%w: %s: %w", core.ErrQueueNotFound, b.queueName, err)
	}
	var sbErr *azservicebus.Error
	if errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeNotFound {
		return fmt.Errorf("%w: %s: %w", core.ErrQueueNotFound, b.queueName, err)
	}
	return fmt.Errorf("%w: %w", base, err)
}
