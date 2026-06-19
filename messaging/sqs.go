package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// AWSSQSBackend is the AWS SQS messaging backend.
//
// A single SQS client is held for the lifetime of the backend and reused
// across operations. Authentication is chosen by NewSQS based on which
// Config fields are set: static access key, named profile, or the default
// AWS credential chain.
type AWSSQSBackend struct {
	queueURL string
	client   *sqs.Client

	mu sync.Mutex
	// dlqURL is the explicit DLQ URL; if empty it is resolved lazily from the
	// source queue's RedrivePolicy the first time DeadLetter is called.
	dlqURL string
	// pending maps receipt handle → raw message body (JSON string), retained
	// between Receive and Delete/DeadLetter so emulated dead-lettering can
	// re-send the original payload to the DLQ.
	pending map[string]string
}

var _ Backend = (*AWSSQSBackend)(nil)

// NewSQS constructs an SQS backend. Region defaults to "us-east-1".
// cfg.EndpointURL points the client at a custom endpoint (LocalStack).
func NewSQS(ctx context.Context, cfg Config) (*AWSSQSBackend, error) {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if cfg.AWSAccessKeyID != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSSessionToken)))
	} else if cfg.ProfileName != "" {
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(cfg.ProfileName))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrMessaging, err)
	}
	client := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		}
	})
	return &AWSSQSBackend{
		queueURL: cfg.QueueURL,
		client:   client,
		dlqURL:   cfg.DLQURL,
		pending:  make(map[string]string),
	}, nil
}

func (b *AWSSQSBackend) Send(ctx context.Context, message map[string]any, delay time.Duration) (string, error) {
	body, err := json.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("%w: %w", core.ErrMessageSend, err)
	}
	resp, err := b.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:     aws.String(b.queueURL),
		MessageBody:  aws.String(string(body)),
		DelaySeconds: int32(delay / time.Second),
	})
	if err != nil {
		return "", b.mapErr(err)
	}
	return aws.ToString(resp.MessageId), nil
}

func (b *AWSSQSBackend) SendBatch(ctx context.Context, messages []map[string]any) ([]string, error) {
	entries := make([]types.SendMessageBatchRequestEntry, 0, len(messages))
	for i, msg := range messages {
		body, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrMessageSend, err)
		}
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.Itoa(i)),
			MessageBody: aws.String(string(body)),
		})
	}
	resp, err := b.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(b.queueURL),
		Entries:  entries,
	})
	if err != nil {
		return nil, b.mapErr(err)
	}
	if len(resp.Failed) > 0 {
		ids := make([]string, 0, len(resp.Failed))
		for _, f := range resp.Failed {
			ids = append(ids, aws.ToString(f.Id))
		}
		return nil, fmt.Errorf("%w: failed to send messages with IDs: %v", core.ErrMessageSend, ids)
	}
	out := make([]string, 0, len(resp.Successful))
	for _, s := range resp.Successful {
		out = append(out, aws.ToString(s.MessageId))
	}
	return out, nil
}

func (b *AWSSQSBackend) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]Message, error) {
	resp, err := b.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(b.queueURL),
		MaxNumberOfMessages:         int32(min(maxMessages, 10)),
		WaitTimeSeconds:             int32(waitTime / time.Second),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{"All"},
	})
	if err != nil {
		return nil, b.mapErr(err)
	}
	messages := make([]Message, 0, len(resp.Messages))
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, m := range resp.Messages {
		rawBody := aws.ToString(m.Body)
		b.pending[aws.ToString(m.ReceiptHandle)] = rawBody
		var body map[string]any
		if err := json.Unmarshal([]byte(rawBody), &body); err != nil {
			return nil, fmt.Errorf("%w: decoding message body: %w", core.ErrMessaging, err)
		}
		attrs := make(map[string]string, len(m.Attributes))
		for k, v := range m.Attributes {
			attrs[k] = v
		}
		messages = append(messages, Message{
			ID:            aws.ToString(m.MessageId),
			Body:          body,
			ReceiptHandle: aws.ToString(m.ReceiptHandle),
			Attributes:    attrs,
		})
	}
	return messages, nil
}

func (b *AWSSQSBackend) Delete(ctx context.Context, receiptHandle string) error {
	_, err := b.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(b.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	b.mu.Lock()
	delete(b.pending, receiptHandle)
	b.mu.Unlock()
	if err != nil {
		return b.mapErr(err)
	}
	return nil
}

func (b *AWSSQSBackend) DeadLetter(ctx context.Context, receiptHandle, reason string) error {
	b.mu.Lock()
	body, ok := b.pending[receiptHandle]
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: no pending message for receipt handle %q; call Receive first and use the returned ReceiptHandle",
			core.ErrMessaging, receiptHandle)
	}
	dlqURL, err := b.resolveDLQURL(ctx)
	if err != nil {
		return err
	}
	if _, err := b.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(dlqURL),
		MessageBody: aws.String(body),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"DeadLetterReason": {DataType: aws.String("String"), StringValue: aws.String(reason)},
		},
	}); err != nil {
		return b.mapErr(err)
	}
	if _, err := b.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(b.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	}); err != nil {
		return b.mapErr(err)
	}
	b.mu.Lock()
	delete(b.pending, receiptHandle)
	b.mu.Unlock()
	return nil
}

func (b *AWSSQSBackend) GetQueueDepth(ctx context.Context) (int64, error) {
	resp, err := b.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(b.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	if err != nil {
		return 0, b.mapErr(err)
	}
	n, err := strconv.ParseInt(resp.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: parsing queue depth: %w", core.ErrMessaging, err)
	}
	return n, nil
}

// resolveDLQURL returns the configured DLQ URL, deriving it from the source
// queue's RedrivePolicy if needed.
func (b *AWSSQSBackend) resolveDLQURL(ctx context.Context) (string, error) {
	b.mu.Lock()
	cached := b.dlqURL
	b.mu.Unlock()
	if cached != "" {
		return cached, nil
	}
	resp, err := b.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(b.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy},
	})
	if err != nil {
		return "", b.mapErr(err)
	}
	redrive, ok := resp.Attributes[string(types.QueueAttributeNameRedrivePolicy)]
	if !ok || redrive == "" {
		return "", fmt.Errorf("%w: no dead-letter queue configured for %s; set Config.DLQURL or a RedrivePolicy on the queue",
			core.ErrMessaging, b.queueURL)
	}
	var policy struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}
	if err := json.Unmarshal([]byte(redrive), &policy); err != nil {
		return "", fmt.Errorf("%w: parsing RedrivePolicy: %w", core.ErrMessaging, err)
	}
	parts := strings.Split(policy.DeadLetterTargetArn, ":")
	dlqName := parts[len(parts)-1]
	urlResp, err := b.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(dlqName)})
	if err != nil {
		return "", b.mapErr(err)
	}
	dlqURL := aws.ToString(urlResp.QueueUrl)
	b.mu.Lock()
	b.dlqURL = dlqURL
	b.mu.Unlock()
	return dlqURL, nil
}

func (b *AWSSQSBackend) Purge(ctx context.Context) error {
	if _, err := b.client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(b.queueURL)}); err != nil {
		return b.mapErr(err)
	}
	return nil
}

func (b *AWSSQSBackend) HealthCheck(ctx context.Context) bool {
	_, err := b.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(b.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return err == nil
}

// Close is a no-op: the aws-sdk-go-v2 client shares the default HTTP
// transport, which Go manages.
func (b *AWSSQSBackend) Close(ctx context.Context) error { return nil }

func (b *AWSSQSBackend) mapErr(err error) error {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist":
			return fmt.Errorf("%w: %s: %w", core.ErrQueueNotFound, b.queueURL, err)
		case "InvalidMessageContents":
			return fmt.Errorf("%w: %w", core.ErrMessageSend, err)
		}
	}
	return fmt.Errorf("%w: %w", core.ErrMessaging, err)
}
