package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/smithy-go"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// AWSSNSBackend is the AWS SNS pub/sub backend.
//
// A single SNS client is held for the lifetime of the backend. Authentication
// is chosen by NewSNS based on which Config fields are set: static access
// key, named profile, or the default AWS credential chain.
type AWSSNSBackend struct {
	client *sns.Client
}

var _ Backend = (*AWSSNSBackend)(nil)

// snsBatchLimit is the SNS PublishBatch entry limit.
const snsBatchLimit = 10

// NewSNS constructs an SNS backend. Region defaults to "us-east-1".
func NewSNS(ctx context.Context, cfg Config) (*AWSSNSBackend, error) {
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
		return nil, fmt.Errorf("%w: %w", core.ErrPubSub, err)
	}
	client := sns.NewFromConfig(awsCfg, func(o *sns.Options) {
		if cfg.EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		}
	})
	return &AWSSNSBackend{client: client}, nil
}

func (b *AWSSNSBackend) Publish(ctx context.Context, topic, message string, attributes map[string]string) (string, error) {
	input := &sns.PublishInput{
		TopicArn: aws.String(topic),
		Message:  aws.String(message),
	}
	if len(attributes) > 0 {
		input.MessageAttributes = snsAttributes(attributes)
	}
	resp, err := b.client.Publish(ctx, input)
	if err != nil {
		return "", mapSNSErr(err, topic)
	}
	return aws.ToString(resp.MessageId), nil
}

// PublishBatch publishes messages in chunks of the SNS batch limit (10).
func (b *AWSSNSBackend) PublishBatch(ctx context.Context, topic string, messages []BatchMessage) ([]string, error) {
	var allIDs []string
	for i := 0; i < len(messages); i += snsBatchLimit {
		chunk := messages[i:min(i+snsBatchLimit, len(messages))]
		entries := make([]types.PublishBatchRequestEntry, 0, len(chunk))
		for j, msg := range chunk {
			entry := types.PublishBatchRequestEntry{
				Id:      aws.String(strconv.Itoa(j)),
				Message: aws.String(msg.Message),
			}
			if len(msg.Attributes) > 0 {
				entry.MessageAttributes = snsAttributes(msg.Attributes)
			}
			entries = append(entries, entry)
		}
		resp, err := b.client.PublishBatch(ctx, &sns.PublishBatchInput{
			TopicArn:                   aws.String(topic),
			PublishBatchRequestEntries: entries,
		})
		if err != nil {
			return nil, mapSNSErr(err, topic)
		}
		if len(resp.Failed) > 0 {
			ids := make([]string, 0, len(resp.Failed))
			for _, f := range resp.Failed {
				ids = append(ids, aws.ToString(f.Id))
			}
			return nil, fmt.Errorf("%w: failed to publish messages: %v", core.ErrPublish, ids)
		}
		for _, s := range resp.Successful {
			allIDs = append(allIDs, aws.ToString(s.MessageId))
		}
	}
	return allIDs, nil
}

func (b *AWSSNSBackend) HealthCheck(ctx context.Context) bool {
	_, err := b.client.ListTopics(ctx, &sns.ListTopicsInput{})
	return err == nil
}

// Close is a no-op: the aws-sdk-go-v2 client shares the default HTTP
// transport, which Go manages.
func (b *AWSSNSBackend) Close(ctx context.Context) error { return nil }

func snsAttributes(attrs map[string]string) map[string]types.MessageAttributeValue {
	out := make(map[string]types.MessageAttributeValue, len(attrs))
	for k, v := range attrs {
		out[k] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	return out
}

func mapSNSErr(err error, topic string) error {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NotFound", "NotFoundException":
			return fmt.Errorf("%w: %s: %w", core.ErrTopicNotFound, topic, err)
		case "AuthorizationError", "AccessDenied":
			return fmt.Errorf("%w: access denied for topic %s: %w", core.ErrPubSub, topic, err)
		}
	}
	return fmt.Errorf("%w: %w", core.ErrPubSub, err)
}
