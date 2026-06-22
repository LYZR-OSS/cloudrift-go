package email

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/smithy-go"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// SESBackend is the AWS SES (SESv2) email backend.
//
// A single client is held for the lifetime of the backend. Authentication is
// chosen by NewSES based on which Config fields are set: static access key,
// named profile, or the default AWS credential chain.
type SESBackend struct {
	client      *sesv2.Client
	defaultFrom string
}

var _ Backend = (*SESBackend)(nil)

// NewSES constructs an SES backend. Region defaults to "us-east-1".
// cfg.EndpointURL points the client at a custom endpoint (LocalStack).
func NewSES(ctx context.Context, cfg Config) (*SESBackend, error) {
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
		return nil, fmt.Errorf("%w: %w", core.ErrEmail, err)
	}
	client := sesv2.NewFromConfig(awsCfg, func(o *sesv2.Options) {
		if cfg.EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		}
	})
	return &SESBackend{client: client, defaultFrom: cfg.DefaultFrom}, nil
}

func (b *SESBackend) Send(ctx context.Context, msg EmailMessage) (string, error) {
	sender, err := resolveSender(msg, b.defaultFrom)
	if err != nil {
		return "", err
	}

	dest := &sesv2types.Destination{
		ToAddresses:  msg.To,
		CcAddresses:  msg.Cc,
		BccAddresses: msg.Bcc,
	}

	var content *sesv2types.EmailContent
	if len(msg.Attachments) > 0 || len(msg.Headers) > 0 {
		raw, err := buildMIME(sender, msg, "")
		if err != nil {
			return "", fmt.Errorf("%w: building MIME message: %w", core.ErrEmailSend, err)
		}
		content = &sesv2types.EmailContent{
			Raw: &sesv2types.RawMessage{Data: raw},
		}
	} else {
		body := &sesv2types.Body{}
		if msg.BodyText != "" {
			body.Text = &sesv2types.Content{Data: aws.String(msg.BodyText), Charset: aws.String("UTF-8")}
		}
		if msg.BodyHTML != "" {
			body.Html = &sesv2types.Content{Data: aws.String(msg.BodyHTML), Charset: aws.String("UTF-8")}
		}
		content = &sesv2types.EmailContent{
			Simple: &sesv2types.Message{
				Subject: &sesv2types.Content{Data: aws.String(msg.Subject), Charset: aws.String("UTF-8")},
				Body:    body,
			},
		}
	}

	resp, err := b.client.SendEmail(ctx, &sesv2.SendEmailInput{
		FromEmailAddress: aws.String(sender),
		Destination:      dest,
		Content:          content,
		ReplyToAddresses: msg.ReplyTo,
	})
	if err != nil {
		return "", mapSESErr(err)
	}
	return aws.ToString(resp.MessageId), nil
}

func (b *SESBackend) SendBatch(ctx context.Context, msgs []EmailMessage) ([]string, error) {
	return sendBatchLoop(ctx, b, msgs)
}

func (b *SESBackend) HealthCheck(ctx context.Context) bool {
	_, err := b.client.GetAccount(ctx, &sesv2.GetAccountInput{})
	return err == nil
}

// Close is a no-op: the aws-sdk-go-v2 client shares the default HTTP
// transport, which Go manages.
func (b *SESBackend) Close(ctx context.Context) error { return nil }

func mapSESErr(err error) error {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "MessageRejected":
			return fmt.Errorf("%w: %w", core.ErrRecipientRejected, err)
		case "MailFromDomainNotVerified", "MailFromDomainNotVerifiedException",
			"FromEmailAddressNotVerified", "AccountSendingPausedException":
			return fmt.Errorf("%w: %w", core.ErrSenderUnverified, err)
		case "Throttling", "TooManyRequestsException", "SendingPausedException":
			return fmt.Errorf("%w: %w", core.ErrEmailThrottled, err)
		}
	}
	// Fall back to message inspection for cases the code alone misses.
	low := strings.ToLower(err.Error())
	switch {
	case strings.Contains(low, "not verified"):
		return fmt.Errorf("%w: %w", core.ErrSenderUnverified, err)
	case strings.Contains(low, "throttl"):
		return fmt.Errorf("%w: %w", core.ErrEmailThrottled, err)
	}
	return fmt.Errorf("%w: %w", core.ErrEmailSend, err)
}
