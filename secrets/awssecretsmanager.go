package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"

	"github.com/NeuralgoLyzr/cloudrift-go/core"
)

// AWSSecretsManagerBackend is the AWS Secrets Manager backend.
//
// A single client is held for the lifetime of the backend. Authentication is
// chosen by NewAWSSecretsManager based on which Config fields are set:
// static access key, named profile, or the default AWS credential chain.
type AWSSecretsManagerBackend struct {
	client *secretsmanager.Client
}

var _ Backend = (*AWSSecretsManagerBackend)(nil)

// NewAWSSecretsManager constructs a Secrets Manager backend. Region defaults
// to "us-east-1". cfg.EndpointURL points at a custom endpoint (LocalStack).
func NewAWSSecretsManager(ctx context.Context, cfg Config) (*AWSSecretsManagerBackend, error) {
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
		return nil, fmt.Errorf("%w: %w", core.ErrSecret, err)
	}
	client := secretsmanager.NewFromConfig(awsCfg, func(o *secretsmanager.Options) {
		if cfg.EndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		}
	})
	return &AWSSecretsManagerBackend{client: client}, nil
}

func (b *AWSSecretsManagerBackend) GetSecret(ctx context.Context, name string) (string, error) {
	resp, err := b.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(name),
	})
	if err != nil {
		return "", mapSMErr(err, name)
	}
	return aws.ToString(resp.SecretString), nil
}

func (b *AWSSecretsManagerBackend) GetSecretJSON(ctx context.Context, name string) (map[string]any, error) {
	raw, err := b.GetSecret(ctx, name)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("%w: secret %q is not valid JSON: %w", core.ErrSecret, name, err)
	}
	return out, nil
}

// SetSecret updates the secret's value, creating the secret if it does not exist.
func (b *AWSSecretsManagerBackend) SetSecret(ctx context.Context, name, value string) error {
	_, err := b.client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(name),
		SecretString: aws.String(value),
	})
	if err == nil {
		return nil
	}
	var nf *types.ResourceNotFoundException
	if errors.As(err, &nf) {
		if _, err := b.client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
			Name:         aws.String(name),
			SecretString: aws.String(value),
		}); err != nil {
			return mapSMErr(err, name)
		}
		return nil
	}
	return mapSMErr(err, name)
}

func (b *AWSSecretsManagerBackend) DeleteSecret(ctx context.Context, name string) error {
	if _, err := b.client.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId:                   aws.String(name),
		ForceDeleteWithoutRecovery: aws.Bool(true),
	}); err != nil {
		return mapSMErr(err, name)
	}
	return nil
}

func (b *AWSSecretsManagerBackend) ListSecrets(ctx context.Context, prefix string) ([]string, error) {
	input := &secretsmanager.ListSecretsInput{}
	if prefix != "" {
		input.Filters = []types.Filter{{Key: types.FilterNameStringTypeName, Values: []string{prefix}}}
	}
	var names []string
	paginator := secretsmanager.NewListSecretsPaginator(b.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, mapSMErr(err, prefix)
		}
		for _, s := range page.SecretList {
			names = append(names, aws.ToString(s.Name))
		}
	}
	return names, nil
}

func (b *AWSSecretsManagerBackend) HealthCheck(ctx context.Context) bool {
	_, err := b.client.ListSecrets(ctx, &secretsmanager.ListSecretsInput{MaxResults: aws.Int32(1)})
	return err == nil
}

// Close is a no-op: the aws-sdk-go-v2 client shares the default HTTP
// transport, which Go manages.
func (b *AWSSecretsManagerBackend) Close(ctx context.Context) error { return nil }

func mapSMErr(err error, name string) error {
	var nf *types.ResourceNotFoundException
	if errors.As(err, &nf) {
		return fmt.Errorf("%w: %s: %w", core.ErrSecretNotFound, name, err)
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "AccessDeniedException", "UnauthorizedAccess":
			return fmt.Errorf("%w: %s: %w", core.ErrSecretPermission, name, err)
		}
	}
	return fmt.Errorf("%w: %w", core.ErrSecret, err)
}
