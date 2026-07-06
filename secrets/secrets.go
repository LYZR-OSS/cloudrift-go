// Package secrets provides a provider-neutral interface over cloud secret
// stores (AWS Secrets Manager, Azure Key Vault) plus non-cloud backends for
// local development, self-hosted deployments, CI, and tests: environment
// variables, a JSON file, or an in-memory mapping.
//
// Construct a backend once at service startup via New (or one of the concrete
// constructors) and reuse it.
package secrets

import (
	"context"
	"fmt"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// Backend is the provider-neutral secret management interface.
type Backend interface {
	// GetSecret retrieves the plaintext value of a secret by name.
	GetSecret(ctx context.Context, name string) (string, error)
	// GetSecretJSON retrieves a secret and parses its value as JSON.
	GetSecretJSON(ctx context.Context, name string) (map[string]any, error)
	// SetSecret creates or updates a secret.
	SetSecret(ctx context.Context, name, value string) error
	// DeleteSecret deletes a secret by name.
	DeleteSecret(ctx context.Context, name string) error
	// ListSecrets lists secret names, optionally filtered by prefix.
	ListSecrets(ctx context.Context, prefix string) ([]string, error)
	// HealthCheck returns true if the secret store is reachable (never errors).
	HealthCheck(ctx context.Context) bool
	// Close releases the underlying client and sockets.
	Close(ctx context.Context) error
}

// Config carries the union of provider options. Only the fields relevant to
// the chosen provider are read; the factory routes to the appropriate auth
// method based on which credential fields are set.
type Config struct {
	// AWS Secrets Manager.
	Region             string // default "us-east-1"
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string
	ProfileName        string
	EndpointURL        string // custom endpoint (LocalStack, ...)

	// Azure Key Vault.
	VaultURL     string // https://<vault>.vault.azure.net
	TenantID     string
	ClientID     string // service principal app ID, or user-assigned MI client ID
	ClientSecret string

	// Local (env / file / memory).
	Prefix   string            // env: namespace prefix, secret "db" → env "{Prefix}db"
	FilePath string            // file: path to the JSON {name: value} store
	Mapping  map[string]string // memory/local: initial in-memory secrets
}

// New instantiates a secret management backend.
//
// provider is "aws_secrets_manager", "azure_keyvault", or a non-cloud source —
// "env" (environment variables), "file" (a JSON file), or "memory"/"local"
// (in-memory mapping, mainly dev/tests). For the cloud providers the auth method
// is inferred from which credential fields are set, exactly as in the Python
// library:
//
//	New(ctx, "aws_secrets_manager", Config{Region: "us-east-1"})  // IAM role / env
//	New(ctx, "aws_secrets_manager", Config{AWSAccessKeyID: "...", AWSSecretAccessKey: "..."})
//	New(ctx, "azure_keyvault", Config{VaultURL: "https://myvault.vault.azure.net"})
//	New(ctx, "azure_keyvault", Config{VaultURL: "...", TenantID: "...",
//	    ClientID: "...", ClientSecret: "..."})
//	New(ctx, "env", Config{Prefix: "SECRET_"})            // read SECRET_<name> env vars
//	New(ctx, "file", Config{FilePath: "/run/secrets.json"}) // JSON {name: value}
//	New(ctx, "memory", Config{Mapping: map[string]string{"db": "..."}}) // in-memory
func New(ctx context.Context, provider string, cfg Config) (Backend, error) {
	switch provider {
	case "aws_secrets_manager":
		return NewAWSSecretsManager(ctx, cfg)
	case "azure_keyvault":
		return NewAzureKeyVault(cfg)
	case "env":
		return NewEnvSecrets(cfg.Prefix), nil
	case "file":
		return NewFileSecrets(cfg.FilePath), nil
	case "memory", "local":
		return NewMappingSecrets(cfg.Mapping), nil
	}
	return nil, fmt.Errorf("%w: unknown secrets provider %q (choose 'aws_secrets_manager', 'azure_keyvault', 'env', 'file', or 'memory')",
		core.ErrSecret, provider)
}
