package cache

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/redis/go-redis/v9"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// AzureRedisCacheBackend is the cache backend for Azure Cache for Redis.
//
// Construct with one of:
//   - NewAzureRedisFromAccessKey        — primary or secondary access key (standard auth)
//   - NewAzureRedisFromManagedIdentity  — Azure Managed Identity via Entra ID token auth
//   - NewAzureRedisFromServicePrincipal — Azure AD service principal via Entra ID token auth
type AzureRedisCacheBackend struct {
	redisOps
}

var _ Backend = (*AzureRedisCacheBackend)(nil)

// entra ID scope for Azure Cache for Redis data-plane access.
const azureRedisScope = "https://redis.azure.com/.default"

// NewAzureRedisFromAccessKey authenticates with an Azure Cache for Redis
// access key (primary or secondary, from the Azure portal). cfg.Host is e.g.
// "<name>.redis.cache.windows.net". Port defaults to 6380 (the TLS port);
// TLS defaults to on (required for Azure Cache for Redis).
func NewAzureRedisFromAccessKey(cfg Config) (*AzureRedisCacheBackend, error) {
	port := portOrDefault(cfg.Port, 6380)
	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, port),
		Password: cfg.AccessKey,
		DB:       cfg.DB,
	}
	if tlsOrDefault(cfg.TLS, true) {
		tlsCfg, err := buildTLSConfig(cfg.Host, cfg.CACerts, "", "")
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = tlsCfg
	}
	return &AzureRedisCacheBackend{redisOps{client: redis.NewClient(opts)}}, nil
}

// NewAzureRedisFromManagedIdentity authenticates via Azure Managed Identity
// (Entra ID token auth). Requires the cache to have Microsoft Entra
// Authentication enabled and the identity to hold a Redis data-access role.
//
// cfg.Username is the object ID (or configured Redis username) of the managed
// identity. Set cfg.ClientID for a user-assigned identity; omit for the
// system-assigned identity.
func NewAzureRedisFromManagedIdentity(cfg Config) (*AzureRedisCacheBackend, error) {
	var miOpts *azidentity.ManagedIdentityCredentialOptions
	if cfg.ClientID != "" {
		miOpts = &azidentity.ManagedIdentityCredentialOptions{ID: azidentity.ClientID(cfg.ClientID)}
	}
	cred, err := azidentity.NewManagedIdentityCredential(miOpts)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to connect to Azure Cache for Redis (Managed Identity): %w",
			core.ErrCacheConnection, err)
	}
	return newAzureRedisWithEntra(cfg, cred)
}

// NewAzureRedisFromServicePrincipal authenticates via an Azure AD service
// principal (Entra ID token auth). Requires the cache to have Microsoft Entra
// Authentication enabled and the principal to hold a Redis data-access role.
//
// cfg.Username is the object ID (or configured Redis username) of the service
// principal; cfg.TenantID, cfg.ClientID, and cfg.ClientSecret identify it.
func NewAzureRedisFromServicePrincipal(cfg Config) (*AzureRedisCacheBackend, error) {
	cred, err := azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to connect to Azure Cache for Redis (Service Principal): %w",
			core.ErrCacheConnection, err)
	}
	return newAzureRedisWithEntra(cfg, cred)
}

// newAzureRedisWithEntra wires an Entra ID token credential into go-redis.
// Tokens are valid for ~1 hour; go-redis re-invokes the provider on each new
// connection, keeping tokens fresh.
func newAzureRedisWithEntra(cfg Config, cred azcore.TokenCredential) (*AzureRedisCacheBackend, error) {
	port := portOrDefault(cfg.Port, 6380)
	opts := &redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.Host, port),
		DB:   cfg.DB,
		CredentialsProviderContext: func(ctx context.Context) (string, string, error) {
			tk, err := cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{azureRedisScope}})
			if err != nil {
				return "", "", fmt.Errorf("%w: acquiring Entra ID token: %w", core.ErrCacheConnection, err)
			}
			return cfg.Username, tk.Token, nil
		},
	}
	if tlsOrDefault(cfg.TLS, true) {
		tlsCfg, err := buildTLSConfig(cfg.Host, cfg.CACerts, "", "")
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = tlsCfg
	}
	return &AzureRedisCacheBackend{redisOps{client: redis.NewClient(opts)}}, nil
}
