package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"

	"github.com/lyzr-ai/cloudrift-go/core"
)

// AzureKeyVaultBackend is the Azure Key Vault secrets backend.
//
// Authentication is chosen by NewAzureKeyVault based on which Config fields
// are set: service principal (ClientSecret) or managed identity.
type AzureKeyVaultBackend struct {
	client *azsecrets.Client
}

var _ Backend = (*AzureKeyVaultBackend)(nil)

// NewAzureKeyVault constructs a Key Vault backend for cfg.VaultURL.
func NewAzureKeyVault(cfg Config) (*AzureKeyVaultBackend, error) {
	var cred azcore.TokenCredential
	var err error
	if cfg.ClientSecret != "" {
		cred, err = azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
	} else {
		var miOpts *azidentity.ManagedIdentityCredentialOptions
		if cfg.ClientID != "" {
			miOpts = &azidentity.ManagedIdentityCredentialOptions{ID: azidentity.ClientID(cfg.ClientID)}
		}
		cred, err = azidentity.NewManagedIdentityCredential(miOpts)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrSecret, err)
	}
	client, err := azsecrets.NewClient(cfg.VaultURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrSecret, err)
	}
	return &AzureKeyVaultBackend{client: client}, nil
}

func (b *AzureKeyVaultBackend) GetSecret(ctx context.Context, name string) (string, error) {
	resp, err := b.client.GetSecret(ctx, name, "", nil)
	if err != nil {
		return "", mapKVErr(err, name)
	}
	if resp.Value == nil {
		return "", nil
	}
	return *resp.Value, nil
}

func (b *AzureKeyVaultBackend) GetSecretJSON(ctx context.Context, name string) (map[string]any, error) {
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

func (b *AzureKeyVaultBackend) SetSecret(ctx context.Context, name, value string) error {
	if _, err := b.client.SetSecret(ctx, name, azsecrets.SetSecretParameters{Value: &value}, nil); err != nil {
		return mapKVErr(err, name)
	}
	return nil
}

func (b *AzureKeyVaultBackend) DeleteSecret(ctx context.Context, name string) error {
	if _, err := b.client.DeleteSecret(ctx, name, nil); err != nil {
		return mapKVErr(err, name)
	}
	return nil
}

func (b *AzureKeyVaultBackend) ListSecrets(ctx context.Context, prefix string) ([]string, error) {
	var names []string
	pager := b.client.NewListSecretPropertiesPager(nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, mapKVErr(err, prefix)
		}
		for _, props := range page.Value {
			if props.ID == nil {
				continue
			}
			name := props.ID.Name()
			if prefix == "" || strings.HasPrefix(name, prefix) {
				names = append(names, name)
			}
		}
	}
	return names, nil
}

func (b *AzureKeyVaultBackend) HealthCheck(ctx context.Context) bool {
	pager := b.client.NewListSecretPropertiesPager(nil)
	if !pager.More() {
		return true
	}
	_, err := pager.NextPage(ctx)
	return err == nil
}

// Close is a no-op: the azsecrets client shares an HTTP transport managed by Go.
func (b *AzureKeyVaultBackend) Close(ctx context.Context) error { return nil }

func mapKVErr(err error, name string) error {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		switch re.StatusCode {
		case 404:
			return fmt.Errorf("%w: %s: %w", core.ErrSecretNotFound, name, err)
		case 403:
			return fmt.Errorf("%w: %s: %w", core.ErrSecretPermission, name, err)
		}
	}
	return fmt.Errorf("%w: %w", core.ErrSecret, err)
}
