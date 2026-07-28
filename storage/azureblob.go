package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// AzureBlobBackend is the Azure Blob Storage backend.
//
// A single service client is held for the lifetime of the backend and reused
// across all operations. Authentication is chosen by NewAzureBlob based on
// which Config fields are set: connection string, account key, SAS token,
// service principal, or managed identity.
type AzureBlobBackend struct {
	container string
	client    *azblob.Client
	// canSAS reports whether the client was built with a shared key
	// credential, which is required to mint SAS URLs (PresignedURL).
	canSAS bool
}

var _ Backend = (*AzureBlobBackend)(nil)

// NewAzureBlob constructs an Azure Blob backend. Routing mirrors the Python
// factory: ConnectionString > AccountKey > SASToken > ClientSecret (service
// principal) > managed identity.
func NewAzureBlob(cfg Config) (*AzureBlobBackend, error) {
	switch {
	case cfg.ConnectionString != "":
		client, err := azblob.NewClientFromConnectionString(cfg.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
		}
		// SAS minting works when the connection string carries an AccountKey.
		canSAS := parseConnStringField(cfg.ConnectionString, "AccountKey") != ""
		return &AzureBlobBackend{container: cfg.Container, client: client, canSAS: canSAS}, nil

	case cfg.AccountKey != "":
		accountName := accountNameFromURL(cfg.AccountURL)
		cred, err := azblob.NewSharedKeyCredential(accountName, cfg.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
		}
		client, err := azblob.NewClientWithSharedKeyCredential(cfg.AccountURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
		}
		return &AzureBlobBackend{container: cfg.Container, client: client, canSAS: true}, nil

	case cfg.SASToken != "":
		serviceURL := cfg.AccountURL
		if !strings.Contains(serviceURL, "?") {
			serviceURL += "?" + strings.TrimPrefix(cfg.SASToken, "?")
		}
		client, err := azblob.NewClientWithNoCredential(serviceURL, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
		}
		return &AzureBlobBackend{container: cfg.Container, client: client}, nil

	case cfg.ClientSecret != "":
		cred, err := azidentity.NewClientSecretCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
		}
		client, err := azblob.NewClient(cfg.AccountURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
		}
		return &AzureBlobBackend{container: cfg.Container, client: client}, nil

	default: // workload identity → managed identity → az CLI (user-assigned via ClientID)
		cred, err := core.NewAzureCredential(cfg.ClientID)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
		}
		client, err := azblob.NewClient(cfg.AccountURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
		}
		return &AzureBlobBackend{container: cfg.Container, client: client}, nil
	}
}

func (b *AzureBlobBackend) blobClient(key string) *blob.Client {
	return b.client.ServiceClient().NewContainerClient(b.container).NewBlobClient(key)
}

func (b *AzureBlobBackend) Upload(ctx context.Context, key string, data []byte, contentType string) (string, error) {
	opts := &azblob.UploadBufferOptions{}
	if contentType != "" {
		opts.HTTPHeaders = &blob.HTTPHeaders{BlobContentType: &contentType}
	}
	if _, err := b.client.UploadBuffer(ctx, b.container, key, data, opts); err != nil {
		return "", mapAzBlobErr(err, key)
	}
	return key, nil
}

func (b *AzureBlobBackend) Download(ctx context.Context, key string) ([]byte, error) {
	resp, err := b.client.DownloadStream(ctx, b.container, key, nil)
	if err != nil {
		return nil, mapAzBlobErr(err, key)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrStorage, err)
	}
	return data, nil
}

func (b *AzureBlobBackend) Delete(ctx context.Context, key string) error {
	if _, err := b.client.DeleteBlob(ctx, b.container, key, nil); err != nil {
		return mapAzBlobErr(err, key)
	}
	return nil
}

func (b *AzureBlobBackend) Exists(ctx context.Context, key string) (bool, error) {
	_, err := b.blobClient(key).GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
			return false, nil
		}
		return false, mapAzBlobErr(err, key)
	}
	return true, nil
}

func (b *AzureBlobBackend) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for key, err := range b.ListIter(ctx, prefix) {
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (b *AzureBlobBackend) ListIter(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		opts := &azblob.ListBlobsFlatOptions{}
		if prefix != "" {
			opts.Prefix = &prefix
		}
		pager := b.client.NewListBlobsFlatPager(b.container, opts)
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				yield("", mapAzBlobErr(err, prefix))
				return
			}
			for _, item := range page.Segment.BlobItems {
				name := ""
				if item.Name != nil {
					name = *item.Name
				}
				if !yield(name, nil) {
					return
				}
			}
		}
	}
}

// PresignedURL mints a read-only SAS URL. It requires shared-key
// authentication (connection string with AccountKey, or AccountKey directly).
func (b *AzureBlobBackend) PresignedURL(ctx context.Context, key string, expiresIn time.Duration) (string, error) {
	if !b.canSAS {
		return "", fmt.Errorf(
			"%w: PresignedURL requires account-key authentication (use a connection string or AccountKey)",
			core.ErrStorage)
	}
	url, err := b.blobClient(key).GetSASURL(
		sas.BlobPermissions{Read: true},
		time.Now().UTC().Add(expiresIn),
		nil,
	)
	if err != nil {
		return "", mapAzBlobErr(err, key)
	}
	return url, nil
}

func (b *AzureBlobBackend) Copy(ctx context.Context, srcKey, dstKey string) (string, error) {
	src := b.blobClient(srcKey)
	dst := b.blobClient(dstKey)
	if _, err := dst.StartCopyFromURL(ctx, src.URL(), nil); err != nil {
		return "", mapAzBlobErr(err, srcKey)
	}
	return dstKey, nil
}

func (b *AzureBlobBackend) Move(ctx context.Context, srcKey, dstKey string) (string, error) {
	if _, err := b.Copy(ctx, srcKey, dstKey); err != nil {
		return "", err
	}
	if err := b.Delete(ctx, srcKey); err != nil {
		return "", err
	}
	return dstKey, nil
}

func (b *AzureBlobBackend) GetMetadata(ctx context.Context, key string) (Metadata, error) {
	props, err := b.blobClient(key).GetProperties(ctx, nil)
	if err != nil {
		return Metadata{}, mapAzBlobErr(err, key)
	}
	md := Metadata{Custom: map[string]string{}}
	if props.ContentType != nil {
		md.ContentType = *props.ContentType
	}
	if props.ContentLength != nil {
		md.Size = *props.ContentLength
	}
	if props.LastModified != nil {
		md.LastModified = *props.LastModified
	}
	if props.ETag != nil {
		md.ETag = string(*props.ETag)
	}
	for k, v := range props.Metadata {
		if v != nil {
			md.Custom[k] = *v
		}
	}
	return md, nil
}

// UploadStream uploads from a byte stream with true streaming (no in-memory
// buffering of the whole payload).
func (b *AzureBlobBackend) UploadStream(ctx context.Context, key string, r io.Reader, contentType string) (string, error) {
	opts := &azblob.UploadStreamOptions{}
	if contentType != "" {
		opts.HTTPHeaders = &blob.HTTPHeaders{BlobContentType: &contentType}
	}
	if _, err := b.client.UploadStream(ctx, b.container, key, r, opts); err != nil {
		return "", mapAzBlobErr(err, key)
	}
	return key, nil
}

func (b *AzureBlobBackend) HealthCheck(ctx context.Context) bool {
	_, err := b.Exists(ctx, "__cloudrift_health__")
	return err == nil
}

// Close is a no-op: the azblob client shares an HTTP transport managed by Go.
func (b *AzureBlobBackend) Close(ctx context.Context) error { return nil }

func mapAzBlobErr(err error, key string) error {
	var re *azcore.ResponseError
	if errors.As(err, &re) {
		switch re.StatusCode {
		case 404:
			return fmt.Errorf("%w: %s: %w", core.ErrObjectNotFound, key, err)
		case 403:
			return fmt.Errorf("%w: %s: %w", core.ErrStoragePermission, key, err)
		}
	}
	return fmt.Errorf("%w: %w", core.ErrStorage, err)
}

// parseConnStringField extracts a field (e.g. "AccountKey") from an Azure
// storage connection string.
func parseConnStringField(connString, field string) string {
	for _, part := range strings.Split(connString, ";") {
		if v, ok := strings.CutPrefix(part, field+"="); ok {
			return v
		}
	}
	return ""
}

// accountNameFromURL extracts the storage account name from an account URL
// like https://<account>.blob.core.windows.net.
func accountNameFromURL(accountURL string) string {
	u := strings.TrimPrefix(strings.TrimPrefix(accountURL, "https://"), "http://")
	if i := strings.IndexAny(u, "./:"); i > 0 {
		return u[:i]
	}
	return u
}
