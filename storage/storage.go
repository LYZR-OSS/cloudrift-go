// Package storage provides a provider-neutral interface over cloud object
// storage: AWS S3 and Azure Blob Storage.
//
// Construct a backend once at service startup via New (or NewS3 /
// NewAzureBlob) and reuse it — the underlying client is connection-pooled.
package storage

import (
	"context"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// Metadata describes an object (the result of GetMetadata).
type Metadata struct {
	ContentType  string
	Size         int64
	LastModified time.Time
	ETag         string
	// Custom holds user-defined object metadata.
	Custom map[string]string
}

// Backend is the provider-neutral object storage interface.
type Backend interface {
	// Upload stores data under key. contentType may be "" to omit it.
	// Returns the object key.
	Upload(ctx context.Context, key string, data []byte, contentType string) (string, error)
	// Download returns the raw bytes of the object at key.
	Download(ctx context.Context, key string) ([]byte, error)
	// Delete removes the object at key.
	Delete(ctx context.Context, key string) error
	// Exists reports whether the object exists.
	Exists(ctx context.Context, key string) (bool, error)
	// List returns object keys, optionally filtered by prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	// ListIter yields object keys lazily with true pagination.
	ListIter(ctx context.Context, prefix string) iter.Seq2[string, error]
	// PresignedURL generates a presigned GET URL for the object.
	PresignedURL(ctx context.Context, key string, expiresIn time.Duration) (string, error)
	// Copy copies an object within the same bucket/container. Returns dstKey.
	Copy(ctx context.Context, srcKey, dstKey string) (string, error)
	// Move moves an object (copy + delete). Returns dstKey.
	Move(ctx context.Context, srcKey, dstKey string) (string, error)
	// GetMetadata returns object metadata (content type, size, etag, ...).
	GetMetadata(ctx context.Context, key string) (Metadata, error)
	// UploadStream uploads from a byte stream. Returns the object key.
	UploadStream(ctx context.Context, key string, r io.Reader, contentType string) (string, error)
	// HealthCheck returns true if the storage backend is reachable (never errors).
	HealthCheck(ctx context.Context) bool
	// Close releases the underlying client and sockets.
	Close(ctx context.Context) error
}

// Config carries the union of provider options. Only the fields relevant to
// the chosen provider are read; the factory routes to the appropriate auth
// method based on which credential fields are set.
type Config struct {
	// AWS S3.
	Bucket             string
	Region             string // default "us-east-1"
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string
	ProfileName        string
	EndpointURL        string // custom endpoint (LocalStack, MinIO, ...)
	// RoleARN, when set, is assumed via STS on top of the base credentials.
	// ExternalID is passed in the AssumeRole call when set. Cross-account S3.
	RoleARN    string
	ExternalID string

	// Azure Blob.
	Container        string
	ConnectionString string
	AccountURL       string // https://<acct>.blob.core.windows.net
	AccountKey       string
	SASToken         string
	TenantID         string
	ClientID         string // service principal app ID, or user-assigned MI client ID
	ClientSecret     string
}

// New instantiates a storage backend.
//
// provider is "s3" or "azure_blob". The auth method is inferred from which
// credential fields are set, exactly as in the Python library:
//
//	New(ctx, "s3", Config{Bucket: "b", Region: "us-east-1"})                  // IAM role / env
//	New(ctx, "s3", Config{Bucket: "b", AWSAccessKeyID: "...", AWSSecretAccessKey: "..."})
//	New(ctx, "s3", Config{Bucket: "b", ProfileName: "dev"})
//	New(ctx, "azure_blob", Config{ConnectionString: "...", Container: "c"})
//	New(ctx, "azure_blob", Config{AccountURL: "...", AccountKey: "...", Container: "c"})
//	New(ctx, "azure_blob", Config{AccountURL: "...", SASToken: "...", Container: "c"})
//	New(ctx, "azure_blob", Config{AccountURL: "...", Container: "c"})         // managed identity
func New(ctx context.Context, provider string, cfg Config) (Backend, error) {
	switch provider {
	case "s3":
		return NewS3(ctx, cfg)
	case "azure_blob":
		return NewAzureBlob(cfg)
	}
	return nil, fmt.Errorf("%w: unknown storage provider %q (choose 's3' or 'azure_blob')",
		core.ErrStorage, provider)
}
