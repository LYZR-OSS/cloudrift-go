// Package document provides a provider-neutral, MongoDB-style interface over
// cloud document databases: AWS DocumentDB (MongoDB-compatible) and Azure
// Cosmos DB (Core/SQL API).
//
// Construct a backend once at service startup via New (or NewDocumentDB /
// NewCosmos) and reuse it — the underlying client is connection-pooled.
package document

import (
	"context"
	"fmt"
	"iter"

	"github.com/NeuralgoLyzr/cloudrift-go/core"
)

// IndexKey is one field of a compound index. Order is 1 (ascending) or -1
// (descending), MongoDB-style.
type IndexKey struct {
	Field string
	Order int
}

// Backend is the provider-neutral document database interface. Queries and
// updates use MongoDB-style maps on both providers; the Cosmos backend
// translates a supported subset to Cosmos SQL.
type Backend interface {
	// InsertOne inserts a document. Returns the inserted document ID.
	InsertOne(ctx context.Context, collection string, document map[string]any) (string, error)
	// InsertMany inserts multiple documents. Returns the inserted IDs.
	InsertMany(ctx context.Context, collection string, documents []map[string]any) ([]string, error)
	// FindOne returns the first document matching query, or (nil, nil) if
	// none matches.
	FindOne(ctx context.Context, collection string, query map[string]any) (map[string]any, error)
	// Find returns documents matching query with pagination.
	Find(ctx context.Context, collection string, query map[string]any, limit, skip int64) ([]map[string]any, error)
	// FindIter yields documents matching query lazily (cursor-based).
	FindIter(ctx context.Context, collection string, query map[string]any) iter.Seq2[map[string]any, error]
	// UpdateOne updates the first document matching query. Returns the
	// modified count.
	UpdateOne(ctx context.Context, collection string, query, update map[string]any) (int64, error)
	// UpdateMany updates all documents matching query. Returns the modified count.
	UpdateMany(ctx context.Context, collection string, query, update map[string]any) (int64, error)
	// DeleteOne deletes the first document matching query. Returns the deleted count.
	DeleteOne(ctx context.Context, collection string, query map[string]any) (int64, error)
	// DeleteMany deletes all documents matching query. Returns the deleted count.
	DeleteMany(ctx context.Context, collection string, query map[string]any) (int64, error)
	// Count counts documents matching query (nil counts all).
	Count(ctx context.Context, collection string, query map[string]any) (int64, error)
	// CreateIndex creates an index. Returns the index name.
	CreateIndex(ctx context.Context, collection string, keys []IndexKey, unique bool) (string, error)
	// Aggregate runs an aggregation pipeline. The Cosmos backend supports the
	// $match, $count, $sort, $project, $limit, and $skip stages.
	Aggregate(ctx context.Context, collection string, pipeline []map[string]any) ([]map[string]any, error)
	// UpsertOne inserts or updates a document. Returns the document ID.
	UpsertOne(ctx context.Context, collection string, query, update map[string]any) (string, error)
	// HealthCheck returns true if the database is reachable (never errors).
	HealthCheck(ctx context.Context) bool
	// Close releases the underlying client and sockets.
	Close(ctx context.Context) error
}

// Config carries the union of provider options. Only the fields relevant to
// the chosen provider are read; the factory routes to the appropriate auth
// method based on which credential fields are set.
type Config struct {
	Database string

	// AWS DocumentDB (MongoDB-compatible).
	URI            string // full MongoDB URI (most flexible)
	Host           string
	Port           int
	Username       string
	Password       string
	TLS            *bool  // nil = true for DocumentDB credentials auth
	TLSCAFile      string // CA certificate bundle (PEM)
	TLSCertKeyFile string // combined client key + certificate PEM (mTLS)
	MaxPoolSize    uint64 // 0 = default (100)
	MinPoolSize    uint64

	// Azure Cosmos DB (Core/SQL API).
	ConnectionString string
	URL              string // https://<account>.documents.azure.com:443/
	AccountKey       string
	PartitionKey     string // partition key path, default "/id"
	TenantID         string
	ClientID         string // service principal app ID, or user-assigned MI client ID
	ClientSecret     string
}

// New instantiates a document database backend.
//
// provider is "documentdb" or "cosmos". The auth method is inferred from
// which credential fields are set, exactly as in the Python library:
//
//	New(ctx, "documentdb", Config{URI: "mongodb://...", Database: "mydb"})
//	New(ctx, "documentdb", Config{Host: "...", Port: 27017, Username: "u",
//	    Password: "p", Database: "mydb"})
//	New(ctx, "cosmos", Config{ConnectionString: "...", Database: "mydb"})
//	New(ctx, "cosmos", Config{URL: "...", AccountKey: "...", Database: "mydb"})
func New(ctx context.Context, provider string, cfg Config) (Backend, error) {
	switch provider {
	case "documentdb":
		return NewDocumentDB(ctx, cfg)
	case "cosmos":
		return NewCosmos(cfg)
	}
	return nil, fmt.Errorf("%w: unknown document DB provider %q (choose 'documentdb' or 'cosmos')",
		core.ErrDocument, provider)
}
