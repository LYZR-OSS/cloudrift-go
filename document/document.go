// Package document is a connection factory for MongoDB-compatible cloud
// document databases: AWS DocumentDB, Azure Cosmos DB (MongoDB API), and
// Firestore with MongoDB compatibility.
//
// New returns a configured *mongo.Client from the official MongoDB Go driver
// regardless of provider — all three speak the MongoDB wire protocol —
// so the caller selects database and collection and uses the driver's native
// API directly:
//
//	client, err := document.New("documentdb", document.Config{URI: "mongodb://..."})
//	coll := client.Database("mydb").Collection("users")
//	_, err = coll.InsertOne(ctx, bson.M{"name": "Alice"})
//
// The client is connection-pooled: construct once at service startup and
// reuse. Lifecycle is caller-managed — call client.Disconnect(ctx) at
// shutdown.
package document

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// Config carries the union of provider options. Only the fields relevant to
// the chosen provider are read; the factory routes to the appropriate auth
// method based on which credential fields are set.
type Config struct {
	// AWS DocumentDB (MongoDB-compatible).
	URI            string // full MongoDB URI (most flexible)
	Host           string // cluster endpoint hostname
	Port           int    // 0 = provider default (DocumentDB 27017, Cosmos 10255, Firestore 443)
	Username       string
	Password       string
	TLS            *bool  // nil = true (required for AWS DocumentDB)
	TLSCAFile      string // CA certificate bundle (PEM)
	TLSCertKeyFile string // combined client key + certificate PEM (mTLS)

	// Azure Cosmos DB (MongoDB API). Key-based auth only: Cosmos for
	// MongoDB (RU) does not accept Azure AD tokens at the wire-protocol
	// layer.
	ConnectionString string // Mongo-format URI from the Cosmos portal
	Account          string // account name (<account>.mongo.cosmos.azure.com)
	AccountKey       string // primary or secondary account key
	AppName          string // appName URI parameter; default "@<account>@"

	// GCP Firestore with MongoDB compatibility. The UID/Location/Database
	// triple alone authenticates via OIDC as the workload's Google identity;
	// Username+Password selects SCRAM-SHA-256 and AccessToken a short-lived
	// OAuth token. URI or ConnectionString is used as given, plus Firestore's
	// mandatory options.
	UID         string // database UID — the UUID in the endpoint host, not the database ID
	Location    string // database location, e.g. "nam5" or "us-central1"
	Database    string // database ID: the only name client.Database can select on this endpoint
	AccessToken string // OAuth 2.0 token; the client stops authenticating when it expires

	// Connection pool (all providers).
	MaxPoolSize uint64 // 0 = default (100)
	MinPoolSize uint64
}

// New builds a *mongo.Client for the given provider.
//
// provider is "documentdb", "cosmos", or "firestore". The auth method is
// inferred from which credential fields are set, exactly as in the Python
// library:
//
//	New("documentdb", Config{URI: "mongodb://..."})
//	New("documentdb", Config{Host: "...", Port: 27017, Username: "u", Password: "p"})
//	New("documentdb", Config{Host: "...", Port: 27017, Username: "u", Password: "p",
//	    TLSCertKeyFile: "/path/to/client.pem"})
//	New("cosmos", Config{ConnectionString: "mongodb://..."})
//	New("cosmos", Config{Account: "myacct", AccountKey: "..."})
//	New("firestore", Config{UID: "f116f93a-...", Location: "nam5", Database: "mydb"}) // OIDC
//	New("firestore", Config{UID: "...", Location: "nam5", Database: "mydb",
//	    Username: "u", Password: "p"})                                                  // SCRAM
func New(provider string, cfg Config) (*mongo.Client, error) {
	switch provider {
	case "documentdb":
		return NewDocumentDB(cfg)
	case "cosmos":
		return NewCosmos(cfg)
	case "firestore":
		return NewFirestore(cfg)
	}
	return nil, fmt.Errorf("%w: unknown document DB provider %q (choose 'documentdb', 'cosmos', or 'firestore')",
		core.ErrDocument, provider)
}

// connect applies the shared pool options and constructs the client.
// Like Motor, mongo.Connect does not dial; the first operation does.
func connect(uri string, cfg Config) (*mongo.Client, error) {
	maxPool := cfg.MaxPoolSize
	if maxPool == 0 {
		maxPool = 100
	}
	opts := options.Client().ApplyURI(uri).SetMaxPoolSize(maxPool).SetMinPoolSize(cfg.MinPoolSize)
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", core.ErrDocumentConnection, err)
	}
	return client, nil
}
