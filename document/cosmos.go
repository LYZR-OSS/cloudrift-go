// Azure Cosmos DB (MongoDB API) connection factory.
//
// Cosmos DB exposes a MongoDB wire-protocol endpoint. We connect with the
// official MongoDB Go driver and return a *mongo.Client, identical in shape
// to the DocumentDB factory.
//
// Only key-based auth is supported here: Cosmos for MongoDB (RU) does not
// accept Azure AD tokens at the Mongo wire-protocol layer. Use a connection
// string from the portal or build one from the account name + key.

package document

import (
	"fmt"
	"net/url"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// NewCosmos builds a *mongo.Client for Azure Cosmos DB's MongoDB API
// endpoint. Routing mirrors the Python factory: ConnectionString >
// Account+AccountKey.
func NewCosmos(cfg Config) (*mongo.Client, error) {
	uri := cfg.ConnectionString
	if uri == "" {
		uri = cosmosURI(cfg)
	}
	return connect(uri, cfg)
}

// cosmosURI builds a Cosmos MongoDB-API URI from the account name and key,
// with the Cosmos-required parameters (ssl, globaldb replica set, no retryable
// writes). cfg.AppName defaults to "@<account>@" (Cosmos uses it for telemetry
// and routing); cfg.Port defaults to 10255.
func cosmosURI(cfg Config) string {
	port := cfg.Port
	if port == 0 {
		port = 10255
	}
	app := cfg.AppName
	if app == "" {
		app = "@" + cfg.Account + "@"
	}
	query := "ssl=true" +
		"&replicaSet=globaldb" +
		"&retryWrites=false" +
		"&maxIdleTimeMS=120000" +
		"&appName=" + url.QueryEscape(app)
	return fmt.Sprintf("mongodb://%s:%s@%s.mongo.cosmos.azure.com:%d/?%s",
		url.QueryEscape(cfg.Account), url.QueryEscape(cfg.AccountKey), cfg.Account, port, query)
}
