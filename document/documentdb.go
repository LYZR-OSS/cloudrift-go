// AWS DocumentDB connection factory.
//
// Builds a configured *mongo.Client. The caller selects database and
// collection (client.Database(db).Collection(coll)) and uses the driver's
// native API directly.

package document

import (
	"fmt"
	"net/url"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// NewDocumentDB builds a *mongo.Client for AWS DocumentDB (also works against
// plain MongoDB). Routing mirrors the Python factory: URI > TLSCertKeyFile
// (mTLS) > Host/Port credentials. TLS defaults to on (required for AWS
// DocumentDB).
func NewDocumentDB(cfg Config) (*mongo.Client, error) {
	return connect(documentDBURI(cfg), cfg)
}

// documentDBURI returns cfg.URI (appending tlsCAFile if needed) or builds a
// mongodb:// URI from host/port credentials.
func documentDBURI(cfg Config) string {
	if cfg.URI != "" {
		uri := cfg.URI
		if cfg.TLSCAFile != "" && !strings.Contains(uri, "tlsCAFile=") {
			sep := "?"
			if strings.Contains(uri, "?") {
				sep = "&"
			}
			uri += sep + "tlsCAFile=" + url.QueryEscape(cfg.TLSCAFile)
		}
		return uri
	}

	port := cfg.Port
	if port == 0 {
		port = 27017
	}
	params := url.Values{}
	if cfg.TLSCertKeyFile != "" {
		params.Set("tls", "true")
		params.Set("tlsCertificateKeyFile", cfg.TLSCertKeyFile)
	} else if cfg.TLS == nil || *cfg.TLS {
		params.Set("tls", "true")
	}
	if cfg.TLSCAFile != "" {
		params.Set("tlsCAFile", cfg.TLSCAFile)
	}
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d/",
		url.QueryEscape(cfg.Username), url.QueryEscape(cfg.Password), cfg.Host, port)
	if encoded := params.Encode(); encoded != "" {
		uri += "?" + encoded
	}
	return uri
}
