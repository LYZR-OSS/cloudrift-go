// Firestore with MongoDB compatibility connection factory.
//
// A Firestore database created in MongoDB-compatibility mode (Enterprise
// edition) exposes a MongoDB wire-protocol endpoint, so this returns a
// *mongo.Client identical in shape to the DocumentDB and Cosmos factories.
// Classic Firestore does not speak the wire protocol and is not supported.

package document

import (
	"cmp"
	"fmt"
	"net/url"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// firestoreRequired are the URI options Firestore mandates and no driver
// defaults to: loadBalanced (the endpoint sits behind a load balancer and
// exposes no replica-set topology), tls (TLS-only), and retryWrites=false
// (Firestore has no retryable writes; the driver enables them by default).
// Getting any of them wrong surfaces as an opaque server-selection timeout, so
// every path applies them.
var firestoreRequired = []string{"loadBalanced=true", "tls=true", "retryWrites=false"}

// firestoreOIDC authenticates as the workload's Google identity: the driver
// fetches and refreshes an identity token from the metadata server (GKE
// Workload Identity, Cloud Run, GCE), so there is no secret to store or rotate.
const firestoreOIDC = "authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE"

// NewFirestore builds a *mongo.Client for Firestore with MongoDB
// compatibility. Routing mirrors the Python factory: URI (or ConnectionString)
// > AccessToken > Password (SCRAM-SHA-256) > OIDC, the default when only the
// UID/Location/Database triple is set. Grant the caller roles/datastore.user.
func NewFirestore(cfg Config) (*mongo.Client, error) {
	return connect(firestoreURI(cfg), cfg)
}

// firestoreURI returns the caller's URI with the mandatory options filled in,
// or builds one for the endpoint <UID>.<Location>.firestore.goog.
func firestoreURI(cfg Config) string {
	if uri := cmp.Or(cfg.URI, cfg.ConnectionString); uri != "" {
		return ensureFirestoreParams(uri)
	}
	port := cfg.Port
	if port == 0 {
		port = 443
	}
	// url.UserPassword escapes exactly what the driver's PathUnescape reverses;
	// QueryEscape would turn a space in a password into a literal '+'.
	var userinfo, auth string
	switch {
	case cfg.AccessToken != "":
		// PLAIN against $external carries the token as the password; the
		// username is ignored but must be present for the driver to send it.
		userinfo = url.UserPassword("oidc", cfg.AccessToken).String() + "@"
		auth = "authMechanism=PLAIN&authSource=$external"
	case cfg.Password != "":
		userinfo = url.UserPassword(cfg.Username, cfg.Password).String() + "@"
		auth = "authMechanism=SCRAM-SHA-256"
	default:
		auth = firestoreOIDC
	}
	return fmt.Sprintf("mongodb://%s%s.%s.firestore.goog:%d/%s?%s&%s",
		userinfo, cfg.UID, cfg.Location, port, url.QueryEscape(cfg.Database),
		strings.Join(firestoreRequired, "&"), auth)
}

// ensureFirestoreParams appends whichever mandatory options uri lacks, so a
// connection string from `gcloud firestore databases connection-string`
// passes through untouched while a hand-written one still connects. Names
// match case-insensitively, as MongoDB URI options do, and ssl counts as tls —
// the driver rejects a URI that sets both.
//
// A URI with neither credentials nor an authMechanism also gets OIDC. That is
// what gcloud's default connection string looks like, and Firestore rejects
// every operation on it ("Please specify a valid authentication mechanism") —
// on first use, not at startup. The workload identity is the only
// credential-free way in. This deliberately diverges from the Python library,
// which leaves such a URI unauthenticated.
func ensureFirestoreParams(uri string) string {
	base, query, _ := strings.Cut(uri, "?")
	present := map[string]bool{}
	for _, kv := range strings.Split(query, "&") {
		k, _, _ := strings.Cut(kv, "=")
		present[strings.ToLower(k)] = true
	}
	var add []string
	for _, kv := range firestoreRequired {
		k, _, _ := strings.Cut(kv, "=")
		if !present[strings.ToLower(k)] && !(k == "tls" && present["ssl"]) {
			add = append(add, kv)
		}
	}
	_, rest, _ := strings.Cut(base, "://")
	authority, _, _ := strings.Cut(rest, "/")
	if !present["authmechanism"] && !strings.Contains(authority, "@") {
		add = append(add, firestoreOIDC)
	}
	if len(add) == 0 {
		return uri
	}
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	return uri + sep + strings.Join(add, "&")
}
