package document

import (
	"context"
	"errors"
	"testing"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

func TestNewUnknownProvider(t *testing.T) {
	_, err := New("dynamodb", Config{})
	if !errors.Is(err, core.ErrDocument) {
		t.Fatalf("err = %v; want core.ErrDocument", err)
	}
}

func TestNewReturnsNativeClient(t *testing.T) {
	// mongo.Connect is lazy (no dial), so a valid URI yields a usable client.
	client, err := New("documentdb", Config{URI: "mongodb://localhost:27017"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if client == nil {
		t.Fatal("client is nil")
	}
	if err := client.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
}

func TestDocumentDBURI(t *testing.T) {
	// URI passthrough.
	if got := documentDBURI(Config{URI: "mongodb://u:p@h:27017/?tls=true"}); got != "mongodb://u:p@h:27017/?tls=true" {
		t.Fatalf("uri = %q", got)
	}
	// URI + CA file appends the parameter.
	got := documentDBURI(Config{URI: "mongodb://u:p@h:27017/?tls=true", TLSCAFile: "/ca.pem"})
	if got != "mongodb://u:p@h:27017/?tls=true&tlsCAFile=%2Fca.pem" {
		t.Fatalf("uri = %q", got)
	}
	// Credentials: TLS on by default, port defaults to 27017, password escaped.
	got = documentDBURI(Config{Host: "h", Username: "u", Password: "p@ss"})
	if got != "mongodb://u:p%40ss@h:27017/?tls=true" {
		t.Fatalf("uri = %q", got)
	}
	// mTLS routes to tlsCertificateKeyFile.
	got = documentDBURI(Config{Host: "h", Port: 27017, Username: "u", Password: "p",
		TLSCertKeyFile: "/client.pem", TLSCAFile: "/ca.pem"})
	if got != "mongodb://u:p@h:27017/?tls=true&tlsCAFile=%2Fca.pem&tlsCertificateKeyFile=%2Fclient.pem" {
		t.Fatalf("uri = %q", got)
	}
	// TLS explicitly off yields a bare URI.
	got = documentDBURI(Config{Host: "h", Port: 27018, Username: "u", Password: "p", TLS: core.Ptr(false)})
	if got != "mongodb://u:p@h:27018/" {
		t.Fatalf("uri = %q", got)
	}
}

func TestCosmosURI(t *testing.T) {
	// Defaults: port 10255, appName "@<account>@", Cosmos-required params.
	got := cosmosURI(Config{Account: "myacct", AccountKey: "k+y/=="})
	want := "mongodb://myacct:k%2By%2F%3D%3D@myacct.mongo.cosmos.azure.com:10255/" +
		"?ssl=true&replicaSet=globaldb&retryWrites=false&maxIdleTimeMS=120000&appName=%40myacct%40"
	if got != want {
		t.Fatalf("uri = %q; want %q", got, want)
	}
	// Custom port and app name.
	got = cosmosURI(Config{Account: "a", AccountKey: "k", Port: 10256, AppName: "svc"})
	want = "mongodb://a:k@a.mongo.cosmos.azure.com:10256/" +
		"?ssl=true&replicaSet=globaldb&retryWrites=false&maxIdleTimeMS=120000&appName=svc"
	if got != want {
		t.Fatalf("uri = %q; want %q", got, want)
	}
}
