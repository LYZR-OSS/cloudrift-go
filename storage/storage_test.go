package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/lyzr-ai/cloudrift-go/core"
)

func TestNewUnknownProvider(t *testing.T) {
	_, err := New(context.Background(), "gcs", Config{})
	if !errors.Is(err, core.ErrStorage) {
		t.Fatalf("err = %v; want core.ErrStorage", err)
	}
}

func TestNewS3DefaultsToIAMChain(t *testing.T) {
	// No credentials set: the default chain constructor must not error at
	// build time (mirrors Python's from_iam_role).
	b, err := NewS3(context.Background(), Config{Bucket: "b", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if b.bucket != "b" {
		t.Fatalf("bucket = %q", b.bucket)
	}
}

func TestNewS3StaticCredentials(t *testing.T) {
	_, err := NewS3(context.Background(), Config{
		Bucket:             "b",
		AWSAccessKeyID:     "AKIAEXAMPLE",
		AWSSecretAccessKey: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAzureBlobConnStringParsing(t *testing.T) {
	if got := parseConnStringField("AccountName=acct;AccountKey=a2V5;EndpointSuffix=x", "AccountKey"); got != "a2V5" {
		t.Fatalf("AccountKey = %q", got)
	}
	if got := parseConnStringField("AccountName=acct", "AccountKey"); got != "" {
		t.Fatalf("missing AccountKey = %q", got)
	}
}

func TestAccountNameFromURL(t *testing.T) {
	if got := accountNameFromURL("https://acct.blob.core.windows.net"); got != "acct" {
		t.Fatalf("got %q", got)
	}
	if got := accountNameFromURL("http://localhost:10000"); got != "localhost" {
		t.Fatalf("got %q", got)
	}
}
