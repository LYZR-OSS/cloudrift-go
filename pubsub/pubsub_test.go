package pubsub

import (
	"context"
	"errors"
	"testing"

	"github.com/lyzr-ai/cloudrift-go/core"
)

func TestNewUnknownProvider(t *testing.T) {
	_, err := New(context.Background(), "kafka", Config{})
	if !errors.Is(err, core.ErrPubSub) {
		t.Fatalf("err = %v; want core.ErrPubSub", err)
	}
}

func TestNewSNSDefaultsToIAMChain(t *testing.T) {
	if _, err := NewSNS(context.Background(), Config{Region: "us-east-1"}); err != nil {
		t.Fatal(err)
	}
}

func TestNewAzureEventGridFromAccessKey(t *testing.T) {
	b, err := NewAzureEventGrid(Config{Endpoint: "https://t.eventgrid.azure.net/api/events", AccessKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if b.client == nil {
		t.Fatal("client not constructed")
	}
}
