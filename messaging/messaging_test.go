package messaging

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

func TestNewUnknownProvider(t *testing.T) {
	_, err := New(context.Background(), "rabbitmq", Config{})
	if !errors.Is(err, core.ErrMessaging) {
		t.Fatalf("err = %v; want core.ErrMessaging", err)
	}
}

func TestNewAzureServiceBusRequiresConnectionInfo(t *testing.T) {
	_, err := NewAzureServiceBus(Config{QueueName: "q"})
	if !errors.Is(err, core.ErrMessaging) {
		t.Fatalf("err = %v; want core.ErrMessaging", err)
	}
}

func TestNewSQSDefaultsToIAMChain(t *testing.T) {
	b, err := NewSQS(context.Background(), Config{QueueURL: "https://sqs.us-east-1.amazonaws.com/1/q"})
	if err != nil {
		t.Fatal(err)
	}
	if b.queueURL == "" {
		t.Fatal("queueURL not set")
	}
}

func TestDeadLetterWithoutReceiveFails(t *testing.T) {
	b, err := NewSQS(context.Background(), Config{QueueURL: "https://sqs.us-east-1.amazonaws.com/1/q"})
	if err != nil {
		t.Fatal(err)
	}
	err = b.DeadLetter(context.Background(), "bogus-handle", "reason")
	if !errors.Is(err, core.ErrMessaging) {
		t.Fatalf("err = %v; want core.ErrMessaging", err)
	}
}

func TestNewIDIsUUIDv4(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := make(map[string]struct{})
	for range 100 {
		id := newID()
		if !re.MatchString(id) {
			t.Fatalf("invalid UUID: %s", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate UUID: %s", id)
		}
		seen[id] = struct{}{}
	}
}
