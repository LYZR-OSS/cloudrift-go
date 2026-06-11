package secrets

import (
	"context"
	"errors"
	"testing"

	"github.com/NeuralgoLyzr/cloudrift-go/core"
)

func TestNewUnknownProvider(t *testing.T) {
	_, err := New(context.Background(), "vault", Config{})
	if !errors.Is(err, core.ErrSecret) {
		t.Fatalf("err = %v; want core.ErrSecret", err)
	}
}

func TestNewAWSSecretsManagerDefaultsToIAMChain(t *testing.T) {
	if _, err := NewAWSSecretsManager(context.Background(), Config{Region: "us-east-1"}); err != nil {
		t.Fatal(err)
	}
}
