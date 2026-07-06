package email

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

func TestNewUnknownProvider(t *testing.T) {
	_, err := New(context.Background(), "mailchimp", Config{})
	if !errors.Is(err, core.ErrEmail) {
		t.Fatalf("err = %v; want core.ErrEmail", err)
	}
}

func TestFactoryRouting(t *testing.T) {
	ctx := context.Background()

	// The SES "from profile" path eagerly loads the named shared-config
	// profile, so point the AWS SDK at a temp credentials file that defines
	// "dev" — keeps the routing assertion hermetic (no machine AWS config).
	credsFile := filepath.Join(t.TempDir(), "credentials")
	if err := os.WriteFile(credsFile, []byte("[dev]\naws_access_key_id = AKIATEST\naws_secret_access_key = testsecret\nregion = us-east-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credsFile)
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "config-empty"))
	tests := []struct {
		name     string
		provider string
		cfg      Config
		wantType any
		wantErr  bool
	}{
		{"ses iam role", "ses", Config{Region: "us-east-1", DefaultFrom: "a@b.com"}, &SESBackend{}, false},
		{"ses access key", "ses", Config{AWSAccessKeyID: "AKIA", AWSSecretAccessKey: "secret"}, &SESBackend{}, false},
		{"ses profile", "ses", Config{ProfileName: "dev"}, &SESBackend{}, false},
		{"acs connection string", "azure_acs", Config{ConnectionString: "endpoint=https://x.communication.azure.com;accesskey=" + b64key}, &AzureACSBackend{}, false},
		{"acs service principal", "azure_acs", Config{Endpoint: "https://x.communication.azure.com", TenantID: "t", ClientID: "c", ClientSecret: "s"}, &AzureACSBackend{}, false},
		{"acs managed identity", "azure_acs", Config{Endpoint: "https://x.communication.azure.com"}, &AzureACSBackend{}, false},
		{"acs missing endpoint", "azure_acs", Config{}, nil, true},
		{"smtp default starttls", "smtp", Config{Host: "smtp.example.com"}, &SMTPBackend{}, false},
		{"smtp tls", "smtp", Config{Mode: "tls", Host: "smtp.example.com"}, &SMTPBackend{}, false},
		{"smtp plaintext", "smtp", Config{Mode: "plaintext", Host: "localhost"}, &SMTPBackend{}, false},
		{"smtp bad mode", "smtp", Config{Mode: "ssl", Host: "smtp.example.com"}, nil, true},
		{"smtp missing host", "smtp", Config{}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := New(ctx, tt.provider, tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got backend %T", b)
				}
				if !errors.Is(err, core.ErrEmail) {
					t.Fatalf("err = %v; want core.ErrEmail", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch tt.wantType.(type) {
			case *SESBackend:
				if _, ok := b.(*SESBackend); !ok {
					t.Fatalf("got %T; want *SESBackend", b)
				}
			case *AzureACSBackend:
				if _, ok := b.(*AzureACSBackend); !ok {
					t.Fatalf("got %T; want *AzureACSBackend", b)
				}
			case *SMTPBackend:
				if _, ok := b.(*SMTPBackend); !ok {
					t.Fatalf("got %T; want *SMTPBackend", b)
				}
			}
		})
	}
}

func TestACSAuthMode(t *testing.T) {
	b, err := NewAzureACS(Config{ConnectionString: "endpoint=https://x.communication.azure.com;accesskey=" + b64key})
	if err != nil {
		t.Fatal(err)
	}
	if b.accessKey == nil {
		t.Fatal("connection-string backend should use access-key auth")
	}
	if b.cred != nil {
		t.Fatal("connection-string backend should not have a token credential")
	}

	b2, err := NewAzureACS(Config{Endpoint: "https://x.communication.azure.com", TenantID: "t", ClientID: "c", ClientSecret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if b2.accessKey != nil {
		t.Fatal("service-principal backend should not have an access key")
	}
	if b2.cred == nil {
		t.Fatal("service-principal backend should have a token credential")
	}
}

func TestSMTPPortDefaults(t *testing.T) {
	tests := []struct {
		mode string
		want int
	}{
		{"", 587},
		{"starttls", 587},
		{"tls", 465},
		{"plaintext", 25},
	}
	for _, tt := range tests {
		b, err := NewSMTP(Config{Host: "h", Mode: tt.mode})
		if err != nil {
			t.Fatalf("mode %q: %v", tt.mode, err)
		}
		if b.port != tt.want {
			t.Fatalf("mode %q: port = %d; want %d", tt.mode, b.port, tt.want)
		}
	}
	// Explicit port wins.
	b, err := NewSMTP(Config{Host: "h", Mode: "tls", Port: 2525})
	if err != nil {
		t.Fatal(err)
	}
	if b.port != 2525 {
		t.Fatalf("explicit port = %d; want 2525", b.port)
	}
}

func TestAttachmentContentTypeDefault(t *testing.T) {
	a := Attachment{Filename: "f.bin", Content: []byte("x")}
	if got := a.contentType(); got != "application/octet-stream" {
		t.Fatalf("default content type = %q; want application/octet-stream", got)
	}
	a2 := Attachment{Filename: "f.pdf", ContentType: "application/pdf"}
	if got := a2.contentType(); got != "application/pdf" {
		t.Fatalf("content type = %q; want application/pdf", got)
	}
}

func TestResolveSender(t *testing.T) {
	tests := []struct {
		name       string
		msg        EmailMessage
		def        string
		wantSender string
		wantErr    bool
	}{
		{"from wins", EmailMessage{From: "a@x.com", BodyText: "hi"}, "b@x.com", "a@x.com", false},
		{"falls back to default", EmailMessage{BodyText: "hi"}, "b@x.com", "b@x.com", false},
		{"no sender at all", EmailMessage{BodyText: "hi"}, "", "", true},
		{"no body", EmailMessage{From: "a@x.com"}, "", "", true},
		{"html only ok", EmailMessage{From: "a@x.com", BodyHTML: "<p>hi</p>"}, "", "a@x.com", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveSender(tt.msg, tt.def)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if !errors.Is(err, core.ErrEmail) {
					t.Fatalf("err = %v; want core.ErrEmail", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantSender {
				t.Fatalf("sender = %q; want %q", got, tt.wantSender)
			}
		})
	}
}

// b64key is a deterministic base64-encoded 32-byte key used across tests.
const b64key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
