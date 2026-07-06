package secrets

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

func TestEnvSecretBackend(t *testing.T) {
	ctx := context.Background()
	// Isolate env mutations to this test.
	t.Setenv("SECRET_db", "")
	os.Unsetenv("SECRET_db")

	b := NewEnvSecrets("SECRET_")

	if err := b.SetSecret(ctx, "db", "postgres://x"); err != nil {
		t.Fatalf("SetSecret: %v", err)
	}
	got, err := b.GetSecret(ctx, "db")
	if err != nil {
		t.Fatalf("GetSecret: %v", err)
	}
	if got != "postgres://x" {
		t.Fatalf("GetSecret = %q; want %q", got, "postgres://x")
	}
	// Confirm the env var is namespaced by the constructor prefix.
	if os.Getenv("SECRET_db") != "postgres://x" {
		t.Fatalf("env SECRET_db = %q; want %q", os.Getenv("SECRET_db"), "postgres://x")
	}

	if err := b.SetSecret(ctx, "api_key", "k"); err != nil {
		t.Fatalf("SetSecret: %v", err)
	}

	// ListSecrets("") returns all names under the constructor prefix, stripped.
	names, err := b.ListSecrets(ctx, "")
	if err != nil {
		t.Fatalf("ListSecrets: %v", err)
	}
	if !contains(names, "db") || !contains(names, "api_key") {
		t.Fatalf("ListSecrets = %v; want db and api_key", names)
	}

	// ListSecrets with a filter prefix narrows further.
	filtered, err := b.ListSecrets(ctx, "api")
	if err != nil {
		t.Fatalf("ListSecrets: %v", err)
	}
	if len(filtered) != 1 || filtered[0] != "api_key" {
		t.Fatalf("ListSecrets(api) = %v; want [api_key]", filtered)
	}

	if err := b.DeleteSecret(ctx, "db"); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}
	if _, err := b.GetSecret(ctx, "db"); !errors.Is(err, core.ErrSecretNotFound) {
		t.Fatalf("GetSecret after delete err = %v; want ErrSecretNotFound", err)
	}

	if _, err := b.GetSecret(ctx, "missing"); !errors.Is(err, core.ErrSecretNotFound) {
		t.Fatalf("GetSecret missing err = %v; want ErrSecretNotFound", err)
	}

	if !b.HealthCheck(ctx) {
		t.Fatal("HealthCheck = false; want true")
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMappingSecretBackend(t *testing.T) {
	ctx := context.Background()

	// Constructor copies the input map (mutating the source must not leak).
	seed := map[string]string{"db": "v1"}
	b := NewMappingSecrets(seed)
	seed["db"] = "mutated"

	got, err := b.GetSecret(ctx, "db")
	if err != nil {
		t.Fatalf("GetSecret: %v", err)
	}
	if got != "v1" {
		t.Fatalf("GetSecret = %q; want v1 (input map should have been copied)", got)
	}

	if err := b.SetSecret(ctx, "api", "k"); err != nil {
		t.Fatalf("SetSecret: %v", err)
	}
	names, err := b.ListSecrets(ctx, "")
	if err != nil {
		t.Fatalf("ListSecrets: %v", err)
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "api" || names[1] != "db" {
		t.Fatalf("ListSecrets = %v; want [api db]", names)
	}

	if err := b.DeleteSecret(ctx, "db"); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}
	if _, err := b.GetSecret(ctx, "db"); !errors.Is(err, core.ErrSecretNotFound) {
		t.Fatalf("GetSecret after delete err = %v; want ErrSecretNotFound", err)
	}

	// GetSecretJSON happy path.
	if err := b.SetSecret(ctx, "cfg", `{"a":1,"b":"x"}`); err != nil {
		t.Fatalf("SetSecret: %v", err)
	}
	m, err := b.GetSecretJSON(ctx, "cfg")
	if err != nil {
		t.Fatalf("GetSecretJSON: %v", err)
	}
	if m["b"] != "x" {
		t.Fatalf("GetSecretJSON[b] = %v; want x", m["b"])
	}

	// GetSecretJSON invalid JSON.
	if err := b.SetSecret(ctx, "bad", "not json"); err != nil {
		t.Fatalf("SetSecret: %v", err)
	}
	if _, err := b.GetSecretJSON(ctx, "bad"); !errors.Is(err, core.ErrSecret) {
		t.Fatalf("GetSecretJSON invalid err = %v; want ErrSecret", err)
	}

	// GetSecretJSON missing.
	if _, err := b.GetSecretJSON(ctx, "missing"); !errors.Is(err, core.ErrSecretNotFound) {
		t.Fatalf("GetSecretJSON missing err = %v; want ErrSecretNotFound", err)
	}

	// Nil mapping is fine.
	nb := NewMappingSecrets(nil)
	if err := nb.SetSecret(ctx, "x", "y"); err != nil {
		t.Fatalf("SetSecret on nil-seeded backend: %v", err)
	}

	if !b.HealthCheck(ctx) {
		t.Fatal("HealthCheck = false; want true")
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFileSecretBackend(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "secrets.json")

	b := NewFileSecrets(path)

	// HealthCheck is true even before the file exists.
	if !b.HealthCheck(ctx) {
		t.Fatal("HealthCheck on missing file = false; want true")
	}

	// Set → get round trip.
	if err := b.SetSecret(ctx, "db", "postgres://x"); err != nil {
		t.Fatalf("SetSecret: %v", err)
	}
	got, err := b.GetSecret(ctx, "db")
	if err != nil {
		t.Fatalf("GetSecret: %v", err)
	}
	if got != "postgres://x" {
		t.Fatalf("GetSecret = %q; want postgres://x", got)
	}

	if err := b.SetSecret(ctx, "api", "k"); err != nil {
		t.Fatalf("SetSecret: %v", err)
	}

	// Persistence: a fresh backend instance over the same path sees the value.
	b2 := NewFileSecrets(path)
	got, err = b2.GetSecret(ctx, "db")
	if err != nil {
		t.Fatalf("GetSecret (fresh instance): %v", err)
	}
	if got != "postgres://x" {
		t.Fatalf("GetSecret (fresh instance) = %q; want postgres://x", got)
	}

	// List with prefix.
	names, err := b2.ListSecrets(ctx, "ap")
	if err != nil {
		t.Fatalf("ListSecrets: %v", err)
	}
	if len(names) != 1 || names[0] != "api" {
		t.Fatalf("ListSecrets(ap) = %v; want [api]", names)
	}

	// Delete.
	if err := b.DeleteSecret(ctx, "db"); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}
	if _, err := b.GetSecret(ctx, "db"); !errors.Is(err, core.ErrSecretNotFound) {
		t.Fatalf("GetSecret after delete err = %v; want ErrSecretNotFound", err)
	}

	// Missing key.
	if _, err := b.GetSecret(ctx, "nope"); !errors.Is(err, core.ErrSecretNotFound) {
		t.Fatalf("GetSecret missing err = %v; want ErrSecretNotFound", err)
	}

	// Corrupt file content → ErrSecret.
	corrupt := filepath.Join(t.TempDir(), "corrupt.json")
	if err := os.WriteFile(corrupt, []byte("not json"), 0o600); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}
	cb := NewFileSecrets(corrupt)
	if _, err := cb.GetSecret(ctx, "x"); !errors.Is(err, core.ErrSecret) {
		t.Fatalf("GetSecret corrupt err = %v; want ErrSecret", err)
	}
	if cb.HealthCheck(ctx) {
		t.Fatal("HealthCheck on corrupt file = true; want false")
	}

	// Non-object JSON → ErrSecret.
	arr := filepath.Join(t.TempDir(), "arr.json")
	if err := os.WriteFile(arr, []byte("[1,2,3]"), 0o600); err != nil {
		t.Fatalf("write arr: %v", err)
	}
	ab := NewFileSecrets(arr)
	if _, err := ab.GetSecret(ctx, "x"); !errors.Is(err, core.ErrSecret) {
		t.Fatalf("GetSecret array-json err = %v; want ErrSecret", err)
	}

	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNewFactoryLocalProviders(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		provider string
		cfg      Config
		want     any
	}{
		{"env", Config{Prefix: "SECRET_"}, (*EnvSecretBackend)(nil)},
		{"file", Config{FilePath: filepath.Join(t.TempDir(), "s.json")}, (*FileSecretBackend)(nil)},
		{"memory", Config{Mapping: map[string]string{"a": "b"}}, (*MappingSecretBackend)(nil)},
		{"local", Config{}, (*MappingSecretBackend)(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			b, err := New(ctx, tt.provider, tt.cfg)
			if err != nil {
				t.Fatalf("New(%q): %v", tt.provider, err)
			}
			switch tt.want.(type) {
			case *EnvSecretBackend:
				if _, ok := b.(*EnvSecretBackend); !ok {
					t.Fatalf("New(%q) type = %T; want *EnvSecretBackend", tt.provider, b)
				}
			case *FileSecretBackend:
				if _, ok := b.(*FileSecretBackend); !ok {
					t.Fatalf("New(%q) type = %T; want *FileSecretBackend", tt.provider, b)
				}
			case *MappingSecretBackend:
				if _, ok := b.(*MappingSecretBackend); !ok {
					t.Fatalf("New(%q) type = %T; want *MappingSecretBackend", tt.provider, b)
				}
			}
		})
	}

	if _, err := New(ctx, "bogus", Config{}); !errors.Is(err, core.ErrSecret) {
		t.Fatalf("New(bogus) err = %v; want ErrSecret", err)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
