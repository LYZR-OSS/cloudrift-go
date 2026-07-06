package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// EnvSecretBackend reads and writes secrets from process environment variables.
//
// A secret named "db" maps to the environment variable "{prefix}db" — the
// prefix lets you namespace secrets, e.g. "SECRET_". This backend is useful for
// local development, containers, and CI where secrets arrive as env vars.
type EnvSecretBackend struct {
	prefix string
}

var _ Backend = (*EnvSecretBackend)(nil)

// NewEnvSecrets constructs an environment-variable secret backend. prefix
// namespaces secret names onto env vars; pass "" for no namespace.
func NewEnvSecrets(prefix string) *EnvSecretBackend {
	return &EnvSecretBackend{prefix: prefix}
}

func (b *EnvSecretBackend) key(name string) string {
	return b.prefix + name
}

func (b *EnvSecretBackend) GetSecret(_ context.Context, name string) (string, error) {
	v, ok := os.LookupEnv(b.key(name))
	if !ok {
		return "", fmt.Errorf("%w: %s: not found in environment", core.ErrSecretNotFound, name)
	}
	return v, nil
}

func (b *EnvSecretBackend) GetSecretJSON(ctx context.Context, name string) (map[string]any, error) {
	return getSecretJSON(ctx, b, name)
}

func (b *EnvSecretBackend) SetSecret(_ context.Context, name, value string) error {
	if err := os.Setenv(b.key(name), value); err != nil {
		return fmt.Errorf("%w: %s: %w", core.ErrSecret, name, err)
	}
	return nil
}

func (b *EnvSecretBackend) DeleteSecret(_ context.Context, name string) error {
	if err := os.Unsetenv(b.key(name)); err != nil {
		return fmt.Errorf("%w: %s: %w", core.ErrSecret, name, err)
	}
	return nil
}

// ListSecrets scans the environment for variables under the constructor prefix,
// strips that prefix, and returns names further filtered by the prefix arg.
func (b *EnvSecretBackend) ListSecrets(_ context.Context, prefix string) ([]string, error) {
	var names []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(k, b.prefix) {
			continue
		}
		name := k[len(b.prefix):]
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	return names, nil
}

func (b *EnvSecretBackend) HealthCheck(_ context.Context) bool { return true }

// Close is a no-op.
func (b *EnvSecretBackend) Close(_ context.Context) error { return nil }

// MappingSecretBackend holds secrets in an in-memory map. Useful for tests and
// dev seeding; nothing is persisted.
type MappingSecretBackend struct {
	store map[string]string
}

var _ Backend = (*MappingSecretBackend)(nil)

// NewMappingSecrets constructs an in-memory secret backend, copying the given
// mapping (nil is fine).
func NewMappingSecrets(mapping map[string]string) *MappingSecretBackend {
	store := make(map[string]string, len(mapping))
	for k, v := range mapping {
		store[k] = v
	}
	return &MappingSecretBackend{store: store}
}

func (b *MappingSecretBackend) GetSecret(_ context.Context, name string) (string, error) {
	v, ok := b.store[name]
	if !ok {
		return "", fmt.Errorf("%w: %s", core.ErrSecretNotFound, name)
	}
	return v, nil
}

func (b *MappingSecretBackend) GetSecretJSON(ctx context.Context, name string) (map[string]any, error) {
	return getSecretJSON(ctx, b, name)
}

func (b *MappingSecretBackend) SetSecret(_ context.Context, name, value string) error {
	b.store[name] = value
	return nil
}

func (b *MappingSecretBackend) DeleteSecret(_ context.Context, name string) error {
	delete(b.store, name)
	return nil
}

func (b *MappingSecretBackend) ListSecrets(_ context.Context, prefix string) ([]string, error) {
	var names []string
	for n := range b.store {
		if strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}
	return names, nil
}

func (b *MappingSecretBackend) HealthCheck(_ context.Context) bool { return true }

// Close is a no-op.
func (b *MappingSecretBackend) Close(_ context.Context) error { return nil }

// FileSecretBackend persists secrets in a JSON file mapping name → value. Writes
// are atomic via a temp file plus os.Rename. The on-disk format is a single JSON
// object of string values; store structured data by serializing it to a string.
type FileSecretBackend struct {
	path string
}

var _ Backend = (*FileSecretBackend)(nil)

// NewFileSecrets constructs a file-backed secret store at path. The file is
// created on first write; a missing file reads as empty.
func NewFileSecrets(path string) *FileSecretBackend {
	return &FileSecretBackend{path: path}
}

// load reads the backing file. A missing file yields an empty map; an
// unreadable file or a non-object payload is wrapped as core.ErrSecret.
func (b *FileSecretBackend) load() (map[string]string, error) {
	raw, err := os.ReadFile(b.path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("%w: secret file %q is unreadable: %w", core.ErrSecret, b.path, err)
	}
	var data map[string]string
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("%w: secret file %q must contain a JSON object: %w", core.ErrSecret, b.path, err)
	}
	if data == nil {
		data = map[string]string{}
	}
	return data, nil
}

// save atomically writes data via a temp file plus os.Rename.
func (b *FileSecretBackend) save(data map[string]string) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("%w: %w", core.ErrSecret, err)
	}
	tmp := b.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("%w: %w", core.ErrSecret, err)
	}
	if err := os.Rename(tmp, b.path); err != nil {
		return fmt.Errorf("%w: %w", core.ErrSecret, err)
	}
	return nil
}

func (b *FileSecretBackend) GetSecret(_ context.Context, name string) (string, error) {
	data, err := b.load()
	if err != nil {
		return "", err
	}
	v, ok := data[name]
	if !ok {
		return "", fmt.Errorf("%w: %s: not found in %s", core.ErrSecretNotFound, name, b.path)
	}
	return v, nil
}

func (b *FileSecretBackend) GetSecretJSON(ctx context.Context, name string) (map[string]any, error) {
	return getSecretJSON(ctx, b, name)
}

func (b *FileSecretBackend) SetSecret(_ context.Context, name, value string) error {
	data, err := b.load()
	if err != nil {
		return err
	}
	data[name] = value
	return b.save(data)
}

func (b *FileSecretBackend) DeleteSecret(_ context.Context, name string) error {
	data, err := b.load()
	if err != nil {
		return err
	}
	delete(data, name)
	return b.save(data)
}

func (b *FileSecretBackend) ListSecrets(_ context.Context, prefix string) ([]string, error) {
	data, err := b.load()
	if err != nil {
		return nil, err
	}
	var names []string
	for n := range data {
		if strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}
	return names, nil
}

// HealthCheck returns true if the backing file loads without error (a missing
// file is healthy — it just hasn't been written yet).
func (b *FileSecretBackend) HealthCheck(_ context.Context) bool {
	_, err := b.load()
	return err == nil
}

// Close is a no-op.
func (b *FileSecretBackend) Close(_ context.Context) error { return nil }

// getSecretJSON fetches a secret via the backend and parses its value as a JSON
// object. Invalid JSON is wrapped as core.ErrSecret.
func getSecretJSON(ctx context.Context, b Backend, name string) (map[string]any, error) {
	raw, err := b.GetSecret(ctx, name)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("%w: secret %q is not valid JSON: %w", core.ErrSecret, name, err)
	}
	return out, nil
}
