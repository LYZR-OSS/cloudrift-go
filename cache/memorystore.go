package cache

import (
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// GCPMemorystoreBackend is the cache backend for Google Cloud Memorystore for
// Redis.
//
// Construct with one of:
//   - NewMemorystoreFromAuthString   — AUTH string (shared secret), TLS opt-in
//   - NewMemorystoreFromServerCACert — in-transit encryption pinned to the instance CA
//
// Unlike ElastiCache and Azure Cache for Redis, Memorystore leaves both AUTH
// and in-transit encryption off by default, so the defaults here match the
// product rather than the other providers: TLS is off unless asked for, and
// the TLS port is 6378, not 6379.
type GCPMemorystoreBackend struct {
	redisOps
}

var _ Backend = (*GCPMemorystoreBackend)(nil)

// NewMemorystoreFromAuthString connects with the instance's AUTH string
// (cfg.AuthString; leave empty for an instance with AUTH disabled). Port
// defaults to 6379 and TLS to off, matching Memorystore's opt-in encryption.
// For an instance with in-transit encryption, set cfg.TLS, cfg.CACerts, and
// Port 6378 — or use NewMemorystoreFromServerCACert, which requires the CA.
func NewMemorystoreFromAuthString(cfg Config) (*GCPMemorystoreBackend, error) {
	port := portOrDefault(cfg.Port, 6379)
	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, port),
		Password: cfg.AuthString,
		DB:       cfg.DB,
	}
	if tlsOrDefault(cfg.TLS, false) {
		tlsCfg, err := buildTLSConfig(cfg.Host, cfg.CACerts, "", "")
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = tlsCfg
	}
	return &GCPMemorystoreBackend{redisOps{client: redis.NewClient(opts)}}, nil
}

// NewMemorystoreFromServerCACert connects with in-transit encryption, verifying
// the server against cfg.CACerts. The CA is required rather than optional:
// Memorystore signs its server certificate with a per-instance CA absent from
// the system trust store, so TLS without it can never verify. Include every
// CA from `gcloud redis instances describe` — an instance carries two during a
// CA rotation. Port defaults to 6378; cfg.AuthString is optional.
func NewMemorystoreFromServerCACert(cfg Config) (*GCPMemorystoreBackend, error) {
	if cfg.CACerts == "" {
		return nil, fmt.Errorf("%w: from_server_ca_cert requires CACerts (the instance's server CA bundle)",
			core.ErrCacheConnection)
	}
	port := portOrDefault(cfg.Port, 6378)
	tlsCfg, err := buildTLSConfig(cfg.Host, cfg.CACerts, "", "")
	if err != nil {
		return nil, err
	}
	opts := &redis.Options{
		Addr:      fmt.Sprintf("%s:%d", cfg.Host, port),
		Password:  cfg.AuthString,
		DB:        cfg.DB,
		TLSConfig: tlsCfg,
	}
	return &GCPMemorystoreBackend{redisOps{client: redis.NewClient(opts)}}, nil
}
