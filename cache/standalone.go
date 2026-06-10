package cache

import (
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/lyzr-ai/cloudrift-go/core"
)

// StandaloneRedisBackend is the cache backend for self-hosted Redis (e.g. on
// EC2 or bare-metal).
//
// Construct with one of:
//   - NewStandaloneFromURL         — full Redis URL (most flexible)
//   - NewStandaloneFromCredentials — host/port + optional password/username/TLS
//   - NewStandaloneFromTLSCert     — mTLS with client certificate and key files
type StandaloneRedisBackend struct {
	redisOps
}

var _ Backend = (*StandaloneRedisBackend)(nil)

// NewStandaloneFromURL connects using a Redis URL, e.g.
// "redis://user:pass@localhost:6379/0" or "rediss://user:pass@localhost:6380/0"
// (TLS). cfg.CACerts optionally points at a CA bundle (PEM) for TLS.
func NewStandaloneFromURL(cfg Config) (*StandaloneRedisBackend, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to connect to Redis: %w", core.ErrCacheConnection, err)
	}
	if cfg.CACerts != "" {
		host := opts.Addr
		if opts.TLSConfig != nil && opts.TLSConfig.ServerName != "" {
			host = opts.TLSConfig.ServerName
		}
		tlsCfg, err := buildTLSConfig(host, cfg.CACerts, "", "")
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = tlsCfg
	}
	return &StandaloneRedisBackend{redisOps{client: redis.NewClient(opts)}}, nil
}

// NewStandaloneFromCredentials connects using explicit host, port (default
// 6379), and optional username/password, database index, and TLS settings.
func NewStandaloneFromCredentials(cfg Config) (*StandaloneRedisBackend, error) {
	port := portOrDefault(cfg.Port, 6379)
	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, port),
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}
	if tlsOrDefault(cfg.TLS, false) {
		tlsCfg, err := buildTLSConfig(cfg.Host, cfg.CACerts, "", "")
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = tlsCfg
	}
	return &StandaloneRedisBackend{redisOps{client: redis.NewClient(opts)}}, nil
}

// NewStandaloneFromTLSCert connects using mutual TLS (mTLS) with a client
// certificate (cfg.CertFile) and key (cfg.KeyFile). Port defaults to 6380.
func NewStandaloneFromTLSCert(cfg Config) (*StandaloneRedisBackend, error) {
	port := portOrDefault(cfg.Port, 6380)
	tlsCfg, err := buildTLSConfig(cfg.Host, cfg.CACerts, cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	opts := &redis.Options{
		Addr:      fmt.Sprintf("%s:%d", cfg.Host, port),
		Username:  cfg.Username,
		Password:  cfg.Password,
		DB:        cfg.DB,
		TLSConfig: tlsCfg,
	}
	return &StandaloneRedisBackend{redisOps{client: redis.NewClient(opts)}}, nil
}

// newStandaloneFromClient wraps an existing go-redis client (used in tests).
func newStandaloneFromClient(client *redis.Client) *StandaloneRedisBackend {
	return &StandaloneRedisBackend{redisOps{client: client}}
}
