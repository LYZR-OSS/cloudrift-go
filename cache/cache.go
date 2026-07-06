// Package cache provides a provider-neutral interface over Redis-compatible
// caches: self-hosted Redis, AWS ElastiCache, and Azure Cache for Redis.
//
// Construct a backend once at service startup via New (or a typed
// New*From* constructor) and reuse it — the underlying client is
// connection-pooled. Release sockets at shutdown with Close.
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// ExpireMode controls the conditional flags of Expire (Redis NX/XX).
type ExpireMode int

const (
	// ExpireAlways sets the TTL unconditionally.
	ExpireAlways ExpireMode = iota
	// ExpireNX sets the TTL only if the key has no existing TTL.
	ExpireNX
	// ExpireXX sets the TTL only if the key already has a TTL.
	ExpireXX
)

// TTL sentinel return values, matching Redis semantics (go-redis surfaces the
// raw -1/-2 replies as bare negative durations).
const (
	// TTLNoExpiry is returned by TTL when the key exists but has no expiry.
	TTLNoExpiry = time.Duration(-1)
	// TTLKeyMissing is returned by TTL when the key does not exist.
	TTLKeyMissing = time.Duration(-2)
)

// Pipeliner queues commands inside a Pipeline block. Commands execute
// atomically (MULTI/EXEC) when the block returns.
type Pipeliner interface {
	Set(key string, value any, ttl time.Duration)
	Delete(keys ...string)
	Expire(key string, ttl time.Duration)
	HSet(key, field string, value any)
	HDel(key string, fields ...string)
	SAdd(key string, members ...any)
	SRem(key string, members ...any)
	LPush(key string, values ...any)
	RPush(key string, values ...any)
	Incr(key string)
	Decr(key string)
}

// Backend is the provider-neutral cache interface (KV, hash, set, list,
// counters). Values are binary-safe: Get/MGet return []byte; collection
// operations return string (Go strings hold arbitrary bytes).
type Backend interface {
	// Get returns the value for key, or (nil, nil) if it does not exist.
	Get(ctx context.Context, key string) ([]byte, error)
	// Set sets key to value. ttl <= 0 means no expiry.
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	// SetEx is an atomic set-with-TTL (equivalent to Set with a positive ttl).
	SetEx(ctx context.Context, key string, value any, ttl time.Duration) error
	// Delete removes one or more keys. Returns the number of keys removed.
	Delete(ctx context.Context, keys ...string) (int64, error)
	// Exists reports whether key exists.
	Exists(ctx context.Context, key string) (bool, error)
	// Expire sets a timeout on key. Returns true if the timeout was set.
	Expire(ctx context.Context, key string, ttl time.Duration, mode ExpireMode) (bool, error)
	// TTL returns the remaining TTL. TTLNoExpiry = no expiry, TTLKeyMissing = key missing.
	TTL(ctx context.Context, key string) (time.Duration, error)
	// Keys returns all keys matching pattern. Avoid on large keyspaces in production.
	Keys(ctx context.Context, pattern string) ([]string, error)

	// HGet returns the value of field in the hash at key, or (nil, nil) if missing.
	HGet(ctx context.Context, key, field string) ([]byte, error)
	// HSet sets field in the hash at key. Returns 1 if new, 0 if updated.
	HSet(ctx context.Context, key, field string, value any) (int64, error)
	// HGetAll returns all fields and values of the hash at key.
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	// HDel deletes fields from the hash at key. Returns the number removed.
	HDel(ctx context.Context, key string, fields ...string) (int64, error)

	// SAdd adds members to the set at key. Returns the number newly added —
	// this "was-new" signal is the foundation of dedup patterns (DAU/MAU).
	SAdd(ctx context.Context, key string, members ...any) (int64, error)
	// SRem removes members from the set at key. Returns the number removed.
	SRem(ctx context.Context, key string, members ...any) (int64, error)
	// SCard returns the number of elements in the set at key.
	SCard(ctx context.Context, key string) (int64, error)
	// SIsMember reports whether member is in the set at key.
	SIsMember(ctx context.Context, key string, member any) (bool, error)
	// SMembers returns all members of the set at key.
	SMembers(ctx context.Context, key string) ([]string, error)
	// SInter returns the members common to all sets at keys. A missing key is
	// treated as an empty set, so any missing key yields an empty result.
	SInter(ctx context.Context, keys ...string) ([]string, error)

	// LPush prepends values to the list at key. Returns the new list length.
	LPush(ctx context.Context, key string, values ...any) (int64, error)
	// RPush appends values to the list at key. Returns the new list length.
	RPush(ctx context.Context, key string, values ...any) (int64, error)
	// LRange returns the slice [start, stop] of the list at key.
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	// LLen returns the length of the list at key.
	LLen(ctx context.Context, key string) (int64, error)

	// Incr increments the integer value of key by 1. Returns the new value.
	Incr(ctx context.Context, key string) (int64, error)
	// Decr decrements the integer value of key by 1. Returns the new value.
	Decr(ctx context.Context, key string) (int64, error)

	// MGet returns values for multiple keys at once (nil for missing keys).
	MGet(ctx context.Context, keys ...string) ([][]byte, error)
	// MSet sets multiple key-value pairs at once.
	MSet(ctx context.Context, mapping map[string]any) error

	// Pipeline queues the commands issued in fn and executes them atomically
	// (MULTI/EXEC) in a single round trip.
	Pipeline(ctx context.Context, fn func(Pipeliner)) error

	// Eval runs a Lua script atomically on the server and returns its raw
	// result (the value the script returns, or nil if it returns nothing).
	// keys are the KEYS[] the script reads/writes; args become ARGV[]. This is
	// the one primitive that expresses race-free check-then-act logic (rate
	// limiters, quota counters, slot reservation) that the individual commands
	// above cannot. All cache providers are Redis-protocol, so it is supported
	// on each.
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)

	// Ping reports whether the cache server is reachable.
	Ping(ctx context.Context) (bool, error)
	// Flush removes all keys from the current database. Use with caution.
	Flush(ctx context.Context) error
	// HealthCheck returns true if the cache server is reachable (never errors).
	HealthCheck(ctx context.Context) bool
	// Close releases the underlying connection pool.
	Close(ctx context.Context) error
}

// Config carries the union of provider/auth-method options. Only the fields
// relevant to the chosen provider + auth method are read. TLS is a *bool so
// that nil means "use the provider default" (false for self-hosted Redis,
// true for ElastiCache and Azure Cache for Redis). Use core.Ptr(false) to
// disable explicitly.
type Config struct {
	// Common connection fields.
	URL      string // full redis:// or rediss:// URL (redis from_url)
	Host     string
	Port     int // 0 = provider/auth-method default
	Username string
	Password string
	DB       int
	TLS      *bool  // nil = provider default
	CACerts  string // path to CA bundle (PEM)
	CertFile string // client certificate (mTLS)
	KeyFile  string // client private key (mTLS)

	// AWS ElastiCache.
	AuthToken          string // from_auth_token / from_tls_cert
	Region             string // from_iam_auth
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string
	ProfileName        string

	// Azure Cache for Redis.
	AccessKey    string // from_access_key
	TenantID     string // from_service_principal
	ClientID     string // service principal app ID, or user-assigned MI client ID
	ClientSecret string
}

// New instantiates a cache backend.
//
// provider is "redis", "elasticache", or "azure_redis". authMethod names the
// constructor to use, exactly as in the Python library:
//
//	New(ctx, "redis", "from_url", Config{URL: "rediss://user:pass@host:6380/0"})
//	New(ctx, "redis", "from_credentials", Config{Host: "localhost", Port: 6379})
//	New(ctx, "elasticache", "from_auth_token", Config{Host: "...", AuthToken: "..."})
//	New(ctx, "elasticache", "from_iam_auth", Config{Host: "...", Username: "...", Region: "us-east-1"})
//	New(ctx, "azure_redis", "from_access_key", Config{Host: "...", AccessKey: "..."})
//	New(ctx, "azure_redis", "from_managed_identity", Config{Host: "...", Username: "..."})
func New(ctx context.Context, provider, authMethod string, cfg Config) (Backend, error) {
	switch provider {
	case "redis":
		switch authMethod {
		case "from_url":
			return NewStandaloneFromURL(cfg)
		case "from_credentials":
			return NewStandaloneFromCredentials(cfg)
		case "from_tls_cert":
			return NewStandaloneFromTLSCert(cfg)
		}
	case "elasticache":
		switch authMethod {
		case "from_auth_token":
			return NewElastiCacheFromAuthToken(cfg)
		case "from_iam_auth":
			return NewElastiCacheFromIAMAuth(ctx, cfg)
		case "from_tls_cert":
			return NewElastiCacheFromTLSCert(cfg)
		}
	case "azure_redis":
		switch authMethod {
		case "from_access_key":
			return NewAzureRedisFromAccessKey(cfg)
		case "from_managed_identity":
			return NewAzureRedisFromManagedIdentity(cfg)
		case "from_service_principal":
			return NewAzureRedisFromServicePrincipal(cfg)
		}
	default:
		return nil, fmt.Errorf("%w: unknown cache provider %q (choose 'redis', 'elasticache', or 'azure_redis')",
			core.ErrCache, provider)
	}
	return nil, fmt.Errorf("%w: provider %q has no auth method %q", core.ErrCache, provider, authMethod)
}
