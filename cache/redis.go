package cache

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/LYZR-OSS/cloudrift-go/core"
)

// redisOps is the concrete Redis implementation shared by all Redis-backed
// cache backends (the analogue of Python's _RedisMixin). Provider structs
// embed it; a new Redis command is implemented once here and added to the
// Backend interface — never reimplemented per provider.
type redisOps struct {
	client *redis.Client
}

func wrapCache(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", core.ErrCache, err)
}

func (b *redisOps) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := b.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return val, wrapCache(err)
}

func (b *redisOps) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	if ttl < 0 {
		ttl = 0
	}
	return wrapCache(b.client.Set(ctx, key, value, ttl).Err())
}

func (b *redisOps) SetEx(ctx context.Context, key string, value any, ttl time.Duration) error {
	return b.Set(ctx, key, value, ttl)
}

func (b *redisOps) Delete(ctx context.Context, keys ...string) (int64, error) {
	n, err := b.client.Del(ctx, keys...).Result()
	return n, wrapCache(err)
}

func (b *redisOps) Exists(ctx context.Context, key string) (bool, error) {
	n, err := b.client.Exists(ctx, key).Result()
	return n > 0, wrapCache(err)
}

func (b *redisOps) Expire(ctx context.Context, key string, ttl time.Duration, mode ExpireMode) (bool, error) {
	var cmd *redis.BoolCmd
	switch mode {
	case ExpireNX:
		cmd = b.client.ExpireNX(ctx, key, ttl)
	case ExpireXX:
		cmd = b.client.ExpireXX(ctx, key, ttl)
	default:
		cmd = b.client.Expire(ctx, key, ttl)
	}
	ok, err := cmd.Result()
	return ok, wrapCache(err)
}

func (b *redisOps) TTL(ctx context.Context, key string) (time.Duration, error) {
	d, err := b.client.TTL(ctx, key).Result()
	return d, wrapCache(err)
}

func (b *redisOps) Keys(ctx context.Context, pattern string) ([]string, error) {
	keys, err := b.client.Keys(ctx, pattern).Result()
	return keys, wrapCache(err)
}

func (b *redisOps) HGet(ctx context.Context, key, field string) ([]byte, error) {
	val, err := b.client.HGet(ctx, key, field).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return val, wrapCache(err)
}

func (b *redisOps) HSet(ctx context.Context, key, field string, value any) (int64, error) {
	n, err := b.client.HSet(ctx, key, field, value).Result()
	return n, wrapCache(err)
}

func (b *redisOps) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	m, err := b.client.HGetAll(ctx, key).Result()
	return m, wrapCache(err)
}

func (b *redisOps) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	n, err := b.client.HDel(ctx, key, fields...).Result()
	return n, wrapCache(err)
}

func (b *redisOps) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	n, err := b.client.SAdd(ctx, key, members...).Result()
	return n, wrapCache(err)
}

func (b *redisOps) SRem(ctx context.Context, key string, members ...any) (int64, error) {
	n, err := b.client.SRem(ctx, key, members...).Result()
	return n, wrapCache(err)
}

func (b *redisOps) SCard(ctx context.Context, key string) (int64, error) {
	n, err := b.client.SCard(ctx, key).Result()
	return n, wrapCache(err)
}

func (b *redisOps) SIsMember(ctx context.Context, key string, member any) (bool, error) {
	ok, err := b.client.SIsMember(ctx, key, member).Result()
	return ok, wrapCache(err)
}

func (b *redisOps) SMembers(ctx context.Context, key string) ([]string, error) {
	members, err := b.client.SMembers(ctx, key).Result()
	return members, wrapCache(err)
}

func (b *redisOps) SInter(ctx context.Context, keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: SInter requires at least one key", core.ErrCache)
	}
	members, err := b.client.SInter(ctx, keys...).Result()
	return members, wrapCache(err)
}

func (b *redisOps) LPush(ctx context.Context, key string, values ...any) (int64, error) {
	n, err := b.client.LPush(ctx, key, values...).Result()
	return n, wrapCache(err)
}

func (b *redisOps) RPush(ctx context.Context, key string, values ...any) (int64, error) {
	n, err := b.client.RPush(ctx, key, values...).Result()
	return n, wrapCache(err)
}

func (b *redisOps) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	vals, err := b.client.LRange(ctx, key, start, stop).Result()
	return vals, wrapCache(err)
}

func (b *redisOps) LLen(ctx context.Context, key string) (int64, error) {
	n, err := b.client.LLen(ctx, key).Result()
	return n, wrapCache(err)
}

func (b *redisOps) Incr(ctx context.Context, key string) (int64, error) {
	n, err := b.client.Incr(ctx, key).Result()
	return n, wrapCache(err)
}

func (b *redisOps) Decr(ctx context.Context, key string) (int64, error) {
	n, err := b.client.Decr(ctx, key).Result()
	return n, wrapCache(err)
}

func (b *redisOps) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	vals, err := b.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, wrapCache(err)
	}
	out := make([][]byte, len(vals))
	for i, v := range vals {
		switch t := v.(type) {
		case nil:
			out[i] = nil
		case string:
			out[i] = []byte(t)
		case []byte:
			out[i] = t
		default:
			out[i] = []byte(fmt.Sprint(t))
		}
	}
	return out, nil
}

func (b *redisOps) MSet(ctx context.Context, mapping map[string]any) error {
	args := make([]any, 0, len(mapping)*2)
	for k, v := range mapping {
		args = append(args, k, v)
	}
	return wrapCache(b.client.MSet(ctx, args...).Err())
}

func (b *redisOps) Pipeline(ctx context.Context, fn func(Pipeliner)) error {
	_, err := b.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		fn(&redisPipeliner{p})
		return nil
	})
	return wrapCache(err)
}

func (b *redisOps) Ping(ctx context.Context) (bool, error) {
	err := b.client.Ping(ctx).Err()
	if err != nil {
		return false, wrapCache(err)
	}
	return true, nil
}

func (b *redisOps) Flush(ctx context.Context) error {
	return wrapCache(b.client.FlushDB(ctx).Err())
}

func (b *redisOps) HealthCheck(ctx context.Context) bool {
	ok, err := b.Ping(ctx)
	return err == nil && ok
}

func (b *redisOps) Close(ctx context.Context) error {
	return wrapCache(b.client.Close())
}

// redisPipeliner adapts a go-redis Pipeliner to the cache.Pipeliner subset.
type redisPipeliner struct {
	p redis.Pipeliner
}

func (rp *redisPipeliner) Set(key string, value any, ttl time.Duration) {
	if ttl < 0 {
		ttl = 0
	}
	rp.p.Set(context.Background(), key, value, ttl)
}
func (rp *redisPipeliner) Delete(keys ...string) { rp.p.Del(context.Background(), keys...) }
func (rp *redisPipeliner) Expire(key string, ttl time.Duration) {
	rp.p.Expire(context.Background(), key, ttl)
}
func (rp *redisPipeliner) HSet(key, field string, value any) {
	rp.p.HSet(context.Background(), key, field, value)
}
func (rp *redisPipeliner) HDel(key string, fields ...string) {
	rp.p.HDel(context.Background(), key, fields...)
}
func (rp *redisPipeliner) SAdd(key string, members ...any) {
	rp.p.SAdd(context.Background(), key, members...)
}
func (rp *redisPipeliner) SRem(key string, members ...any) {
	rp.p.SRem(context.Background(), key, members...)
}
func (rp *redisPipeliner) LPush(key string, values ...any) {
	rp.p.LPush(context.Background(), key, values...)
}
func (rp *redisPipeliner) RPush(key string, values ...any) {
	rp.p.RPush(context.Background(), key, values...)
}
func (rp *redisPipeliner) Incr(key string) { rp.p.Incr(context.Background(), key) }
func (rp *redisPipeliner) Decr(key string) { rp.p.Decr(context.Background(), key) }

// buildTLSConfig assembles a *tls.Config from optional CA bundle and client
// certificate/key paths. host is used for SNI/verification.
func buildTLSConfig(host, caCerts, certFile, keyFile string) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: host,
	}
	if caCerts != "" {
		pem, err := os.ReadFile(caCerts)
		if err != nil {
			return nil, fmt.Errorf("%w: reading CA bundle: %w", core.ErrCacheConnection, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%w: no certificates parsed from %s", core.ErrCacheConnection, caCerts)
		}
		cfg.RootCAs = pool
	}
	if certFile != "" || keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("%w: loading client certificate: %w", core.ErrCacheConnection, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// tlsOrDefault resolves a *bool TLS flag against the provider default.
func tlsOrDefault(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

// portOrDefault resolves a port against the constructor default.
func portOrDefault(p, def int) int {
	if p == 0 {
		return def
	}
	return p
}
