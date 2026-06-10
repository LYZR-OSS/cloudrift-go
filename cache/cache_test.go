package cache

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/lyzr-ai/cloudrift-go/core"
)

// newTestBackend returns a StandaloneRedisBackend wired to an in-process
// miniredis instance (the Go analogue of the Python suite's fakeredis).
func newTestBackend(t *testing.T) (*StandaloneRedisBackend, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	backend := newStandaloneFromClient(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	return backend, mr
}

// ---------------------------------------------------------------------------
// get / set / delete / exists
// ---------------------------------------------------------------------------

func TestSetAndGet(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()
	if err := c.Set(ctx, "k1", "hello", 0); err != nil {
		t.Fatal(err)
	}
	val, err := c.Get(ctx, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "hello" {
		t.Fatalf("got %q, want %q", val, "hello")
	}
}

func TestGetMissingReturnsNil(t *testing.T) {
	c, _ := newTestBackend(t)
	val, err := c.Get(context.Background(), "nope")
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		t.Fatalf("got %q, want nil", val)
	}
}

func TestSetWithTTL(t *testing.T) {
	c, mr := newTestBackend(t)
	ctx := context.Background()
	if err := c.Set(ctx, "k_ttl", "v", 60*time.Second); err != nil {
		t.Fatal(err)
	}
	val, err := c.Get(ctx, "k_ttl")
	if err != nil || string(val) != "v" {
		t.Fatalf("got %q, %v", val, err)
	}
	// After the TTL elapses the key is gone.
	mr.FastForward(61 * time.Second)
	val, err = c.Get(ctx, "k_ttl")
	if err != nil || val != nil {
		t.Fatalf("expected expired key, got %q, %v", val, err)
	}
}

func TestDelete(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()
	_ = c.Set(ctx, "del_me", "x", 0)
	removed, err := c.Delete(ctx, "del_me")
	if err != nil || removed != 1 {
		t.Fatalf("removed = %d, err = %v", removed, err)
	}
	if val, _ := c.Get(ctx, "del_me"); val != nil {
		t.Fatal("key still present after delete")
	}
}

func TestDeleteMultiple(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()
	_ = c.Set(ctx, "a", "1", 0)
	_ = c.Set(ctx, "b", "2", 0)
	removed, err := c.Delete(ctx, "a", "b", "missing")
	if err != nil || removed != 2 {
		t.Fatalf("removed = %d, err = %v", removed, err)
	}
}

func TestExists(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()
	if ok, _ := c.Exists(ctx, "ghost"); ok {
		t.Fatal("ghost should not exist")
	}
	_ = c.Set(ctx, "ghost", "boo", 0)
	if ok, _ := c.Exists(ctx, "ghost"); !ok {
		t.Fatal("ghost should exist")
	}
}

// ---------------------------------------------------------------------------
// expire / ttl
// ---------------------------------------------------------------------------

func TestExpireAndTTL(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()
	_ = c.Set(ctx, "k", "v", 0)

	if d, err := c.TTL(ctx, "k"); err != nil || d != TTLNoExpiry {
		t.Fatalf("TTL = %v, err = %v; want TTLNoExpiry", d, err)
	}
	ok, err := c.Expire(ctx, "k", 100*time.Second, ExpireAlways)
	if err != nil || !ok {
		t.Fatalf("Expire = %v, err = %v", ok, err)
	}
	if d, err := c.TTL(ctx, "k"); err != nil || d <= 0 || d > 100*time.Second {
		t.Fatalf("TTL = %v, err = %v; want (0, 100s]", d, err)
	}
	if d, err := c.TTL(ctx, "missing"); err != nil || d != TTLKeyMissing {
		t.Fatalf("TTL(missing) = %v, err = %v; want TTLKeyMissing", d, err)
	}
}

func TestExpireNXAndXX(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()
	_ = c.Set(ctx, "k", "v", 0)

	// XX on a key without TTL fails; NX succeeds.
	if ok, err := c.Expire(ctx, "k", time.Minute, ExpireXX); err != nil || ok {
		t.Fatalf("ExpireXX on no-TTL key = %v, err = %v; want false", ok, err)
	}
	if ok, err := c.Expire(ctx, "k", time.Minute, ExpireNX); err != nil || !ok {
		t.Fatalf("ExpireNX on no-TTL key = %v, err = %v; want true", ok, err)
	}
	// Now the key has a TTL: NX fails; XX succeeds.
	if ok, err := c.Expire(ctx, "k", time.Hour, ExpireNX); err != nil || ok {
		t.Fatalf("ExpireNX on TTL key = %v, err = %v; want false", ok, err)
	}
	if ok, err := c.Expire(ctx, "k", time.Hour, ExpireXX); err != nil || !ok {
		t.Fatalf("ExpireXX on TTL key = %v, err = %v; want true", ok, err)
	}
}

// ---------------------------------------------------------------------------
// hashes
// ---------------------------------------------------------------------------

func TestHashOperations(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()

	n, err := c.HSet(ctx, "user:1", "name", "Alice")
	if err != nil || n != 1 {
		t.Fatalf("HSet new = %d, err = %v", n, err)
	}
	n, err = c.HSet(ctx, "user:1", "name", "Bob")
	if err != nil || n != 0 {
		t.Fatalf("HSet update = %d, err = %v", n, err)
	}
	val, err := c.HGet(ctx, "user:1", "name")
	if err != nil || string(val) != "Bob" {
		t.Fatalf("HGet = %q, err = %v", val, err)
	}
	if val, err := c.HGet(ctx, "user:1", "missing"); err != nil || val != nil {
		t.Fatalf("HGet missing = %q, err = %v; want nil", val, err)
	}
	_, _ = c.HSet(ctx, "user:1", "age", "30")
	all, err := c.HGetAll(ctx, "user:1")
	if err != nil || len(all) != 2 || all["name"] != "Bob" || all["age"] != "30" {
		t.Fatalf("HGetAll = %v, err = %v", all, err)
	}
	removed, err := c.HDel(ctx, "user:1", "name", "missing")
	if err != nil || removed != 1 {
		t.Fatalf("HDel = %d, err = %v", removed, err)
	}
}

// ---------------------------------------------------------------------------
// sets
// ---------------------------------------------------------------------------

func TestSetOperations(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()

	added, err := c.SAdd(ctx, "s", "a", "b", "c")
	if err != nil || added != 3 {
		t.Fatalf("SAdd = %d, err = %v", added, err)
	}
	// Re-adding an existing member reports only the newly added count.
	added, err = c.SAdd(ctx, "s", "a", "d")
	if err != nil || added != 1 {
		t.Fatalf("SAdd dedup = %d, err = %v", added, err)
	}
	n, err := c.SCard(ctx, "s")
	if err != nil || n != 4 {
		t.Fatalf("SCard = %d, err = %v", n, err)
	}
	if ok, _ := c.SIsMember(ctx, "s", "a"); !ok {
		t.Fatal("a should be a member")
	}
	if ok, _ := c.SIsMember(ctx, "s", "z"); ok {
		t.Fatal("z should not be a member")
	}
	members, err := c.SMembers(ctx, "s")
	if err != nil || len(members) != 4 {
		t.Fatalf("SMembers = %v, err = %v", members, err)
	}
	removed, err := c.SRem(ctx, "s", "a", "z")
	if err != nil || removed != 1 {
		t.Fatalf("SRem = %d, err = %v", removed, err)
	}
}

func TestSInter(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()

	_, _ = c.SAdd(ctx, "s1", "a", "b", "c")
	_, _ = c.SAdd(ctx, "s2", "b", "c", "d")
	common, err := c.SInter(ctx, "s1", "s2")
	if err != nil || len(common) != 2 {
		t.Fatalf("SInter = %v, err = %v", common, err)
	}
	// A missing key yields an empty intersection.
	common, err = c.SInter(ctx, "s1", "missing")
	if err != nil || len(common) != 0 {
		t.Fatalf("SInter with missing = %v, err = %v", common, err)
	}
	// Single key behaves like SMembers.
	common, err = c.SInter(ctx, "s1")
	if err != nil || len(common) != 3 {
		t.Fatalf("SInter single = %v, err = %v", common, err)
	}
	// Zero keys is an error.
	if _, err := c.SInter(ctx); !errors.Is(err, core.ErrCache) {
		t.Fatalf("SInter() err = %v; want core.ErrCache", err)
	}
}

// ---------------------------------------------------------------------------
// lists
// ---------------------------------------------------------------------------

func TestListOperations(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()

	n, err := c.LPush(ctx, "jobs", "job-2", "job-1")
	if err != nil || n != 2 {
		t.Fatalf("LPush = %d, err = %v", n, err)
	}
	n, err = c.RPush(ctx, "jobs", "job-3")
	if err != nil || n != 3 {
		t.Fatalf("RPush = %d, err = %v", n, err)
	}
	batch, err := c.LRange(ctx, "jobs", 0, 99)
	if err != nil || len(batch) != 3 {
		t.Fatalf("LRange = %v, err = %v", batch, err)
	}
	if batch[0] != "job-1" || batch[2] != "job-3" {
		t.Fatalf("unexpected order: %v", batch)
	}
	if n, _ := c.LLen(ctx, "jobs"); n != 3 {
		t.Fatalf("LLen = %d", n)
	}
}

// ---------------------------------------------------------------------------
// counters / mget / mset
// ---------------------------------------------------------------------------

func TestIncrDecr(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()

	if n, err := c.Incr(ctx, "hits"); err != nil || n != 1 {
		t.Fatalf("Incr = %d, err = %v", n, err)
	}
	if n, err := c.Incr(ctx, "hits"); err != nil || n != 2 {
		t.Fatalf("Incr = %d, err = %v", n, err)
	}
	if n, err := c.Decr(ctx, "hits"); err != nil || n != 1 {
		t.Fatalf("Decr = %d, err = %v", n, err)
	}
}

func TestMGetMSet(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()

	if err := c.MSet(ctx, map[string]any{"a": "1", "b": "2"}); err != nil {
		t.Fatal(err)
	}
	vals, err := c.MGet(ctx, "a", "b", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 3 || string(vals[0]) != "1" || string(vals[1]) != "2" || vals[2] != nil {
		t.Fatalf("MGet = %v", vals)
	}
}

// ---------------------------------------------------------------------------
// keys / flush / ping / pipeline
// ---------------------------------------------------------------------------

func TestKeysAndFlush(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()

	_ = c.Set(ctx, "user:1", "a", 0)
	_ = c.Set(ctx, "user:2", "b", 0)
	_ = c.Set(ctx, "other", "c", 0)
	keys, err := c.Keys(ctx, "user:*")
	if err != nil || len(keys) != 2 {
		t.Fatalf("Keys = %v, err = %v", keys, err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	keys, _ = c.Keys(ctx, "*")
	if len(keys) != 0 {
		t.Fatalf("Keys after flush = %v", keys)
	}
}

func TestPingAndHealthCheck(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()
	if ok, err := c.Ping(ctx); err != nil || !ok {
		t.Fatalf("Ping = %v, err = %v", ok, err)
	}
	if !c.HealthCheck(ctx) {
		t.Fatal("HealthCheck should be true")
	}
}

func TestPipeline(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()

	err := c.Pipeline(ctx, func(p Pipeliner) {
		p.SAdd("dau:2026-06-10", "user-1", "user-2")
		p.Expire("dau:2026-06-10", 24*time.Hour)
		p.Set("k", "v", 0)
		p.Incr("counter")
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := c.SCard(ctx, "dau:2026-06-10"); n != 2 {
		t.Fatalf("SCard = %d", n)
	}
	if d, _ := c.TTL(ctx, "dau:2026-06-10"); d <= 0 {
		t.Fatalf("TTL = %v", d)
	}
	if v, _ := c.Get(ctx, "k"); string(v) != "v" {
		t.Fatalf("Get = %q", v)
	}
	if n, _ := c.Incr(ctx, "counter"); n != 2 {
		t.Fatalf("counter = %d", n)
	}
}

func TestSetEx(t *testing.T) {
	c, _ := newTestBackend(t)
	ctx := context.Background()
	if err := c.SetEx(ctx, "k", "v", time.Minute); err != nil {
		t.Fatal(err)
	}
	if d, _ := c.TTL(ctx, "k"); d <= 0 {
		t.Fatalf("TTL = %v; want positive", d)
	}
}

// ---------------------------------------------------------------------------
// factory
// ---------------------------------------------------------------------------

func TestNewUnknownProvider(t *testing.T) {
	_, err := New(context.Background(), "memcached", "from_url", Config{})
	if !errors.Is(err, core.ErrCache) {
		t.Fatalf("err = %v; want core.ErrCache", err)
	}
}

func TestNewUnknownAuthMethod(t *testing.T) {
	_, err := New(context.Background(), "redis", "from_magic", Config{})
	if !errors.Is(err, core.ErrCache) {
		t.Fatalf("err = %v; want core.ErrCache", err)
	}
}

func TestNewStandaloneFromURLAgainstMiniredis(t *testing.T) {
	mr := miniredis.RunT(t)
	b, err := New(context.Background(), "redis", "from_url", Config{URL: "redis://" + mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	if ok, err := b.Ping(context.Background()); err != nil || !ok {
		t.Fatalf("Ping = %v, err = %v", ok, err)
	}
}

func TestNewStandaloneFromCredentialsAgainstMiniredis(t *testing.T) {
	mr := miniredis.RunT(t)
	host, portStr, err := net.SplitHostPort(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(context.Background(), "redis", "from_credentials", Config{Host: host, Port: port})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	if ok, err := b.Ping(context.Background()); err != nil || !ok {
		t.Fatalf("Ping = %v, err = %v", ok, err)
	}
}
