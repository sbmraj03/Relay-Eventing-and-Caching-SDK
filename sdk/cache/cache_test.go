package cache_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/cache"
)

// newTestCache spins up an in-memory Redis server and returns a Cache backed by it.
// miniredis lets us manipulate time (FastForward) to test TTL expiry without real sleeps.
func newTestCache(t *testing.T) (*cache.Cache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return cache.New(client), mr
}

func TestGetOrLoad_CacheHit(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := context.Background()

	_ = mr.Set("user:1", `{"name":"alice"}`)

	loadCalls := 0
	val, err := c.GetOrLoad(ctx, "user:1", time.Minute, func(_ context.Context) ([]byte, error) {
		loadCalls++
		return []byte(`{"name":"from-db"}`), nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != `{"name":"alice"}` {
		t.Fatalf("expected cached value, got %s", val)
	}
	if loadCalls != 0 {
		t.Fatal("load should not be called on a cache hit")
	}
}

func TestGetOrLoad_CacheMiss_InvokesLoader(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	loadCalls := 0
	val, err := c.GetOrLoad(ctx, "user:99", time.Minute, func(_ context.Context) ([]byte, error) {
		loadCalls++
		return []byte(`{"name":"bob"}`), nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != `{"name":"bob"}` {
		t.Fatalf("unexpected value: %s", val)
	}
	if loadCalls != 1 {
		t.Fatalf("expected load called once, got %d", loadCalls)
	}
}

func TestGetOrLoad_LoadedValueIsStoredInRedis(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := context.Background()

	_, _ = c.GetOrLoad(ctx, "user:5", time.Minute, func(_ context.Context) ([]byte, error) {
		return []byte(`{"name":"carol"}`), nil
	})

	stored, err := mr.Get("user:5")
	if err != nil {
		t.Fatalf("expected key in Redis after load, got error: %v", err)
	}
	if stored != `{"name":"carol"}` {
		t.Fatalf("stored value mismatch: %s", stored)
	}
}

func TestGetOrLoad_TTLExpiry_ReloadsAfterExpiry(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := context.Background()

	loadCalls := 0
	load := func(_ context.Context) ([]byte, error) {
		loadCalls++
		return []byte(`{"v":"fresh"}`), nil
	}

	_, _ = c.GetOrLoad(ctx, "ttl-key", 100*time.Millisecond, load)
	if loadCalls != 1 {
		t.Fatalf("expected 1 load, got %d", loadCalls)
	}

	mr.FastForward(200 * time.Millisecond) // expire the key

	_, _ = c.GetOrLoad(ctx, "ttl-key", 100*time.Millisecond, load)
	if loadCalls != 2 {
		t.Fatalf("expected 2 loads after expiry, got %d", loadCalls)
	}
}

func TestGetOrLoad_SingleflightDeduplication(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	var loadCalls atomic.Int64
	// load has a deliberate delay so concurrent goroutines are still in-flight
	load := func(_ context.Context) ([]byte, error) {
		loadCalls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return []byte(`{"name":"shared"}`), nil
	}

	const goroutines = 10
	var wg sync.WaitGroup
	results := make([][]byte, goroutines)
	errs := make([]error, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = c.GetOrLoad(ctx, "shared-key", time.Minute, load)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: unexpected error: %v", i, err)
		}
	}
	for i, v := range results {
		if string(v) != `{"name":"shared"}` {
			t.Errorf("goroutine %d: unexpected value: %s", i, v)
		}
	}
	// The key guarantee of singleflight: only ONE call to load, regardless
	// of how many concurrent goroutines raced on the same key.
	if n := loadCalls.Load(); n != 1 {
		t.Fatalf("singleflight broken: load called %d times, expected 1", n)
	}
}

func TestGetOrLoad_LoaderError_PropagatedToCallers(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	boom := errors.New("db down")
	_, err := c.GetOrLoad(ctx, "bad-key", time.Minute, func(_ context.Context) ([]byte, error) {
		return nil, boom
	})
	if err == nil {
		t.Fatal("expected error from loader to propagate")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom in error chain, got: %v", err)
	}
}

func TestSet_WritesValueAndRetrievedOnHit(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	if err := c.Set(ctx, "prefs:1", []byte(`{"theme":"dark"}`), time.Minute); err != nil {
		t.Fatalf("Set error: %v", err)
	}

	loadCalls := 0
	val, err := c.GetOrLoad(ctx, "prefs:1", time.Minute, func(_ context.Context) ([]byte, error) {
		loadCalls++
		return nil, nil
	})
	if err != nil {
		t.Fatalf("GetOrLoad error: %v", err)
	}
	if string(val) != `{"theme":"dark"}` {
		t.Fatalf("unexpected value: %s", val)
	}
	if loadCalls != 0 {
		t.Fatal("Set value should be returned as a cache hit")
	}
}

func TestDelete_RemovesKey(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := context.Background()

	_ = mr.Set("session:1", "token")
	if err := c.Delete(ctx, "session:1"); err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	if mr.Exists("session:1") {
		t.Fatal("key should be gone after Delete")
	}
}
