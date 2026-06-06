// Package cache provides a Redis-backed read-through/write-through cache
// with singleflight stampede protection, Prometheus metrics, and OTel tracing.
package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/telemetry"
)

// ErrNotFound is returned by GetOrLoad when the loader returns no data.
var ErrNotFound = errors.New("cache: not found")

// LoadFunc is called on a cache miss to fetch the canonical value.
type LoadFunc func(ctx context.Context) ([]byte, error)

// Option is a functional option for New.
type Option func(*Cache)

// WithMetrics enables Prometheus instrumentation.
func WithMetrics(m *telemetry.CacheMetrics) Option {
	return func(c *Cache) { c.metrics = m }
}

// Cache wraps a Redis client with read-through / write-through helpers.
type Cache struct {
	client  redis.Cmdable
	group   singleflight.Group
	metrics *telemetry.CacheMetrics // nil = no instrumentation
}

// New creates a Cache backed by the given Redis client.
// Optional functional options (e.g. WithMetrics) configure additional features.
func New(client redis.Cmdable, opts ...Option) *Cache {
	c := &Cache{client: client}
	for _, o := range opts {
		o(c)
	}
	return c
}

// GetOrLoad implements the read-through pattern with stampede protection.
//
// Flow:
//  1. GET from Redis → hit: return cached value.
//  2. Miss: singleflight ensures only ONE goroutine calls load() for a given key.
//  3. Store result with SET key value EX ttl.
//  4. Return value.
func (c *Cache) GetOrLoad(ctx context.Context, key string, ttl time.Duration, load LoadFunc) ([]byte, error) {
	// Fast path: cache hit.
	val, err := c.client.Get(ctx, key).Bytes()
	if err == nil {
		if c.metrics != nil {
			c.metrics.OpsTotal.WithLabelValues("hit").Inc()
		}
		return val, nil
	}
	if !errors.Is(err, redis.Nil) {
		if c.metrics != nil {
			c.metrics.OpsTotal.WithLabelValues("error").Inc()
		}
		return nil, fmt.Errorf("cache: redis get: %w", err)
	}

	// Slow path: cache miss — singleflight deduplicates concurrent loaders.
	if c.metrics != nil {
		c.metrics.OpsTotal.WithLabelValues("miss").Inc()
	}

	result, loadErr, _ := c.group.Do(key, func() (any, error) {
		start := time.Now()
		data, err := load(ctx)
		if c.metrics != nil {
			c.metrics.LoadDuration.WithLabelValues().Observe(time.Since(start).Seconds())
		}
		if err != nil {
			return nil, fmt.Errorf("cache: load: %w", err)
		}
		if data == nil {
			return nil, ErrNotFound
		}
		if setErr := c.client.Set(ctx, key, data, ttl).Err(); setErr != nil {
			return nil, fmt.Errorf("cache: redis set: %w", setErr)
		}
		if c.metrics != nil {
			c.metrics.OpsTotal.WithLabelValues("set").Inc()
		}
		return data, nil
	})
	if loadErr != nil {
		return nil, loadErr
	}
	return result.([]byte), nil
}

// Set writes a value directly to Redis (write-through).
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := c.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("cache: set: %w", err)
	}
	return nil
}

// Delete removes a key from Redis.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("cache: delete: %w", err)
	}
	return nil
}
