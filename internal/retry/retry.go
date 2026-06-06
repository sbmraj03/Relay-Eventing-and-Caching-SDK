// Package retry provides context-aware exponential backoff with full jitter.
package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Config controls retry behaviour.
type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64 // e.g. 2.0 for doubling
}

// DefaultConfig is a sensible starting point.
var DefaultConfig = Config{
	MaxAttempts: 3,
	BaseDelay:   100 * time.Millisecond,
	MaxDelay:    10 * time.Second,
	Multiplier:  2.0,
}

// Do calls fn up to cfg.MaxAttempts times with exponential backoff + full jitter.
// Returns nil on the first success. Respects ctx cancellation between attempts.
func Do(ctx context.Context, cfg Config, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if attempt == cfg.MaxAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("retry cancelled: %w", ctx.Err())
		case <-time.After(jitteredDelay(cfg, attempt)):
		}
	}
	return fmt.Errorf("after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

// jitteredDelay computes the backoff for a given attempt using "full jitter":
// a uniform random value in [0, min(BaseDelay * Multiplier^attempt, MaxDelay)].
// Full jitter spreads load across retrying callers better than fixed backoff.
func jitteredDelay(cfg Config, attempt int) time.Duration {
	ceiling := float64(cfg.BaseDelay) * math.Pow(cfg.Multiplier, float64(attempt))
	if ceiling > float64(cfg.MaxDelay) {
		ceiling = float64(cfg.MaxDelay)
	}
	return time.Duration(rand.Float64() * ceiling)
}
