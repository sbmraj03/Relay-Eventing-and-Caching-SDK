package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/internal/retry"
)

var errBoom = errors.New("boom")

func TestDo_SuccessOnFirstAttempt(t *testing.T) {
	calls := 0
	err := retry.Do(context.Background(), retry.DefaultConfig, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestDo_RetriesOnTransientFailure(t *testing.T) {
	calls := 0
	cfg := fastCfg(3)
	err := retry.Do(context.Background(), cfg, func() error {
		calls++
		if calls < 3 {
			return errBoom
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil after eventual success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestDo_ExhaustsAllAttempts(t *testing.T) {
	calls := 0
	cfg := fastCfg(4)
	err := retry.Do(context.Background(), cfg, func() error {
		calls++
		return errBoom
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
	// The original error must be unwrappable.
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected wrapped errBoom, got %v", err)
	}
}

func TestDo_ContextCancelledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// cfg with long delays so the test doesn't time out waiting for real backoffs
	cfg := retry.Config{
		MaxAttempts: 10,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    time.Second,
		Multiplier:  2.0,
	}
	calls := 0
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err := retry.Do(ctx, cfg, func() error {
		calls++
		return errBoom
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Must have stopped well before the maximum attempts
	if calls >= 10 {
		t.Fatalf("ctx cancellation had no effect; calls = %d", calls)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled in error chain, got %v", err)
	}
}

// fastCfg returns a Config with tiny delays so tests don't take long.
func fastCfg(maxAttempts int) retry.Config {
	return retry.Config{
		MaxAttempts: maxAttempts,
		BaseDelay:   time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
		Multiplier:  2.0,
	}
}
