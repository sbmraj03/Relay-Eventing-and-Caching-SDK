package breaker_test

import (
	"errors"
	"testing"
	"time"

	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/sdk/breaker"
)

var errBoom = errors.New("boom")

func TestBreaker_StartsInClosedState(t *testing.T) {
	b := breaker.New(breaker.DefaultConfig)
	if got := b.State(); got != breaker.StateClosed {
		t.Fatalf("expected closed, got %s", got)
	}
}

func TestBreaker_PassesCallsWhenClosed(t *testing.T) {
	b := breaker.New(breaker.DefaultConfig)
	called := false
	err := b.Do(func() error { called = true; return nil })
	if err != nil || !called {
		t.Fatalf("expected fn to be called and return nil; err=%v called=%v", err, called)
	}
}

func TestBreaker_OpensAfterMaxFailures(t *testing.T) {
	cfg := breaker.Config{MaxFailures: 3, ResetDelay: time.Minute}
	b := breaker.New(cfg)

	for i := 0; i < 3; i++ {
		_ = b.Do(func() error { return errBoom })
	}

	if got := b.State(); got != breaker.StateOpen {
		t.Fatalf("expected open after 3 failures, got %s", got)
	}
}

func TestBreaker_RejectsFnWhenOpen(t *testing.T) {
	cfg := breaker.Config{MaxFailures: 1, ResetDelay: time.Minute}
	b := breaker.New(cfg)
	_ = b.Do(func() error { return errBoom }) // trip it

	called := false
	err := b.Do(func() error { called = true; return nil })

	if !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("expected ErrOpen, got %v", err)
	}
	if called {
		t.Fatal("fn must not be called when breaker is open")
	}
}

func TestBreaker_TransitionsToHalfOpenAfterResetDelay(t *testing.T) {
	cfg := breaker.Config{MaxFailures: 1, ResetDelay: 20 * time.Millisecond}
	b := breaker.New(cfg)
	_ = b.Do(func() error { return errBoom })

	if got := b.State(); got != breaker.StateOpen {
		t.Fatalf("expected open, got %s", got)
	}

	time.Sleep(30 * time.Millisecond)

	if got := b.State(); got != breaker.StateHalfOpen {
		t.Fatalf("expected half-open after reset delay, got %s", got)
	}
}

func TestBreaker_ClosesAfterSuccessfulProbe(t *testing.T) {
	cfg := breaker.Config{MaxFailures: 1, ResetDelay: 20 * time.Millisecond}
	b := breaker.New(cfg)
	_ = b.Do(func() error { return errBoom })
	time.Sleep(30 * time.Millisecond) // → half-open

	_ = b.Do(func() error { return nil }) // probe succeeds

	if got := b.State(); got != breaker.StateClosed {
		t.Fatalf("expected closed after successful probe, got %s", got)
	}
}

func TestBreaker_ReOpensAfterFailedProbe(t *testing.T) {
	cfg := breaker.Config{MaxFailures: 1, ResetDelay: 20 * time.Millisecond}
	b := breaker.New(cfg)
	_ = b.Do(func() error { return errBoom })
	time.Sleep(30 * time.Millisecond) // → half-open

	_ = b.Do(func() error { return errBoom }) // probe fails

	if got := b.State(); got != breaker.StateOpen {
		t.Fatalf("expected open after failed probe, got %s", got)
	}
}

func TestBreaker_SuccessResetsFailureCount(t *testing.T) {
	cfg := breaker.Config{MaxFailures: 3, ResetDelay: time.Minute}
	b := breaker.New(cfg)

	_ = b.Do(func() error { return errBoom }) // 1 failure
	_ = b.Do(func() error { return errBoom }) // 2 failures
	_ = b.Do(func() error { return nil })     // success — resets count

	// 2 more failures should NOT open it (counter was reset to 0)
	_ = b.Do(func() error { return errBoom })
	_ = b.Do(func() error { return errBoom })

	if got := b.State(); got != breaker.StateClosed {
		t.Fatalf("expected closed (failures reset), got %s", got)
	}
}
