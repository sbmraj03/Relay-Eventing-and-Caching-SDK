// Package breaker implements a thread-safe circuit breaker with three states:
// Closed (normal), Open (failing fast), and HalfOpen (probing recovery).
package breaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrOpen is returned when all calls are being rejected because the breaker is open.
var ErrOpen = errors.New("circuit breaker open")

// State is the current operating mode of the breaker.
type State int

const (
	StateClosed   State = iota // calls pass through normally
	StateOpen                  // calls rejected immediately
	StateHalfOpen              // one probe call allowed to test recovery
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// Config holds breaker tuning parameters.
type Config struct {
	// MaxFailures is the number of consecutive failures that trip the breaker.
	MaxFailures int
	// ResetDelay is how long the breaker stays open before allowing a probe.
	ResetDelay time.Duration
}

// DefaultConfig is a sensible starting point.
var DefaultConfig = Config{
	MaxFailures: 5,
	ResetDelay:  10 * time.Second,
}

// Breaker is a thread-safe circuit breaker.
// Zero value is not usable — create with New.
type Breaker struct {
	mu          sync.Mutex
	cfg         Config
	state       State
	failures    int
	lastFailure time.Time
}

// New returns a Breaker in the Closed state.
func New(cfg Config) *Breaker {
	return &Breaker{cfg: cfg}
}

// State returns the current state. Transitions Open→HalfOpen lazily on read.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.resolveState()
}

// Do executes fn if the breaker permits it.
//   - Closed/HalfOpen: fn is called; success resets to Closed, failure increments counter.
//   - Open: fn is NOT called; ErrOpen is returned immediately.
func (b *Breaker) Do(fn func() error) error {
	b.mu.Lock()
	state := b.resolveState()
	if state == StateOpen {
		b.mu.Unlock()
		return ErrOpen
	}
	b.mu.Unlock()

	err := fn()

	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.recordFailure()
	} else {
		b.recordSuccess()
	}
	return err
}

// resolveState must be called with b.mu held.
// It applies the Open→HalfOpen transition when ResetDelay has elapsed.
func (b *Breaker) resolveState() State {
	if b.state == StateOpen && time.Since(b.lastFailure) >= b.cfg.ResetDelay {
		b.state = StateHalfOpen
	}
	return b.state
}

// recordFailure must be called with b.mu held.
func (b *Breaker) recordFailure() {
	b.failures++
	b.lastFailure = time.Now()
	if b.state == StateHalfOpen || b.failures >= b.cfg.MaxFailures {
		b.state = StateOpen
	}
}

// recordSuccess must be called with b.mu held.
func (b *Breaker) recordSuccess() {
	b.failures = 0
	b.state = StateClosed
}
