package producer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/internal/retry"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/breaker"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/producer"
)

// fakeWriter records what was written and returns a configurable error sequence.
type fakeWriter struct {
	written []kafka.Message
	errs    []error // errs[i] is returned on the i-th call; nil after exhaustion
	closed  bool
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	idx := len(f.written)
	f.written = append(f.written, msgs...)
	if idx < len(f.errs) {
		return f.errs[idx]
	}
	return nil
}

func (f *fakeWriter) Close() error {
	f.closed = true
	return nil
}

// cfg returns a test config with instant retries and a wide-open breaker.
func cfg(maxRetries int) producer.Config {
	return producer.Config{
		Brokers: []string{"localhost:9092"},
		Retry: retry.Config{
			MaxAttempts: maxRetries,
			BaseDelay:   time.Millisecond,
			MaxDelay:    5 * time.Millisecond,
			Multiplier:  2.0,
		},
		Breaker: breaker.Config{MaxFailures: 100, ResetDelay: time.Minute},
	}
}

var validMsg = producer.Message{
	Topic: "orders",
	Value: []byte(`{"id":"1"}`),
}

func TestPublish_SuccessOnFirstAttempt(t *testing.T) {
	fw := &fakeWriter{}
	p := producer.New(cfg(3), fw)

	if err := p.Publish(context.Background(), validMsg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fw.written) != 1 {
		t.Fatalf("expected 1 written message, got %d", len(fw.written))
	}
}

func TestPublish_RetriesOnTransientWriterError(t *testing.T) {
	boom := errors.New("broker unavailable")
	fw := &fakeWriter{errs: []error{boom, boom, nil}} // fails twice, then succeeds
	p := producer.New(cfg(3), fw)

	if err := p.Publish(context.Background(), validMsg); err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if len(fw.written) != 3 {
		t.Fatalf("expected 3 write attempts, got %d", len(fw.written))
	}
}

func TestPublish_FailsAfterMaxRetries(t *testing.T) {
	boom := errors.New("broker unavailable")
	fw := &fakeWriter{errs: []error{boom, boom, boom}}
	p := producer.New(cfg(3), fw)

	err := p.Publish(context.Background(), validMsg)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
}

func TestPublish_SetsIdempotencyKeyHeader(t *testing.T) {
	fw := &fakeWriter{}
	p := producer.New(cfg(1), fw)
	msg := producer.Message{Topic: "orders", Value: []byte(`{}`), IdempotencyKey: "order-42"}

	_ = p.Publish(context.Background(), msg)

	if len(fw.written) == 0 {
		t.Fatal("no message written")
	}
	var found bool
	for _, h := range fw.written[0].Headers {
		if h.Key == "x-idempotency-key" && string(h.Value) == "order-42" {
			found = true
		}
	}
	if !found {
		t.Fatal("x-idempotency-key header not set correctly")
	}
}

func TestPublish_AutoGeneratesIdempotencyKey(t *testing.T) {
	fw := &fakeWriter{}
	p := producer.New(cfg(1), fw)
	_ = p.Publish(context.Background(), validMsg) // no IdempotencyKey set

	if len(fw.written) == 0 {
		t.Fatal("no message written")
	}
	var key string
	for _, h := range fw.written[0].Headers {
		if h.Key == "x-idempotency-key" {
			key = string(h.Value)
		}
	}
	if key == "" {
		t.Fatal("auto-generated idempotency key is empty")
	}
}

func TestPublish_ValidationRejectsEmptyTopic(t *testing.T) {
	p := producer.New(cfg(1), &fakeWriter{})
	err := p.Publish(context.Background(), producer.Message{Value: []byte(`{}`)})
	if err == nil {
		t.Fatal("expected validation error for empty topic")
	}
}

func TestPublish_ValidationRejectsEmptyValue(t *testing.T) {
	p := producer.New(cfg(1), &fakeWriter{})
	err := p.Publish(context.Background(), producer.Message{Topic: "t"})
	if err == nil {
		t.Fatal("expected validation error for empty value")
	}
}

func TestPublish_ValidationRejectsNonJSON(t *testing.T) {
	p := producer.New(cfg(1), &fakeWriter{})
	err := p.Publish(context.Background(), producer.Message{
		Topic: "t", Value: []byte("not-json"),
	})
	if err == nil {
		t.Fatal("expected validation error for non-JSON value")
	}
}

func TestPublish_CircuitBreakerTripsAfterMaxFailures(t *testing.T) {
	boom := errors.New("broker down")
	// Breaker trips after 2 failures; each Publish makes 1 retry attempt
	brkCfg := breaker.Config{MaxFailures: 2, ResetDelay: time.Minute}
	retryCfg := retry.Config{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1}
	fw := &fakeWriter{errs: []error{boom, boom, boom, boom, boom}}
	p := producer.New(producer.Config{Brokers: nil, Retry: retryCfg, Breaker: brkCfg}, fw)

	_ = p.Publish(context.Background(), validMsg) // failure 1
	_ = p.Publish(context.Background(), validMsg) // failure 2 → trips breaker

	err := p.Publish(context.Background(), validMsg) // should be rejected by breaker
	if !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("expected ErrOpen, got %v", err)
	}
}
