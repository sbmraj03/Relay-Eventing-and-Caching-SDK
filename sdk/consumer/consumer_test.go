package consumer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/internal/retry"
	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/sdk/consumer"
)

// --- test doubles ------------------------------------------------------------

type fakeReader struct {
	messages  []kafka.Message
	idx       int
	committed []kafka.Message
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.idx < len(f.messages) {
		msg := f.messages[f.idx]
		f.idx++
		return msg, nil
	}
	// Block until context is cancelled — simulates a reader with no more messages.
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.committed = append(f.committed, msgs...)
	return nil
}

func (f *fakeReader) Close() error { return nil }

type fakeDLQ struct {
	written []kafka.Message
}

func (f *fakeDLQ) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.written = append(f.written, msgs...)
	return nil
}

// --- helpers -----------------------------------------------------------------

func fastCfg() consumer.Config {
	return consumer.Config{
		Topic:    "orders",
		GroupID:  "test",
		DLQTopic: "orders.dlq",
		Retry: retry.Config{
			MaxAttempts: 3,
			BaseDelay:   time.Millisecond,
			MaxDelay:    5 * time.Millisecond,
			Multiplier:  2.0,
		},
	}
}

func singleMessage() kafka.Message {
	return kafka.Message{Topic: "orders", Value: []byte(`{"id":"1"}`), Offset: 42}
}

// runUntilDone starts Run in a goroutine, cancels the context after all
// pre-loaded messages are delivered, and waits for Run to return.
func runUntilDone(t *testing.T, c *consumer.Consumer, reader *fakeReader, msgCount int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx)
	}()

	// Wait until all messages have been fetched, then shut down.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if reader.idx >= msgCount {
			time.Sleep(20 * time.Millisecond) // let the last commit happen
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

// --- tests -------------------------------------------------------------------

func TestConsumer_CallsHandlerAndCommitsOnSuccess(t *testing.T) {
	msg := singleMessage()
	reader := &fakeReader{messages: []kafka.Message{msg}}
	dlq := &fakeDLQ{}

	handled := 0
	h := consumer.HandlerFunc(func(_ context.Context, _ kafka.Message) error {
		handled++
		return nil
	})

	c := consumer.New(fastCfg(), reader, dlq, h)
	runUntilDone(t, c, reader, 1)

	if handled != 1 {
		t.Fatalf("expected handler called once, got %d", handled)
	}
	if len(reader.committed) != 1 {
		t.Fatalf("expected 1 committed message, got %d", len(reader.committed))
	}
	if len(dlq.written) != 0 {
		t.Fatalf("expected no DLQ messages, got %d", len(dlq.written))
	}
}

func TestConsumer_RetriesHandlerOnTransientError(t *testing.T) {
	msg := singleMessage()
	reader := &fakeReader{messages: []kafka.Message{msg}}
	dlq := &fakeDLQ{}

	calls := 0
	h := consumer.HandlerFunc(func(_ context.Context, _ kafka.Message) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})

	c := consumer.New(fastCfg(), reader, dlq, h)
	runUntilDone(t, c, reader, 1)

	if calls != 3 {
		t.Fatalf("expected 3 handler calls, got %d", calls)
	}
	if len(dlq.written) != 0 {
		t.Fatal("message should NOT go to DLQ when handler eventually succeeds")
	}
	if len(reader.committed) != 1 {
		t.Fatalf("expected message committed after eventual success, got %d", len(reader.committed))
	}
}

func TestConsumer_SendsPoisonMessageToDLQ(t *testing.T) {
	msg := singleMessage()
	reader := &fakeReader{messages: []kafka.Message{msg}}
	dlq := &fakeDLQ{}

	h := consumer.HandlerFunc(func(_ context.Context, _ kafka.Message) error {
		return errors.New("always fails")
	})

	c := consumer.New(fastCfg(), reader, dlq, h)
	runUntilDone(t, c, reader, 1)

	if len(dlq.written) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(dlq.written))
	}
	// Original value must be preserved in the DLQ message.
	if string(dlq.written[0].Value) != string(msg.Value) {
		t.Fatalf("DLQ message value mismatch: got %s", dlq.written[0].Value)
	}
	// DLQ reason header must be present.
	var reasonFound bool
	for _, h := range dlq.written[0].Headers {
		if h.Key == "x-dlq-reason" {
			reasonFound = true
		}
	}
	if !reasonFound {
		t.Fatal("x-dlq-reason header missing from DLQ message")
	}
	// Offset still committed so processing can continue past poison message.
	if len(reader.committed) != 1 {
		t.Fatalf("expected offset committed even for poison message, got %d", len(reader.committed))
	}
}

func TestConsumer_ProcessesMultipleMessages(t *testing.T) {
	msgs := []kafka.Message{
		{Topic: "orders", Value: []byte(`{"id":"1"}`), Offset: 1},
		{Topic: "orders", Value: []byte(`{"id":"2"}`), Offset: 2},
		{Topic: "orders", Value: []byte(`{"id":"3"}`), Offset: 3},
	}
	reader := &fakeReader{messages: msgs}
	dlq := &fakeDLQ{}
	handled := 0
	h := consumer.HandlerFunc(func(_ context.Context, _ kafka.Message) error {
		handled++
		return nil
	})

	c := consumer.New(fastCfg(), reader, dlq, h)
	runUntilDone(t, c, reader, 3)

	if handled != 3 {
		t.Fatalf("expected 3 handled, got %d", handled)
	}
	if len(reader.committed) != 3 {
		t.Fatalf("expected 3 committed, got %d", len(reader.committed))
	}
}
