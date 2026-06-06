//go:build integration

// Integration tests spin up real Kafka and Redis via testcontainers-go.
// Run them with: go test -v -tags integration ./integration/
//
// These tests are intentionally slow (container startup ~10s) and are
// excluded from the default `go test ./...` run.
package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"

	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/internal/retry"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/internal/testutil"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/cache"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/consumer"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/producer"
)

// TestEndToEnd_PublishConsumeAndDLQ is the single comprehensive E2E test:
//
//  1. Publish a valid "OrderCreated" event via the SDK producer.
//  2. A consumer processes it, reads user preferences through the Redis cache.
//  3. Publish a poison message (non-JSON), verify it ends up in the DLQ.
func TestEndToEnd_PublishConsumeAndDLQ(t *testing.T) {
	broker := testutil.StartKafka(t)
	redisAddr := testutil.StartRedis(t)

	const (
		topic    = "orders"
		dlqTopic = "orders.dlq"
		groupID  = "test-consumers"
	)

	// --- set up cache --------------------------------------------------------
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	c := cache.New(rdb)

	// Pre-populate user preferences so the consumer gets a cache hit.
	ctx := context.Background()
	if err := c.Set(ctx, "prefs:user-1", []byte(`{"notify":true}`), time.Hour); err != nil {
		t.Fatalf("cache set: %v", err)
	}

	// --- produce valid message ------------------------------------------------
	prod := producer.New(producer.Config{
		Brokers: []string{broker},
		Retry:   producer.DefaultConfig.Retry,
		Breaker: producer.DefaultConfig.Breaker,
	}, nil)
	t.Cleanup(func() { _ = prod.Close() })

	if err := prod.Publish(ctx, producer.Message{
		Topic:          topic,
		Key:            []byte("order-1"),
		Value:          []byte(`{"orderId":"order-1","userId":"user-1"}`),
		IdempotencyKey: "idem-order-1",
	}); err != nil {
		t.Fatalf("publish valid message: %v", err)
	}

	// --- produce poison message -----------------------------------------------
	// We bypass the SDK producer's JSON validation by writing directly to Kafka.
	poisonWriter := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic}
	if err := poisonWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte("poison"),
		Value: []byte("this is not json"),
	}); err != nil {
		t.Fatalf("write poison message: %v", err)
	}
	_ = poisonWriter.Close()

	// --- consume both messages ------------------------------------------------
	dlqWriter := &kafka.Writer{Addr: kafka.TCP(broker), Topic: dlqTopic, Balancer: &kafka.LeastBytes{}}
	t.Cleanup(func() { _ = dlqWriter.Close() })

	var handledMessages []string
	var cacheHit bool

	handler := consumer.HandlerFunc(func(hctx context.Context, msg kafka.Message) error {
		// Try to parse; if invalid JSON it's a poison message
		if string(msg.Value) == "this is not json" {
			return errors.New("invalid JSON payload")
		}
		// Simulate reading user preferences through cache
		prefs, err := c.GetOrLoad(hctx, "prefs:user-1", time.Hour, func(_ context.Context) ([]byte, error) {
			return []byte(`{"notify":false}`), nil // fallback — should not be called
		})
		if err != nil {
			return fmt.Errorf("cache lookup: %w", err)
		}
		// If we got the pre-populated value, the cache hit worked
		if string(prefs) == `{"notify":true}` {
			cacheHit = true
		}
		handledMessages = append(handledMessages, string(msg.Value))
		return nil
	})

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	t.Cleanup(func() { _ = reader.Close() })

	cons := consumer.New(consumer.Config{
		Topic:    topic,
		GroupID:  groupID,
		Brokers:  []string{broker},
		DLQTopic: dlqTopic,
		Retry: retry.Config{
			MaxAttempts: 3,
			BaseDelay:   200 * time.Millisecond,
			MaxDelay:    2 * time.Second,
			Multiplier:  2.0,
		},
	}, reader, dlqWriter, handler)

	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Run until we've processed both messages.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cons.Run(runCtx)
	}()

	// Poll until both conditions are satisfied or timeout.
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			t.Fatal("timed out waiting for messages to be processed")
		case <-ticker.C:
			if len(handledMessages) >= 1 {
				cancel() // stop the consumer
				<-done
				goto verify
			}
		}
	}

verify:
	// --- verify DLQ ----------------------------------------------------------
	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       dlqTopic,
		GroupID:     "dlq-verifier",
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	t.Cleanup(func() { _ = dlqReader.Close() })

	dlqCtx, dlqCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("expected a DLQ message, got error: %v", err)
	}
	if string(dlqMsg.Value) != "this is not json" {
		t.Fatalf("DLQ message value mismatch: %s", dlqMsg.Value)
	}

	// --- verify cache hit ----------------------------------------------------
	if !cacheHit {
		t.Fatal("expected cache hit when reading user preferences")
	}

	t.Logf("handled messages: %v", handledMessages)
	t.Logf("DLQ message received: %s", dlqMsg.Value)
	t.Log("E2E test passed: publish → consume → cache hit + DLQ routing all verified")
}
