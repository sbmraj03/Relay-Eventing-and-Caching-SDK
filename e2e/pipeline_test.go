//go:build e2e

// Functional (end-to-end) tests for the Relay notification pipeline.
//
// These tests verify *business outcomes*, not SDK mechanics:
//   - A published order causes the correct notification decision per user preference.
//   - User preferences are cached in Redis after the first lookup.
//   - A poison (non-JSON) message is routed to the DLQ without stalling the pipeline.
//
// Run with: go test -v -tags e2e ./e2e/
// Requires Docker. Container startup adds ~30s on a cold run.
package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"

	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/internal/testutil"
	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/sdk/cache"
	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/sdk/consumer"
	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/sdk/producer"
)

// userPrefs mirrors the shape stored in Redis by the notification-consumer.
type userPrefs struct {
	Email  string `json:"email"`
	Notify bool   `json:"notify"`
}

// fakeDB is the same lookup table the notification-consumer uses.
var fakeDB = map[string]userPrefs{
	"user-1": {Email: "alice@example.com", Notify: true},
	"user-2": {Email: "bob@example.com", Notify: false},
	"user-3": {Email: "carol@example.com", Notify: true},
}

// notificationEvent captures the notification decision for assertion.
type notificationEvent struct {
	orderID string
	userID  string
	sent    bool // true = notification was sent; false = suppressed
}

// TestPipeline_NotificationDecision is the primary functional test.
// It verifies that the pipeline makes the correct per-user notification decision
// and that user preferences are stored in the Redis cache after first access.
func TestPipeline_NotificationDecision(t *testing.T) {
	broker := testutil.StartKafka(t)
	testutil.EnsureTopics(t, broker, "orders", "orders.dlq")
	redisAddr := testutil.StartRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	// Wire up shared Redis cache (the same cache the consumer uses).
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	c := cache.New(rdb)

	// Wire up the producer (same config as order-service uses).
	prod := producer.New(producer.Config{
		Brokers: []string{broker},
		Retry:   producer.DefaultConfig.Retry,
		Breaker: producer.DefaultConfig.Breaker,
	}, nil)
	t.Cleanup(func() { _ = prod.Close() })

	// Capture notification decisions via a buffered channel.
	events := make(chan notificationEvent, 10)

	// The handler mirrors the notification-consumer's business logic.
	handler := consumer.HandlerFunc(func(hctx context.Context, msg kafka.Message) error {
		var evt map[string]any
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		userID, _ := evt["userId"].(string)
		orderID, _ := evt["orderId"].(string)
		if userID == "" || orderID == "" {
			return fmt.Errorf("missing fields")
		}

		prefs, err := loadPrefs(hctx, c, userID)
		if err != nil {
			return err
		}
		events <- notificationEvent{orderID: orderID, userID: userID, sent: prefs.Notify}
		return nil
	})

	cons := consumer.New(consumer.Config{
		Topic:       "orders",
		GroupID:     "e2e-notification-consumer",
		Brokers:     []string{broker},
		DLQTopic:    "orders.dlq",
		StartOffset: kafka.FirstOffset,
		Retry:       consumer.DefaultConfig.Retry,
	}, nil, nil, handler)
	t.Cleanup(func() { _ = cons.Close() })
	go cons.Run(ctx) //nolint:errcheck

	// Publish one order each for alice (opt-in) and bob (opt-out).
	aliceOrderID := publishOrder(t, prod, "user-1", []string{"sneakers"})
	bobOrderID := publishOrder(t, prod, "user-2", []string{"laptop"})
	_ = bobOrderID

	// Collect exactly two events (one per order).
	received := collectEvents(t, events, 2, 30*time.Second)

	// ── Assert: notification decisions ───────────────────────────────────────
	aliceEvt, ok := received["user-1"]
	if !ok {
		t.Fatal("no event received for user-1 (alice)")
	}
	if !aliceEvt.sent {
		t.Errorf("user-1 alice (Notify=true): expected notification sent, got suppressed")
	}
	if aliceEvt.orderID != aliceOrderID {
		t.Errorf("order ID mismatch for alice: got %s, want %s", aliceEvt.orderID, aliceOrderID)
	}

	bobEvt, ok := received["user-2"]
	if !ok {
		t.Fatal("no event received for user-2 (bob)")
	}
	if bobEvt.sent {
		t.Errorf("user-2 bob (Notify=false): expected notification suppressed, got sent")
	}

	// ── Assert: Redis cache populated after first lookup ──────────────────────
	aliceRaw, err := rdb.Get(ctx, "prefs:user-1").Bytes()
	if err != nil {
		t.Fatalf("prefs:user-1 not in Redis after processing: %v", err)
	}
	if !bytes.Contains(aliceRaw, []byte("alice@example.com")) {
		t.Errorf("alice's prefs not correctly cached; got: %s", aliceRaw)
	}

	bobRaw, err := rdb.Get(ctx, "prefs:user-2").Bytes()
	if err != nil {
		t.Fatalf("prefs:user-2 not in Redis after processing: %v", err)
	}
	if !bytes.Contains(bobRaw, []byte("bob@example.com")) {
		t.Errorf("bob's prefs not correctly cached; got: %s", bobRaw)
	}
}

// TestPipeline_PoisonMessage_RoutedToDLQ verifies that a non-JSON message
// is retried the configured number of times and then routed to the DLQ,
// while subsequent valid messages are still processed normally.
func TestPipeline_PoisonMessage_RoutedToDLQ(t *testing.T) {
	broker := testutil.StartKafka(t)
	testutil.EnsureTopics(t, broker, "orders", "orders.dlq")
	redisAddr := testutil.StartRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	c := cache.New(rdb)

	events := make(chan notificationEvent, 10)
	handler := consumer.HandlerFunc(func(hctx context.Context, msg kafka.Message) error {
		var evt map[string]any
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		userID, _ := evt["userId"].(string)
		orderID, _ := evt["orderId"].(string)
		prefs, _ := loadPrefs(hctx, c, userID)
		events <- notificationEvent{orderID: orderID, userID: userID, sent: prefs.Notify}
		return nil
	})

	cons := consumer.New(consumer.Config{
		Topic:       "orders",
		GroupID:     "e2e-dlq-test",
		Brokers:     []string{broker},
		DLQTopic:    "orders.dlq",
		StartOffset: kafka.FirstOffset,
		Retry:       consumer.DefaultConfig.Retry,
	}, nil, nil, handler)
	t.Cleanup(func() { _ = cons.Close() })
	go cons.Run(ctx) //nolint:errcheck

	// Write a poison message directly to Kafka, bypassing the SDK's JSON validation.
	poisonWriter := &kafka.Writer{Addr: kafka.TCP(broker), Topic: "orders"}
	if err := poisonWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte("poison-key"),
		Value: []byte("this-is-not-json"),
	}); err != nil {
		t.Fatalf("write poison message: %v", err)
	}
	_ = poisonWriter.Close()

	// After the poison message, publish a valid order to prove the pipeline
	// continues processing (poison didn't stall the consumer).
	prod := producer.New(producer.Config{
		Brokers: []string{broker},
		Retry:   producer.DefaultConfig.Retry,
		Breaker: producer.DefaultConfig.Breaker,
	}, nil)
	t.Cleanup(func() { _ = prod.Close() })

	carolOrderID := publishOrder(t, prod, "user-3", []string{"book"})

	// ── Assert: valid order processed despite preceding poison message ────────
	received := collectEvents(t, events, 1, 30*time.Second)
	carolEvt, ok := received["user-3"]
	if !ok {
		t.Fatal("pipeline stalled: no event for user-3 after poison message")
	}
	if carolEvt.orderID != carolOrderID {
		t.Errorf("carol order ID mismatch: got %s, want %s", carolEvt.orderID, carolOrderID)
	}
	if !carolEvt.sent {
		t.Error("carol (Notify=true) should have received a notification")
	}

	// ── Assert: poison message landed in DLQ ─────────────────────────────────
	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "orders.dlq",
		GroupID:     "e2e-dlq-reader",
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    1e6,
	})
	t.Cleanup(func() { _ = dlqReader.Close() })

	dlqCtx, dlqCancel := context.WithTimeout(ctx, 20*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("expected poison message in DLQ, got: %v", err)
	}
	if string(dlqMsg.Value) != "this-is-not-json" {
		t.Errorf("DLQ payload mismatch: got %q", dlqMsg.Value)
	}

	// The DLQ message must carry the reason header the SDK attaches.
	var hasReason bool
	for _, h := range dlqMsg.Headers {
		if h.Key == "x-dlq-reason" {
			hasReason = true
		}
	}
	if !hasReason {
		t.Error("DLQ message missing x-dlq-reason header")
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func publishOrder(t *testing.T, prod *producer.Producer, userID string, items []string) string {
	t.Helper()
	orderID := newUUID()
	payload, _ := json.Marshal(map[string]any{
		"orderId": orderID,
		"userId":  userID,
		"items":   items,
	})
	if err := prod.Publish(context.Background(), producer.Message{
		Topic:          "orders",
		Key:            []byte(orderID),
		Value:          payload,
		IdempotencyKey: orderID,
	}); err != nil {
		t.Fatalf("publishOrder: %v", err)
	}
	return orderID
}

func loadPrefs(ctx context.Context, c *cache.Cache, userID string) (userPrefs, error) {
	key := "prefs:" + userID
	raw, err := c.GetOrLoad(ctx, key, 5*time.Minute, func(_ context.Context) ([]byte, error) {
		prefs, ok := fakeDB[userID]
		if !ok {
			prefs = userPrefs{Email: userID + "@unknown.example.com", Notify: true}
		}
		return json.Marshal(prefs)
	})
	if err != nil {
		return userPrefs{}, err
	}
	var p userPrefs
	return p, json.Unmarshal(raw, &p)
}

// collectEvents reads exactly n events from ch within the timeout.
// Returns a map keyed by userID.
func collectEvents(t *testing.T, ch <-chan notificationEvent, n int, timeout time.Duration) map[string]notificationEvent {
	t.Helper()
	out := make(map[string]notificationEvent, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case e := <-ch:
			out[e.userID] = e
		case <-deadline:
			t.Fatalf("timed out after %s waiting for events; got %d/%d: %v", timeout, len(out), n, out)
		}
	}
	return out
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
