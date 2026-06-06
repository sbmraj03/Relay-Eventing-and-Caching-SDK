package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	kafka "github.com/segmentio/kafka-go"
	"github.com/redis/go-redis/v9"

	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/internal/retry"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/cache"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/consumer"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/telemetry"
)

type userPrefs struct {
	Email  string `json:"email"`
	Notify bool   `json:"notify"`
}

var fakeDB = map[string]userPrefs{
	"user-1": {Email: "alice@example.com", Notify: true},
	"user-2": {Email: "bob@example.com", Notify: false},
	"user-3": {Email: "carol@example.com", Notify: true},
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	brokers := envOr("KAFKA_BROKERS", "localhost:9092")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")

	// ── Observability setup ──────────────────────────────────────────────────
	reg := prometheus.NewRegistry()
	consMetrics := telemetry.NewConsumerMetrics(reg)
	cacheMetrics := telemetry.NewCacheMetrics(reg)

	otlpEndpoint := envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4318")
	shutdown, err := telemetry.InitTracer(ctx, "notification-consumer", otlpEndpoint)
	if err != nil {
		slog.Warn("tracing unavailable", "error", err)
		shutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = shutdown(context.Background()) }()

	tracer := otel.Tracer("notification-consumer")

	// ── Cache ─────────────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	c := cache.New(rdb, cache.WithMetrics(cacheMetrics))

	// ── Consumer ──────────────────────────────────────────────────────────────
	cfg := consumer.Config{
		Topic:       "orders",
		GroupID:     "notification-consumer",
		Brokers:     []string{brokers},
		DLQTopic:    "orders.dlq",
		StartOffset: kafka.FirstOffset,
		Retry: retry.Config{
			MaxAttempts: 3,
			BaseDelay:   200 * time.Millisecond,
			MaxDelay:    2 * time.Second,
			Multiplier:  2.0,
		},
		Metrics: consMetrics,
		Tracer:  tracer,
	}

	cons := consumer.New(cfg, nil, nil, consumer.HandlerFunc(makeHandler(c)))
	defer func() { _ = cons.Close() }()

	// ── Metrics server (port 9090) ────────────────────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsSrv := &http.Server{Addr: ":9090", Handler: metricsMux}
	go func() {
		slog.Info("notification-consumer metrics listening", "addr", metricsSrv.Addr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()

	slog.Info("notification-consumer starting", "topic", cfg.Topic, "group", cfg.GroupID)
	if err := cons.Run(ctx); err != nil {
		slog.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutCtx)
	slog.Info("notification-consumer stopped cleanly")
}

func makeHandler(c *cache.Cache) func(context.Context, kafka.Message) error {
	return func(ctx context.Context, msg kafka.Message) error {
		var event map[string]any
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("invalid JSON payload: %w", err)
		}

		userID, _ := event["userId"].(string)
		orderID, _ := event["orderId"].(string)
		if userID == "" || orderID == "" {
			return fmt.Errorf("missing required fields: userId=%q orderId=%q", userID, orderID)
		}

		prefs, err := loadPrefs(ctx, c, userID)
		if err != nil {
			return fmt.Errorf("load prefs for %s: %w", userID, err)
		}

		if prefs.Notify {
			slog.Info("notification sent", "orderId", orderID, "userId", userID, "email", prefs.Email)
		} else {
			slog.Info("notification suppressed (user opted out)", "orderId", orderID, "userId", userID)
		}
		return nil
	}
}

func loadPrefs(ctx context.Context, c *cache.Cache, userID string) (userPrefs, error) {
	key := "prefs:" + userID
	raw, err := c.GetOrLoad(ctx, key, 5*time.Minute, func(_ context.Context) ([]byte, error) {
		prefs, ok := fakeDB[userID]
		if !ok {
			prefs = userPrefs{Email: userID + "@unknown.example.com", Notify: true}
		}
		slog.Debug("cache miss: loaded from db", "userId", userID)
		return json.Marshal(prefs)
	})
	if err != nil {
		return userPrefs{}, err
	}
	var p userPrefs
	return p, json.Unmarshal(raw, &p)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
