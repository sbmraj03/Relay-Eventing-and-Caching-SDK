package main

import (
	"context"
	"crypto/rand"
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

	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/sdk/producer"
	"github.com/sbmraj03/Relay-Eventing-and-Caching-SDK/sdk/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// ── Observability setup ──────────────────────────────────────────────────
	reg := prometheus.NewRegistry()
	prodMetrics := telemetry.NewProducerMetrics(reg)

	otlpEndpoint := envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4318")
	shutdown, err := telemetry.InitTracer(ctx, "order-service", otlpEndpoint)
	if err != nil {
		slog.Warn("tracing unavailable", "error", err)
		shutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = shutdown(context.Background()) }()

	tracer := otel.Tracer("order-service")

	// ── Producer ─────────────────────────────────────────────────────────────
	prod := producer.New(producer.Config{
		Brokers: []string{envOr("KAFKA_BROKERS", "localhost:9092")},
		Retry:   producer.DefaultConfig.Retry,
		Breaker: producer.DefaultConfig.Breaker,
		Metrics: prodMetrics,
		Tracer:  tracer,
	}, nil)
	defer func() { _ = prod.Close() }()

	// ── HTTP API server (port 8080) ───────────────────────────────────────────
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("POST /orders", handleCreateOrder(prod))

	apiSrv := &http.Server{Addr: ":8080", Handler: apiMux}
	go func() {
		slog.Info("order-service API listening", "addr", apiSrv.Addr)
		if err := apiSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("API server error", "error", err)
			os.Exit(1)
		}
	}()

	// ── Metrics server (port 9090) ────────────────────────────────────────────
	// Kept on a separate port so the metrics endpoint is never accidentally
	// exposed to the public internet through the same ingress as the API.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsSrv := &http.Server{Addr: ":9090", Handler: metricsMux}
	go func() {
		slog.Info("order-service metrics listening", "addr", metricsSrv.Addr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("order-service shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = apiSrv.Shutdown(shutCtx)
	_ = metricsSrv.Shutdown(shutCtx)
}

// --- HTTP handler ------------------------------------------------------------

type createOrderRequest struct {
	UserID string   `json:"userId"`
	Items  []string `json:"items"`
}

type createOrderResponse struct {
	OrderID string `json:"orderId"`
}

func handleCreateOrder(prod *producer.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if req.UserID == "" {
			http.Error(w, "userId is required", http.StatusBadRequest)
			return
		}

		orderID := newUUID()
		event := map[string]any{
			"orderId":   orderID,
			"userId":    req.UserID,
			"items":     req.Items,
			"createdAt": time.Now().UTC().Format(time.RFC3339),
		}
		payload, _ := json.Marshal(event)

		if err := prod.Publish(r.Context(), producer.Message{
			Topic:          "orders",
			Key:            []byte(orderID),
			Value:          payload,
			IdempotencyKey: orderID,
		}); err != nil {
			slog.Error("publish failed", "error", err)
			http.Error(w, "failed to publish event", http.StatusInternalServerError)
			return
		}

		slog.Info("order created", "orderId", orderID, "userId", req.UserID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(createOrderResponse{OrderID: orderID})
	}
}

// --- helpers -----------------------------------------------------------------

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
