// Package producer provides a Kafka producer with idempotency, retry,
// circuit-breaking, schema validation, Prometheus metrics, and OTel tracing.
package producer

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/internal/retry"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/breaker"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/telemetry"
)

// Message is what callers pass to Publish.
type Message struct {
	Topic          string
	Key            []byte
	Value          []byte            // must be non-empty, valid JSON
	IdempotencyKey string            // auto-generated if empty
	Headers        map[string]string // extra metadata headers
}

// Config holds producer tuning parameters.
type Config struct {
	Brokers []string
	Retry   retry.Config
	Breaker breaker.Config
	// Metrics and Tracer are optional; nil disables the respective feature.
	// Existing callers that leave them unset continue to work without changes.
	Metrics *telemetry.ProducerMetrics
	Tracer  trace.Tracer
}

// DefaultConfig is a sensible starting point for local development.
var DefaultConfig = Config{
	Brokers: []string{"localhost:9092"},
	Retry: retry.Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    5 * time.Second,
		Multiplier:  2.0,
	},
	Breaker: breaker.DefaultConfig,
}

// Writer is the Kafka write interface. kafka.Writer satisfies this; so does
// any test double.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Producer publishes messages to Kafka.
type Producer struct {
	cfg     Config
	writer  Writer
	breaker *breaker.Breaker
}

// New creates a Producer. Pass writer=nil to use the default kafka-go writer.
func New(cfg Config, writer Writer) *Producer {
	if writer == nil {
		writer = &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
		}
	}
	return &Producer{cfg: cfg, writer: writer, breaker: breaker.New(cfg.Breaker)}
}

// Publish validates and publishes msg with circuit-breaker, retry, metrics, and tracing.
func (p *Producer) Publish(ctx context.Context, msg Message) error {
	if err := validate(msg); err != nil {
		return fmt.Errorf("producer: validation: %w", err)
	}

	// Start a span if a tracer is configured.
	if p.cfg.Tracer != nil {
		var span trace.Span
		ctx, span = p.cfg.Tracer.Start(ctx, "relay.producer.publish",
			trace.WithAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination", msg.Topic),
			),
		)
		defer span.End()
	}

	km := toKafkaMessage(ctx, msg) // trace context injected into headers here

	start := time.Now()
	err := p.breaker.Do(func() error {
		return retry.Do(ctx, p.cfg.Retry, func() error {
			return p.writer.WriteMessages(ctx, km)
		})
	})

	// Record Prometheus metrics (skipped when Metrics is nil).
	if p.cfg.Metrics != nil {
		status := "success"
		if err != nil {
			status = "error"
		}
		p.cfg.Metrics.PublishTotal.WithLabelValues(msg.Topic, status).Inc()
		p.cfg.Metrics.PublishDuration.WithLabelValues(msg.Topic).Observe(time.Since(start).Seconds())
	}

	return err
}

// Close shuts down the underlying Kafka writer.
func (p *Producer) Close() error { return p.writer.Close() }

// BreakerState exposes the circuit-breaker state for health checks.
func (p *Producer) BreakerState() breaker.State { return p.breaker.State() }

// --- private helpers ---------------------------------------------------------

func validate(msg Message) error {
	if msg.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if len(msg.Value) == 0 {
		return fmt.Errorf("value must not be empty")
	}
	if !json.Valid(msg.Value) {
		return fmt.Errorf("value is not valid JSON")
	}
	return nil
}

func toKafkaMessage(ctx context.Context, msg Message) kafka.Message {
	headers := []kafka.Header{
		{Key: "x-idempotency-key", Value: []byte(resolveKey(msg.IdempotencyKey))},
	}
	for k, v := range msg.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	// Inject the active OTel span into headers so the consumer can create a
	// child span. This is a no-op when no tracer provider is configured.
	headers = append(headers, telemetry.InjectContext(ctx)...)
	return kafka.Message{
		Topic:   msg.Topic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}
}

func resolveKey(provided string) string {
	if provided != "" {
		return provided
	}
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
