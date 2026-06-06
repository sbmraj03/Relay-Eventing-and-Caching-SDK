// Package consumer provides a Kafka consumer with at-least-once delivery,
// retry with backoff, dead-letter queue routing, Prometheus metrics, and OTel tracing.
package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/internal/retry"
	"github.com/sbmraj03/Relay---Eventing-Caching-SDK/sdk/telemetry"
)

// Handler processes a single Kafka message.
type Handler interface {
	Handle(ctx context.Context, msg kafka.Message) error
}

// HandlerFunc is a convenience adapter so plain functions can be Handlers.
type HandlerFunc func(ctx context.Context, msg kafka.Message) error

func (f HandlerFunc) Handle(ctx context.Context, msg kafka.Message) error { return f(ctx, msg) }

// MessageReader is satisfied by kafka.Reader.
type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// MessageWriter is satisfied by kafka.Writer. Used for DLQ publishing.
type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Config controls consumer behaviour.
type Config struct {
	Topic       string
	GroupID     string
	Brokers     []string
	DLQTopic    string       // messages that exhaust retries are sent here
	Retry       retry.Config // retry config applied to the Handler
	StartOffset int64        // kafka.FirstOffset (-2) or kafka.LastOffset (-1, default)
	// Metrics and Tracer are optional; nil disables the respective feature.
	Metrics *telemetry.ConsumerMetrics
	Tracer  trace.Tracer
}

// DefaultConfig is a sensible starting point.
var DefaultConfig = Config{
	DLQTopic: "relay.dlq",
	Retry: retry.Config{
		MaxAttempts: 3,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    5 * time.Second,
		Multiplier:  2.0,
	},
}

// Consumer reads from Kafka and dispatches messages to a Handler.
type Consumer struct {
	cfg     Config
	reader  MessageReader
	dlq     MessageWriter
	handler Handler
}

// New creates a Consumer. Pass reader=nil / dlqWriter=nil to use real kafka-go clients.
func New(cfg Config, reader MessageReader, dlqWriter MessageWriter, handler Handler) *Consumer {
	if reader == nil {
		reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.Brokers,
			Topic:       cfg.Topic,
			GroupID:     cfg.GroupID,
			MinBytes:    1,
			MaxBytes:    10e6,
			StartOffset: cfg.StartOffset,
		})
	}
	if dlqWriter == nil {
		dlqWriter = &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    cfg.DLQTopic,
			Balancer: &kafka.LeastBytes{},
		}
	}
	return &Consumer{cfg: cfg, reader: reader, dlq: dlqWriter, handler: handler}
}

// Run processes messages until ctx is cancelled.
// At-least-once: commits offset only after handler success or DLQ delivery.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consumer: fetch: %w", err)
		}

		// Extract the OTel trace context propagated from the producer via headers.
		msgCtx := ctx
		if c.cfg.Tracer != nil {
			msgCtx = telemetry.ExtractContext(ctx, msg.Headers)
			var span trace.Span
			msgCtx, span = c.cfg.Tracer.Start(msgCtx, "relay.consumer.process",
				trace.WithAttributes(
					attribute.String("messaging.system", "kafka"),
					attribute.String("messaging.source", msg.Topic),
					attribute.Int64("messaging.kafka.offset", msg.Offset),
					attribute.String("messaging.kafka.consumer.group", c.cfg.GroupID),
				),
			)
			defer span.End()
		}

		processErr := c.process(msgCtx, msg)

		// Record metrics regardless of outcome.
		if c.cfg.Metrics != nil {
			status := "success"
			if processErr != nil {
				status = "error"
			}
			c.cfg.Metrics.MessagesTotal.WithLabelValues(msg.Topic, c.cfg.GroupID, status).Inc()
		}

		if processErr != nil {
			slog.Error("consumer: failed to process message",
				"topic", msg.Topic, "offset", msg.Offset, "error", processErr)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consumer: commit: %w", err)
		}
	}
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	err := retry.Do(ctx, c.cfg.Retry, func() error {
		return c.handler.Handle(ctx, msg)
	})
	if err == nil {
		return nil
	}
	return c.sendToDLQ(ctx, msg, err)
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, reason error) error {
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.DLQTotal.WithLabelValues(msg.Topic, c.cfg.GroupID).Inc()
	}
	dlqMsg := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: append(msg.Headers,
			kafka.Header{Key: "x-dlq-reason", Value: []byte(reason.Error())},
			kafka.Header{Key: "x-dlq-source-topic", Value: []byte(msg.Topic)},
			kafka.Header{Key: "x-dlq-source-offset", Value: []byte(fmt.Sprintf("%d", msg.Offset))},
		),
	}
	if err := c.dlq.WriteMessages(ctx, dlqMsg); err != nil {
		return fmt.Errorf("consumer: dlq write: %w", err)
	}
	return fmt.Errorf("consumer: sent to DLQ after handler exhausted retries: %w", reason)
}

func (c *Consumer) Close() error { return c.reader.Close() }
