package telemetry

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitTracer configures the global OTel TracerProvider with an OTLP/HTTP exporter.
// Call once in main(); defer the returned shutdown function.
//
//	shutdown, err := telemetry.InitTracer(ctx, "order-service", "http://jaeger:4318")
//	if err != nil { ... }
//	defer shutdown(context.Background())
func InitTracer(ctx context.Context, serviceName, otlpEndpoint string) (func(context.Context) error, error) {
	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(otlpEndpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	// W3C TraceContext propagation: spans follow requests across process boundaries.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}

// InjectContext serializes the active span from ctx into Kafka message headers.
// Call this in the producer before WriteMessages so the trace follows the event.
func InjectContext(ctx context.Context) []kafka.Header {
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers := make([]kafka.Header, 0, len(carrier))
	for k, v := range carrier {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	return headers
}

// ExtractContext reads Kafka message headers and returns a new context that
// carries the parent span. Call this in the consumer before dispatching to
// the handler so the handler's spans are children of the producer's span.
func ExtractContext(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := make(propagation.MapCarrier)
	for _, h := range headers {
		carrier[h.Key] = string(h.Value)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
