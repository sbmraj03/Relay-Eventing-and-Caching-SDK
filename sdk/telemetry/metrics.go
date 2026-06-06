// Package telemetry provides Prometheus metrics and OpenTelemetry tracing
// helpers shared across all Relay SDK packages.
package telemetry

import "github.com/prometheus/client_golang/prometheus"

// ProducerMetrics holds the Prometheus instruments for the producer package.
// Passed to producer.Config.Metrics; nil disables all instrumentation.
type ProducerMetrics struct {
	// PublishTotal counts every publish attempt by topic and outcome.
	PublishTotal *prometheus.CounterVec
	// PublishDuration records the end-to-end latency of each Publish call.
	PublishDuration *prometheus.HistogramVec
}

// NewProducerMetrics registers and returns producer metrics on reg.
// Pass prometheus.DefaultRegisterer for the global registry.
func NewProducerMetrics(reg prometheus.Registerer) *ProducerMetrics {
	m := &ProducerMetrics{
		PublishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_kafka_publishes_total",
			Help: "Total Kafka publish calls, by topic and status (success|error).",
		}, []string{"topic", "status"}),

		PublishDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "relay_kafka_publish_duration_seconds",
			Help:    "End-to-end latency of Publish calls (including retries).",
			Buckets: prometheus.DefBuckets,
		}, []string{"topic"}),
	}
	reg.MustRegister(m.PublishTotal, m.PublishDuration)
	return m
}

// ConsumerMetrics holds the Prometheus instruments for the consumer package.
type ConsumerMetrics struct {
	// MessagesTotal counts every message processed, by topic, group, and outcome.
	MessagesTotal *prometheus.CounterVec
	// DLQTotal counts messages routed to the dead-letter queue.
	DLQTotal *prometheus.CounterVec
}

// NewConsumerMetrics registers and returns consumer metrics on reg.
func NewConsumerMetrics(reg prometheus.Registerer) *ConsumerMetrics {
	m := &ConsumerMetrics{
		MessagesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_consumer_messages_total",
			Help: "Total messages processed by the consumer, by topic, group, and status.",
		}, []string{"topic", "group", "status"}),

		DLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_consumer_dlq_total",
			Help: "Total messages routed to the dead-letter queue.",
		}, []string{"topic", "group"}),
	}
	reg.MustRegister(m.MessagesTotal, m.DLQTotal)
	return m
}

// CacheMetrics holds the Prometheus instruments for the cache package.
type CacheMetrics struct {
	// OpsTotal counts cache operations by type: hit, miss, set, error.
	OpsTotal *prometheus.CounterVec
	// LoadDuration records how long the loader function takes on a cache miss.
	LoadDuration *prometheus.HistogramVec
}

// NewCacheMetrics registers and returns cache metrics on reg.
func NewCacheMetrics(reg prometheus.Registerer) *CacheMetrics {
	m := &CacheMetrics{
		OpsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_cache_operations_total",
			Help: "Total cache operations by type (hit, miss, set, error).",
		}, []string{"op"}),

		LoadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "relay_cache_load_duration_seconds",
			Help:    "Latency of the loader (DB) function invoked on a cache miss.",
			Buckets: prometheus.DefBuckets,
		}, []string{}),
	}
	reg.MustRegister(m.OpsTotal, m.LoadDuration)
	return m
}
