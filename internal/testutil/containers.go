//go:build integration || e2e

// Package testutil provides shared container helpers for integration and e2e tests.
// The build tag keeps testcontainers-go out of regular `go build ./...` compilations.
package testutil

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// StartKafka starts a single-node KRaft-mode Kafka container and returns
// its broker address (host:port) accessible from the test process.
func StartKafka(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	// confluent-local bundles KRaft + broker in one image; WithClusterID
	// injects the required CLUSTER_ID env var so KRaft initialization succeeds.
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID("relay-test-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
		}),
	)
	if err != nil {
		t.Fatalf("testutil: start kafka: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = kc.Terminate(ctx)
	})
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("testutil: kafka brokers: %v", err)
	}
	return brokers[0]
}

// EnsureTopics creates the given topics on the broker, retrying until the
// broker accepts the request (it may not be fully ready immediately after
// StartKafka returns). Topics that already exist are silently skipped.
func EnsureTopics(t *testing.T, broker string, topics ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var conn *kafka.Conn
	var err error
	for {
		conn, err = kafka.DialContext(ctx, "tcp", broker)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("EnsureTopics: could not connect to %s: %v", broker, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	defer conn.Close()

	// Kafka admin operations must go to the controller.
	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("EnsureTopics: get controller: %v", err)
	}
	ctrlConn, err := kafka.DialContext(ctx, "tcp",
		net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	if err != nil {
		t.Fatalf("EnsureTopics: dial controller: %v", err)
	}
	defer ctrlConn.Close()

	topicConfigs := make([]kafka.TopicConfig, len(topics))
	for i, topic := range topics {
		topicConfigs[i] = kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}
	// CreateTopics returns an error slice; ignore "topic already exists".
	_ = ctrlConn.CreateTopics(topicConfigs...)
}

// StartRedis starts a Redis container and returns its address (host:port).
func StartRedis(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("testutil: start redis: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = rc.Terminate(ctx)
	})
	addr, err := rc.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("testutil: redis connection string: %v", err)
	}
	// ConnectionString returns "redis://host:port" — strip the scheme.
	return addr[len("redis://"):]
}
