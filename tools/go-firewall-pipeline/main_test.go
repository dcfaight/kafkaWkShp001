package main

import (
	"context"
	"errors"
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

// --- envOrDefault ---

func TestEnvOrDefault_returnsDefault_whenUnset(t *testing.T) {
	t.Setenv("TEST_STRING_MISSING", "")
	got := envOrDefault("TEST_STRING_MISSING", "fallback")
	if got != "fallback" {
		t.Errorf("expected %q, got %q", "fallback", got)
	}
}

func TestEnvOrDefault_returnsEnvValue(t *testing.T) {
	t.Setenv("TEST_STRING_SET", "myvalue")
	got := envOrDefault("TEST_STRING_SET", "fallback")
	if got != "myvalue" {
		t.Errorf("expected %q, got %q", "myvalue", got)
	}
}

// --- envIntOrDefault ---

func TestEnvIntOrDefault_returnsDefault_whenUnset(t *testing.T) {
	t.Setenv("TEST_INT_MISSING", "")
	got := envIntOrDefault("TEST_INT_MISSING", 42)
	if got != 42 {
		t.Errorf("expected %d, got %d", 42, got)
	}
}

func TestEnvIntOrDefault_returnsDefault_whenInvalid(t *testing.T) {
	t.Setenv("TEST_INT_BAD", "notanumber")
	got := envIntOrDefault("TEST_INT_BAD", 42)
	if got != 42 {
		t.Errorf("expected %d, got %d", 42, got)
	}
}

func TestEnvIntOrDefault_returnsDefault_whenZero(t *testing.T) {
	t.Setenv("TEST_INT_ZERO", "0")
	got := envIntOrDefault("TEST_INT_ZERO", 42)
	if got != 42 {
		t.Errorf("expected %d, got %d", 42, got)
	}
}

func TestEnvIntOrDefault_returnsDefault_whenNegative(t *testing.T) {
	t.Setenv("TEST_INT_NEG", "-5")
	got := envIntOrDefault("TEST_INT_NEG", 42)
	if got != 42 {
		t.Errorf("expected %d, got %d", 42, got)
	}
}

func TestEnvIntOrDefault_returnsParsedValue(t *testing.T) {
	t.Setenv("TEST_INT_GOOD", "16")
	got := envIntOrDefault("TEST_INT_GOOD", 42)
	if got != 16 {
		t.Errorf("expected %d, got %d", 16, got)
	}
}

// --- loadConfig ---

func TestLoadConfig_defaults(t *testing.T) {
	for _, k := range []string{
		"KAFKA_BROKER", "KAFKA_TOPIC", "KAFKA_GROUP_ID",
		"ELASTICSEARCH_URL", "ELASTICSEARCH_INDEX", "NUM_WORKERS", "MSG_CHAN_BUF",
	} {
		t.Setenv(k, "")
	}

	cfg := loadConfig()

	if cfg.KafkaBroker != "192.168.1.5:9092" {
		t.Errorf("KafkaBroker: expected %q, got %q", "192.168.1.5:9092", cfg.KafkaBroker)
	}
	if cfg.KafkaTopic != "firewall.logs.raw" {
		t.Errorf("KafkaTopic: expected %q, got %q", "firewall.logs.raw", cfg.KafkaTopic)
	}
	if cfg.KafkaGroupID != "dcfaight-test-20240427-1" {
		t.Errorf("KafkaGroupID: expected %q, got %q", "dcfaight-test-20240427-1", cfg.KafkaGroupID)
	}
	if cfg.ElasticsearchURL != "http://localhost:9200" {
		t.Errorf("ElasticsearchURL: expected %q, got %q", "http://localhost:9200", cfg.ElasticsearchURL)
	}
	if cfg.ElasticsearchIndex != "firewall-events-golang" {
		t.Errorf("ElasticsearchIndex: expected %q, got %q", "firewall-events-golang", cfg.ElasticsearchIndex)
	}
	if cfg.NumWorkers != 8 {
		t.Errorf("NumWorkers: expected %d, got %d", 8, cfg.NumWorkers)
	}
	if cfg.MsgChanBuf != 100 {
		t.Errorf("MsgChanBuf: expected %d, got %d", 100, cfg.MsgChanBuf)
	}
}

func TestLoadConfig_envOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "broker:9092")
	t.Setenv("KAFKA_TOPIC", "test.topic")
	t.Setenv("KAFKA_GROUP_ID", "test-group")
	t.Setenv("ELASTICSEARCH_URL", "http://es:9200")
	t.Setenv("ELASTICSEARCH_INDEX", "test-index")
	t.Setenv("NUM_WORKERS", "4")
	t.Setenv("MSG_CHAN_BUF", "50")

	cfg := loadConfig()

	if cfg.KafkaBroker != "broker:9092" {
		t.Errorf("KafkaBroker: expected %q, got %q", "broker:9092", cfg.KafkaBroker)
	}
	if cfg.KafkaTopic != "test.topic" {
		t.Errorf("KafkaTopic: expected %q, got %q", "test.topic", cfg.KafkaTopic)
	}
	if cfg.KafkaGroupID != "test-group" {
		t.Errorf("KafkaGroupID: expected %q, got %q", "test-group", cfg.KafkaGroupID)
	}
	if cfg.ElasticsearchURL != "http://es:9200" {
		t.Errorf("ElasticsearchURL: expected %q, got %q", "http://es:9200", cfg.ElasticsearchURL)
	}
	if cfg.ElasticsearchIndex != "test-index" {
		t.Errorf("ElasticsearchIndex: expected %q, got %q", "test-index", cfg.ElasticsearchIndex)
	}
	if cfg.NumWorkers != 4 {
		t.Errorf("NumWorkers: expected %d, got %d", 4, cfg.NumWorkers)
	}
	if cfg.MsgChanBuf != 50 {
		t.Errorf("MsgChanBuf: expected %d, got %d", 50, cfg.MsgChanBuf)
	}
}

// --- Indexer mock ---

type mockIndexer struct {
	calledWith []indexCall
	errReturn  error
}

type indexCall struct {
	index   string
	payload string
}

func (m *mockIndexer) Index(_ context.Context, index, payload string) error {
	m.calledWith = append(m.calledWith, indexCall{index: index, payload: payload})
	return m.errReturn
}

// --- indexMessage ---

func TestIndexMessage_success(t *testing.T) {
	mock := &mockIndexer{}
	msg := kafka.Message{
		Topic:     "firewall.logs.raw",
		Partition: 0,
		Offset:    1,
		Value:     []byte(`{"src":"10.0.0.1"}`),
	}

	indexMessage(context.Background(), mock, "test-index", msg, 1)

	if len(mock.calledWith) != 1 {
		t.Fatalf("expected 1 Index call, got %d", len(mock.calledWith))
	}
	if mock.calledWith[0].index != "test-index" {
		t.Errorf("expected index %q, got %q", "test-index", mock.calledWith[0].index)
	}
	if mock.calledWith[0].payload != `{"src":"10.0.0.1"}` {
		t.Errorf("unexpected payload: %q", mock.calledWith[0].payload)
	}
}

func TestIndexMessage_error(t *testing.T) {
	mock := &mockIndexer{errReturn: errors.New("es down")}
	msg := kafka.Message{
		Topic:     "firewall.logs.raw",
		Partition: 0,
		Offset:    2,
		Value:     []byte(`{"src":"10.0.0.2"}`),
	}

	// Should not panic; error is only logged
	indexMessage(context.Background(), mock, "test-index", msg, 1)

	if len(mock.calledWith) != 1 {
		t.Fatalf("expected 1 Index call, got %d", len(mock.calledWith))
	}
}
