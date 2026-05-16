package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/olivere/elastic/v7"
	kafka "github.com/segmentio/kafka-go"
)

// envOrDefault returns the value of the environment variable key, or defaultValue if unset or empty.
func envOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// envIntOrDefault returns the integer value of the environment variable key, or defaultValue
// if the variable is unset, empty, or not a valid positive integer.
func envIntOrDefault(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		log.Printf("invalid value for %s=%q, using default %d", key, value, defaultValue)
		return defaultValue
	}

	return parsed
}

// Config holds all runtime configuration for the pipeline.
type Config struct {
	KafkaBroker        string
	KafkaTopic         string
	KafkaGroupID       string
	ElasticsearchURL   string
	ElasticsearchIndex string
	NumWorkers         int
	MsgChanBuf         int
}

// loadConfig reads pipeline configuration from environment variables, falling back to defaults.
func loadConfig() Config {
	return Config{
		KafkaBroker:        envOrDefault("KAFKA_BROKER", "192.168.1.5:9092"),
		KafkaTopic:         envOrDefault("KAFKA_TOPIC", "firewall.logs.raw"),
		KafkaGroupID:       envOrDefault("KAFKA_GROUP_ID", "dcfaight-test-20240427-1"),
		ElasticsearchURL:   envOrDefault("ELASTICSEARCH_URL", "http://localhost:9200"),
		ElasticsearchIndex: envOrDefault("ELASTICSEARCH_INDEX", "firewall-events-golang"),
		NumWorkers:         envIntOrDefault("NUM_WORKERS", 8),
		MsgChanBuf:         envIntOrDefault("MSG_CHAN_BUF", 100),
	}
}

// Indexer is the interface used by workers to index a single message payload into a named index.
type Indexer interface {
	Index(ctx context.Context, index, payload string) error
}

// ElasticIndexer is the production Indexer backed by an Elasticsearch client.
type ElasticIndexer struct {
	client *elastic.Client
}

// Index indexes payload into the named Elasticsearch index.
func (e *ElasticIndexer) Index(ctx context.Context, index, payload string) error {
	_, err := e.client.Index().Index(index).BodyString(payload).Do(ctx)
	return err
}

// indexMessage indexes a single Kafka message using the provided Indexer.
func indexMessage(ctx context.Context, indexer Indexer, index string, msg kafka.Message, workerID int) {
	payload := string(msg.Value)
	if err := indexer.Index(ctx, index, payload); err != nil {
		log.Printf("worker %d failed to index message (topic=%s partition=%d offset=%d): %v",
			workerID, msg.Topic, msg.Partition, msg.Offset, err)
		return
	}
	log.Printf("worker %d indexed message (topic=%s partition=%d offset=%d)",
		workerID, msg.Topic, msg.Partition, msg.Offset)
}

// runWorkers starts numWorkers goroutines that each read from msgChan and index messages.
func runWorkers(indexer Indexer, index string, msgChan <-chan kafka.Message, numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		workerID := i + 1
		go func(id int) {
			ctx := context.Background()
			for msg := range msgChan {
				indexMessage(ctx, indexer, index, msg, id)
			}
		}(workerID)
	}
}

func main() {
	cfg := loadConfig()

	log.Printf(
		"starting kafka->elasticsearch pipeline (broker=%s topic=%s group=%s es=%s index=%s workers=%d buffer=%d)",
		cfg.KafkaBroker, cfg.KafkaTopic, cfg.KafkaGroupID, cfg.ElasticsearchURL, cfg.ElasticsearchIndex,
		cfg.NumWorkers, cfg.MsgChanBuf,
	)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{cfg.KafkaBroker},
		GroupID:  cfg.KafkaGroupID,
		Topic:    cfg.KafkaTopic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	esClient, err := elastic.NewClient(
		elastic.SetURL(cfg.ElasticsearchURL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)
	if err != nil {
		log.Fatalf("failed to create elasticsearch client: %v", err)
	}

	indexer := &ElasticIndexer{client: esClient}
	msgChan := make(chan kafka.Message, cfg.MsgChanBuf)

	runWorkers(indexer, cfg.ElasticsearchIndex, msgChan, cfg.NumWorkers)

	ctx := context.Background()
	for {
		msg, readErr := reader.ReadMessage(ctx)
		if readErr != nil {
			log.Printf("failed to read message from kafka: %v", readErr)
			continue
		}

		msgChan <- msg
	}
}
