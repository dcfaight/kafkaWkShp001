package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/olivere/elastic/v7"
	"github.com/segmentio/kafka-go"
)

func envOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

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

func main() {
	kafkaBroker := envOrDefault("KAFKA_BROKER", "192.168.1.5:9092")
	kafkaTopic := envOrDefault("KAFKA_TOPIC", "firewall.logs.raw")
	kafkaGroupID := envOrDefault("KAFKA_GROUP_ID", "dcfaight-test-20240427-1")
	elasticsearchURL := envOrDefault("ELASTICSEARCH_URL", "http://localhost:9200")
	elasticsearchIndex := envOrDefault("ELASTICSEARCH_INDEX", "firewall-events-golang")
	numWorkers := envIntOrDefault("NUM_WORKERS", 8)
	msgChanBuf := envIntOrDefault("MSG_CHAN_BUF", 100)

	log.Printf(
		"starting kafka->elasticsearch pipeline (broker=%s topic=%s group=%s es=%s index=%s workers=%d buffer=%d)",
		kafkaBroker, kafkaTopic, kafkaGroupID, elasticsearchURL, elasticsearchIndex, numWorkers, msgChanBuf,
	)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  kafkaGroupID,
		Topic:    kafkaTopic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	esClient, err := elastic.NewClient(
		elastic.SetURL(elasticsearchURL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)
	if err != nil {
		log.Fatalf("failed to create elasticsearch client: %v", err)
	}

	msgChan := make(chan kafka.Message, msgChanBuf)

	for i := 0; i < numWorkers; i++ {
		workerID := i + 1
		go func(id int) {
			ctx := context.Background()
			for msg := range msgChan {
				payload := string(msg.Value)

				_, indexErr := esClient.Index().
					Index(elasticsearchIndex).
					BodyString(payload).
					Do(ctx)
				if indexErr != nil {
					log.Printf("worker %d failed to index message (topic=%s partition=%d offset=%d): %v", id, msg.Topic, msg.Partition, msg.Offset, indexErr)
					continue
				}

				log.Printf("worker %d indexed message (topic=%s partition=%d offset=%d)", id, msg.Topic, msg.Partition, msg.Offset)
			}
		}(workerID)
	}

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
