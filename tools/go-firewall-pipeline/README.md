# go-firewall-pipeline

Standalone Go utility that consumes firewall events from Kafka and writes each event payload to Elasticsearch.

## Environment variables

| Variable | Default |
| --- | --- |
| `KAFKA_BROKER` | `192.168.1.5:9092` |
| `KAFKA_TOPIC` | `firewall.logs.raw` |
| `KAFKA_GROUP_ID` | `dcfaight-test-20240427-1` |
| `ELASTICSEARCH_URL` | `http://localhost:9200` |
| `ELASTICSEARCH_INDEX` | `firewall-events-golang` |
| `NUM_WORKERS` | `8` |
| `MSG_CHAN_BUF` | `100` |

## Run

```bash
cd tools/go-firewall-pipeline
go run .
```

## Testing

Run all tests:

```bash
cd tools/go-firewall-pipeline
go test ./...
```

Run tests with a coverage summary:

```bash
go test ./... -cover
```

Generate a detailed coverage report:

```bash
go test ./... -coverprofile=cover.out
go tool cover -func=cover.out
```

The tests cover `envOrDefault`, `envIntOrDefault`, `loadConfig`, and `indexMessage` without requiring a live Kafka or Elasticsearch instance.
`ElasticIndexer.Index`, `runWorkers`, and `main` require live infrastructure and are excluded from unit tests by design.
