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
