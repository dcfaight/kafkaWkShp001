# KafkaWkShp001

A Java/Spring Boot backend for producing and consuming Kafka topics, with a standalone Go utility for ingesting firewall events into Elasticsearch.

## Tech stack

| Layer | Technology |
|---|---|
| Language (main app) | Java 21 |
| Framework | Spring Boot 4 |
| Messaging | Apache Kafka |
| Search / indexing | Elasticsearch |
| Build | Apache Maven (wrapper included) |
| Language (Go tool) | Go 1.22 |

## Project layout

```
KafkaWkShp001/          ← Java/Spring Boot application (Maven)
  src/
    main/java/…         ← application source
    test/java/…         ← JUnit tests (100 % branch coverage)
  pom.xml

tools/
  go-firewall-pipeline/ ← standalone Go utility (isolated module)
    main.go             ← Kafka → Elasticsearch pipeline
    main_test.go        ← unit tests (no live infrastructure required)
    go.mod / go.sum
    README.md           ← Go-specific documentation
```

## Java application

A Spring Boot REST service that exposes endpoints for producing demo Kafka messages and exercising dead-letter-queue flows.

### Run tests (Java)

```bash
./mvnw test
```

> **Java 21 required.** If your `JAVA_HOME` points to an older JDK, set it explicitly:
>
> ```bash
> JAVA_HOME=/path/to/jdk-21 ./mvnw test
> ```

## Go utility — `tools/go-firewall-pipeline`

A lightweight, self-contained Go program that reads raw firewall events from a Kafka topic and indexes each payload into Elasticsearch. It runs independently of the Java application and has no shared build with Maven.

See [`tools/go-firewall-pipeline/README.md`](tools/go-firewall-pipeline/README.md) for environment variables and runtime details.

### Run tests (Go)

```bash
cd tools/go-firewall-pipeline
go test ./...
```

### Run tests with coverage summary

```bash
cd tools/go-firewall-pipeline
go test ./... -cover
```

### Generate a detailed coverage report

```bash
cd tools/go-firewall-pipeline
go test ./... -coverprofile=cover.out
go tool cover -func=cover.out
```

Tests cover `envOrDefault`, `envIntOrDefault`, `loadConfig`, and `indexMessage` without requiring a live Kafka or Elasticsearch instance.
