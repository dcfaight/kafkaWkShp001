package com.dcfaight.kafkawkshp001;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Component
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private final Executor executor;
    private final ObjectMapper objectMapper;
    private final FirewallEventService firewallEventService;
    private final ElasticClient elasticClient;

    public KafkaConsumer(ObjectMapper objectMapper, FirewallEventService firewallEventService) {
        this(objectMapper, firewallEventService, Executors.newFixedThreadPool(4), new ElasticClient());
    }

    KafkaConsumer(ObjectMapper objectMapper,
                  FirewallEventService firewallEventService,
                  Executor executor,
                  ElasticClient elasticClient) {
        this.objectMapper = objectMapper;
        this.firewallEventService = firewallEventService;
        this.executor = executor;
        this.elasticClient = elasticClient;
    }

    @KafkaListener(topics = "firewall.logs.raw", groupId = "nd-parser", concurrency = "4")
    public void onMessage(String message) {
        executor.execute(() -> processEvent(message));
    }

    void processEvent(String message) {
        log.info("Listener triggered!");
        Map<String, Object> event;

        try {
            event = objectMapper.readValue(message, Map.class);
            log.info("Parsed message: {}", event);
        } catch (Exception e) {
            log.error("Exception parsing JSON! Message: {}", message, e);
            return;
        }

        if (!hasRequiredFields(event)) {
            log.warn("Validation failed: missing field, event was: {}", event);
            return;
        }

        String eventJson = null;
        try {
            eventJson = objectMapper.writeValueAsString(event);
            elasticClient.writeToElasticsearch(eventJson);
        } catch (Exception esEx) {
            log.error("Failed to write to Elasticsearch or serialize event! JSON: {}, Event: {}", eventJson, event, esEx);
        }

        try {
            FirewallEvent entity = mapEventToEntity(event);
            log.info("About to save entity to Postgres: {}", entity);
            FirewallEvent saved = firewallEventService.save(entity);
            if (saved != null) {
                log.info("Saved entity to Postgres with ID: {}", saved.getId());
            } else {
                log.warn("FirewallEventService.save returned null");
            }
        } catch (Exception dbEx) {
            log.error("Exception mapping event or saving to Postgres! Event: {}", event, dbEx);
        }

        log.info("Completed processing event: {}", event);
    }

    private boolean hasRequiredFields(Map<String, Object> event) {
        return (event.containsKey("@timestamp") || event.containsKey("timestamp"))
                && (event.containsKey("src_ip") || event.containsKey("srcIp"))
                && (event.containsKey("dst_ip") || event.containsKey("dstIp"))
                && event.containsKey("action");
    }

    private FirewallEvent mapEventToEntity(Map<String, Object> event) {
        FirewallEvent entity = new FirewallEvent();
        if (event.containsKey("@timestamp")) {
            entity.setTimestamp(OffsetDateTime.parse((String) event.get("@timestamp")));
        } else if (event.containsKey("timestamp")) {
            entity.setTimestamp(OffsetDateTime.parse((String) event.get("timestamp")));
        }
        entity.setSrcIp((String) event.getOrDefault("src_ip", event.get("srcIp")));
        entity.setDstIp((String) event.getOrDefault("dst_ip", event.get("dstIp")));
        entity.setAction((String) event.get("action"));
        entity.setSrcPort(
                event.get("src_port") != null ? Integer.parseInt(event.get("src_port").toString()) :
                        event.get("srcPort") != null ? Integer.parseInt(event.get("srcPort").toString()) : null
        );
        entity.setDstPort(
                event.get("dst_port") != null ? Integer.parseInt(event.get("dst_port").toString()) :
                        event.get("dstPort") != null ? Integer.parseInt(event.get("dstPort").toString()) : null
        );
        entity.setDeviceId((String) event.getOrDefault("device_id", event.get("deviceId")));
        entity.setMessage((String) event.get("message"));
        return entity;
    }
}
