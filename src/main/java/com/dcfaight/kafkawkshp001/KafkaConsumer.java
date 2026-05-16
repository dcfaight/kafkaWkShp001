package com.dcfaight.kafkawkshp001;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dcfaight.kafkawkshp001.ElasticClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;

@Component
public class KafkaConsumer {

    private final ExecutorService pool = Executors.newFixedThreadPool(4);
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final int MAX_RETRIES = 3;
    private final Map<String, Integer> retryCounts = new ConcurrentHashMap<>();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper; // Jackson

    @Autowired
    private FirewallEventService firewallEventService;




    @KafkaListener(topics = "firewall.logs.raw", groupId = "nd-parser", concurrency = "4") // or however many threads
    // you want)
    public void onMessage(String message) {
        pool.submit(() -> {
            // All your existing logic: parse, validate, write to ES & Postgres
            this.processEvent(message);
        });
    }

    private void processEvent (String message){
            log.info("Listener triggered!");
            ElasticClient es = new ElasticClient();
            Map<String, Object> event = null;

            // 1. Parse JSON
            try {
                event = objectMapper.readValue(message, Map.class);
                log.info("Parsed message: {}", event);
            } catch (Exception e) {
                log.error("Exception parsing JSON! Message: {}", message, e);
                return; // Can't process non-JSON
            }

            // 2. Validate required fields
            if (
                    (!event.containsKey("@timestamp") && !event.containsKey("timestamp"))
                            || (!event.containsKey("src_ip") && !event.containsKey("srcIp"))
                            || (!event.containsKey("dst_ip") && !event.containsKey("dstIp"))
                            || !event.containsKey("action")
            ) {
                log.warn("Validation failed: missing field, event was: {}", event);
                return;
            }
            log.info("TEST: About to start ES serialization!");
            log.info(">>> BEFORE ES BLOCK <<<");
            log.info("!!! ES TEST LOG FIRED !!!");
            // 3. Serialize and write to Elasticsearch
            String eventJson = null;
            try {
                log.info("About to serialize event for Elasticsearch: {}", event);   // Log the event map before serialization
                eventJson = objectMapper.writeValueAsString(event);
                log.info("Successfully serialized event for ES: {}", eventJson);     // Only prints if serialization succeeds

                log.info("Attempting to write to Elasticsearch: {}", eventJson);
                es.writeToElasticsearch(eventJson);
                log.info("Elasticsearch write completed successfully");
            } catch (Exception esEx) {
                log.error("Failed to write to Elasticsearch or serialize event! JSON: {}, Event: {}", eventJson, event, esEx);
                // Optional: return if ES write is required to be successful
                // return;
            }
//        try {
//            throw new RuntimeException("FORCED ES BLOCK ERROR");
//        } catch (Exception esEx) {
//            log.error("TEST: Failed to write to ES (forced)", esEx);
//        }
            log.info(">>> AFTER ES BLOCK <<<");

            // 4. Map to entity and save to Postgres
            try {
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
//call service and perform some business logic, etc prior to save; example, threat score derived, alert created...
                log.info("About to save entity to Postgres: {}", entity);
                FirewallEvent saved = firewallEventService.save(entity);
                log.info("TEST: Made it past DB save!");
                log.info("Saved entity to Postgres with ID: {}", saved.getId());
            } catch (Exception dbEx) {
                log.error("Exception mapping event or saving to Postgres! Event: {}", event, dbEx);
            }

            log.info("Completed processing event: {}", event);
        }

    //    @KafkaListener(topics = "${app.kafka.topic}", groupId = "wkshp-group")
//    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
//        String value = record.value() == null ? "" : record.value().trim();
//        String key = record.topic() + "-" + record.partition() + "-" + record.offset();
//
//        System.out.printf("DEBUG Value Received: '%s'%n", value);
//
//        if (value.startsWith("fail")) {
//            int attempts = retryCounts.getOrDefault(key, 0) + 1;
//            retryCounts.put(key, attempts);
//
//            System.out.printf("POISON PILL DETECTED partition=%d offset=%d key=%s value=%s [attempt %d]%n",
//                    record.partition(), record.offset(), record.key(), record.value(), attempts);
//
//            if (attempts < MAX_RETRIES) {
//                throw new RuntimeException("Poison pill encountered! Retry: " + attempts);
//            } else {
//                System.out.printf("MAX RETRIES REACHED: SENDING TO DEAD LETTER TOPIC partition=%d offset=%d key=%s value=%s%n",
//                        record.partition(), record.offset(), record.key(), record.value());
//                retryCounts.remove(key);
//
//                // Send to DLQ
//                kafkaTemplate.send("dead-letter-topic", record.key(), record.value());
//                ack.acknowledge();
//                return;
//            }
//        }
//
//        // Normal processing
//        try {
//            Thread.sleep(5000); // Simulate slow consumer
//        } catch (InterruptedException ignored) {}
//        System.out.printf("RECEIVED partition=%d offset=%d key=%s value=%s%n",
//                record.partition(), record.offset(), record.key(), record.value());
//        ack.acknowledge();
//    }
    }
