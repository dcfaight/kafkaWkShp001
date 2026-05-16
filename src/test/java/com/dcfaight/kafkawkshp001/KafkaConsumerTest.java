package com.dcfaight.kafkawkshp001;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class KafkaConsumerTest {

    private FirewallEventService service;
    private ElasticClient elasticClient;
    private KafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        service = Mockito.mock(FirewallEventService.class);
        elasticClient = Mockito.mock(ElasticClient.class);
        consumer = new KafkaConsumer(new ObjectMapper(), service, Runnable::run, elasticClient);
    }

    @Test
    void onMessage_dispatchesToExecutor() {
        KafkaConsumer spyConsumer = Mockito.spy(new KafkaConsumer(new ObjectMapper(), service, Runnable::run, elasticClient));
        doNothing().when(spyConsumer).processEvent("payload");

        spyConsumer.onMessage("payload");

        verify(spyConsumer).processEvent("payload");
    }

    @Test
    void processEvent_ignoresInvalidJson() {
        consumer.processEvent("not json");

        verify(elasticClient, never()).writeToElasticsearch(anyString());
        verify(service, never()).save(Mockito.any());
    }

    @Test
    void processEvent_ignoresMissingRequiredFields() {
        consumer.processEvent("{\"timestamp\":\"2026-05-16T10:00:00Z\",\"src_ip\":\"1.1.1.1\"}");

        verify(elasticClient, never()).writeToElasticsearch(anyString());
        verify(service, never()).save(Mockito.any());
    }

    @Test
    void processEvent_mapsSnakeCaseEventAndPersists() {
        FirewallEvent saved = new FirewallEvent();
        saved.setId(10L);
        Mockito.when(service.save(Mockito.any(FirewallEvent.class))).thenReturn(saved);

        consumer.processEvent("""
                {
                  "@timestamp":"2026-05-16T12:00:00Z",
                  "src_ip":"10.0.0.1",
                  "dst_ip":"10.0.0.2",
                  "src_port":1234,
                  "dst_port":443,
                  "action":"ALLOW",
                  "device_id":"fw-1",
                  "message":"ok"
                }
                """);

        verify(elasticClient).writeToElasticsearch(anyString());
        ArgumentCaptor<FirewallEvent> captor = ArgumentCaptor.forClass(FirewallEvent.class);
        verify(service).save(captor.capture());
        FirewallEvent entity = captor.getValue();
        assertEquals("10.0.0.1", entity.getSrcIp());
        assertEquals("10.0.0.2", entity.getDstIp());
        assertEquals(1234, entity.getSrcPort());
        assertEquals(443, entity.getDstPort());
        assertEquals("ALLOW", entity.getAction());
        assertEquals("fw-1", entity.getDeviceId());
        assertEquals("ok", entity.getMessage());
    }

    @Test
    void processEvent_mapsCamelCaseEventAndAllowsNullSaveResult() {
        Mockito.when(service.save(Mockito.any(FirewallEvent.class))).thenReturn(null);

        consumer.processEvent("""
                {
                  "timestamp":"2026-05-16T12:00:00Z",
                  "srcIp":"10.10.0.1",
                  "dstIp":"10.10.0.2",
                  "srcPort":"2222",
                  "dstPort":"22",
                  "action":"DENY",
                  "deviceId":"fw-2",
                  "message":"blocked"
                }
                """);

        ArgumentCaptor<FirewallEvent> captor = ArgumentCaptor.forClass(FirewallEvent.class);
        verify(service).save(captor.capture());
        FirewallEvent entity = captor.getValue();
        assertEquals("10.10.0.1", entity.getSrcIp());
        assertEquals("10.10.0.2", entity.getDstIp());
        assertEquals(2222, entity.getSrcPort());
        assertEquals(22, entity.getDstPort());
        assertEquals("DENY", entity.getAction());
        assertEquals("fw-2", entity.getDeviceId());
        assertEquals("blocked", entity.getMessage());
    }

    @Test
    void processEvent_handlesMappingFailureAfterElasticsearchWrite() {
        consumer.processEvent("""
                {
                  "timestamp":"not-a-time",
                  "srcIp":"10.10.0.1",
                  "dstIp":"10.10.0.2",
                  "action":"DENY"
                }
                """);

        verify(elasticClient).writeToElasticsearch(anyString());
        verify(service, never()).save(Mockito.any());
    }
}
