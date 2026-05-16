package com.dcfaight.kafkawkshp001.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class KafkaListenerConfigTest {

    @Test
    void errorHandler_returnsConfiguredHandler() {
        KafkaListenerConfig config = new KafkaListenerConfig();

        DefaultErrorHandler handler = config.errorHandler(mock(KafkaTemplate.class));

        assertNotNull(handler);
    }

    @Test
    void resolveDestination_delegatesToDlqPartition() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("firewall.logs.raw", 2, 0L, "k", "v");

        TopicPartition partition = KafkaListenerConfig.resolveDestination(record, new RuntimeException("err"));

        assertEquals("firewall.logs.raw.dlq", partition.topic());
        assertEquals(2, partition.partition());
    }

    @Test
    void dlqPartition_appendsDlqSuffix() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("firewall.logs.raw", 2, 0L, "k", "v");

        TopicPartition partition = KafkaListenerConfig.dlqPartition(record);

        assertEquals("firewall.logs.raw.dlq", partition.topic());
        assertEquals(2, partition.partition());
    }
}
