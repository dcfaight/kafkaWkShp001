package com.dcfaight.kafkawkshp001.config;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class KafkaListenerConfigTest {

    @Test
    void errorHandler_returnsConfiguredHandler() {
        KafkaListenerConfig config = new KafkaListenerConfig();

        DefaultErrorHandler handler = config.errorHandler(mock(KafkaTemplate.class));

        assertNotNull(handler);
    }
}
