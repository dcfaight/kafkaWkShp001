package com.dcfaight.kafkawkshp001.networkevents;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class NetworkEventConsumerTest {

    @Test
    void consumer_canBeCreatedAndWired() {
        NetworkEventConsumer consumer = new NetworkEventConsumer();
        ReflectionTestUtils.setField(consumer, "alertTemplate", mock(KafkaTemplate.class));

        assertNotNull(consumer);
    }
}
