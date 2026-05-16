package com.dcfaight.kafkawkshp001.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

class KafkaConsumerConfigTest {

    @Test
    void consumerFactory_setsExpectedProperties() {
        KafkaConsumerConfig config = new KafkaConsumerConfig();
        ReflectionTestUtils.setField(config, "bootstrapServers", "localhost:9092");

        ConsumerFactory<String, String> consumerFactory = config.consumerFactory();

        assertEquals("localhost:9092",
                consumerFactory.getConfigurationProperties().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("nd-parser",
                consumerFactory.getConfigurationProperties().get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(false,
                consumerFactory.getConfigurationProperties().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(StringDeserializer.class,
                consumerFactory.getConfigurationProperties().get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(StringDeserializer.class,
                consumerFactory.getConfigurationProperties().get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    }

    @Test
    void kafkaListenerContainerFactory_setsAckModeAndErrorHandler() {
        KafkaConsumerConfig config = new KafkaConsumerConfig();
        ReflectionTestUtils.setField(config, "bootstrapServers", "localhost:9092");
        DefaultErrorHandler errorHandler = mock(DefaultErrorHandler.class);

        var factory = config.kafkaListenerContainerFactory(
                config.consumerFactory(),
                errorHandler
        );

        assertNotNull(factory.getConsumerFactory());
        assertEquals(ContainerProperties.AckMode.MANUAL_IMMEDIATE, factory.getContainerProperties().getAckMode());
        assertSame(errorHandler, ReflectionTestUtils.getField(factory, "commonErrorHandler"));
    }
}
