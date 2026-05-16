package com.dcfaight.kafkawkshp001.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaListenerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                KafkaListenerConfig::resolveDestination
        );
        return new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0));
    }

    static TopicPartition resolveDestination(ConsumerRecord<?, ?> record, Exception ex) {
        return dlqPartition(record);
    }

    static TopicPartition dlqPartition(ConsumerRecord<?, ?> record) {
        return new TopicPartition(record.topic() + ".dlq", record.partition());
    }
}
