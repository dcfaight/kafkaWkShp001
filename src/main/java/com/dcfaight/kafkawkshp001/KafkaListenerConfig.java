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
                (record, ex) -> {
                    // Route DLQs: "firewall.logs.raw" → "firewall.logs.raw.dlq"
                    String dlqTopic = record.topic() + ".dlq";
                    return new TopicPartition(dlqTopic, record.partition());
                }
        );
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(0L, 0)
        );
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            System.out.println("DLQ HANDLER: Failed record will go to DLQ: key=" + record.key() + " val=" + record.value() + " reason=" + ex);
        });

        return new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0));

        // 3 retries, then DLQ
    }
}