package com.dcfaight.kafkawkshp001;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaProducerControllerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaProducerController controller;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        controller = new KafkaProducerController(kafkaTemplate, "firewall.logs.raw");
    }

    @Test
    void send_returnsAckDetails() throws Exception {
        when(kafkaTemplate.send("firewall.logs.raw", "hello"))
                .thenReturn(CompletableFuture.completedFuture(sendResult("firewall.logs.raw", null, "hello", 0, 12L)));

        String response = controller.send("hello");

        assertEquals("sent to firewall.logs.raw partition=0 offset=12", response);
    }

    @Test
    void sendWithKey_returnsAckDetails() throws Exception {
        when(kafkaTemplate.send("firewall.logs.raw", "key-a", "value-a"))
                .thenReturn(CompletableFuture.completedFuture(sendResult("firewall.logs.raw", "key-a", "value-a", 1, 99L)));

        String response = controller.sendWithKey("key-a", "value-a");

        assertEquals("sent key=key-a partition=1 offset=99", response);
    }

    @Test
    void sendMessageToTopic_sendsWithoutBlocking() {
        ResponseEntity<?> response = controller.sendMessageToTopic("custom-topic", "my-key", "payload");

        verify(kafkaTemplate).send("custom-topic", "my-key", "payload");
        assertEquals(200, response.getStatusCode().value());
        assertEquals("Message sent to custom-topic!", response.getBody());
    }

    @Test
    void send_propagatesKafkaSendFailure() {
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("kafka unavailable"));
        when(kafkaTemplate.send("firewall.logs.raw", "hello")).thenReturn(failed);

        ExecutionException exception = assertThrows(ExecutionException.class, () -> controller.send("hello"));
        assertEquals("kafka unavailable", exception.getCause().getMessage());
    }

    private SendResult<String, String> sendResult(String topic, String key, String value, int partition, long offset) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(topic, partition),
                0L,
                (int) offset,
                System.currentTimeMillis(),
                0,
                0
        );
        return new SendResult<>(record, metadata);
    }
}
