package com.dcfaight.kafkawkshp001;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public KafkaProducerController(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topic:test}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @PostMapping("/send")
    public String send(@RequestBody String message) throws Exception {
        var result = kafkaTemplate.send(topic, message).get(); // waits for ack
        return "sent to " + topic +
                " partition=" + result.getRecordMetadata().partition() +
                " offset=" + result.getRecordMetadata().offset();
    }

    @PostMapping("/send/{key}")
    public String sendWithKey(@PathVariable String key, @RequestBody String message) throws Exception {
        var result = kafkaTemplate.send(topic, key, message).get();
        return "sent key=" + key +
                " partition=" + result.getRecordMetadata().partition() +
                " offset=" + result.getRecordMetadata().offset();
    }

    @PostMapping("/send/{topic}/{key}")
    public ResponseEntity<?> sendMessageToTopic(
            @PathVariable String topic,
            @PathVariable String key,
            @RequestBody String value) {
        kafkaTemplate.send(topic, key, value);
        return ResponseEntity.ok("Message sent to " + topic + "!");
    }
}