//package com.dcfaight.kafkawkshp001;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.support.Acknowledgment;
//import org.springframework.stereotype.Service;
//
//@Service
//public class DlqReplayer {
//
//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;
//
//    private final ObjectMapper objectMapper = new ObjectMapper();
//
//    // Define all required fields here
//    private static final String[] REQUIRED_FIELDS = {"timestamp", "srcIp", "dstIp", "action"};
//
////    @KafkaListener(
////            topics = "firewall.logs.raw.dlq",
////            groupId = "dlq-replayer"
////    )
//    public void replayDlq(ConsumerRecord<String, String> record, Acknowledgment ack) {
//        String value = record.value();
//        String key = record.key();
//
//        try {
//            JsonNode event = objectMapper.readTree(value);
//
//            // Patch: Add empty string for any missing fields
//            if (event.isObject()) {
//                ObjectNode patched = (ObjectNode) event;
//                boolean patchedSomething = false;
//                for (String field : REQUIRED_FIELDS) {
//                    if (!patched.has(field) || patched.get(field).isNull()) {
//                        patched.put(field, "");
//                        patchedSomething = true;
//                        System.out.println("Auto-patched missing field: " + field);
//                    }
//                }
//
//                String newValue = patched.toString();
//                if (patchedSomething) {
//                    System.out.println("Replaying DLQ message with patched fields: " + newValue);
//                } else {
//                    System.out.println("Replaying DLQ message (no patch needed): " + value);
//                }
//                kafkaTemplate.send("firewall.logs.raw", key, newValue);
//            } else {
//                System.out.println("SKIPPING non-object JSON event: " + value);
//            }
//        } catch (Exception e) {
//            System.out.println("SKIPPING non-JSON DLQ message: " + value + " due to: " + e.getMessage());
//        } finally {
//            ack.acknowledge();
//        }
//    }
//}