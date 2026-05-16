package com.dcfaight.kafkawkshp001.networkEvents;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;

//@Component
public class NetworkEventConsumer {

//    @Autowired
    private KafkaTemplate<String, Alert> alertTemplate; // you can serialize Alert as JSON

//    @KafkaListener(topics = "network-events")
//    public void handle(NetworkEvent event) {
//        if (event.score() >= 8) {
//            Alert alert = new Alert("THREAT_SCORE",
//                    "IP exceeded threshold", event, Instant.now());
//            alertTemplate.send("alerts", event.sourceIp(), alert);
//        }
//        // Add more rules as needed
//    }
}
