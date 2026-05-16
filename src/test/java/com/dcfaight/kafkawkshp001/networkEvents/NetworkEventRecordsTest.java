package com.dcfaight.kafkawkshp001.networkEvents;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NetworkEventRecordsTest {

    @Test
    void networkEventRecord_accessorsWork() {
        Instant now = Instant.parse("2026-05-16T13:00:00Z");
        NetworkEvent event = new NetworkEvent("1.1.1.1", "2.2.2.2", "example.com", 9, now);

        assertEquals("1.1.1.1", event.sourceIp());
        assertEquals("2.2.2.2", event.destIp());
        assertEquals("example.com", event.domain());
        assertEquals(9, event.score());
        assertEquals(now, event.timestamp());
    }

    @Test
    void alertRecord_accessorsWork() {
        Instant now = Instant.parse("2026-05-16T13:00:00Z");
        NetworkEvent event = new NetworkEvent("1.1.1.1", "2.2.2.2", "example.com", 9, now);
        Alert alert = new Alert("THREAT_SCORE", "IP exceeded threshold", event, now);

        assertEquals("THREAT_SCORE", alert.alertType());
        assertEquals("IP exceeded threshold", alert.message());
        assertEquals(event, alert.event());
        assertEquals(now, alert.detectedAt());
    }
}
