package com.dcfaight.kafkawkshp001;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FirewallEventModelTest {

    @Test
    void firewallEvent_gettersSettersAndToStringWork() {
        OffsetDateTime timestamp = OffsetDateTime.parse("2026-05-16T12:34:56Z");
        FirewallEvent event = new FirewallEvent();
        event.setId(7L);
        event.setTimestamp(timestamp);
        event.setSrcIp("10.0.0.1");
        event.setDstIp("10.0.0.2");
        event.setSrcPort(1000);
        event.setDstPort(2000);
        event.setAction("ALLOW");
        event.setDeviceId("fw-xyz");
        event.setMessage("hello");

        assertEquals(7L, event.getId());
        assertEquals(timestamp, event.getTimestamp());
        assertEquals("10.0.0.1", event.getSrcIp());
        assertEquals("10.0.0.2", event.getDstIp());
        assertEquals(1000, event.getSrcPort());
        assertEquals(2000, event.getDstPort());
        assertEquals("ALLOW", event.getAction());
        assertEquals("fw-xyz", event.getDeviceId());
        assertEquals("hello", event.getMessage());
        assertNotNull(event.toString());
    }

    @Test
    void firewallEventDto_gettersSettersWork() {
        OffsetDateTime timestamp = OffsetDateTime.parse("2026-05-16T12:34:56Z");
        FirewallEventDTO dto = new FirewallEventDTO();
        dto.setTimestamp(timestamp);
        dto.setSrcIp("10.1.0.1");
        dto.setDstIp("10.1.0.2");
        dto.setSrcPort(3000);
        dto.setDstPort(4000);
        dto.setAction("DENY");
        dto.setDeviceId("fw-abc");
        dto.setMessage("blocked");

        assertEquals(timestamp, dto.getTimestamp());
        assertEquals("10.1.0.1", dto.getSrcIp());
        assertEquals("10.1.0.2", dto.getDstIp());
        assertEquals(3000, dto.getSrcPort());
        assertEquals(4000, dto.getDstPort());
        assertEquals("DENY", dto.getAction());
        assertEquals("fw-abc", dto.getDeviceId());
        assertEquals("blocked", dto.getMessage());
    }
}
