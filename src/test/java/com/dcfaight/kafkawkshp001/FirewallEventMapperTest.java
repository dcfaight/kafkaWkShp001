package com.dcfaight.kafkawkshp001;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FirewallEventMapperTest {

    @Test
    void mapperClass_canBeInstantiated() {
        FirewallEventMapper mapper = new FirewallEventMapper();
        org.junit.jupiter.api.Assertions.assertNotNull(mapper);
    }

    @Test
    void toEntity_mapsAllFields() {
        OffsetDateTime timestamp = OffsetDateTime.parse("2026-05-16T10:15:30+00:00");

        FirewallEventDTO dto = new FirewallEventDTO();
        dto.setTimestamp(timestamp);
        dto.setSrcIp("10.0.0.1");
        dto.setDstIp("10.0.0.2");
        dto.setSrcPort(44321);
        dto.setDstPort(443);
        dto.setAction("ALLOW");
        dto.setDeviceId("fw-001");
        dto.setMessage("allowed tls traffic");

        FirewallEvent entity = FirewallEventMapper.toEntity(dto);

        assertEquals(timestamp, entity.getTimestamp());
        assertEquals("10.0.0.1", entity.getSrcIp());
        assertEquals("10.0.0.2", entity.getDstIp());
        assertEquals(44321, entity.getSrcPort());
        assertEquals(443, entity.getDstPort());
        assertEquals("ALLOW", entity.getAction());
        assertEquals("fw-001", entity.getDeviceId());
        assertEquals("allowed tls traffic", entity.getMessage());
    }

    @Test
    void toDto_mapsAllFields() {
        OffsetDateTime timestamp = OffsetDateTime.parse("2026-05-16T11:22:33+00:00");

        FirewallEvent entity = new FirewallEvent();
        entity.setTimestamp(timestamp);
        entity.setSrcIp("192.168.1.10");
        entity.setDstIp("172.16.0.5");
        entity.setSrcPort(51515);
        entity.setDstPort(22);
        entity.setAction("DENY");
        entity.setDeviceId("fw-002");
        entity.setMessage("blocked ssh traffic");

        FirewallEventDTO dto = FirewallEventMapper.toDTO(entity);

        assertEquals(timestamp, dto.getTimestamp());
        assertEquals("192.168.1.10", dto.getSrcIp());
        assertEquals("172.16.0.5", dto.getDstIp());
        assertEquals(51515, dto.getSrcPort());
        assertEquals(22, dto.getDstPort());
        assertEquals("DENY", dto.getAction());
        assertEquals("fw-002", dto.getDeviceId());
        assertEquals("blocked ssh traffic", dto.getMessage());
    }
}
