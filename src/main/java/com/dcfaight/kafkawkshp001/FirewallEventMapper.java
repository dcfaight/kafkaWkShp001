package com.dcfaight.kafkawkshp001;

public class FirewallEventMapper {
    public static FirewallEvent toEntity(FirewallEventDTO dto) {
        FirewallEvent entity = new FirewallEvent();
        entity.setTimestamp(dto.getTimestamp());
        entity.setSrcIp(dto.getSrcIp());
        entity.setDstIp(dto.getDstIp());
        entity.setSrcPort(dto.getSrcPort());
        entity.setDstPort(dto.getDstPort());
        entity.setAction(dto.getAction());
        entity.setDeviceId(dto.getDeviceId());
        entity.setMessage(dto.getMessage());
        return entity;
    }

    public static FirewallEventDTO toDTO(FirewallEvent entity) {
        FirewallEventDTO dto = new FirewallEventDTO();
        dto.setTimestamp(entity.getTimestamp());
        dto.setSrcIp(entity.getSrcIp());
        dto.setDstIp(entity.getDstIp());
        dto.setSrcPort(entity.getSrcPort());
        dto.setDstPort(entity.getDstPort());
        dto.setAction(entity.getAction());
        dto.setDeviceId(entity.getDeviceId());
        dto.setMessage(entity.getMessage());
        return dto;
    }
}