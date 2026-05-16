package com.dcfaight.kafkawkshp001;

import java.time.OffsetDateTime;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class FirewallEventDTO {
    private OffsetDateTime timestamp;
    private String srcIp;
    private String dstIp;
    private Integer srcPort;
    private Integer dstPort;
    private String action;
    private String deviceId;
    private String message;

    public FirewallEventDTO() {}
}
