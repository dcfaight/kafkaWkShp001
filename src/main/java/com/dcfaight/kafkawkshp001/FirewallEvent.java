package com.dcfaight.kafkawkshp001;

import jakarta.persistence.*;
import lombok.*;


import java.time.OffsetDateTime;

@Entity
@Table(name = "firewall_events")
@Getter
@Setter
@NoArgsConstructor
@ToString // Optional: for logging/debugging
public class FirewallEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "timestamp")
    private OffsetDateTime timestamp;

    @Column(name = "src_ip")
    private String srcIp;

    @Column(name = "dst_ip")
    private String dstIp;

    @Column(name = "src_port")
    private Integer srcPort;

    @Column(name = "dst_port")
    private Integer dstPort;

    private String action;

    @Column(name = "device_id")
    private String deviceId;

    private String message;
}