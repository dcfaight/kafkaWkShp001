package com.dcfaight.kafkawkshp001.networkEvents;

import java.time.Instant;

public record NetworkEvent(String sourceIp, String destIp, String domain, int score, Instant timestamp) {
}
