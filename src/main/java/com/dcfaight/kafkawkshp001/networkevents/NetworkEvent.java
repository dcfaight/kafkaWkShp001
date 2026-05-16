package com.dcfaight.kafkawkshp001.networkevents;

import java.time.Instant;

public record NetworkEvent(String sourceIp, String destIp, String domain, int score, Instant timestamp) {
}
