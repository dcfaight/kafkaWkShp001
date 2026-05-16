package com.dcfaight.kafkawkshp001.networkevents;

import java.time.Instant;

public record Alert(String alertType, String message, NetworkEvent event, Instant detectedAt) {
}
