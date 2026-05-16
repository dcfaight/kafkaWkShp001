package com.dcfaight.kafkawkshp001.networkEvents;

import java.time.Instant;

public record Alert(String alertType, String message, NetworkEvent event, Instant detectedAt) {
}
