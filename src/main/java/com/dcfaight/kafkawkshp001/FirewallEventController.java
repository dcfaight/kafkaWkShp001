package com.dcfaight.kafkawkshp001;

import org.springframework.web.bind.annotation.*;

import java.util.List;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Operation;

@RestController
@RequestMapping("/firewall-events")
@Tag(name = "Firewall Events", description = "API for firewall event logs")
public class FirewallEventController {

    private final FirewallEventService service;

    public FirewallEventController(FirewallEventService service) {
        this.service = service;
    }

    // Create a new firewall event
    @PostMapping
    public FirewallEventDTO createEvent(@RequestBody FirewallEventDTO dto) {
        // Map DTO to entity
        FirewallEvent entity = FirewallEventMapper.toEntity(dto);
        // Save using service
        FirewallEvent saved = service.save(entity);
        // Map saved entity back to DTO
        return FirewallEventMapper.toDTO(saved);
    }

    // Get all firewall events
    @GetMapping
    @Operation(summary = "Get all firewall events",
            description = "Retrieves a list of all firewall events from the database.")
    public List<FirewallEventDTO> getAll() {
        return service.findAll().stream()
                .map(FirewallEventMapper::toDTO)
                .toList();
    }
}
