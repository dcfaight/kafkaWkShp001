package com.dcfaight.kafkawkshp001.firewall.api;

import com.dcfaight.kafkawkshp001.firewall.domain.FirewallEvent;
import com.dcfaight.kafkawkshp001.firewall.dto.FirewallEventDTO;
import com.dcfaight.kafkawkshp001.firewall.mapper.FirewallEventMapper;
import com.dcfaight.kafkawkshp001.firewall.service.FirewallEventService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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
