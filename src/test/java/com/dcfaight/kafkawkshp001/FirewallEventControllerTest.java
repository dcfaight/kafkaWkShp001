package com.dcfaight.kafkawkshp001;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class FirewallEventControllerTest {

    private MockMvc mockMvc;
    private FirewallEventService service;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        service = Mockito.mock(FirewallEventService.class);
        FirewallEventController controller = new FirewallEventController(service);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();

        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    void getFirewallEvents_returnsMappedDtos() throws Exception {
        OffsetDateTime firstTs = OffsetDateTime.parse("2026-05-16T08:00:00+00:00");
        OffsetDateTime secondTs = OffsetDateTime.parse("2026-05-16T09:00:00+00:00");

        FirewallEvent first = new FirewallEvent();
        first.setTimestamp(firstTs);
        first.setSrcIp("10.0.0.1");
        first.setDstIp("10.0.0.2");
        first.setSrcPort(12345);
        first.setDstPort(80);
        first.setAction("ALLOW");
        first.setDeviceId("fw-a");
        first.setMessage("http allow");

        FirewallEvent second = new FirewallEvent();
        second.setTimestamp(secondTs);
        second.setSrcIp("10.0.0.3");
        second.setDstIp("10.0.0.4");
        second.setSrcPort(54321);
        second.setDstPort(25);
        second.setAction("DENY");
        second.setDeviceId("fw-b");
        second.setMessage("smtp deny");

        when(service.findAll()).thenReturn(List.of(first, second));

        mockMvc.perform(get("/firewall-events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].timestamp").value("2026-05-16T08:00:00Z"))
                .andExpect(jsonPath("$[0].srcIp").value("10.0.0.1"))
                .andExpect(jsonPath("$[0].dstIp").value("10.0.0.2"))
                .andExpect(jsonPath("$[0].srcPort").value(12345))
                .andExpect(jsonPath("$[0].dstPort").value(80))
                .andExpect(jsonPath("$[0].action").value("ALLOW"))
                .andExpect(jsonPath("$[0].deviceId").value("fw-a"))
                .andExpect(jsonPath("$[0].message").value("http allow"))
                .andExpect(jsonPath("$[1].timestamp").value("2026-05-16T09:00:00Z"))
                .andExpect(jsonPath("$[1].srcIp").value("10.0.0.3"))
                .andExpect(jsonPath("$[1].dstIp").value("10.0.0.4"))
                .andExpect(jsonPath("$[1].srcPort").value(54321))
                .andExpect(jsonPath("$[1].dstPort").value(25))
                .andExpect(jsonPath("$[1].action").value("DENY"))
                .andExpect(jsonPath("$[1].deviceId").value("fw-b"))
                .andExpect(jsonPath("$[1].message").value("smtp deny"));

        verify(service, times(1)).findAll();
    }

    @Test
    void postFirewallEvents_mapsDtoToEntityAndReturnsMappedDto() throws Exception {
        OffsetDateTime requestTs = OffsetDateTime.parse("2026-05-16T10:10:10+00:00");
        OffsetDateTime savedTs = OffsetDateTime.parse("2026-05-16T10:10:11+00:00");

        FirewallEventDTO requestDto = new FirewallEventDTO();
        requestDto.setTimestamp(requestTs);
        requestDto.setSrcIp("192.168.10.10");
        requestDto.setDstIp("192.168.10.20");
        requestDto.setSrcPort(40000);
        requestDto.setDstPort(8080);
        requestDto.setAction("ALLOW");
        requestDto.setDeviceId("fw-post");
        requestDto.setMessage("allow app traffic");

        FirewallEvent saved = new FirewallEvent();
        saved.setId(99L);
        saved.setTimestamp(savedTs);
        saved.setSrcIp("192.168.10.10");
        saved.setDstIp("192.168.10.20");
        saved.setSrcPort(40000);
        saved.setDstPort(8080);
        saved.setAction("ALLOW");
        saved.setDeviceId("fw-post");
        saved.setMessage("saved message");

        when(service.save(any(FirewallEvent.class))).thenReturn(saved);

        mockMvc.perform(post("/firewall-events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requestDto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.timestamp").value(savedTs.toString()))
                .andExpect(jsonPath("$.srcIp").value("192.168.10.10"))
                .andExpect(jsonPath("$.dstIp").value("192.168.10.20"))
                .andExpect(jsonPath("$.srcPort").value(40000))
                .andExpect(jsonPath("$.dstPort").value(8080))
                .andExpect(jsonPath("$.action").value("ALLOW"))
                .andExpect(jsonPath("$.deviceId").value("fw-post"))
                .andExpect(jsonPath("$.message").value("saved message"));

        ArgumentCaptor<FirewallEvent> captor = ArgumentCaptor.forClass(FirewallEvent.class);
        verify(service, times(1)).save(captor.capture());
        FirewallEvent captured = captor.getValue();

        assertEquals(requestTs, captured.getTimestamp());
        assertEquals("192.168.10.10", captured.getSrcIp());
        assertEquals("192.168.10.20", captured.getDstIp());
        assertEquals(40000, captured.getSrcPort());
        assertEquals(8080, captured.getDstPort());
        assertEquals("ALLOW", captured.getAction());
        assertEquals("fw-post", captured.getDeviceId());
        assertEquals("allow app traffic", captured.getMessage());
    }
}