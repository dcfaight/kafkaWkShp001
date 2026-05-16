package com.dcfaight.kafkawkshp001.firewall.service;

import com.dcfaight.kafkawkshp001.firewall.domain.FirewallEvent;
import com.dcfaight.kafkawkshp001.firewall.persistence.FirewallEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FirewallEventServiceTest {

    @Mock
    private FirewallEventRepository repository;

    @InjectMocks
    private FirewallEventService service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        ReflectionTestUtils.setField(service, "repository", repository);
    }

    @Test
    void save_returnsSavedEntity() {
        FirewallEvent event = new FirewallEvent();
        FirewallEvent saved = new FirewallEvent();
        saved.setId(42L);

        when(repository.save(event)).thenReturn(saved);

        FirewallEvent result = service.save(event);

        assertSame(saved, result);
        verify(repository).save(event);
    }

    @Test
    void save_returnsNullWhenRepositoryThrows() {
        FirewallEvent event = new FirewallEvent();
        when(repository.save(any())).thenThrow(new RuntimeException("db down"));

        FirewallEvent result = service.save(event);

        assertNull(result);
        verify(repository).save(event);
    }

    @Test
    void findAll_delegatesToRepository() {
        FirewallEvent first = new FirewallEvent();
        FirewallEvent second = new FirewallEvent();
        when(repository.findAll()).thenReturn(List.of(first, second));

        List<FirewallEvent> result = service.findAll();

        assertEquals(2, result.size());
        assertSame(first, result.get(0));
        assertSame(second, result.get(1));
        verify(repository).findAll();
    }
}
