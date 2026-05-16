package com.dcfaight.kafkawkshp001.firewall.persistence;

import com.dcfaight.kafkawkshp001.firewall.domain.FirewallEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FirewallEventRepository extends JpaRepository<FirewallEvent, Long> {}
