package com.dcfaight.kafkawkshp001;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FirewallEventRepository extends JpaRepository<FirewallEvent, Long> {}