package com.dcfaight.kafkawkshp001;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class FirewallEventService {
    private static final Logger log = LoggerFactory.getLogger(FirewallEventService.class);

    @Autowired
    private FirewallEventRepository repository;

    public FirewallEvent save(FirewallEvent event) {
        log.info("Saving FirewallEvent: {}", event);
FirewallEvent saved = null;
        try {
            saved = repository.save(event);

        log.info("Saved FirewallEvent with ID: {}", saved.getId());
        }catch(Exception ex){
            System.out.println("problem with save in service class!");
        }
        return saved;

    }

    // Add this method for retrieval
    public List<FirewallEvent> findAll() {
        return repository.findAll();
    }
}