package com.dcfaight.kafkawkshp001;

import org.junit.jupiter.api.Test;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;

import static org.mockito.Mockito.mockStatic;

class KafkaWkShp001ApplicationTests {

    @Test
    void applicationClass_hasOpenApiDefinition() {
        OpenAPIDefinition annotation = KafkaWkShp001Application.class.getAnnotation(OpenAPIDefinition.class);
        org.junit.jupiter.api.Assertions.assertNotNull(annotation);
    }

    @Test
    void applicationClass_canBeInstantiated() {
        org.junit.jupiter.api.Assertions.assertNotNull(new KafkaWkShp001Application());
    }

    @Test
    void main_delegatesToSpringApplicationRun() {
        try (MockedStatic<SpringApplication> springApp = mockStatic(SpringApplication.class)) {
            KafkaWkShp001Application.main(new String[]{"--debug"});
            springApp.verify(() -> SpringApplication.run(KafkaWkShp001Application.class, new String[]{"--debug"}));
        }
    }

}
