package com.dcfaight.kafkawkshp001;

import org.junit.jupiter.api.Test;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;

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

}
