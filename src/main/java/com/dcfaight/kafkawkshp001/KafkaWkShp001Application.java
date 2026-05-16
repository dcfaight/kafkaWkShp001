package com.dcfaight.kafkawkshp001;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
@OpenAPIDefinition(
        info = @Info(
                title = "Kafka Workshop API",
                version = "1.0",
                description = "REST endpoints for producing demo Kafka topics and testing DLQ flows",
                contact = @Contact(name = "dcfaight", email = "dcfaight@gmail.com"),
                license = @License(name = "Apache 2.0", url = "https://www.apache.org/licenses/LICENSE-2.0.html")

        )
)
@SpringBootApplication
public class KafkaWkShp001Application {

    public static void main(String[] args) {
        SpringApplication.run(KafkaWkShp001Application.class, args);
    }

}
