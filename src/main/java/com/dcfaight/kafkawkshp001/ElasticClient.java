package com.dcfaight.kafkawkshp001;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ElasticClient {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public void writeToElasticsearch(String eventJson) {
        String elasticUrl = "http://192.168.1.5:9200/firewall-events-000001/_doc";
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = null;

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(elasticUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(eventJson))
                    .build();

            response = client.send(request, HttpResponse.BodyHandlers.ofString());

            log.info("ES response status: {}", response.statusCode());
            log.info("ES response body: {}", response.body());

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                log.error("Elasticsearch rejected document! Status: {}, Body: {}",
                        response.statusCode(), response.body());
            }
        } catch (Exception e) {
            log.error("Exception writing to Elasticsearch!", e);
            log.error("Event JSON: {}", eventJson);
            if (response != null) {
                log.error("Partial/failed ES response status: {}", response.statusCode());
                log.error("Partial/failed ES response body: {}", response.body());
            }
        }
    }
}
