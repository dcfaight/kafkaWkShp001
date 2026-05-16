package com.dcfaight.kafkawkshp001;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ElasticClientTest {

    @Test
    void writeToElasticsearch_sendsRequest() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(201);
        when(response.body()).thenReturn("created");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);

        ElasticClient client = new ElasticClient(httpClient, "http://localhost:9200/index/_doc");

        assertDoesNotThrow(() -> client.writeToElasticsearch("{\"k\":\"v\"}"));

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));
        assertEquals("http://localhost:9200/index/_doc", requestCaptor.getValue().uri().toString());
    }

    @Test
    void writeToElasticsearch_handlesNon2xxResponse() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(500);
        when(response.body()).thenReturn("boom");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);

        ElasticClient client = new ElasticClient(httpClient, "http://localhost:9200/index/_doc");

        assertDoesNotThrow(() -> client.writeToElasticsearch("{\"k\":\"v\"}"));
    }

    @Test
    void writeToElasticsearch_handlesHttpClientException() throws Exception {
        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new IOException("connection refused"));

        ElasticClient client = new ElasticClient(httpClient, "http://localhost:9200/index/_doc");

        assertDoesNotThrow(() -> client.writeToElasticsearch("{\"k\":\"v\"}"));
    }
}
