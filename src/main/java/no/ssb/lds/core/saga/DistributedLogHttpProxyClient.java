package no.ssb.lds.core.saga;

import no.ssb.saga.execution.sagalog.SagaLog;
import no.ssb.saga.execution.sagalog.SagaLogEntry;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;

class DistributedLogHttpProxyClient implements SagaLog {

    private final HttpClient client = HttpClient.newBuilder().build();
    private final String baseurl;
    private final URI uri;

    DistributedLogHttpProxyClient(String baseurl) {
        this.baseurl = baseurl.endsWith("/") ? baseurl : baseurl + "/";
        try {
            uri = new URI(this.baseurl + "sagalog-1");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String write(SagaLogEntry entry) {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/octet-stream")
                .PUT(HttpRequest.BodyPublishers.ofString(entry.toString(), StandardCharsets.UTF_8))
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || 300 <= response.statusCode()) {
                throw new RuntimeException("Write to distributedlog-http-proxy failed. StatusCode: " + response.statusCode());
            }
            String json = response.body();
            System.out.println(json);
            return json;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SagaLogEntry> readEntries(String executionId) {
        throw new UnsupportedOperationException("TODO: implement saga-log read");
    }
}
