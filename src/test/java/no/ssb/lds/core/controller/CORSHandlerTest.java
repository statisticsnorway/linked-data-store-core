package no.ssb.lds.core.controller;


import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class CORSHandlerTest {

    volatile private HttpServerExchange httpServerExchange;
    volatile private HttpHandler customHandler;
    private OkHttpClient client;
    private Undertow server;
    private Request.Builder requestBuilder;

    /**
     * Find a free port for the undertow server.
     */
    private static int findFree() {
        int port = 0;
        while (port == 0) {
            ServerSocket socket = null;
            try {
                socket = new ServerSocket(0);
                port = socket.getLocalPort();
            } catch (IOException e) {
                throw new RuntimeException("Failed finding free port", e);
            } finally {
                try {
                    if (socket != null) socket.close();
                } catch (IOException ignore) {
                }
            }
        }
        return port;
    }

    @AfterMethod
    public void tearDown() {
        server.stop();
    }

    @BeforeMethod
    public void setUp() {

        httpServerExchange = null;
        customHandler = null;
        HttpHandler httpHandler = exchange -> {
            exchange.setStatusCode(222);
            if(customHandler != null ) {
                customHandler.handleRequest(exchange);
            }
            this.httpServerExchange = exchange;
        };

        List<Pattern> patterns = List.of(
                Pattern.compile("http://foo"),
                Pattern.compile("https://bar"),
                Pattern.compile("https?://(regexp|pxeger)"));
        CORSHandler corsHandler = new CORSHandler(httpHandler, patterns, true, 444,
                123456, Set.of("GET", "PUT"), Set.of("x-foo", "y-bar"));

        int port = findFree();
        server = Undertow.builder().addHttpListener(port, "localhost").setHandler(corsHandler).build();
        client = new OkHttpClient();
        server.start();

        requestBuilder = new Request.Builder().url("http://localhost:" + port)
                .addHeader("Origin", "http://foo");
    }

    @Test
    public void testThatExchangeIsForwarded() throws IOException {
        Request request = requestBuilder.get().build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(222);
            assertThat(response.header(CORSHandler.ACCESS_CONTROL_ALLOW_ORIGIN.toString())).isEqualTo("http://foo");
            assertThat(response.header(CORSHandler.ACCESS_CONTROL_EXPOSE_HEADERS.toString()))
                    .contains("y-bar").contains("x-foo").contains(",");
            assertThat(httpServerExchange).isNotNull();
        }
    }

    @Test
    public void testThatPreflightShortCircuitsHandler() throws IOException {
        Request request = requestBuilder.method("OPTIONS", null)
                .addHeader("Access-Control-Request-Method", "PUT")
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(444);
            assertThat(response.header(CORSHandler.ACCESS_CONTROL_ALLOW_METHODS.toString()))
                    .contains("PUT").contains("GET").contains(",");
            assertThat(response.header(CORSHandler.ACCESS_CONTROL_ALLOW_ORIGIN.toString())).isEqualTo("http://foo");
            assertThat(response.header(CORSHandler.ACCESS_CONTROL_ALLOW_HEADERS.toString()))
                    .contains("y-bar").contains("x-foo").contains(",");
            assertThat(response.header(CORSHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS.toString()))
                    .isEqualTo("true");
            assertThat(response.header(CORSHandler.ACCESS_CONTROL_MAX_AGE.toString()))
                    .isEqualTo("123456");
            assertThat(httpServerExchange).isNull();
        }
    }

    @Test
    public void testThatItIncludesVary() throws IOException {
        Request request = requestBuilder.get().build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.header(Headers.VARY_STRING)).isEqualTo("Origin");
            assertThat(httpServerExchange).isNotNull();
        }
    }

    @Test
    public void testThatItAppendsVary() throws IOException {
        Request request = requestBuilder.get().build();
        customHandler = exchange -> exchange.getResponseHeaders().add(Headers.VARY, "Foo");
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.headers(Headers.VARY_STRING))
                    .containsExactlyInAnyOrder("Origin", "Foo");
            assertThat(httpServerExchange).isNotNull();
        }
    }

    @Test
    public void testMatchesRegex() throws IOException {
        Request request1 = requestBuilder.get()
                .removeHeader("Origin")
                .addHeader("Origin", "http://regexp")
                .build();
        try (Response response = client.newCall(request1).execute()) {
            assertThat(response.header(CORSHandler.ACCESS_CONTROL_ALLOW_ORIGIN.toString()))
                    .isEqualTo("http://regexp");
            assertThat(httpServerExchange).isNotNull();
        }
        Request request2 = requestBuilder.get()
                .removeHeader("Origin")
                .addHeader("Origin", "https://pxeger")
                .build();
        try (Response response = client.newCall(request2).execute()) {
            assertThat(response.header(CORSHandler.ACCESS_CONTROL_ALLOW_ORIGIN.toString()))
                    .isEqualTo("https://pxeger");
            assertThat(httpServerExchange).isNotNull();
        }
    }
}