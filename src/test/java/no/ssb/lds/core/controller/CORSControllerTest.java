package no.ssb.lds.core.controller;

import no.ssb.lds.test.client.ResponseHelper;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Ignore;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Listeners(TestServerListener.class)
public class CORSControllerTest {

    private static final Logger LOG = LoggerFactory.getLogger(CORSControllerTest.class);

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

    static {
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
    }

    @Test
    public void thatCORSOptionsRespondsWithCORSHeaders() throws IOException {
            URL url = new URL(server.testURL(""));
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("OPTIONS");
            conn.setRequestProperty("Origin", "http://example.com");
            conn.setRequestProperty("Access-Control-Request-Method", "PUT");
            conn.setRequestProperty("Access-Control-Request-Headers", "X-Requested-With");
            conn.connect();
            int code = conn.getResponseCode();
            assertEquals(Integer.valueOf(code), Integer.valueOf(HTTP_ACCEPTED));
            assertEquals(conn.getHeaderField("access-control-allow-methods"), "GET, PUT, DELETE, OPTIONS, HEAD");
            assertEquals(conn.getHeaderField("access-control-allow-headers"), "Content-Type");
            assertEquals(conn.getHeaderField("access-control-allow-origin"), "http://localhost:9090,http://0.0.0.0:9090");
            conn.disconnect();
    }

    // TODO figure out how to circumvent the "not allowed to set restricted header 'Origin'). Ref jdk.internal.net.http.common.Utils#132
    @Ignore
    @Test(expectedExceptions = {IllegalArgumentException.class, RuntimeException.class})
    public void thatCORSOptionsRespondsWithCORSHeadersFailsWithJavaNetHttpClient() {
        ResponseHelper<String> response = client.options("/", "Origin", "http://example.com", "Access-Control-Request-Method", "PUT", "Access-Control-Request-Headers", "X-Requested-With");
        response.expectAnyOf(HTTP_ACCEPTED);
        assertEquals(response.response().headers().map().get("access-control-allow-methods").get(0), "GET, PUT, DELETE, OPTIONS, HEAD");
        assertEquals(response.response().headers().map().get("access-control-allow-headers").get(0), "Content-Type");
        assertEquals(response.response().headers().map().get("access-control-allow-origin").get(0), "http://localhost:9090");
    }

    @Test
    public void that404NotFoundDoesNotLeakCORSHeaders() {
        ResponseHelper<String> response = client.get("/");
        response.expect404NotFound();
        assertTrue(response.response().headers().map().get("access-control-allow-methods") == null);
        assertTrue(response.response().headers().map().get("access-control-allow-headers") == null);
        assertTrue(response.response().headers().map().get("access-control-allow-origin") == null);
    }

    @Test
    public void thatPing200OkHasCORSHeaders() {
        ResponseHelper<String> response = client.get("/ping");
        response.expect200Ok();
        assertEquals(response.response().headers().map().get("access-control-allow-methods").get(0), "GET, PUT, DELETE, OPTIONS, HEAD");
        assertEquals(response.response().headers().map().get("access-control-allow-headers").get(0), "Content-Type");
        assertEquals(response.response().headers().map().get("access-control-allow-origin").get(0), "http://localhost:9090,http://0.0.0.0:9090");
    }
}
