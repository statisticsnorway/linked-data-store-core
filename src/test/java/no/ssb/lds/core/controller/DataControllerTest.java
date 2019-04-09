package no.ssb.lds.core.controller;

import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.time.Duration;

import static no.ssb.lds.core.utils.FileAndClasspathReaderUtils.readFileOrClasspathResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
@Test(singleThreaded = true)
public class DataControllerTest {

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

    private OkHttpClient okHttpClient;
    private Request.Builder request = new Request.Builder();

    @BeforeMethod
    public void setUp() {
        okHttpClient = new OkHttpClient.Builder()
                .callTimeout(Duration.ofDays(1))
                .readTimeout(Duration.ofDays(1))
                .writeTimeout(Duration.ofDays(1))
                .connectTimeout(Duration.ofDays(1))
                .build();
    }

    private HttpUrl.Builder newUrl() {
        return new HttpUrl.Builder().scheme("http").host(server.getTestServerHost())
                .port(server.getTestServerServicePort());
    }

    @Test(enabled = false)
    public void testGetEmptyDomainList() throws IOException {
        HttpUrl listContactUrl = newUrl().addPathSegments("data/contact").build();
        Request listContactRequest = request.url(listContactUrl).build();
        try (Response response = okHttpClient.newCall(listContactRequest).execute()) {
            assertThat(response.code())
                    .as("response code of %s", listContactUrl)
                    .isEqualTo(StatusCodes.OK);
            assertThat(response.header(Headers.CONTENT_TYPE_STRING))
                    .as("content type header in response to %s", listContactUrl)
                    .startsWith("application/json");
            assertThat(response.body().string()).isEqualTo("[]");
        }
    }

    @Test(dependsOnMethods = "testGetEmptyDomainList", enabled = false)
    public void testPutDomain() throws IOException {
        HttpUrl putContact = newUrl()
                .addPathSegments("data/contact/donald")
                .query("sync")
                .build();

        RequestBody body = RequestBody.create(
                MediaType.get("application/json"),
                readFileOrClasspathResource("demo/4-donald.json")
        );

        Request listContactRequest = request.url(putContact).put(body).build();
        try (Response response = okHttpClient.newCall(listContactRequest).execute()) {
            assertThat(response.code())
                    .as("response code of %s", putContact)
                    .isEqualTo(StatusCodes.CREATED);
        }
    }

    @Ignore
    @Test(dependsOnMethods = "testPutDomain", enabled = false)
    public void testGetDomainList() throws IOException {
        HttpUrl getContacts = newUrl().addPathSegments("data/contact").build();
        Request listContactRequest = request.url(getContacts).get().build();
        try (Response response = okHttpClient.newCall(listContactRequest).execute()) {
            assertThat(response.code())
                    .as("response code in response to %s", getContacts)
                    .isEqualTo(StatusCodes.OK);
            assertThat(response.header(Headers.CONTENT_TYPE_STRING))
                    .as("content type header in response to %s", getContacts)
                    .startsWith("application/json");
            assertThat(response.body().string()).isEqualTo("[{\"email\":\"donald@duck.no\",\"name\":\"Donald Duck\"}]");
        }
    }

    @Test(dependsOnMethods = "testPutDomain", enabled = false) //, invocationCount = 10, successPercentage = 90)
    public void testGetDomain() throws IOException {
        HttpUrl getContact = newUrl().addPathSegments("data/contact/donald").build();
        Request listContactRequest = request.url(getContact).get().build();
        try (Response response = okHttpClient.newCall(listContactRequest).execute()) {
            assertThat(response.code())
                    .as("response code of %s", getContact)
                    .isEqualTo(StatusCodes.OK);
            assertThat(response.header(Headers.CONTENT_TYPE_STRING))
                    .as("content type header in response to %s", getContact)
                    .startsWith("application/json");
            assertThat(response.body().string())
                    .as("body in response to %s").isEqualTo("{\"email\":\"donald@duck.no\",\"name\":\"Donald Duck\"}");
        }
    }

    @Test(dependsOnMethods = "testPutDomain", enabled = false)
    public void testGetMissingDomain() throws IOException {
        HttpUrl getContact = newUrl().addPathSegments("data/contact/missing").build();
        Request listContactRequest = request.url(getContact).get().build();
        try (Response response = okHttpClient.newCall(listContactRequest).execute()) {
            assertThat(response.code())
                    .as("response code of %s", getContact)
                    .isEqualTo(StatusCodes.NOT_FOUND);
            assertThat(response.header(Headers.CONTENT_TYPE_STRING))
                    .as("content type header in response to %s", getContact)
                    .startsWith("application/json");
        }
    }

    @Test
    public void thatGETWithIllegalRequestPathFailsWith400() {
        String response = client.get("/data/bull/foo/bar").expect400BadRequest().body();
        assertEquals(response, "Not a managed resource name: \"bull\"");
    }
}
