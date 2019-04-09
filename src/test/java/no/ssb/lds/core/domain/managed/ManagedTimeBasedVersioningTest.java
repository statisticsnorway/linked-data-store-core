package no.ssb.lds.core.domain.managed;

import no.ssb.lds.test.client.TestClient;
import no.ssb.lds.test.server.TestServer;
import no.ssb.lds.test.server.TestServerListener;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

@Listeners(TestServerListener.class)
public class ManagedTimeBasedVersioningTest {

    @Inject
    private TestClient client;

    @Inject
    private TestServer server;

    @Test(enabled = false)
    public void thatCorrectVersionIsReturned() {
        String body2016 = "{\"name\":\"version2016\"}";
        String body2017 = "{\"name\":\"version2017\"}";
        String body2018 = "{\"name\":\"version2018\"}";
        client.put("/data/provisionagreement/A1?sync=true&timestamp=2016-01-01T00%3A00%3A00.000%2B01", body2016).expect200Ok();
        client.put("/data/provisionagreement/A1?sync=true&timestamp=2017-01-01T00%3A00%3A00.000%2B01", body2017).expect200Ok();
        client.put("/data/provisionagreement/A1?sync=true&timestamp=2018-01-01T00%3A00%3A00.000%2B01", body2018).expect200Ok();
        client.get("/data/provisionagreement/A1?timestamp=2015-07-01T00%3A00%3A00.000%2B01").expect404NotFound();
        JSONAssert.assertEquals(client.get("/data/provisionagreement/A1?timestamp=2016-07-01T00%3A00%3A00.000%2B01").expect200Ok().body(), body2016, false);
        JSONAssert.assertEquals(client.get("/data/provisionagreement/A1?timestamp=2017-07-01T00%3A00%3A00.000%2B01").expect200Ok().body(), body2017, false);
        JSONAssert.assertEquals(client.get("/data/provisionagreement/A1?timestamp=2018-07-01T00%3A00%3A00.000%2B01").expect200Ok().body(), body2018, false);
        JSONAssert.assertEquals(client.get("/data/provisionagreement/A1?timestamp=2019-07-01T00%3A00%3A00.000%2B01").expect200Ok().body(), body2018, false);
    }

    @Test
    public void thatRequestInvalidTimestampRespondsWith400() {
        String body = "{\"name\":\"invalid-timestamp-test\",\"contacts\":[]}";
        client.put("/data/provisionagreement/m1?sync=true&timestamp=invalid", body).expect400BadRequest();
    }
}
