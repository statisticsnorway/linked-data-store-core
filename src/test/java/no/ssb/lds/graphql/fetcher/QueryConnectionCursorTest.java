package no.ssb.lds.graphql.fetcher;

import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryConnectionCursorTest {

    @Test
    public void shouldSerializeCursorString() {
        QueryConnectionCursor sut = new QueryConnectionCursor(5);
        Assert.assertEquals(QueryConnectionCursor.fromValue(sut.getValue()), sut);
        Assert.assertEquals(QueryConnectionCursor.fromValue(sut.toString()), sut);
    }
}
