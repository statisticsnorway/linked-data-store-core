package no.ssb.lds.graphql.fetcher;

import no.ssb.lds.api.persistence.reactivex.Range;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryConnectionFetcherTest {

    @Test
    public void createPaginationSettingsFromRange() {
        final int TOTAL_RESULTS = 60;
        QueryConnectionFetcher.IndexBasedRange sut = QueryConnectionFetcher.IndexBasedRange.fromRange(Range.first(10), TOTAL_RESULTS);
        Assert.assertEquals(sut.getFrom(), 0);
        Assert.assertEquals(sut.getSize(), 10);

        sut = QueryConnectionFetcher.IndexBasedRange.fromRange(Range.firstAfter(10, new QueryConnectionCursor(10).getValue()), TOTAL_RESULTS);
        Assert.assertEquals(sut.getFrom(), 10);
        Assert.assertEquals(sut.getSize(), 10);

        sut = QueryConnectionFetcher.IndexBasedRange.fromRange(Range.last(10), TOTAL_RESULTS);
        Assert.assertEquals(sut.getFrom(), 50);
        Assert.assertEquals(sut.getSize(), 10);

        sut = QueryConnectionFetcher.IndexBasedRange.fromRange(Range.lastBefore(5, new QueryConnectionCursor(20).getValue()), TOTAL_RESULTS);
        Assert.assertEquals(sut.getFrom(), 15);
        Assert.assertEquals(sut.getSize(), 5);

        sut = QueryConnectionFetcher.IndexBasedRange.fromRange(Range.between(
                new QueryConnectionCursor(10).getValue(),
                new QueryConnectionCursor(12).getValue()),
                TOTAL_RESULTS);
        Assert.assertEquals(sut.getFrom(), 10);
        Assert.assertEquals(sut.getSize(), 2);
    }
}
