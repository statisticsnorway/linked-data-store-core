package no.ssb.lds.graphql.fetcher;

import graphql.relay.ConnectionCursor;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Creates a cursor for index based pagination. The cursor is mapped directly to the index.
 */
public class QueryConnectionCursor implements ConnectionCursor {

    private static final java.util.Base64.Encoder encoder = java.util.Base64.getUrlEncoder().withoutPadding();
    private static final java.util.Base64.Decoder decoder = java.util.Base64.getUrlDecoder();

    private static final String PREFIX = "qc";
    private final long index;

    public QueryConnectionCursor(long index) {
        this.index = index;
    }

    public long getIndex() {
        return index;
    }

    @Override
    public String getValue() {
        return encoder.encodeToString((PREFIX + ":" + index).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Converts from a cursor value to this index based pagination object.
     *
     * @param cursorValue an opaque string representing the cursor
     * @return this class
     */
    public static QueryConnectionCursor fromValue(String cursorValue) {
        String[] split = new String(decoder.decode(cursorValue), StandardCharsets.UTF_8).split(":", 2);
        if (split.length != 2) {
            throw new IllegalArgumentException(String.format("expecting a valid cursor value, got %s", cursorValue));
        }
        return new QueryConnectionCursor(Long.valueOf(split[1]));
    }

    @Override
    public String toString() {
        return getValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryConnectionCursor that = (QueryConnectionCursor) o;
        return index == that.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }
}
