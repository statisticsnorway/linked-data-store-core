package no.ssb.lds.graphql.fetcher.api;

import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.streaming.Fragment;

import java.time.ZonedDateTime;
import java.util.concurrent.Flow;
import java.util.stream.Stream;

/**
 * API proposal.
 */
public interface SimplePersistence {

    /**
     * Returns all the {@link Fragment} for a given snapshot, nameSpace, entityName and id.
     *
     * @return a {@link Flow.Publisher} of {@link Fragment}
     * @throws PersistenceException if the persistence failed.
     */
    Flow.Publisher<Fragment> readFragments(Transaction tx, ZonedDateTime snapshot, String nameSpace, String entityName, String id)
            throws PersistenceException;

    Flow.Publisher<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String nameSpace, String entityName, String id)
            throws PersistenceException;

    /**
     * Returns all the {@link Fragment} for a given snapshot, nameSpace and entityName and where the id is after
     * {@link Range#getAfter()} and before {@link Range#getBefore()}.
     * <p>
     * The fragments are published ordered by id last. If {@link Range#getAfter()} or before {@link Range#getBefore()}
     * return null, the range is unbounded.
     *
     * @return a {@link Flow.Publisher} of {@link Fragment}
     * @throws PersistenceException if the persistence failed.
     */
    Flow.Publisher<Fragment> readFragments(Transaction tx, ZonedDateTime snapshot, String nameSpace,
                                           String entityName, Range range) throws PersistenceException;

    Flow.Publisher<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String nameSpace,
                                               String entityName, Range range) throws PersistenceException;

    Flow.Publisher<Fragment> findFragments(Transaction tx, ZonedDateTime snapshot, String nameSpace, String entityName,
                                           Filter filter) throws PersistenceException;

    Flow.Publisher<Fragment> findFragments(Transaction tx, ZonedDateTime snapshot, String nameSpace,
                                           String entityName, Filter filter, Range range)
            throws PersistenceException;

    boolean hasPrevious(Transaction tx, ZonedDateTime snapshot, String nameSpace, String entityName, String id);

    boolean hasNext(Transaction tx, ZonedDateTime snapshot, String nameSpace, String entityName, String id);

    /**
     * Uses the transaction-factory to create a new transaction. This method is equivalent
     * to calling <code>transactionFactory.createTransaction()</code>.
     *
     * @param readOnly true if the transaction will only perform read operations, false if at least one write operation
     *                 will be performed, and false if the caller is unsure. Note that the underlying persistence
     *                 provider may be able to optimize performance and contention related issues when read-only
     *                 transactions are involved.
     * @return the newly created transaction
     * @throws PersistenceException
     */
    Transaction createTransaction(boolean readOnly) throws PersistenceException;

    class Range {

        private final String before;
        private final String after;

        private Range(String after, String before) {
            this.before = before;
            this.after = after;
        }

        public static Range after(String value) {
            return between(value, null);
        }

        public static Range before(String value) {
            return between(null, value);
        }

        public static Range between(String first, String last) {
            return new Range(first, last);
        }

        static Range unbounded() {
            return between(null, null);
        }

        public String getBefore() {
            return before;
        }

        public String getAfter() {
            return after;
        }
    }

    class Filter {

        private final String path;
        private final byte[] pathValue;
        private final String id;

        public Filter(String path, byte[] pathValue, String id) {
            this.path = path;
            this.pathValue = pathValue;
            this.id = id;
        }

        public String getPath() {
            return path;
        }

        public byte[] getPathValue() {
            return pathValue;
        }

        public String getId() {
            return id;
        }
    }
}
