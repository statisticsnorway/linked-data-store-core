package no.ssb.lds.graphql.schemas;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.lds.api.json.JsonNavigationPath;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionStatistics;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.specification.Specification;

import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;

class EmptyPersistence implements RxJsonPersistence {
    @Override
    public Maybe<JsonDocument> readDocument(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        return Maybe.empty();
    }

    @Override
    public Flowable<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, Range<String> range) {
        return Flowable.empty();
    }

    @Override
    public Flowable<JsonDocument> readDocumentVersions(Transaction tx, String ns, String entityName, String id, Range<ZonedDateTime> range) {
        return Flowable.empty();
    }

    @Override
    public Flowable<JsonDocument> readTargetDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String sourceEntityName, String sourceId, JsonNavigationPath relationPath, String targetEntityName, Range<String> range) {
        return Flowable.empty();
    }

    @Override
    public Flowable<JsonDocument> readSourceDocuments(Transaction tx, ZonedDateTime snapshot, String ns, String targetEntityName, String targetId, JsonNavigationPath relationPath, String sourceEntityName, Range<String> range) {
        return Flowable.empty();
    }

    @Override
    public Completable createOrOverwrite(Transaction tx, JsonDocument document, Specification specification) {
        return Completable.complete();
    }

    @Override
    public Completable createOrOverwrite(Transaction tx, Flowable<JsonDocument> documentFlowable, Specification specification) {
        return Completable.complete();
    }

    @Override
    public Completable deleteDocument(Transaction tx, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
        return Completable.complete();
    }

    @Override
    public Completable deleteAllDocumentVersions(Transaction tx, String ns, String entity, String id, PersistenceDeletePolicy policy) {
        return Completable.complete();
    }

    @Override
    public Completable deleteAllEntities(Transaction tx, String namespace, String entity, Specification specification) {
        return Completable.complete();
    }

    @Override
    public Completable markDocumentDeleted(Transaction transaction, String ns, String entityName, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
        return Completable.complete();
    }

    @Override
    public Single<Boolean> hasPrevious(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        return Single.just(false);
    }

    @Override
    public Single<Boolean> hasNext(Transaction tx, ZonedDateTime snapshot, String ns, String entityName, String id) {
        return Single.just(false);
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        return new Transaction() {
            @Override
            public CompletableFuture<TransactionStatistics> commit() {
                return CompletableFuture.completedFuture(new TransactionStatistics());
            }

            @Override
            public CompletableFuture<TransactionStatistics> cancel() {
                return CompletableFuture.completedFuture(new TransactionStatistics());
            }
        };
    }

    @Override
    public Flowable<JsonDocument> findDocument(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName, JsonNavigationPath path, String value, Range<String> range) {
        return Flowable.empty();
    }

    @Override
    public void close() throws PersistenceException {

    }
}
