package no.ssb.lds.graphql.fetcher.api;

import io.reactivex.Flowable;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.flattened.FlattenedDocument;
import no.ssb.lds.api.persistence.json.FlattenedDocumentToJson;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.Persistence;

import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;

import static hu.akarnokd.rxjava2.interop.FlowInterop.fromFlowPublisher;
import static hu.akarnokd.rxjava2.interop.FlowInterop.toFlowPublisher;

/**
 * Wraps the persistence.
 */
public class SimplePersistenceImplementation implements SimplePersistence {

    private final Persistence persistence;
    private int capacityBytes;

    public SimplePersistenceImplementation(Persistence persistence) {
        this(persistence, null);
    }

    public SimplePersistenceImplementation(Persistence persistence, Integer capacityBytes) {
        this.persistence = Objects.requireNonNull(persistence);
        this.capacityBytes = Optional.ofNullable(capacityBytes).orElse(8192);
    }


    @Override
    public Flow.Publisher<Fragment> readFragments(Transaction tx, ZonedDateTime snapshot, String nameSpace,
                                                  String entityName, String id) throws PersistenceException {
        Objects.requireNonNull(id);
        Objects.requireNonNull(entityName);
        Objects.requireNonNull(nameSpace);
        Objects.requireNonNull(snapshot);
        Objects.requireNonNull(tx);

        Flowable<Fragment> fragmentFlowable = fromFlowPublisher(persistence.read(tx, snapshot, nameSpace, entityName, id));
        fragmentFlowable = fragmentFlowable.filter(fragment -> {
            // Remove the fragment that indicates end of stream.
            return !fragment.isStreamingControl();
        });
        return toFlowPublisher(fragmentFlowable);
    }

    @Override
    public Flow.Publisher<JsonDocument> readDocument(Transaction tx, ZonedDateTime snapshot, String nameSpace,
                                                      String entityName, String id) throws PersistenceException {
        Flow.Publisher<Fragment> fragmentPublisher = readFragments(tx, snapshot, nameSpace, entityName, id);
        Flowable<Fragment> fragmentFlowable = fromFlowPublisher(fragmentPublisher);
        Flowable<JsonDocument> documentFlowable = toDocuments(fragmentFlowable);
        return toFlowPublisher(documentFlowable);
    }

    private Flowable<JsonDocument> toDocuments(Flowable<Fragment> fragmentFlowable) {
        return fragmentFlowable.groupBy(fragment -> {
            // Fragments by id.
            return DocumentKey.from(fragment);
        }).concatMapEager(fragments -> {
            // For each group, create a FlattenedDocument.
            DocumentKey key = fragments.getKey();
            // Note that we return a Single<FlattenedDocument> so we use flatMap.
            return fragments.toMultimap(Fragment::path).map(map -> {
                return FlattenedDocument.decodeDocument(key, map, capacityBytes);
            }).toFlowable();
        }, Integer.MAX_VALUE, 1).filter(flattenedDocument -> {
            // Filter out the deleted documents.
            return !flattenedDocument.deleted();
        }).map(flattenedDocument -> {
            // Convert to JsonDocument.
            return new JsonDocument(
                    flattenedDocument.key(),
                    new FlattenedDocumentToJson(flattenedDocument).toJSONObject()
            );
        });
    }

    @Override
    public Flow.Publisher<Fragment> readFragments(Transaction tx, ZonedDateTime snapshot, String nameSpace,
                                                  String entityName, Range range) throws PersistenceException {
        Objects.requireNonNull(tx);
        Objects.requireNonNull(snapshot);
        Objects.requireNonNull(nameSpace);
        Objects.requireNonNull(entityName);
        Objects.requireNonNull(range);

        Flow.Publisher<Fragment> fragmentPublisher = persistence.findAll(tx, snapshot, nameSpace, entityName,
                range.getAfter(), Integer.MAX_VALUE);
        Flowable<Fragment> fragmentFlowable = fromFlowPublisher(fragmentPublisher)
                .filter(fragment -> {
                    // Remove the fragment that indicates end of stream.
                    return !fragment.isStreamingControl();
                });
        fragmentFlowable = limitFragments(fragmentFlowable, range);
        return toFlowPublisher(fragmentFlowable);
    }

    private Flowable<Fragment> limitFragments(Flowable<Fragment> fragmentFlowable, Range range) {
        if (range.getAfter() != null) {
            fragmentFlowable = fragmentFlowable.skipWhile(fragment -> fragment.id().compareTo(range.getAfter()) > 0);
        }
        if (range.getBefore() != null) {
            fragmentFlowable = fragmentFlowable.takeWhile(fragment -> fragment.id().compareTo(range.getBefore()) < 0);
        }
        return fragmentFlowable;
    }

    @Override
    public Flow.Publisher<JsonDocument> readDocuments(Transaction tx, ZonedDateTime snapshot, String nameSpace,
                                                      String entityName, Range range) throws PersistenceException {
        Flow.Publisher<Fragment> fragmentPublisher = readFragments(tx, snapshot, nameSpace, entityName, range);
        Flowable<Fragment> fragmentFlowable = fromFlowPublisher(fragmentPublisher);
        Flowable<JsonDocument> documentFlowable = toDocuments(fragmentFlowable);
        return toFlowPublisher(documentFlowable);
    }

    @Override
    public Flow.Publisher<Fragment> findFragments(Transaction tx, ZonedDateTime snapshot, String nameSpace,
                                                  String entityName, Filter filter) throws PersistenceException {
        return findFragments(tx, snapshot, nameSpace, entityName, filter, Range.unbounded());
    }

    @Override
    public Flow.Publisher<Fragment> findFragments(Transaction tx, ZonedDateTime snapshot, String nameSpace,
                                                  String entityName, Filter filter, Range range) throws PersistenceException {
        Objects.requireNonNull(tx);
        Objects.requireNonNull(snapshot);
        Objects.requireNonNull(nameSpace);
        Objects.requireNonNull(entityName);
        Objects.requireNonNull(filter);

        Flow.Publisher<Fragment> fragmentPublisher = persistence.find(tx, snapshot, nameSpace, entityName,
                filter.getPath(), filter.getPathValue(), filter.getId(), Integer.MAX_VALUE);
        Flowable<Fragment> fragmentFlowable = fromFlowPublisher(fragmentPublisher);
        fragmentFlowable = limitFragments(fragmentFlowable, range);
        return toFlowPublisher(fragmentFlowable);
    }

    @Override
    public boolean hasPrevious(Transaction tx, ZonedDateTime snapshot, String nameSpace, String entityName, String id) {
        Flow.Publisher<Fragment> fragmentPublisher = readFragments(tx, snapshot, nameSpace, entityName, Range.before(id));
        Flowable<Fragment> fragmentFlowable = fromFlowPublisher(fragmentPublisher);
        return !fragmentFlowable.isEmpty().blockingGet();
    }

    @Override
    public boolean hasNext(Transaction tx, ZonedDateTime snapshot, String nameSpace, String entityName, String id) {
        Flow.Publisher<Fragment> fragmentPublisher = readFragments(tx, snapshot, nameSpace, entityName, Range.after(id));
        Flowable<Fragment> fragmentFlowable = fromFlowPublisher(fragmentPublisher);
        return !fragmentFlowable.isEmpty().blockingGet();
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        return persistence.createTransaction(readOnly);
    }
}
