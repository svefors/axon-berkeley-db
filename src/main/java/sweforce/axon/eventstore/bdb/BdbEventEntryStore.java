package sweforce.axon.eventstore.bdb;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.*;
import org.axonframework.domain.DomainEventMessage;
import org.axonframework.serializer.SerializedDomainEventData;
import org.axonframework.serializer.SerializedObject;
import sweforce.axon.eventstore.bdb.DomainEventEntry;
import sweforce.axon.eventstore.bdb.IdentifierTypeKey;
import sweforce.axon.eventstore.bdb.SnapshotEventEntry;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: sveffa
 * Date: 5/29/12
 * Time: 8:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class BdbEventEntryStore implements EventEntryStore {

    final PrimaryIndex<String, DomainEventEntry> domainEventIndex;

    final PrimaryIndex<String, SnapshotEventEntry> snapshotEventIndex;

    final SecondaryIndex<IdentifierTypeKey, String, DomainEventEntry> aggregateDomainEventIndex;

    final SecondaryIndex<IdentifierTypeKey, String, SnapshotEventEntry> aggregateSnapshotEventIndex;

    public BdbEventEntryStore(EntityStore entityStore) {
        this.domainEventIndex = entityStore.getPrimaryIndex(String.class, DomainEventEntry.class);
        this.snapshotEventIndex = entityStore.getPrimaryIndex(String.class, SnapshotEventEntry.class);
        this.aggregateDomainEventIndex =
                entityStore.getSecondaryIndex(
                        domainEventIndex,
                        IdentifierTypeKey.class,
                        "identifier_type_domainevents");
        this.aggregateSnapshotEventIndex =
                entityStore.getSecondaryIndex(
                        snapshotEventIndex,
                        IdentifierTypeKey.class,
                        "identifier_type_snapshotevents");
    }

    @Override
    public void persistEvent(String aggregateType, DomainEventMessage event, SerializedObject serializedPayload, SerializedObject serializedMetaData) {
        DomainEventEntry bdbEventEntry = new DomainEventEntry(aggregateType, event, serializedPayload, serializedMetaData);
        if (!domainEventIndex.putNoOverwrite(bdbEventEntry)) {
            throw new RuntimeException("EventIdentifier not unique! value= " + event.getIdentifier());
        }
    }

    @Override
    public SerializedDomainEventData loadLastSnapshotEvent(String aggregateType, Object identifier) {
        List<SerializedDomainEventData> result = findSnapshotEvents(aggregateType, identifier);
        if (result.size() == 0)
            return null;
        Collections.sort(result, new EventEntrySequenceComparator().withLatestFirst());
        return result.get(0);
    }

    private List<SerializedDomainEventData> findSnapshotEvents(String aggregateType, Object identifier) {
        IdentifierTypeKey key = new IdentifierTypeKey().withAggregateIdentifier(identifier).withType(aggregateType);
        EntityCursor<SnapshotEventEntry> snapshotEventEntries = aggregateSnapshotEventIndex.entities(key, true, key, true);
        List<SerializedDomainEventData> result = new ArrayList<SerializedDomainEventData>(5);
        try {
            SnapshotEventEntry snapshotEventEntry = null;
            while ((snapshotEventEntry = snapshotEventEntries.next()) != null) {
                result.add(snapshotEventEntry);
            }
        } finally {
            snapshotEventEntries.close();
        }
        Collections.sort(result, new EventEntrySequenceComparator().withLatestFirst());
        return result;
    }

    @Override
    public List<? extends SerializedDomainEventData> fetchBatch(String aggregateType, Object identifier, long firstSequenceNumber, int batchSize) {
        IdentifierTypeKey key = new IdentifierTypeKey().withAggregateIdentifier(identifier).withType(aggregateType);
        EntityCursor<DomainEventEntry> domainEventEntries = aggregateDomainEventIndex.entities(key, true, key, true);
        List<SerializedDomainEventData> result = new ArrayList<SerializedDomainEventData>();
        try {
            DomainEventEntry domainEventEntry = null;
            //TODO: is batching really interesting when using berkeley db?
            while ((domainEventEntry = domainEventEntries.next()) != null) {
                if (domainEventEntry.getSequenceNumber() >= firstSequenceNumber
                        && domainEventEntry.getSequenceNumber() <= (firstSequenceNumber + batchSize))
                    result.add(domainEventEntry);
            }
        } finally {
            domainEventEntries.close();
        }
        Collections.sort(result, new EventEntrySequenceComparator());
        return result;
    }



    @Override
    public void pruneSnapshots(String type, DomainEventMessage mostRecentSnapshotEvent, int maxSnapshotsArchived) {
        if (maxSnapshotsArchived < 0) return;

        List<SerializedDomainEventData> result = findSnapshotEvents(type, mostRecentSnapshotEvent.getAggregateIdentifier().toString());
        Collections.sort(result, new EventEntrySequenceComparator().withLatestFirst());
        //remove
        List<SerializedDomainEventData> prunableDomainEventDatas = result.subList(maxSnapshotsArchived, result.size());
        for (SerializedDomainEventData domainEventData : prunableDomainEventDatas) {
            domainEventIndex.delete(domainEventData.getEventIdentifier());
        }
    }


    @Override
    public void persistSnapshot(String aggregateType, DomainEventMessage snapshotEvent, SerializedObject serializedPayload, SerializedObject serializedMetaData) {
        SnapshotEventEntry snapshotEventEntry = new SnapshotEventEntry(aggregateType, snapshotEvent, serializedPayload, serializedMetaData);
        if (!snapshotEventIndex.putNoOverwrite(snapshotEventEntry)) {
            throw new RuntimeException("EventIdentifier not unique! value= " + snapshotEvent.getIdentifier());
        }
    }

    /**
     * TODO, consider writing a different mechanism for visiting the event store.
     * @return
     */
    @Override
    public Iterator<? extends SerializedDomainEventData> fetchAllDomainEvents() {
        final EntityCursor<DomainEventEntry> entityCursor = aggregateDomainEventIndex.entities();
        final List<DomainEventEntry> domainEventEntries = new ArrayList<DomainEventEntry>();
        DomainEventEntry domainEventEntry = null;
        while((domainEventEntry = entityCursor.next()) != null ){
            domainEventEntries.add(domainEventEntry);
        }
        entityCursor.close();
        return domainEventEntries.iterator();
//
//        final Iterator<DomainEventEntry> domainEventEntries = entityCursor.iterator();
//        return new Iterator<SerializedDomainEventData>() {
//            @Override
//            public boolean hasNext() {
//                return domainEventEntries.hasNext();
//            }
//
//            @Override
//            public SerializedDomainEventData next() {
//                return domainEventEntries.next();
//            }
//
//            @Override
//            public void remove() {
//                throw new UnsupportedOperationException("doesnt support removal");
//            }
//
//            @Override
//            protected void finalize() throws Throwable {
//                entityCursor.close();
//            }
//        };
    }
}
