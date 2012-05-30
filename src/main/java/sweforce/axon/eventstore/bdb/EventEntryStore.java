package sweforce.axon.eventstore.bdb;

import org.axonframework.domain.DomainEventMessage;
import org.axonframework.serializer.SerializedDomainEventData;
import org.axonframework.serializer.SerializedObject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface EventEntryStore {

    /**
     * Persists the given <code>event</code> which has been serialized into <code>serializedEvent</code> in the
     * backing data store using given <code>entityManager</code>.
     * <p/>
     * These events should be returned by the <code>fetchBatch(...)</code> methods.
     *
     * @param aggregateType      The type identifier of the aggregate that generated the event
     * @param event              The actual event instance. May be used to extract relevant meta data
     * @param serializedPayload  The serialized payload of the event
     * @param serializedMetaData The serialized MetaData of the event
     */
    void persistEvent(String aggregateType, DomainEventMessage event, SerializedObject serializedPayload,
                      SerializedObject serializedMetaData);


    /**
     * Load the last known snapshot event for aggregate of given <code>type</code> with given <code>identifier</code>
     * using given <code>entityManager</code>.
     *
     * @param aggregateType The type identifier of the aggregate that generated the event
     * @param identifier    The identifier of the aggregate to load the snapshot for
     * @return the serialized representation of the last known snapshot event
     */
    SerializedDomainEventData loadLastSnapshotEvent(String aggregateType, Object identifier);

    /**
     * Fetches a selection of events for an aggregate of given <code>type</code> and given <code>identifier</code>
     * starting at given <code>firstSequenceNumber</code> with given <code>batchSize</code>. The given
     * <code>entityManager</code> provides access to the backing data store.
     * <p/>
     * Note that the result is expected to be ordered by sequence number, with the lowest number first.
     *
     * @param aggregateType       The type identifier of the aggregate that generated the event
     * @param identifier          The identifier of the aggregate to load the snapshot for
     * @param firstSequenceNumber The sequence number of the first event to include in the batch
     * @param batchSize           The number of entries to include in the batch (if available)
     * @return a List of serialized representations of Events included in this batch
     */
    List<? extends SerializedDomainEventData> fetchBatch(String aggregateType, Object identifier,
                                                         long firstSequenceNumber, int batchSize);

    /**
     * Removes old snapshots from the storage for an aggregate of given <code>type</code> that generated the given
     * <code>mostRecentSnapshotEvent</code>. A number of <code>maxSnapshotsArchived</code> is expected to remain in the
     * archive after pruning, unless that number of snapshots has not been created yet. The given
     * <code>entityManager</code> provides access to the data store.
     *
     * @param type                    the type of the aggregate for which to prune snapshots
     * @param mostRecentSnapshotEvent the last appended snapshot event
     * @param maxSnapshotsArchived    the number of snapshots that may remain archived
     */
    void pruneSnapshots(String type, DomainEventMessage mostRecentSnapshotEvent, int maxSnapshotsArchived);

    /**
     * Persists the given <code>event</code> which has been serialized into <code>serializedEvent</code> in the
     * backing data store using given <code>entityManager</code>.
     * <p/>
     * These snapshot events should be returned by the <code>loadLastSnapshotEvent(...)</code> methods.
     *
     * @param aggregateType      The type identifier of the aggregate that generated the event
     * @param snapshotEvent      The actual snapshot event instance. May be used to extract relevant meta data
     * @param serializedPayload  The serialized payload of the event
     * @param serializedMetaData The serialized MetaData of the event
     */
    void persistSnapshot(String aggregateType, DomainEventMessage snapshotEvent, SerializedObject serializedPayload,
                         SerializedObject serializedMetaData);


    Iterator<? extends SerializedDomainEventData> fetchAllDomainEvents();
}
