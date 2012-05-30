/*
 * Copyright (c) 2012. Mats Svefors
 */

package sweforce.axon.eventstore.bdb;

import org.axonframework.domain.DomainEventMessage;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.domain.GenericDomainEventMessage;
import org.axonframework.domain.SimpleDomainEventStream;
import org.axonframework.eventstore.EventStore;
import org.axonframework.eventstore.EventStreamNotFoundException;
import org.axonframework.eventstore.EventVisitor;
import org.axonframework.eventstore.SnapshotEventStore;
import org.axonframework.eventstore.management.Criteria;
import org.axonframework.eventstore.management.CriteriaBuilder;
import org.axonframework.eventstore.management.EventStoreManagement;
import org.axonframework.serializer.SerializedDomainEventData;
import org.axonframework.serializer.SerializedDomainEventMessage;
import org.axonframework.serializer.SerializedObject;
import org.axonframework.serializer.Serializer;
import org.axonframework.serializer.xml.XStreamSerializer;
import org.axonframework.upcasting.SimpleUpcasterChain;
import org.axonframework.upcasting.UpcastSerializedDomainEventData;
import org.axonframework.upcasting.UpcasterChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.axonframework.common.IdentifierValidator.validateIdentifier;

/**
 * An EventStore implementation that uses Berkeley DB instances to store DomainEvents in a database. The actual DomainEvent is stored as
 * a
 * serialized blob of bytes. Other columns are used to store meta-data that allow quick finding of DomainEvents for a
 * specific aggregate in the correct order.
 * <p/>
 * This EventStore supports snapshots pruning, which can enabled by configuring a {@link #setMaxSnapshotsArchived(int)
 * maximum number of snapshots to archive}. By default snapshot pruning is configured to archive only {@value
 * #DEFAULT_MAX_SNAPSHOTS_ARCHIVED} snapshot per aggregate.
 * <p/>
 * The serializer used to serialize the events is configurable. By default, the {@link XStreamSerializer} is used.
 *
 * @author Mats Svefors
 * @see org.axonframework.eventstore.jpa.JpaEventStore
 */
public class BdbEventStore implements SnapshotEventStore, EventStoreManagement {

    private static final Logger logger = LoggerFactory.getLogger(BdbEventStore.class);

    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_MAX_SNAPSHOTS_ARCHIVED = 1;

    private int batchSize = DEFAULT_BATCH_SIZE;
    private int maxSnapshotsArchived = DEFAULT_MAX_SNAPSHOTS_ARCHIVED;

    private UpcasterChain upcasterChain = SimpleUpcasterChain.EMPTY;

    private final EventEntryStore eventEntryStore;

    private final Serializer eventSerializer;

//    public BdbEventStore(Serializer eventSerializer) {
//        this.eventSerializer = eventSerializer;
//    }
//
//    public BdbEventStore() {
//        this(new XStreamSerializer());
//    }

    public BdbEventStore(EventEntryStore eventEntryStore, Serializer eventSerializer) {
        this.eventEntryStore = eventEntryStore;
        this.eventSerializer = eventSerializer;
    }

    public BdbEventStore(EventEntryStore eventEntryStore) {
        this(eventEntryStore, new XStreamSerializer());
    }

    public void setUpcasterChain(UpcasterChain upcasterChain) {
        this.upcasterChain = upcasterChain;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setMaxSnapshotsArchived(int maxSnapshotsArchived) {
        this.maxSnapshotsArchived = maxSnapshotsArchived;
    }

    @Override
    public void appendEvents(String type, DomainEventStream domainEventStream) {
        /*
          DomainEventMessage event = null;
                  try {
                      EntityManager entityManager = entityManagerProvider.getEntityManager();
                      while (events.hasNext()) {
                          event = events.next();
                          validateIdentifier(event.getAggregateIdentifier().getClass());
                          eventEntryStore.persistEvent(type, event, eventSerializer.serialize(event.getPayload(), byte[].class),
                                                       eventSerializer.serialize(event.getMetaData(), byte[].class),
                                                       entityManager);
                      }
                      entityManager.flush();
                  } catch (RuntimeException exception) {
                      if (persistenceExceptionResolver != null
                              && persistenceExceptionResolver.isDuplicateKeyViolation(exception)) {
                          throw new ConcurrencyException(
                                  String.format("Concurrent modification detected for Aggregate identifier [%s], sequence: [%s]",
                                                event.getAggregateIdentifier(),
                                                event.getSequenceNumber()),
                                  exception);
                      }
                      throw exception;
                  }
         */
        DomainEventMessage event = null;
//        try {
        while (domainEventStream.hasNext()) {
            event = domainEventStream.next();
            validateIdentifier(event.getAggregateIdentifier().getClass());
            eventEntryStore.persistEvent(type, event, eventSerializer.serialize(event.getPayload(), byte[].class),
                    eventSerializer.serialize(event.getMetaData(),
                            byte[].class));
        }
//        } catch (RuntimeException exception) {
//
//        }
    }

    @Override
    public DomainEventStream readEvents(String type, Object identifier) {
        long snapshotSequenceNumber = -1;
        SerializedDomainEventData lastSnapshotEvent = eventEntryStore.loadLastSnapshotEvent(type, identifier);
        DomainEventMessage snapshotEvent = null;
        if (lastSnapshotEvent != null) {
            try {
                snapshotEvent = new GenericDomainEventMessage<Object>(
                        identifier,
                        lastSnapshotEvent.getSequenceNumber(),
                        eventSerializer.deserialize(lastSnapshotEvent.getPayload()),
                        (Map<String, Object>) eventSerializer.deserialize(lastSnapshotEvent.getMetaData()));
                snapshotSequenceNumber = snapshotEvent.getSequenceNumber();
            } catch (RuntimeException ex) {
                logger.warn("Error while reading snapshot event entry. "
                        + "Reconstructing aggregate on entire event stream. Caused by: {} {}",
                        ex.getClass().getName(),
                        ex.getMessage());
            } catch (LinkageError error) {
                logger.warn("Error while reading snapshot event entry. "
                        + "Reconstructing aggregate on entire event stream. Caused by: {} {}",
                        error.getClass().getName(),
                        error.getMessage());
            }
        }
        List<DomainEventMessage> events = fetchBatch(type, identifier, snapshotSequenceNumber + 1);
        if (snapshotEvent != null) {
            events.add(0, snapshotEvent);
        }
        if (events.isEmpty()) {
            throw new EventStreamNotFoundException(type, identifier);
        }
        return new BatchingDomainEventStream(events, identifier, type);
//        return new SimpleDomainEventStream(events);
    }

    private List<DomainEventMessage> fetchBatch(String type, Object identifier, long firstSequenceNumber) {
        List<? extends SerializedDomainEventData> entries = eventEntryStore.fetchBatch(type, identifier, firstSequenceNumber, batchSize);
        List<DomainEventMessage> events = new ArrayList<DomainEventMessage>(entries.size());
        for (SerializedDomainEventData entry : entries) {
            events.addAll(upcastAndDeserialize(entry, identifier));
        }
        return events;

    }

    @SuppressWarnings("unchecked")
    private List<DomainEventMessage> upcastAndDeserialize(SerializedDomainEventData entry, Object identifier) {
        List<SerializedObject> objects = upcasterChain.upcast(entry.getPayload());
        List<DomainEventMessage> events = new ArrayList<DomainEventMessage>(objects.size());
        for (SerializedObject object : objects) {
            events.add(new SerializedDomainEventMessage<Object>(
                    new UpcastSerializedDomainEventData(entry, identifier, object), eventSerializer));
        }
        return events;
    }


    @Override
    public void appendSnapshotEvent(String type, DomainEventMessage snapshotEvent) {
        eventEntryStore.persistSnapshot(type, snapshotEvent,
                eventSerializer.serialize(snapshotEvent.getPayload(), byte[].class),
                eventSerializer.serialize(snapshotEvent.getMetaData(), byte[].class)
        );
        if (maxSnapshotsArchived > 0)
            eventEntryStore.pruneSnapshots(type, snapshotEvent, maxSnapshotsArchived);
    }


    /**
     * Is batching useful in a berkeley db scenario?
     */
    private final class BatchingDomainEventStream implements DomainEventStream {

        private int currentBatchSize;
        private Iterator<DomainEventMessage> currentBatch;
        private DomainEventMessage next;
        private final Object id;
        private final String typeId;

        private BatchingDomainEventStream(List<DomainEventMessage> firstBatch, Object id, String typeId) {
            this.id = id;
            this.typeId = typeId;
            this.currentBatchSize = firstBatch.size();
            this.currentBatch = firstBatch.iterator();
            if (currentBatch.hasNext()) {
                next = currentBatch.next();
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public DomainEventMessage next() {
            DomainEventMessage current = next;
            if (next != null && !currentBatch.hasNext() && currentBatchSize >= batchSize) {
                logger.debug("Fetching new batch for Aggregate [{}]", id);
                List<DomainEventMessage> newBatch = fetchBatch(typeId, id, next.getSequenceNumber() + 1);
                currentBatchSize = newBatch.size();
                currentBatch = newBatch.iterator();
            }
            next = currentBatch.hasNext() ? currentBatch.next() : null;
            return current;
        }

        @Override
        public DomainEventMessage peek() {
            return next;
        }
    }

    @Override
    public void visitEvents(EventVisitor eventVisitor) {
        Iterator<? extends SerializedDomainEventData> iterator = eventEntryStore.fetchAllDomainEvents();
        SerializedDomainEventData domainEventData = null;
        while (iterator.hasNext()) {
            domainEventData = iterator.next();
            List<DomainEventMessage> domainEventMessages = upcastAndDeserialize(domainEventData,
                    domainEventData.getAggregateIdentifier());
            for (DomainEventMessage domainEventMessage : domainEventMessages) {
                eventVisitor.doWithEvent(domainEventMessage);
            }
        }


        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visitEvents(Criteria criteria, EventVisitor eventVisitor) {
        throw new UnsupportedOperationException("Doesn't support criteria filtering. use: visitEvents(EventVisitor) instead");
    }

    @Override
    public CriteriaBuilder newCriteriaBuilder() {
        throw new UnsupportedOperationException("Doesn't support criteria filtering. use: visitEvents(EventVisitor) instead");
    }
}
