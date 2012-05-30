package sweforce.axon.eventstore.bdb;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import org.axonframework.domain.*;
import org.axonframework.eventhandling.annotation.EventHandler;
import org.axonframework.eventsourcing.annotation.AbstractAnnotatedAggregateRoot;
import org.axonframework.eventstore.EventStreamNotFoundException;
import org.axonframework.eventstore.EventVisitor;
import org.axonframework.serializer.SerializedObject;
import org.axonframework.serializer.SerializedType;
import org.axonframework.serializer.Serializer;
import org.axonframework.serializer.SimpleSerializedObject;
import org.axonframework.upcasting.UpcasterChain;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import sweforce.bdb.util.DbShutdownHook;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 * @author Mats Svefors
 * @see org.axonframework.eventstore.jpa.JpaEventStoreTest
 */
public class BdbEventStoreTest {

    private BdbEventStore testSubject;

    private BdbEventEntryStore eventEntryStore;
    private EntityStore store;

    private StubAggregateRoot aggregate1;
    private StubAggregateRoot aggregate2;
    private Object mockAggregateIdentifier;

    @Before
    public void setUp() throws IOException {
        mockAggregateIdentifier = UUID.randomUUID();
        aggregate1 = new StubAggregateRoot(mockAggregateIdentifier);
        for (int t = 0; t < 10; t++) {
            aggregate1.changeState();
        }

        aggregate2 = new StubAggregateRoot();
        aggregate2.changeState();
        aggregate2.changeState();
        aggregate2.changeState();

        EnvironmentConfig envConfig = new EnvironmentConfig();
        StoreConfig storeConfig = new StoreConfig();

        envConfig.setAllowCreate(true);
        storeConfig.setAllowCreate(true);
        TemporaryFolder temporaryFolder = new TemporaryFolder();

        File tmp = temporaryFolder.newFolder(); //File.createTempFile("teststore", UUID.randomUUID().toString());
        tmp.mkdirs();
        Environment env = new Environment(tmp, envConfig);
        store = new EntityStore(env, "eventEntryStore", storeConfig);
        Runtime.getRuntime().addShutdownHook(new DbShutdownHook(env, store));
        eventEntryStore = new BdbEventEntryStore(store);
        testSubject = new BdbEventStore(eventEntryStore);
    }

    @After
    public void tearDown() {

    }

    @Test(expected = IllegalArgumentException.class)
    public void testStoreAndLoadEvents_BadIdentifierType() {
        testSubject.appendEvents("type", new SimpleDomainEventStream(
                new GenericDomainEventMessage<Object>(new BadIdentifierType(), 1, new Object())));
    }

    @Test
    public void testStoreAndLoadEvents() {
        assertNotNull(testSubject);
        testSubject.appendEvents("test", aggregate1.getUncommittedEvents());
        assertEquals((long) aggregate1.getUncommittedEventCount(),
                store.getPrimaryIndex(String.class, sweforce.axon.eventstore.bdb.DomainEventEntry.class).count());

        // we store some more events to make sure only correct events are retrieved
        testSubject.appendEvents("test", new SimpleDomainEventStream(
                new GenericDomainEventMessage<Object>(aggregate2.getIdentifier(),
                        0,
                        new Object(),
                        Collections.singletonMap("key", (Object) "Value"))));
//        entityManager.flush();
//        entityManager.clear();

        DomainEventStream events = testSubject.readEvents("test", aggregate1.getIdentifier());
        List<DomainEventMessage> actualEvents = new ArrayList<DomainEventMessage>();
        while (events.hasNext()) {
            DomainEventMessage event = events.next();
            event.getPayload();
            event.getMetaData();
            actualEvents.add(event);
        }
        assertEquals(aggregate1.getUncommittedEventCount(), actualEvents.size());

        /// we make sure persisted events have the same MetaData alteration logic
        DomainEventStream other = testSubject.readEvents("test", aggregate2.getIdentifier());
        assertTrue(other.hasNext());
        DomainEventMessage messageWithMetaData = other.next();
        DomainEventMessage altered = messageWithMetaData.withMetaData(Collections.singletonMap("key2",
                (Object) "value"));
        DomainEventMessage combined = messageWithMetaData.andMetaData(Collections.singletonMap("key2",
                (Object) "value"));
        assertTrue(altered.getMetaData().containsKey("key2"));
        altered.getPayload();
        assertFalse(altered.getMetaData().containsKey("key"));
        assertTrue(altered.getMetaData().containsKey("key2"));
        assertTrue(combined.getMetaData().containsKey("key"));
        assertTrue(combined.getMetaData().containsKey("key2"));
        assertNotNull(messageWithMetaData.getPayload());
        assertNotNull(messageWithMetaData.getMetaData());
        assertFalse(messageWithMetaData.getMetaData().isEmpty());
    }

    //
//    //    @DirtiesContext
    @Test
    public void testStoreAndLoadEvents_WithUpcaster() {
        assertNotNull(testSubject);
        UpcasterChain mockUpcasterChain = mock(UpcasterChain.class);
        when(mockUpcasterChain.upcast(isA(SerializedObject.class))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                SerializedObject serializedObject = (SerializedObject) invocation.getArguments()[0];
                return Arrays.asList(serializedObject, serializedObject);
            }
        });

        testSubject.appendEvents("test", aggregate1.getUncommittedEvents());

        testSubject.setUpcasterChain(mockUpcasterChain);
//        entityManager.flush();
        assertEquals((long) aggregate1.getUncommittedEventCount(),
                store.getPrimaryIndex(String.class, sweforce.axon.eventstore.bdb.DomainEventEntry.class).count());


        // we store some more events to make sure only correct events are retrieved
        testSubject.appendEvents("test", new SimpleDomainEventStream(
                new GenericDomainEventMessage<Object>(aggregate2.getIdentifier(),
                        0,
                        new Object(),
                        Collections.singletonMap("key", (Object) "Value"))));

        DomainEventStream events = testSubject.readEvents("test", aggregate1.getIdentifier());
        List<DomainEventMessage> actualEvents = new ArrayList<DomainEventMessage>();
        while (events.hasNext()) {
            DomainEventMessage event = events.next();
            event.getPayload();
            event.getMetaData();
            actualEvents.add(event);
        }

        assertEquals(20, actualEvents.size());
        for (int t = 0; t < 20; t = t + 2) {
            assertEquals(actualEvents.get(t).getSequenceNumber(), actualEvents.get(t + 1).getSequenceNumber());
            assertEquals(actualEvents.get(t).getAggregateIdentifier(),
                    actualEvents.get(t + 1).getAggregateIdentifier());
            assertEquals(actualEvents.get(t).getMetaData(), actualEvents.get(t + 1).getMetaData());
            assertNotNull(actualEvents.get(t).getPayload());
            assertNotNull(actualEvents.get(t + 1).getPayload());
        }
    }

    @Test
    public void testLoad_LargeAmountOfEvents() {
        List<DomainEventMessage<String>> domainEvents = new ArrayList<DomainEventMessage<String>>(110);
        String aggregateIdentifier = "id";
        for (int t = 0; t < 110; t++) {
            domainEvents.add(new GenericDomainEventMessage<String>(aggregateIdentifier, (long) t,
                    "Mock contents", MetaData.emptyInstance()));
        }
        testSubject.appendEvents("test", new SimpleDomainEventStream(domainEvents));

        DomainEventStream events = testSubject.readEvents("test", aggregateIdentifier);
        long t = 0L;
        while (events.hasNext()) {
            DomainEventMessage event = events.next();
            assertEquals(t, event.getSequenceNumber());
            t++;
        }
        assertEquals(110L, t);
    }

    @Test
    public void testLoad_LargeAmountOfEventsInSmallBatches() {
        testSubject.setBatchSize(10);
        testLoad_LargeAmountOfEvents();
    }

    @Test
    public void testEntireStreamIsReadOnUnserializableSnapshot_WithException() {
        List<DomainEventMessage<String>> domainEvents = new ArrayList<DomainEventMessage<String>>(110);
        String aggregateIdentifier = "id";
        for (int t = 0; t < 110; t++) {
            domainEvents.add(new GenericDomainEventMessage<String>(aggregateIdentifier, (long) t,
                    "Mock contents", MetaData.emptyInstance()));
        }
        testSubject.appendEvents("test", new SimpleDomainEventStream(domainEvents));
        final Serializer serializer = new Serializer() {

            @Override
            public <T> SerializedObject<T> serialize(Object object, Class<T> expectedType) {
                Assert.assertEquals(byte[].class, expectedType);
                return new SimpleSerializedObject("this ain't gonna work".getBytes(), byte[].class, "failingType", "0");
            }

            @Override
            public <T> boolean canSerializeTo(Class<T> expectedRepresentation) {
                return byte[].class.equals(expectedRepresentation);
            }

            @Override
            public Object deserialize(SerializedObject serializedObject) {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public Class classForType(SerializedType type) {
                try {
                    return Class.forName(type.getName());
                } catch (ClassNotFoundException e) {
                    return null;
                }
            }
        };
        final DomainEventMessage<String> stubDomainEvent = new GenericDomainEventMessage<String>(
                aggregateIdentifier,
                (long) 30,
                "Mock contents", MetaData.emptyInstance()
        );
        SnapshotEventEntry entry = new SnapshotEventEntry(
                "test", stubDomainEvent,
                serializer.serialize(stubDomainEvent.getPayload(), byte[].class),
                serializer.serialize(stubDomainEvent.getMetaData(), byte[].class));
        assertTrue(
                store.getPrimaryIndex(
                        String.class,
                        SnapshotEventEntry.class
                ).putNoOverwrite(entry));

        DomainEventStream stream = testSubject.readEvents("test", aggregateIdentifier);
        assertEquals(0L, stream.peek().getSequenceNumber());
    }


    @Test
    public void testEntireStreamIsReadOnUnserializableSnapshot_WithError() {
        List<DomainEventMessage<String>> domainEvents = new ArrayList<DomainEventMessage<String>>(110);
        String aggregateIdentifier = "id";
        for (int t = 0; t < 110; t++) {
            domainEvents.add(new GenericDomainEventMessage<String>(aggregateIdentifier, (long) t,
                    "Mock contents", MetaData.emptyInstance()));
        }
        testSubject.appendEvents("test", new SimpleDomainEventStream(domainEvents));
        final Serializer serializer = new Serializer() {

            @Override
            public <T> SerializedObject<T> serialize(Object object, Class<T> expectedType) {
                // this will cause InstantiationError, since it is an interface
                Assert.assertEquals(byte[].class, expectedType);
                return new SimpleSerializedObject("<org.axonframework.eventhandling.EventListener />".getBytes(),
                        byte[].class,
                        "failingType",
                        "0");
            }

            @Override
            public <T> boolean canSerializeTo(Class<T> expectedRepresentation) {
                return byte[].class.equals(expectedRepresentation);
            }

            @Override
            public Object deserialize(SerializedObject serializedObject) {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public Class classForType(SerializedType type) {
                try {
                    return Class.forName(type.getName());
                } catch (ClassNotFoundException e) {
                    return null;
                }
            }
        };
        final DomainEventMessage<String> stubDomainEvent = new GenericDomainEventMessage<String>(
                aggregateIdentifier,
                (long) 30,
                "Mock contents", MetaData.emptyInstance()
        );
        SnapshotEventEntry entry = new SnapshotEventEntry(
                "test", stubDomainEvent,
                serializer.serialize(stubDomainEvent.getPayload(), byte[].class),
                serializer.serialize(stubDomainEvent.getMetaData(), byte[].class));
        assertTrue(
                store.getPrimaryIndex(
                        String.class,
                        SnapshotEventEntry.class
                ).putNoOverwrite(entry));

        DomainEventStream stream = testSubject.readEvents("test", aggregateIdentifier);
        assertEquals(0L, stream.peek().getSequenceNumber());
    }

    @Test
    public void testLoad_LargeAmountOfEventsWithSnapshot() {
        List<DomainEventMessage<String>> domainEvents = new ArrayList<DomainEventMessage<String>>(110);
        String aggregateIdentifier = "id";
        for (int t = 0; t < 110; t++) {
            domainEvents.add(new GenericDomainEventMessage<String>(aggregateIdentifier, (long) t,
                    "Mock contents", MetaData.emptyInstance()));
        }
        testSubject.appendEvents("test", new SimpleDomainEventStream(domainEvents));
        testSubject.appendSnapshotEvent("test", new GenericDomainEventMessage<String>(aggregateIdentifier, (long) 30,
                "Mock contents",
                MetaData.emptyInstance()
        ));

        DomainEventStream events = testSubject.readEvents("test", aggregateIdentifier);
        long t = 30L;
        while (events.hasNext()) {
            DomainEventMessage event = events.next();
            assertEquals(t, event.getSequenceNumber());
            t++;
        }
        assertEquals(110L, t);
    }

    @Test
    public void testLoadWithSnapshotEvent() {
        testSubject.appendEvents("test", aggregate1.getUncommittedEvents());
        aggregate1.commitEvents();

        testSubject.appendSnapshotEvent("test", aggregate1.createSnapshotEvent());

        aggregate1.changeState();
        testSubject.appendEvents("test", aggregate1.getUncommittedEvents());
        aggregate1.commitEvents();

        DomainEventStream actualEventStream = testSubject.readEvents("test", aggregate1.getIdentifier());
        List<DomainEventMessage> domainEvents = new ArrayList<DomainEventMessage>();
        while (actualEventStream.hasNext()) {
            DomainEventMessage next = actualEventStream.next();
            domainEvents.add(next);
            assertEquals(aggregate1.getIdentifier(), next.getAggregateIdentifier());
        }

        assertEquals(2, domainEvents.size());
    }


    @Test(expected = EventStreamNotFoundException.class)
    public void testLoadNonExistent() {
        testSubject.readEvents("Stub", UUID.randomUUID());
    }

    @Test
    public void testVisitAllEvents() {
        EventVisitor eventVisitor = mock(EventVisitor.class);
        testSubject.appendEvents("test", new SimpleDomainEventStream(createDomainEvents(77)));
        testSubject.appendEvents("test", new SimpleDomainEventStream(createDomainEvents(23)));

        testSubject.visitEvents(eventVisitor);
        verify(eventVisitor, times(100)).doWithEvent(isA(DomainEventMessage.class));
    }

//
//    @Test
//    public void testVisitAllEvents() {
//
//    }
//
//    @Test
//    public void testVisitEvents_AfterTimestamp() {
//
//    }
//
//    @Test
//    public void testVisitEvents_BetweenTimestamps() {
//
//    }
//
//    @Test
//    public void testVisitEvents_OnOrAfterTimestamp() {
//
//    }
//
//    @Test(expected = ConcurrencyException.class)
//    public void testStoreDuplicateEvent_WithSqlExceptionTranslator() {
//
//    }
//
//    @Test
//    public void testPrunesSnaphotsWhenNumberOfSnapshotsExceedsConfiguredMaxSnapshotsArchived() {
//
//    }
//
//    @Test
//    public void testCustomEventEntryStore() {
//
//    }


    private SerializedObject<byte[]> mockSerializedObject(byte[] bytes) {
        return new SimpleSerializedObject<byte[]>(bytes, byte[].class, "java.lang.String", "0");
    }

    private List<DomainEventMessage<StubStateChangedEvent>> createDomainEvents(int numberOfEvents) {
        List<DomainEventMessage<StubStateChangedEvent>> events = new ArrayList<DomainEventMessage<StubStateChangedEvent>>();
        final Object aggregateIdentifier = UUID.randomUUID();
        for (int t = 0; t < numberOfEvents; t++) {
            events.add(new GenericDomainEventMessage<StubStateChangedEvent>(
                    aggregateIdentifier,
                    t,
                    new StubStateChangedEvent(), MetaData.emptyInstance()
            ));
        }
        return events;
    }

    private static class StubAggregateRoot extends AbstractAnnotatedAggregateRoot {

        private static final long serialVersionUID = -3656612830058057848L;
        private final Object identifier;

        private StubAggregateRoot() {
            this(UUID.randomUUID());
        }

        private StubAggregateRoot(Object identifier) {
            this.identifier = identifier;
        }

        public void changeState() {
            apply(new StubStateChangedEvent());
        }

        @Override
        public Object getIdentifier() {
            return identifier;
        }

        @EventHandler
        public void handleStateChange(StubStateChangedEvent event) {
        }

        public DomainEventMessage<StubStateChangedEvent> createSnapshotEvent() {
            return new GenericDomainEventMessage<StubStateChangedEvent>(getIdentifier(), getVersion(),
                    new StubStateChangedEvent(),
                    MetaData.emptyInstance()
            );
        }
    }

    private static class StubStateChangedEvent {

        private StubStateChangedEvent() {
        }
    }

    private static class BadIdentifierType {

    }


}
