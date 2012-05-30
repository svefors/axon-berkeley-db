package sweforce.axon.eventstore.bdb;

import org.axonframework.domain.DomainEventMessage;
import org.axonframework.domain.MetaData;
import org.axonframework.serializer.SerializedMetaData;
import org.axonframework.serializer.SerializedObject;
import org.axonframework.serializer.Serializer;
import org.axonframework.serializer.SimpleSerializedObject;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import sweforce.axon.eventstore.bdb.DomainEventEntry;

import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created with IntelliJ IDEA.
 * User: sveffa
 * Date: 5/30/12
 * Time: 6:26 AM
 * To change this template use File | Settings | File Templates.
 */
public class DomainEventEntryTest {

    private DomainEventMessage mockDomainEvent;
        private SerializedObject<byte[]> mockPayload = new SimpleSerializedObject<byte[]>("PayloadBytes".getBytes(),
                                                                                          byte[].class, "Mock", "0");
        private SerializedObject<byte[]> mockMetaData = new SerializedMetaData<byte[]>("MetaDataBytes".getBytes(),
                                                                                       byte[].class);

        @Before
        public void setUp() {
            mockDomainEvent = mock(DomainEventMessage.class);
            Serializer mockSerializer = mock(Serializer.class);
            MetaData metaData = new MetaData(Collections.singletonMap("Key", "Value"));
            when(mockSerializer.deserialize(mockPayload)).thenReturn("Payload");
            when(mockSerializer.deserialize(isA(SerializedMetaData.class))).thenReturn(metaData);
        }

        @Test
        public void testDomainEventEntry_WrapEventsCorrectly() {
            String aggregateIdentifier = UUID.randomUUID().toString();
            DateTime timestamp = new DateTime();
            UUID eventIdentifier = UUID.randomUUID();

            when(mockDomainEvent.getAggregateIdentifier()).thenReturn(aggregateIdentifier);
            when(mockDomainEvent.getSequenceNumber()).thenReturn(2L);
            when(mockDomainEvent.getTimestamp()).thenReturn(timestamp);
            when(mockDomainEvent.getIdentifier()).thenReturn(eventIdentifier.toString());
            when(mockDomainEvent.getPayloadType()).thenReturn(String.class);

            DomainEventEntry actualResult = new DomainEventEntry("test", mockDomainEvent, mockPayload, mockMetaData);

            assertEquals(aggregateIdentifier, actualResult.getAggregateIdentifier());
            assertEquals(2L, actualResult.getSequenceNumber());
            assertEquals(timestamp, actualResult.getTimestamp());
            assertEquals("test", actualResult.getType());
        }
}
