package sweforce.axon.eventstore.bdb;

import org.axonframework.domain.DomainEventMessage;
import org.axonframework.domain.MetaData;
import org.axonframework.serializer.SerializedDomainEventData;
import org.axonframework.serializer.SerializedObject;
import org.joda.time.DateTime;
import org.junit.Test;
import sweforce.axon.eventstore.bdb.EventEntrySequenceComparator;

import static org.junit.Assert.*;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: sveffa
 * Date: 5/30/12
 * Time: 8:28 AM
 * To change this template use File | Settings | File Templates.
 */
public class EventEntrySequenceComparatorTest {

    @Test
    public void testLowestFirst() {
        EventEntrySequenceComparator comparator = new EventEntrySequenceComparator();
        List<SerializedDomainEventData> testdata = createTestData();
        Collections.sort(testdata, comparator);
        assertEquals(1, testdata.iterator().next().getSequenceNumber());
    }

    @Test
    public void testHighestFirst() {
        EventEntrySequenceComparator comparator = new EventEntrySequenceComparator().withLatestFirst();
        List<SerializedDomainEventData> testdata = createTestData();
        Collections.sort(testdata, comparator);
        assertEquals(21, testdata.iterator().next().getSequenceNumber());
    }

    private List<SerializedDomainEventData> createTestData() {

        SerializedDomainEventData message1 = new DumbEvent(1);
        SerializedDomainEventData message2 = new DumbEvent(2);
        SerializedDomainEventData message3 = new DumbEvent(3);
        SerializedDomainEventData message5 = new DumbEvent(5);
        SerializedDomainEventData message8 = new DumbEvent(8);
        SerializedDomainEventData message13 = new DumbEvent(13);
        SerializedDomainEventData message21 = new DumbEvent(21);

        SerializedDomainEventData[] messages = new SerializedDomainEventData[]{
                message1, message2, message3, message5, message8, message13, message21
        };
        List<SerializedDomainEventData> domainEventMessages = Arrays.asList(messages);
        Collections.shuffle(domainEventMessages);
        return domainEventMessages;
    }


    private static class DumbEvent implements SerializedDomainEventData<Object> {

        private DumbEvent(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        private long sequenceNumber;

        private UUID uuid = UUID.randomUUID();

        private String identifier = UUID.randomUUID().toString();

        @Override

        public long getSequenceNumber() {
            return this.sequenceNumber;
        }

        @Override
        public Object getAggregateIdentifier() {
            return uuid.toString();
        }

        @Override
        public String getEventIdentifier() {
            return identifier;  //To change body of implemented methods use File | Settings | File Templates.
        }

//        @Override
//        public DomainEventMessage<Object> withMetaData(Map<String, Object> stringObjectMap) {
//            return this;
//        }
//
//        @Override
//        public DomainEventMessage<Object> andMetaData(Map<String, Object> stringObjectMap) {
//            return this;
//        }

//        @Override
//        public String getIdentifier() {
//            return identifier;
//        }


        @Override
        public SerializedObject<Object> getMetaData() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public SerializedObject<Object> getPayload() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public DateTime getTimestamp() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

//        @Override
//        public MetaData getMetaData() {
//            return null;  //To change body of implemented methods use File | Settings | File Templates.
//        }

//        @Override
//        public Object getPayload() {
//            return null;  //To change body of implemented methods use File | Settings | File Templates.
//        }
//
//        @Override
//        public Class getPayloadType() {
//            return null;  //To change body of implemented methods use File | Settings | File Templates.
//        }
    }
}
