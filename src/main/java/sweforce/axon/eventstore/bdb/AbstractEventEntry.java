package sweforce.axon.eventstore.bdb;

import com.sleepycat.persist.model.*;
import org.axonframework.domain.DomainEventMessage;
import org.axonframework.serializer.SerializedDomainEventData;
import org.axonframework.serializer.SerializedMetaData;
import org.axonframework.serializer.SerializedObject;
import org.axonframework.serializer.SimpleSerializedObject;
import org.joda.time.DateTime;

import static com.sleepycat.persist.model.Relationship.*;

import java.util.Arrays;


/**
 * Base class for a persistent event entry.
 */
@Persistent(version = 1)
public abstract class AbstractEventEntry implements SerializedDomainEventData {

    @PrimaryKey
    private String eventIdentifier;

    @SecondaryKey(relate = ONE_TO_ONE, name = "identifier_type_sequence")
    private IdentifierTypeSequenceKey identifierTypeSequenceKey;


    private String aggregateIdentifier;

    private Long sequenceNumber;

    private String timeStamp;

    private String type;

    private String payloadType;

    private String payloadRevision;

    private byte[] metaData;

    private byte[] payload;



    protected AbstractEventEntry() {
    }

    protected AbstractEventEntry(String type, DomainEventMessage event,
                                 SerializedObject<byte[]> payload, SerializedObject<byte[]> metaData) {
        this.eventIdentifier = event.getIdentifier();
        this.type = type;
        this.payloadType = payload.getType().getName();
        this.payloadRevision = payload.getType().getRevision();
        this.payload = payload.getData();
        this.aggregateIdentifier = event.getAggregateIdentifier().toString();
        this.sequenceNumber = event.getSequenceNumber();
        this.metaData = Arrays.copyOf(metaData.getData(), metaData.getData().length);
        this.timeStamp = event.getTimestamp().toString();
        identifierTypeSequenceKey = new IdentifierTypeSequenceKey(this.getAggregateIdentifier(), this.getType(), this.getSequenceNumber());
    }

    public String getEventIdentifier() {
        return eventIdentifier;
    }

    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Returns the time stamp of the associated event.
     *
     * @return the time stamp of the associated event.
     */
    @Override
    public DateTime getTimestamp() {
        return new DateTime(timeStamp);
    }

    public String getType() {
        return type;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public String getPayloadRevision() {
        return payloadRevision;
    }

    @Override
    public SerializedObject<byte[]> getPayload() {
        return new SimpleSerializedObject<byte[]>(payload, byte[].class, payloadType, payloadRevision);
    }

    @Override
    public SerializedObject<byte[]> getMetaData() {
        return new SerializedMetaData<byte[]>(metaData, byte[].class);
    }
}
