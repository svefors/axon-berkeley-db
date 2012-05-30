package sweforce.axon.eventstore.bdb;

import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;

/**

 */
@Persistent(version = 1)
public class IdentifierTypeSequenceKey {

    @KeyField(1)
    private String aggregateIdentifier;

    @KeyField(2)
    private String type;

    @KeyField(3)
    private long sequenceNumber;


    IdentifierTypeSequenceKey() {
    }

    public IdentifierTypeSequenceKey(String aggregateIdentifier, String type, long sequenceNumber) {
        this.aggregateIdentifier = aggregateIdentifier;
        this.type = type;
        this.sequenceNumber = sequenceNumber;
    }


}
