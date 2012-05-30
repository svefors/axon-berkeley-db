package sweforce.axon.eventstore.bdb;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.SecondaryKey;
import org.axonframework.domain.DomainEventMessage;
import org.axonframework.serializer.SerializedObject;

import static com.sleepycat.persist.model.Relationship.ONE_TO_ONE;
import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;
/**
 * Created with IntelliJ IDEA.
 * User: sveffa
 * Date: 5/29/12
 * Time: 8:32 PM
 * To change this template use File | Settings | File Templates.
 */
@Entity(version = 1)
public class SnapshotEventEntry extends AbstractEventEntry{

    @SecondaryKey(relate = MANY_TO_ONE, name = "identifier_type_snapshotevents")
    private IdentifierTypeKey identifierTypeKey;

    protected SnapshotEventEntry() {
    }

    public SnapshotEventEntry(String type, DomainEventMessage event, SerializedObject<byte[]> payload, SerializedObject<byte[]> metaData) {
        super(type, event, payload, metaData);
        identifierTypeKey  = new IdentifierTypeKey().withAggregateIdentifier(this.getAggregateIdentifier()).withType(this.getType());
    }
}
