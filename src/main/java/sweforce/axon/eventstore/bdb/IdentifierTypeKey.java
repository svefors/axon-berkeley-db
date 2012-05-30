package sweforce.axon.eventstore.bdb;

import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;

/**
 * Created with IntelliJ IDEA.
 * User: sveffa
 * Date: 5/29/12
 * Time: 8:19 PM
 * To change this template use File | Settings | File Templates.
 */
@Persistent(version = 1)
public class IdentifierTypeKey {

    @KeyField(1)
    private String aggregateIdentifier;

    @KeyField(2)
    private String type;

    private IdentifierTypeKey(String aggregateIdentifier, String type) {
        this.aggregateIdentifier = aggregateIdentifier;
        this.type = type;
    }

    public IdentifierTypeKey withAggregateIdentifier(Object identifier) {
        return new IdentifierTypeKey(identifier.toString(), this.type);
    }

    public IdentifierTypeKey withType(String type) {
        return new IdentifierTypeKey(this.aggregateIdentifier, type);
    }

    public IdentifierTypeKey() {
    }
}
