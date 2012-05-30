package sweforce.axon.eventstore.bdb;

import org.axonframework.domain.DomainEventMessage;
import org.axonframework.domain.MetaData;
import org.axonframework.serializer.SerializedDomainEventData;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: sveffa
 * Date: 5/29/12
 * Time: 8:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventEntrySequenceComparator implements Comparator<SerializedDomainEventData>{

    boolean latestFirst = false;

    public EventEntrySequenceComparator withLatestFirst(){
        latestFirst = true;
        return this;
    }

    @Override
    public int compare(SerializedDomainEventData one, SerializedDomainEventData other) {
        int result = new Long(one.getSequenceNumber()).compareTo(new Long(other.getSequenceNumber()));
        return latestFirst ? 0 - result : result;
    }




}
