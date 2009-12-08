package org.jlab.coda.support.transport;

import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.Command;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 10, 2008
 * Time: 1:18:47 PM
 * Implement a DataTransport that uses DataChannel based on BlockingQueue
 */
public class DataTransportImplFifo extends DataTransportCore implements DataTransport {

    public DataTransportImplFifo(String pname, Map<String, String> attrib) throws DataNotFoundException {
        super(pname, attrib);
    }

    /** {@inheritDoc} */
    public void execute(Command cmd) {
        // Dummy, nothing to see here people, move along...
    }

    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap, boolean isInput)
            throws DataTransportException {
System.out.println("    DataTransportImplFifo.createChannel : create channel " + name);
        DataChannel c = new DataChannelImplFifo(name() + ":" + name, this, isInput);
        channels().put(c.getName(), c);
System.out.println("    DataTransportImplFifo.createChannel : put channel " + c.getName());
//System.out.println("    DataTransportImplFifo.createChannel : channels " + channels() + " " + this);
        return c;
    }

}
