package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.Command;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 10, 2008
 * Time: 1:18:47 PM
 * Implement a DataTransport that uses DataCHannel based on EVIO format files.
 */
public class DataTransportImplFile extends DataTransportCore implements DataTransport {

    public DataTransportImplFile(String pname, Map<String, String> attrib) throws DataNotFoundException {
        super(pname, attrib);
    }

    /** {@inheritDoc} */
    public void execute(Command cmd) {
        // Dummy - nothing to see here folks, move along.
    }

    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap, boolean isInput)
            throws DataTransportException {
        System.out.println("create channel " + name);
        DataChannel c = new DataChannelImplFile(name() + ":" + name, this, isInput);
        channels().put(c.getName(), c);
        System.out.println("put channel " + c.getName());
        System.out.println("channels " + channels() + " " + this);
        return c;
    }

}