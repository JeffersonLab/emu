package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.logger.Logger;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 10, 2008
 * Time: 1:18:47 PM
 * Implement a DataTransport that uses DataChannel based on BlockingQueue
 */
public class DataTransportImplFifo extends DataTransportCore implements DataTransport {

    public DataTransportImplFifo(String pname, Map<String, String> attrib, Emu emu) throws DataNotFoundException {
        super(pname, attrib, emu);
    }

    /** {@inheritDoc} */
    public void execute(Command cmd) {
        // Dummy, nothing to see here people, move along...
    }

    /** {@inheritDoc} */
    synchronized public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                                  boolean isInput, Emu emu) {
//System.out.println("    DataTransportImplFifo.createChannel : create channel " + name);
        String channelName = name() + ":" + name;
        // see if channel (queue) has already been created
        DataChannel c = channels().get(channelName);
        // if not, create it
        if (c ==  null) {
            c = new DataChannelImplFifo(channelName, this, isInput, emu);
            channels().put(channelName, c);
        }
//System.out.println("    DataTransportImplFifo.createChannel : put channel " + c.getName());
        return c;
    }

}
