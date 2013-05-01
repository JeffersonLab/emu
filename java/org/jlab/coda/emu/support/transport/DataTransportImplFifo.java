package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;

import java.util.HashMap;
import java.util.Map;

/**
 * Implement a DataTransport that uses DataChannel based on BlockingQueue.
 * 1 Fifo transport object for each EMU object.
 *
 * @author heyes
 * @author timmer
 * Date: Nov 10, 2008
 */
public class DataTransportImplFifo extends DataTransportAdapter {
    /** Map of all Fifo channels. */
    private final HashMap<String, DataChannel> allChannels = new HashMap<String, DataChannel>();

    public DataTransportImplFifo(String pname, Map<String, String> attrib, Emu emu) throws DataNotFoundException {
        super(pname, attrib, emu);
    }

    public void end() {
        allChannels.clear();
    }

    public void reset() {
         allChannels.clear();
    }


    /** {@inheritDoc} */
    synchronized public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                                  boolean isInput, Emu emu) {
//System.out.println("    DataTransportImplFifo.createChannel : create channel " + name);
        String channelName = name() + ":" + name;

        // see if channel (queue) has already been created
        DataChannel c = allChannels.get(channelName);

        // if not, create it
        if (c == null) {
            c = new DataChannelImplFifo(channelName, this, attributeMap, isInput, emu);
            allChannels.put(channelName, c);
System.out.println("    DataTransportImplFifo.createChannel() : create FIFO channel " + c.name());
        }
        else {
System.out.println("    DataTransportImplFifo.createChannel() : return stored FIFO channel " + c.name());
        }
        return c;
    }

}
