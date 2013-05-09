/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;

import java.util.HashMap;
import java.util.Map;

/**
 * Implement a DataTransport that creates FIFO DataChannels based on the BlockingQueue.
 * There is one Fifo transport object for each EMU.
 *
 * @author heyes
 * @author timmer
 * Date: Nov 10, 2008
 */
public class DataTransportImplFifo extends DataTransportAdapter {

    /** Map of all Fifo channels. */
    private final HashMap<String, DataChannel> allChannels = new HashMap<String, DataChannel>();

    /**
     * Constructor.
     *
     * @param pname  name of transport
     * @param attrib transport's attribute map from config file
     * @param emu  emu object this transport belongs to
     *
     * @throws DataNotFoundException when "server" and "class" attributes
     *                               are missing from attrib map.
     */
    public DataTransportImplFifo(String pname, Map<String, String> attrib, Emu emu) throws DataNotFoundException {
        super(pname, attrib, emu);
    }

    /** {@inheritDoc}. Remove all fifo channels. */
    public void end() {
        allChannels.clear();
    }

    /** {@inheritDoc}. Remove all fifo channels. */
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
