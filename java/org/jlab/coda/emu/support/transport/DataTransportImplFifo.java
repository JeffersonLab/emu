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
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;

import java.util.HashMap;
import java.util.Map;

/**
 * Implement a DataTransport that creates FIFO DataChannels based on the RingBuffer.
 *
 * @author heyes
 * @author timmer
 * Date: Nov 10, 2008
 */
public class DataTransportImplFifo extends DataTransportAdapter {

    /** Map of all Fifo channels. */
    private final HashMap<String, DataChannelImplFifo> allChannels =
            new HashMap<String, DataChannelImplFifo>();

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
                                                  boolean isInput, Emu emu,
                                                  EmuModule module)
            throws DataTransportException {

        String channelName = name() + ":" + name;

        // See if channel has already been created
        DataChannelImplFifo c = allChannels.get(channelName);

        // Fifos are both input and output channels at the same time.
        // The object type that the fifo must accept as input must be
        // the same as its expected output, else throw an exception.
        // In the EMU, in prestart, each fifo is "created" twice, once as input and
        // the other as output. So we can check for this (in)compatibility.

        // If not created yet, create it
        if (c == null) {
            c = new DataChannelImplFifo(channelName, this, attributeMap, isInput,
                                        emu, module);
            allChannels.put(channelName, c);
if (isInput) {
    System.out.println("    DataTransport Fifo : create channel " + c.name() + " as input");
}
else {
    System.out.println("    DataTransport Fifo : create channel " + c.name() + " as output");
}
        }
        // If we're trying to "create" it again, make sure things are compatible.
        else {
            EmuModule firstModule = c.getModule();

            if (isInput) {
                if (module.getInputRingItemType() != firstModule.getOutputRingItemType()) {
                    throw new DataTransportException("Modules require inconsistent in/output object types");
                }
            }
            else {
                if (module.getOutputRingItemType() != firstModule.getInputRingItemType()) {
                    throw new DataTransportException("Modules require inconsistent in/output object types");
                }
            }

            // Fifos must be created in 2 stages (this is the 2nd).
            // If it is first created as an input, then when the
            // Fifo is "created" here as an output channel, it must be properly
            // setup for that. Similarly for the other way around.
            if (isInput) {
                c.setupInputRingBuffers();
System.out.println("    DataTransport Fifo : setup channel " + c.name() + " as input");
            }
            else {
                c.setupOutputRingBuffers();
System.out.println("    DataTransport Fifo : setup channel " + c.name() + " as output");
            }

        }
        return c;
    }

}
