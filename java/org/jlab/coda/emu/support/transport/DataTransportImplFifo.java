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
                                                  boolean isInput, Emu emu,
                                                  EmuModule module)
            throws DataTransportException {

//System.out.println("    DataTransportImplFifo.createChannel : create channel " + name);
        String channelName = name() + ":" + name;

        // See if channel (queue) has already been created
        DataChannel c = allChannels.get(channelName);

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
System.out.println("    DataTransportImplFifo.createChannel() : create FIFO channel " + c.name());
        }
        // If we're trying to create it again, make sure things are compatible.
        else {
            EmuModule firstModule = c.getModule();

            if (isInput) {
                if (module.getInputQueueItemType() != firstModule.getOutputQueueItemType()) {
                    throw new DataTransportException("Modules require inconsistent in/output object types");
                }
            }
            else {
                if (module.getOutputQueueItemType() != firstModule.getInputQueueItemType()) {
                    throw new DataTransportException("Modules require inconsistent in/output object types");
                }
            }

System.out.println("    DataTransportImplFifo.createChannel() : return stored FIFO channel " + c.name());
        }
        return c;
    }

}
