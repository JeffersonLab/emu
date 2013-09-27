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
import org.jlab.coda.emu.support.data.QueueItemType;

import java.util.Map;

/**
 * Implementation of a DataChannel reading/writing from/to a fifo.
 *
 * @author heyes
 * @author timmer
 * (Nov 10, 2008)
 */
public class DataChannelImplFifo extends DataChannelAdapter {

    /**
     * Constructor DataChannelImplFifo creates a new DataChannelImplFifo instance.
     *
     * @param name          of type String
     * @param transport     of type DataTransport
     * @param input         true if this is an input
     * @param queueItemType type of object to expect in queue item
     *
     * @throws DataTransportException - unable to create fifo buffer.
     */
    DataChannelImplFifo(String name, DataTransportImplFifo transport,
                        Map<String, String> attributeMap, boolean input, Emu emu ,
                        QueueItemType queueItemType) {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, queueItemType);
    }


    /** {@inheritDoc} */
    public void end() {
        queue.clear();
    }

    /** {@inheritDoc} */
    public void reset() {
        queue.clear();
    }

}
