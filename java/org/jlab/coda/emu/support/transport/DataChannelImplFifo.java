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
import org.jlab.coda.emu.support.data.QueueItem;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.jevio.EvioBank;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 10, 2008
 * Time: 1:37:43 PM
 * Implementation of a DataChannel using BlockingQueue (FIFO)
 */
@SuppressWarnings({"RedundantThrows"})
public class DataChannelImplFifo implements DataChannel {

    /** Field name */
    private final String name;

    /** ID of this channel (corresponds to sourceId of ROCs for CODA event building). */
    private int id;

    /** Field full - filled buffer queue */
    private final BlockingQueue<QueueItem> queue;

    /** Is this channel an input (true) or output (false) channel? */
    private boolean input;

    /** Transport object this channel belongs to. */
    private DataTransportImplFifo dataTransport;


    /**
     * Constructor DataChannelImplFifo creates a new DataChannelImplFifo instance.
     *
     * @param name          of type String
     * @param dataTransport of type DataTransport
     * @param input         true if this is an input
     *
     * @throws DataTransportException - unable to create fifo buffer.
     */
    DataChannelImplFifo(String name, DataTransportImplFifo dataTransport,
                        Map<String, String> attributeMap, boolean input, Emu emu) {

        this.name  = name;
        this.input = input;
        this.dataTransport = dataTransport;

        int capacity = 40;
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
            emu.getLogger().info(e.getMessage() + " default to " + capacity + " records.");
        }

        int size = 20000;
        try {
            size = dataTransport.getIntAttr("size");
        } catch (Exception e) {
            emu.getLogger().info(e.getMessage() + " default to " + size + " byte records.");
        }

        queue = new ArrayBlockingQueue<QueueItem>(capacity);

        // Set id number. Use any defined in config file else use default (0)
        id = 0;
        String idVal = attributeMap.get("id");
        if (idVal != null) {
            try {
                id = Integer.parseInt(idVal);
            }
            catch (NumberFormatException e) {  }
        }

    }

    public String getName() {
        return name;
    }

    public int getID() {
        return id;
    }

    public boolean isInput() {
        return input;
    }

    public DataTransport getDataTransport() {
        return dataTransport;
    }

    /**
     * This method receives or gets QueueItem objects from this object's queue.
     *
     * @return QueueItem object containing data
     * @throws InterruptedException on wakeup of fifo with no data
     */
    public QueueItem receive() throws InterruptedException {
        return queue.take();
    }

    /**
     * {@inheritDoc}
     * QueueItem is sent to this object's queue.
     * @param item {@inheritDoc}
     */
    public void send(QueueItem item) {
        queue.add(item);
    }

    /** {@inheritDoc} */
    public void close() {
        queue.clear();
    }

    /** {@inheritDoc} */
    public void reset() {
        close();
    }

    /**
     * {@inheritDoc}
     * @return {@inheritDoc}
     */
    public BlockingQueue<QueueItem> getQueue() {
        return queue;
    }

}
