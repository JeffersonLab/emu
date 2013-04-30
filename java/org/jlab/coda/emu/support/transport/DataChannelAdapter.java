/*
 * Copyright (c) 2013, Jefferson Science Associates
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
import org.jlab.coda.emu.EmuEventNotify;
import org.jlab.coda.emu.EmuStateMachineAdapter;
import org.jlab.coda.emu.support.data.QueueItem;
import org.jlab.coda.emu.support.logger.Logger;

import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This interface defines an object that can send and
 * receive banks of data in the CODA evio format. It
 * refers to a particular connection (eg. a single socket
 * or cMsg connection id).
 *
 * @author timmer
 *         Created on Apr 25, 2013
 */
public class DataChannelAdapter extends EmuStateMachineAdapter implements DataChannel {

    /** Channel id (corresponds to sourceId of ROCs for CODA event building). */
    protected int id;

    /** Channel name */
    protected final String name;

    /** Is this channel an input (true) or output (false) channel? */
    protected final boolean input;

    /** EMU object that created this channel. */
    protected final Emu emu;

    /** Logger associated with this EMU (convenience member). */
    protected final Logger logger;

    /** Byte order of output data. */
    protected ByteOrder byteOrder;

    /** Object used by Emu to be notified of END event arrival. */
    protected EmuEventNotify endCallback;

    /** Object used by Emu to create this channel. */
    protected final DataTransport dataTransport;

    /** Queue used to hold data for either input or output depending on {@link #input}. */
    protected final BlockingQueue<QueueItem> queue;



    public DataChannelAdapter(String name, DataTransport transport,
                          Map<String, String> attrib, boolean input,
                          Emu emu) {
        this.emu = emu;
        this.name = name;
        this.input = input;
        this.dataTransport = transport;
        logger = emu.getLogger();

        // Set queue capacity.
        // 100 buffers * 100 events/buf * 220 bytes/Roc/ev =  2.2Mb/Roc
        int capacity = 100;
        try {
            capacity = dataTransport.getIntAttr("capacity");
        }
        catch (Exception e) {}
        queue = new LinkedBlockingQueue<QueueItem>(capacity);


        // Set id number. Use any defined in config file, else use default = 0
        id = 0;
        String attribString = attrib.get("id");
        if (attribString != null) {
            try {
                id = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {}
        }


        // Set endianness of output data
        byteOrder = ByteOrder.BIG_ENDIAN;
        try {
            String order = attrib.get("endian");
            if (order != null && order.equalsIgnoreCase("little")) {
                byteOrder = ByteOrder.LITTLE_ENDIAN;
            }
        } catch (Exception e) {}


    }

    /** {@inheritDoc} */
    public int getID() {return id;}

    /** {@inheritDoc} */
    public String getName() {return name;}

    /** {@inheritDoc} */
    public boolean isInput() {return input;}

    /** {@inheritDoc} */
    public DataTransport getDataTransport() {return dataTransport;}

    /** {@inheritDoc}.
     *  Will block until data item becomes available. */
    public QueueItem receive() throws InterruptedException {return queue.take();}
    
    /** {@inheritDoc}.
     *  Will block until space is available in output queue. */
    public void send(QueueItem item) throws InterruptedException {
        queue.put(item);     // blocks if capacity reached
        //queue.add(item);   // throws exception if capacity reached
        //queue.offer(item); // returns false if capacity reached
    }

    /** {@inheritDoc} */
    public BlockingQueue<QueueItem> getQueue() {return queue;}

    /** {@inheritDoc} */
    public void registerEndCallback(EmuEventNotify callback) {endCallback = callback;}

    /** {@inheritDoc} */
    public EmuEventNotify getEndCallback() {return endCallback;}


}
