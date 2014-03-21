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
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAStateMachineAdapter;
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.data.QueueItem;
import org.jlab.coda.emu.support.data.QueueItemType;
import org.jlab.coda.emu.support.logger.Logger;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class provides boilerplate code for the DataChannel
 * interface (which includes the CODAStateMachine interface).
 * Extending this class implements the DataChannel interface and frees
 * any subclass from having to implement common methods or those that aren't used.<p>
 * This class defines an object that can send and
 * receive banks of data in the CODA evio format. It
 * refers to a particular connection (eg. an et open
 * or cMsg connection id).
 *
 * @author timmer
 *         (Apr 25, 2013)
 */
public class DataChannelAdapter extends CODAStateMachineAdapter implements DataChannel {

    /** Channel id (corresponds to sourceId of ROCs for CODA event building). */
    protected int id;

    /** Record id (corresponds to evio events flowing through data channel). */
    protected int recordId;

    /** Channel state. */
    protected State state;

    /**
     * Channel error message. reset() sets it back to null.
     * Making this an atomically settable String ensures that only 1 thread
     * at a time can change its value. That way it's only set once per error.
     */
    protected AtomicReference<String> errorMsg = new AtomicReference<String>();

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

    /** Module to which this channel belongs. */
    protected final EmuModule module;

    /** Object used by Emu to create this channel. */
    protected final DataTransport dataTransport;

    /** Type of object to expect in each queue item. */
    protected QueueItemType queueItemType;

    /** First queue used to hold data for either input or output depending on {@link #input}. */
    protected final BlockingQueue<QueueItem> queue;

    /** Number of data queues (does not include controlQ). */
    protected int qCount;

    /** List holding all queues used to hold data for either input or output depending on {@link #input}. */
    protected final List<BlockingQueue<QueueItem>> queueList;

    /** Queue used to hold incoming, non-buildable, control, & user events. */
    protected final BlockingQueue<QueueItem> controlQ;



    /**
     * Constructor to create a new DataChannel instance.
     * Used only by a transport's createChannel() method
     * which is only called during PRESTART in the Emu.
     *
     * @param name          the name of this channel
     * @param transport     the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     * @param emu           emu this channel belongs to
     * @param module        module this channel belongs to
    */
    public DataChannelAdapter(String name, DataTransport transport,
                              Map<String, String> attributeMap,
                              boolean input, Emu emu,
                              EmuModule module) {
        this.emu = emu;
        this.name = name;
        this.input = input;
        this.module = module;
        this.dataTransport = transport;
        logger = emu.getLogger();

        if (input) {
            queueItemType = module.getInputQueueItemType();
        }
        else {
            queueItemType = module.getOutputQueueItemType();
        }

        // Set queue capacity
        // 100 buffers * 100 events/buf * 220 bytes/Roc/ev =  2.2Mb/Roc
        int capacity = 100;
        try {
            capacity = dataTransport.getIntAttr("capacity");
        }
        catch (Exception e) {}


        // Set number of data queues
        qCount = 1;
        try {
            qCount = module.getIntAttr("qCount");
            if (qCount < 1) {
                qCount = 1;
            }
        }
        catch (Exception e) {}


        // Create data queues
        queueList = new ArrayList<BlockingQueue<QueueItem>>(qCount);
        for (int i=0; i < qCount; i++) {
            queueList.add(new LinkedBlockingQueue<QueueItem>(capacity));
        }
        queue = queueList.get(0);

        // Create non-data queue
        controlQ = new LinkedBlockingQueue<QueueItem>(20);


        // Set id number. Use any defined in config file, else use default = 0
        id = 0;
        String attribString = attributeMap.get("id");
        if (attribString != null) {
            try {
                id = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {}
        }


        // Set endianness of output data
        byteOrder = ByteOrder.BIG_ENDIAN;
        try {
            String order = attributeMap.get("endian");
            if (order != null && order.equalsIgnoreCase("little")) {
                byteOrder = ByteOrder.LITTLE_ENDIAN;
            }
        } catch (Exception e) {}
    }

    /** {@inheritDoc} */
    public int getID() {return id;}

    /** {@inheritDoc} */
    public void setID(int id) {this.id = id;}

    /** {@inheritDoc} */
    public int getRecordId() {return recordId;}

    /** {@inheritDoc} */
    public void setRecordId(int recordId) {this.recordId = recordId;}

    /** {@inheritDoc} */
    public State state() {return state;}

    /** {@inheritDoc} */
    public String getError() {return errorMsg.get();}

    /** {@inheritDoc} */
    public String name() {return name;}

    /** {@inheritDoc} */
    public boolean isInput() {return input;}

    /** {@inheritDoc} */
    public DataTransport getDataTransport() {return dataTransport;}

    /** {@inheritDoc}
     *  Will block until data item becomes available. */
    public QueueItem receive() throws InterruptedException {return queue.take();}
    
    /** {@inheritDoc}
     *  Will block until space is available in output queue. */
    public void send(QueueItem item) throws InterruptedException {
        queue.put(item);     // blocks if capacity reached
        //queue.add(item);   // throws exception if capacity reached
        //queue.offer(item); // returns false if capacity reached
    }

    /** {@inheritDoc} */
    public BlockingQueue<QueueItem> getQueue() {return queue;}

    /** {@inheritDoc} */
    public int getQCount() {return qCount;}

    /** {@inheritDoc} */
    public List<BlockingQueue<QueueItem>> getQueueList() {return queueList;}

    /** {@inheritDoc} */
    public BlockingQueue<QueueItem> getControlQ() {return controlQ;}

    /** {@inheritDoc} */
    public void registerEndCallback(EmuEventNotify callback) {endCallback = callback;}

    /** {@inheritDoc} */
    public EmuEventNotify getEndCallback() {return endCallback;}


}
