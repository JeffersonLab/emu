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


import com.lmax.disruptor.RingBuffer;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAStateMachine;
import org.jlab.coda.emu.support.codaComponent.StatedObject;
import org.jlab.coda.emu.support.data.RingItem;

import java.util.concurrent.BlockingQueue;

/**
 * This interface defines an object that can send and
 * receive data in any format listed in the
 * {@link org.jlab.coda.emu.support.data.QueueItemType} enum.
 * It refers to a particular connection (eg. a single socket
 * or cMsg connection object).
 *
 * @author heyes
 * @author timmer
 * Created on Sep 12, 2008
 */
public interface DataChannel extends CODAStateMachine, StatedObject {

    /**
     * Get the CODA ID number of the CODA component connected to this
     * data channel. In event building, this is used, for example, to check
     * the ROC id of incoming event which allows consistency checking.
     * @return the CODA ID number the CODA component connected to this
     *         data channel.
     */
    public int getID();

    /**
     * Set the CODA ID number of the CODA component connected to this
     * data channel.
     * @return the CODA ID number the CODA component connected to this
     *         data channel.
     */
    public void setID(int id);

    /**
     * Get the module which created this channel.
     * @return module which created this channel.
     */
    public EmuModule getModule();

    /**
     * Get the record ID number of the latest event through this channel.
     * In CODA event building this is used, for example, to track the
     * record ids for input events which allows consistency
     * checks of incoming data.
     * @return the record ID number of the latest event through channel.
     */
    public int getRecordId();

    /**
     * Set the record ID number of the latest event through this channel.
     * @param recordId record ID number of the latest event through channel.
     */
    public void setRecordId(int recordId);

    /**
     * Get the name of this data channel.
     * @return the name of this data channel.
     */
    public String name();

    /**
     * Get whether this channel is an input channel (true),
     * or it is an output channel (false).
     * @return <code>true</code> if input channel, else <code>false</code>
     */
    public boolean isInput();

    /**
     * Get the DataTransport object used to create this data channel.
     * @return the DataTransport object used to create this data channel.
     */
    public DataTransport getDataTransport();

    /**
     * Take an item of data off this channel's queue.
     * @return item of data.
     * @throws InterruptedException on wakeup without data.
     */
    public RingItem receive() throws InterruptedException;
    
    /**
     * Place a data item on this channel's queue.
     * @param item item of data.
     * @throws InterruptedException possible while waiting to place item on queue.
     */
    public void send(RingItem item) throws InterruptedException;

    /**
     * Get the first data queue of this data channel which contains
     * QueueItem objects of data for either receiving or sending data.
     * @return the queue of QueueItem objects for either receiving or sending data.
     */
    public BlockingQueue<RingItem> getQueue();

    /**
     * Get the first ring buffer of this data channel which contains
     * RingItem objects of data for either receiving or sending data.
     * There is only one if this is an input channel.
     * @return the ring buffer of RingItem objects for either receiving or sending data.
     */
    public RingBuffer<RingItem> getRing();

    /**
     * Get the total number of data queues.
     * @return total number of data queues.
     */
    public int getQCount();

    /**
     * Get the array of ring buffers which contains at least one element.
     * @return array of ring buffers.
     */
    public RingBuffer<RingItem>[] getRingBuffers();
}
