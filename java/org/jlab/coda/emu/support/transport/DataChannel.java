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


import org.jlab.coda.emu.support.codaComponent.CODAStateMachine;
import org.jlab.coda.emu.support.codaComponent.StatedObject;
import org.jlab.coda.emu.support.data.QueueItemIF;

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
     * Get the ID number of this data channel.
     * In CODA event building, this is used, for example, to contain the ROC
     * id for input channels which allows consistency checks of incoming data.
     * @return the ID number of this data channel.
     */
    public int getID();

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
    public QueueItemIF receive() throws InterruptedException;
    
    /**
     * Place a data item on this channel's queue.
     * @param item item of data.
     * @throws InterruptedException possible while waiting to place item on queue.
     */
    public void send(QueueItemIF item) throws InterruptedException;

    /**
     * Get the queue of this data channel which contains
     * QueueItem objects of data for either receiving or sending data.
     * @return the queue of QueueItem objects for either receiving or sending data.
     */
    public BlockingQueue<QueueItemIF> getQueue();

}
