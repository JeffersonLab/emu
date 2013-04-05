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


import org.jlab.coda.emu.support.data.QueueItem;
import org.jlab.coda.jevio.EvioBank;

import java.util.concurrent.BlockingQueue;

/**
 * This interface defines an object that can send and
 * receive banks of data in the CODA evio format. It
 * refers to a particular connection (eg. a single socket
 * or cMsg connection id).
 *
 * @author heyes
 *         Created on Sep 12, 2008
 */
public interface DataChannel {

    /**
     * Get the name of the data channel.
     * @return the name of the data channel.
     */
    public String getName();

    /**
     * Get the ID number of the data channel.
     * In CODA event building, this is used, for example, to contain the ROC
     * id for input channels which allows consistency checks of incoming data.
     * @return the ID number of the data channel.
     */
    public int getID();

    /**
     * Get whether this channel is an input channel (returns true),
     * or it is an output channel (returns false).
     * @return <code>true</code> if input channel, else <code>false</code>
     */
    public boolean isInput();

    public DataTransport getDataTransport();

    /**
     * Take a item of data off the queue.
     * @return item of data.
     * @throws InterruptedException on wakeup without data.
     */
    public QueueItem receive() throws InterruptedException;
    
    /**
     * Send a item of data.
     * @param data item of data.
     */
    public void send(QueueItem data);

    /**
     * Close this data channel gracefully, waiting if necessary.
     * For an "END" transition, any data sending or receiving
     * threads will wait for an "END"  event to come through before
     * closing.
     */
    public void close();

    /**
     * Close this data channel immediately, interrupting all threads.
     * Called during "RESET" transition.
     */
    public void reset();

    /**
     * Get the queue of this data channel which contains
     * QueueItems of data.
     *
     * @return the queue of data banks sent to this data channel (type BlockingQueue&lt;QueueItem&gt;).
     */
    public BlockingQueue<QueueItem> getQueue();

}
