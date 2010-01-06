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

package org.jlab.coda.support.transport;


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
     * Take a bank of data off the queue.
     * @return bank of data.
     * @throws InterruptedException on wakeup without data.
     */
    public EvioBank receive() throws InterruptedException;
    
    /**
     * Send a bank of data.
     * @param data bank of data.
     */
    public void send(EvioBank data);

    /** Close this data channel. */
    public void close();

    /**
     * Get the queue of this data channel which contains
     * banks of data.
     *
     * @return the queue of data banks sent to this data channel.
     */
    public BlockingQueue<EvioBank> getQueue();

}
