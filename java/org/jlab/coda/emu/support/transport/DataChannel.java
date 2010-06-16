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
     * @return the queue of data banks sent to this data channel (type BlockingQueue&lt;EvioBank&gt;).
     */
    public BlockingQueue<EvioBank> getQueue();

}
