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

import org.jlab.coda.support.data.DataBank;

import java.util.concurrent.BlockingQueue;

/**
 * Interface DataChannel ...
 *
 * @author heyes
 *         Created on Sep 12, 2008
 */
public interface DataChannel {
    /** @return the name */
    public String getName();

    /**
     * Method receive ...
     *
     * @return int[]
     */
    public DataBank receive() throws InterruptedException;

    /**
     * Method send ...
     *
     * @param data of type long[]
     */
    public void send(DataBank data);

    /** Method close ... */
    public void close();

    /**
     * Method getFull returns the full of this DataChannel object.
     *
     * @return the full (type BlockingQueue<DataBank>) of this DataChannel object.
     */
    public BlockingQueue<DataBank> getQueue();

}
