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


package org.jlab.coda.support.configurer;

/** @author heyes timmer */
@SuppressWarnings("serial")
public class DataNotFoundException extends Exception {

    /**
     * {@inheritDoc}<p/>
     * Constructor DataNotFoundException creates a new DataNotFoundException instance.
     *
     * @param message {@inheritDoc}<p/>
     */
    public DataNotFoundException(String message) {
        super(message);
    }


    /**
     * {@inheritDoc}<p/>
     *
     * @param message {@inheritDoc}<p/>
     * @param cause {@inheritDoc}<p/>
     */
    public DataNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }


    /**
     * {@inheritDoc}<p/>
     *
     * @param cause {@inheritDoc}<p/>
     */
    public DataNotFoundException(Throwable cause) {
        super(cause);
    }
}
