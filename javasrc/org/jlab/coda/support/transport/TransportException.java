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

/**
 * <pre>
 * Class <b>TransportException </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class TransportException extends Exception {

    /** Field serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * Constructor TransportException creates a new TransportException instance.
     *
     * @param message of type String
     */
    public TransportException(String message) {
        super(message);
    }
}
