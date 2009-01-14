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
public class DataTransportException extends Exception {

    /** Field serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** Field cause - cause of this exception if caused by a lower level exception */

    private Exception cause = null;

    /**
     * Constructor TransportException creates a new TransportException instance.
     *
     * @param message of type String
     */
    public DataTransportException(String message) {
        super(message);
    }

    /**
     * Constructor TransportException creates a new TransportException instance.
     *
     * @param message of type String
     * @param cause   of type Exception
     */
    public DataTransportException(String message, Exception cause) {
        super(message);
        this.cause = cause;
    }

    /**
     * Method getCause returns the cause of this TransportException object.
     *
     * @return the cause (type Exception) of this TransportException object.
     */
    public Exception getCause() {
        return cause;
    }
}
