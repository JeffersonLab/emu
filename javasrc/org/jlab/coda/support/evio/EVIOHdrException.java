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

package org.jlab.coda.support.evio;

/**
 * <pre>
 * Class <b>EVIOHdrException </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
@SuppressWarnings("serial")
class EVIOHdrException extends Exception {
    /** Field header */
    private final int header;

    /**
     * Constructor EVIOHdrException creates a new EVIOHdrException instance.
     *
     * @param msg of type String
     * @param hdr of type int
     */
    public EVIOHdrException(String msg, int hdr) {
        super(msg);
        header = hdr;
    }

    /**
     * Method getHeader returns the header of this EVIOHdrException object.
     *
     * @return the header (type int) of this EVIOHdrException object.
     */
    public int getHeader() {
        return header;
    }
}
