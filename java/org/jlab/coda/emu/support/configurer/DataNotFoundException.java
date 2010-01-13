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


package org.jlab.coda.emu.support.configurer;

/** @author heyes timmer */
@SuppressWarnings("serial")
public class DataNotFoundException extends Exception {

    /**
     * Constructor DataNotFoundException creates a new DataNotFoundException instance.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     */
    public DataNotFoundException(String message) {
        super(message);
    }


    /**
     * Constructor DataNotFoundException creates a new DataNotFoundException instance.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public DataNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }


    /**
     * Constructor DataNotFoundException creates a new DataNotFoundException instance.
     *
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public DataNotFoundException(Throwable cause) {
        super(cause);
    }
}
