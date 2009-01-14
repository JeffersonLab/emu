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

/**
 *
 */
package org.jlab.coda.support.configurer;

/** @author heyes */
@SuppressWarnings("serial")
public class DataNotFoundException extends Exception {
    /**
     * Constructor DataNotFoundException creates a new DataNotFoundException instance.
     *
     * @param message of type String
     */
    public DataNotFoundException(String message) {
        super(message);
    }

}
