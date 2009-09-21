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
package org.jlab.coda.support.logger;

/**
 * @author vlads
 *         <p/>
 *         Convinient method to format debug data
 */
class LoggerDataWrapper {

    /** Field text */
    private final String text;

    /**
     * Constructor LoggerDataWrapper creates a new LoggerDataWrapper instance.
     *
     * @param v1 of type boolean
     */
    public LoggerDataWrapper(boolean v1) {
        this.text = String.valueOf(v1);
    }

    /**
     * Constructor LoggerDataWrapper creates a new LoggerDataWrapper instance.
     *
     * @param v1 of type long
     */
    public LoggerDataWrapper(long v1) {
        this.text = String.valueOf(v1);
    }

    /**
     * Constructor LoggerDataWrapper creates a new LoggerDataWrapper instance.
     *
     * @param v1 of type Object
     */
    public LoggerDataWrapper(Object v1) {
        this.text = String.valueOf(v1);
    }

    /**
     * Constructor LoggerDataWrapper creates a new LoggerDataWrapper instance.
     *
     * @param v1 of type long
     * @param v2 of type long
     */
    public LoggerDataWrapper(long v1, long v2) {
        this.text = String.valueOf(v1) + " " + String.valueOf(v2);
    }

    /**
     * Constructor LoggerDataWrapper creates a new LoggerDataWrapper instance.
     *
     * @param v1 of type String
     * @param v2 of type String
     */
    public LoggerDataWrapper(String v1, String v2) {
        this.text = v1 + " " + v2;
    }

    /**
     * Method toString ...
     *
     * @return String
     */
    public String toString() {
        return this.text;
    }
}
