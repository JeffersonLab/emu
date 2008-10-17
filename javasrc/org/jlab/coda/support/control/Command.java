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

package org.jlab.coda.support.control;

import java.util.EnumSet;

/** @author heyes */
public interface Command {

    /**
     * Method name ...
     *
     * @return String
     */
    public String name();

    /** @return the description */
    public String description();

    /** @return the enabled */
    public boolean isEnabled();

    /** Method enable ... */
    public void enable();

    /** Method disable ... */
    public void disable();

    /**
     * Method allow ...
     *
     * @param cmds of type EnumSet
     */
    public void allow(EnumSet cmds);

    /**
     * Method getArg ...
     *
     * @param tag of type String
     * @return Object
     */
    public Object getArg(String tag);

    /**
     * Method setArg ...
     *
     * @param tag   of type String
     * @param value of type Object
     */
    public void setArg(String tag, Object value);

    /**
     * Method hasArgs ...
     *
     * @return boolean
     */
    public boolean hasArgs();

    /** Method clearArgs ... */
    public void clearArgs();

    /**
     * Method success ...
     *
     * @return State
     */
    public State success();
}
