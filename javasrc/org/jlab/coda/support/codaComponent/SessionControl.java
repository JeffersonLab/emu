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

package org.jlab.coda.support.codaComponent;

import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;

import java.util.EnumSet;
import java.util.HashMap;

/** @author heyes */
public enum SessionControl implements Command {
    /** Field setRunNumber */setRunNumber("Set the run number"),
    /** Field getRunNumber */getRunNumber("Get the run number"),
    /** Field setRunType */setRunType("Set the run type"),
    /** Field getRunType */getRunType("Get the run type");

    /** Field args */
    private final HashMap<String, Object> args = new HashMap<String, Object>();

    /** Field description */
    private String description;

    boolean enabled = true;

    /**
     * Constructor CODATransition creates a new CODATransition instance.
     *
     * @param d
     */
    SessionControl(String d) {
        description = d;
    }

    /** @return the description */
    public String description() {
        return description;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void enable() {
        enabled = true;
    }

    public void disable() {
        enabled = false;
    }

    public void allow(EnumSet cmds) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Method getArg ...
     *
     * @param tag of type String
     * @return Object
     */
    public Object getArg(String tag) {
        return args.get(tag);
    }

    /**
     * Method setArg ...
     *
     * @param tag   of type String
     * @param value of type Object
     */
    public void setArg(String tag, Object value) {
        args.put(tag, value);
    }

    /** @see org.jlab.coda.support.control.Command#hasArgs() */
    public boolean hasArgs() {
        return !args.isEmpty();
    }

    /** Method clearArgs ... */
    public void clearArgs() {
        args.clear();
    }

    /**
     * Method success ...
     *
     * @return State
     */
    public State success() {
        // not used
        return null;

    }
}