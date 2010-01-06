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

package org.jlab.coda.emu.support.codaComponent;

import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;

import java.util.EnumSet;
import java.util.HashMap;

/**
 * This enum enumerates the possible actions of a "session object"
 * as opposed to a "run object" or "coda object". The session
 * object is aware only of the run number, run type, and reporting status.
 *
 * Currently this enum does NOT seem to be used.
 *
 * @author heyes
 */
public enum SessionControl implements Command {

    /** Field setRunNumber */
    SET_RUN_NUMBER("Set the run number"),
    /** Field getRunNumber */
    GET_RUN_NUMBER("Get the run number"),
    /** Field setRunType */
    SET_RUN_TYPE("Set the run type"),
    /** Field getRunType */
    GET_RUN_TYPE("Get the run type"),
    /** Field startReporting */
    START_REPORTING("start reporting"),
    /** Field stopReporting */
    STOP_REPORTING("stop reporting");

    /** Field args */
    private final HashMap<String, Object> args = new HashMap<String, Object>();

    /** Field description */
    private String description;

    boolean enabled = true;

    /**
     * Constructor that creates a new SessionControl instance.
     * @param description description of this session control command
     */
    SessionControl(String description) {
        this.description = description;
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

    /** @see org.jlab.coda.emu.support.control.Command#hasArgs() */
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