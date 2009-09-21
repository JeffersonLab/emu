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

/**
 * This enum enumerates the possible actions of a "run object"
 * as opposed to a "session object" or "coda object". The run
 * object is aware of the run but does not respond to CODA
 * run control transition commands as a coda object would.
 *
 * This class does NOT seem much used - only the "configure"
 * value.
 *
 * @author heyes
 */

public enum RunControl implements Command {

    /** Field setState */
    SET_STATE("set state", null),
    /** Field setSession */
    SET_SESSION("set session", null),
    /** Field getSession */
    GET_SESSION("get session", null),
    /** Field setInterval */
    SET_INTERVAL("set interval", null),
    /** Field start */
    START("start", null),
    /** Field stop */
    STOP("stop", null),
    /** Field reset */
    RESET("return to pre-configured state", "UNCONFIGURED"),
    /** Field exit */
    EXIT("shutdown the codaComponent", null),
    /** Field startReporting */
    START_REPORTING("start reporting", null),
    /** Field stopReporting */
    STOP_REPORTING("stop reporting", null),
    /** Field configure */
    CONFIGURE("load the configuration", "CONFIGURED"),
    /** Field releaseSession */
    RELEASE_SESSION("release session", null);

    /** Description of the command to the run object. */
    private final String description;

    /** Field args */
    private HashMap<String, Object> args = new HashMap<String, Object>();

    private String success = null;

    private boolean enabled = true;

    /**
     * Constructor which creates a new RunControl instance.
     *
     * @param description description of the run control command
     * @param success of the run control command
     */
    RunControl (String description, String success) {
        this.description = description;
        this.success = success;
    }

    /** @see org.jlab.coda.support.control.Command#description() */
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
        //ignore
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

    /** @see org.jlab.coda.support.control.Command#success() */
    public State success() {
        if (success != null) {
            return CODAState.valueOf(success);
        }
        return null;
    }

}