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
public enum CODATransition implements Command {

    /** Field download */download("Apply the configuration and load", "DOWNLOADED"),
    /** Field prestart */prestart("Prepare to start", "PRESTARTED"),
    /** Field go */go("Start taking data", "ACTIVE"),
    /** Field end */end("End taking data", "ENDED"),
    /** Field pause */pause("Pause taking data", "PAUSED"),
    /** Field pause */resume("Resume taking data", "ACTIVE");

    /** Field description */
    private final String description;

    /** Field success */
    private final String success;

    /** Field args */
    private final HashMap<String, Object> args = new HashMap<String, Object>();

    /** Field enabled */
    private boolean enabled = false;

    /**
     * Constructor CODATransition creates a new CODATransition instance.
     *
     * @param description of type String
     * @param success     of type String
     */
    CODATransition(String description, String success) {
        this.description = description;
        this.success = success;
    }

    /** @see org.jlab.coda.support.control.Command#description() */
    public String description() {
        return description;
    }

    /** @see org.jlab.coda.support.control.Command#isEnabled() */
    public boolean isEnabled() {

        return enabled;
    }

    /** Method enable ... */
    public void enable() {
        enabled = true;
    }

    /** Method disable ... */
    public void disable() {
        enabled = false;
    }

    /**
     * Method allow ...
     *
     * @param cmds of type EnumSet
     */
    public void allow(EnumSet cmds) {
        for (CODATransition t : CODATransition.values()) {
            t.enabled = cmds.contains(t);
        }
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

    /**
     * Method hasArgs ...
     *
     * @return boolean
     */
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
        return CODAState.valueOf(success);
    }
}
