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

public enum RunControl implements Command {
    /** Field setState */setState("Set State", null),
    /** Field setSession */setSession("Set Session", null),
    /** Field getSession */getSession("getSession", null),
    /** Field setInterval */setInterval("setInterval", null),
    /** Field start */start("start", null),
    /** Field stop */stop("stop", null),
    /** Field reset */reset("Return to pre-configured state", "UNCONFIGURED"),
    /** Field exit */exit("Shutdown the codaComponent", null),
    /** Field startReporting */startReporting("start reporting", null),
    /** Field stopReporting */stopReporting("stop reporting", null),
    /** Field configure */configure("Load the configuration", "CONFIGURED"),
    /** Field releaseSession */releaseSession("release session", null);

    /** Field description */
    private final String description;

    /** Field args */
    private HashMap<String, Object> args = new HashMap<String, Object>();

    private String success = null;

    private boolean enabled = true;

    /**
     * Constructor CODATransition creates a new CODATransition instance.
     *
     * @param d of type String
     */
    RunControl(String d, String s) {
        description = d;
        success = s;
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