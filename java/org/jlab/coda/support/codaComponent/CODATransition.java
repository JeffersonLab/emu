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
 * An enum which contains a list of possible transitions in CODA Emu state machine.
 * Each of these transitions can be enabled or disabled.
 * Notice that "configure" is not listed here as it is considered a command sent by
 * runcontrol and not a transition {@link RunControl#CONFIGURE}.
 *
 * <code><pre>
 *                 *****************
 *                 * State Machine *
 *                 *****************
 * ____________________________________________________
 *                 |                |
 *    transition   |     STATE      |  transition
 * ________________|________________|__________________
 *
 *
 *                UNCONFIGURED/BOOTED
 *
 *
 *                  <- CONFIGURED
 *                 |
 *     download    |
 *                 |
 *                 '-> DOWNLOADED <-,<----------,
 *                  <-              ^           ^
 *                 |                |           |
 *     prestart    |                |   end     |
 *                 |                |           |
 *                 '-> PRESTARTED ->            |  end
 *                  <-            <-            |
 *                 |                ^           |
 *        go       |                |  pause    |
 *                 |                |           |
 *                 '->  ACTIVE  --->'---------->'
 *
 * ____________________________________________________
 *
 *  NOTE: DOWNLOADED can always do a download
 *
 * </pre></code>
 * @author heyes
 */
public enum CODATransition implements Command {

    /** Download. */
    DOWNLOAD("Apply the configuration and load", "DOWNLOADED"),
    /** Prestart. */
    PRESTART("Prepare to start", "PRESTARTED"),
    /** Go. */
    GO("Start taking data", "ACTIVE"),
    /** End. */
    END("End taking data", "DOWNLOADED"),
    /** Pause. */
    PAUSE("Pause taking data", "PRESTARTED");

    
    /** Description of the transition. */
    private final String description;

    /** CODA run control state entered into if transition suceeded. */
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

    /**
     * Get the description of this transition.
     * @see org.jlab.coda.support.control.Command#description()
     */
    public String description() {
        return description;
    }

    /**
     * Is this transition enabled?
     * @see org.jlab.coda.support.control.Command#isEnabled()
     */
    public boolean isEnabled() {

        return enabled;
    }

    /** Set this transition to be enabled. */
    public void enable() {
        enabled = true;
    }

    /** Set this transition to be disabled. */
    public void disable() {
        enabled = false;
    }

    /**
     * Enable the transitions given in the argument set.
     * @param cmds set of transitions to be marked as enabled
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
     * Returns the CODA run control State (CODAState enum object) upon success of this transition.
     * @return State (CODAState enum object) upon success of this transition
     */
    public State success() {
        return CODAState.valueOf(success);
    }
}
