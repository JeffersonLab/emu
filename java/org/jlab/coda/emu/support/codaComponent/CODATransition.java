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
import org.jlab.coda.emu.support.messaging.RCConstants;

import java.util.EnumSet;
import java.util.HashMap;

/**
 * An enum which contains a list of possible transitions in CODA Emu state machine.
 * Each of these transitions can be enabled or disabled.
 *
 * <code><pre>
 *                 *****************
 *                 * State Machine *
 *                 *****************
 * _________________________________________________________________
 *                 |                 |
 *    transition   |      STATE      |  transition
 * ________________|_________________|______________________________
 *
 *
 *                  <- UNCONFIGURED
 *                 |
 *     configure   |
 *                 |               <------------------------,
 *                 '-> CONFIGURED ->----------------------->|
 *                  <-                                      ^
 *                 |                                        |
 *     download    |                                        |
 *                 |                                        |
 *                 '-> DOWNLOADED ->----------------------->|
 *                  <-            <-,<----------,           ^
 *                 |                ^           ^           |
 *                 |                |           |           |
 *     prestart    |                | end       | end       | reset
 *                 |                |           |           |
 *                 '-> PRESTARTED -> -----------|---------->|
 *                  <-            <-            |           ^
 *                 |                ^           |           |
 *        go       |                | pause     |           |
 *                 |                |           |           |
 *                 '->  ACTIVE  --->'---------->'---------->'
 *
 * __________________________________________________________________
 *
 *  NOTE: DOWNLOADED can always do a download,
 *        RESET goes from any state to UNCONFIGURED
 *
 * </pre></code>
 * @author heyes
 */
public enum CODATransition implements Command {

    /** Field configure */
    CONFIGURE("Load the configuration", "CONFIGURED", true),
    /** Download. */
    DOWNLOAD("Apply the configuration and load", "DOWNLOADED", false),
    /** Prestart. */
    PRESTART("Prepare to start", "PRESTARTED", false),
    /** Go. */
    GO("Start taking data", "ACTIVE", false),
    /** End. */
    END("End taking data", "DOWNLOADED", false),
    /** Pause. */
    PAUSE("Pause taking data", "PRESTARTED", false),
    /** Reset. */
    RESET("Return to configured state", "CONFIGURED", true);

    
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
     * @param success state EMU ends up in if transition successful
     * @param enabled true if this transition is enabled to start with
     */
    CODATransition(String description, String success, boolean enabled) {
        this.description = description;
        this.success = success;
        this.enabled = enabled;
    }

    /**
     * Get the description of this transition.
     * @see org.jlab.coda.emu.support.control.Command#description()
     */
    public String description() {
        return description;
    }

    /**
     * Is this transition enabled?
     * @see org.jlab.coda.emu.support.control.Command#isEnabled()
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
