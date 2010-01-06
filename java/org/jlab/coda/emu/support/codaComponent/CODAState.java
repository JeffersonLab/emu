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

import org.jlab.coda.emu.support.control.State;
import java.util.EnumSet;
import java.util.Vector;

/**
 * This class is an enum which lists all the possible CODA Emu
 * state-machine states. Each of these states has a corresponding set
 * of transitions that are allowed.
 * This is the only class that implements the {@link State} interface.
 * 
 * <code><pre>
 *                 *****************
 *                 * State Machine *
 *                 *****************
 * ____________________________________________________
 *                 |                 |
 *    transition   |      STATE      |  transition
 * ________________|_________________|__________________
 *
 *
 *                  <- UNCONFIGURED
 *                 |
 *     configure   |
 *                 |
 *                 '-> CONFIGURED
 *                  <-
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
 *
 * @author heyes
 */
public enum CODAState implements State {

    /** UNCONFIGURED state. */
    UNCONFIGURED("codaComponent is not configured", EnumSet.of(CODATransition.CONFIGURE)),
    /** CONFIGURED state. */
    CONFIGURED("configuration is loaded", EnumSet.of(CODATransition.DOWNLOAD)),
    /** DOWNLOADED state (same as ENDED). */
    DOWNLOADED("external modules loaded", EnumSet.of(CODATransition.DOWNLOAD, CODATransition.PRESTART)),
    /** PRESTARTED state (same as PAUSED). */
    PRESTARTED("modules initialized and ready to go", EnumSet.of(CODATransition.GO, CODATransition.END)),
    /** ACTIVE state. */
    ACTIVE("taking data", EnumSet.of(CODATransition.PAUSE, CODATransition.END)),
    /** ERROR state. */
    ERROR("an error has occured", EnumSet.noneOf(CODATransition.class));

    /** Description of this state. */
    private final String description;

    /** Vector of exception causes. */
    private final Vector<Throwable> causes = new Vector<Throwable>();

    /** Set of all transitions allowed out of this state. */
    private final EnumSet allowed;

    //private boolean reported = false;

    /**
     * Constructor.
     *
     * @param description description of this state
     * @param allowed     set of transitions allowed out of this state
     */
    CODAState(String description, EnumSet allowed) {
        this.description = description;
        this.allowed = allowed;

    }

    /**
     * Get the description of this state.
     * @return description of this state
     * @see org.jlab.coda.emu.support.control.State#getDescription()
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the vector containing the causes of any exceptions
     * of an attempted transition from this state.
     * @return vector(type Vector<Throwable>) of causes of any execeptions
     *         of an attempted transition from this state.
     */
    public Vector<Throwable> getCauses() {
        return causes;
    }

    /**
     * Get the set of transitions allowed out of this state.
     * @return set of transitions allowed out of this state
     */
    public EnumSet allowed() {
        return allowed;
    }

}
