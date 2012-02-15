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
import static org.jlab.coda.emu.support.codaComponent.CODATransition.*;
import java.util.EnumSet;
import java.util.HashMap;
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
 * _________________________________________________________________
 *                 |                 |
 *    transition   |      STATE      |  transition
 * ________________|_________________|______________________________
 *
 *                               (if reset from ERROR or BOOTED)
 *                  <- BOOTED <-----------------------------,
 *                 |                                        |
 *     configure   |                                        |
 *                 |               <------------------------|
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
 *                 '-> PAUSED -----> -----------|---------->|
 *                  <-            <-            |           ^
 *                 |                ^           |           |
 *        go       |                | pause     |           |
 *                 |                |           |           |
 *                 '->  ACTIVE  --->'---------->'---------->'
 *
 * __________________________________________________________________
 *
 *  NOTE: DOWNLOADED can always do a download,
 *        CONFIGURED can always do a configure, &
 *        RESET goes from any state to CONFIGURED or BOOTED if in ERROR state
 *
 * </pre></code>
 *
 * @author heyes
 * @author timmer
 */
public enum CODAState implements State {

    /** BOOTED state. */
    BOOTED("codaComponent is not configured", EnumSet.of(CONFIGURE)),
    
    /** CONFIGURED state. */
    CONFIGURED("configuration is loaded", EnumSet.of(CONFIGURE, DOWNLOAD, RESET)),

    /** DOWNLOADED state (same as ENDED). */
    DOWNLOADED("external modules loaded", EnumSet.of(DOWNLOAD, PRESTART, RESET)),

    /** PAUSED state (same as PAUSED). */
    PAUSED("modules initialized and ready to go", EnumSet.of(GO, END, RESET)),

    /** ACTIVE state. */
    ACTIVE("taking data", EnumSet.of(PAUSE, END, RESET)),

    /** ERROR state. */
    ERROR("an error has occurred", EnumSet.noneOf(CODATransition.class));


    /** Description of this state. */
    private final String description;

    /** Set of all transitions allowed out of this state. */
    private final EnumSet<CODATransition> allowed;


    /** Map of CODAState name to an enum. */
    private final static HashMap<String, CODAState> commandTypeToEnumMap = new HashMap<String, CODAState>();

    // Fill static hashmap after all enum objects created.
    static {
        for (CODAState item : CODAState.values()) {
            commandTypeToEnumMap.put(item.name(), item);
        }
    }

    /**
     * Map from CODAState name to a particular enum.
     * @param s name of CODAState.
     * @return associated enum, else null.
     */
    public static CODAState get(String s) {
        return commandTypeToEnumMap.get(s);
    }


    /**
     * Constructor.
     *
     * @param description description of this state
     * @param allowed     set of transitions allowed out of this state
     */
    CODAState(String description, EnumSet<CODATransition> allowed) {
        this.description = description;
        this.allowed = allowed;
    }

    /**
     * {@inheritDoc}
     */
    public String getDescription() {
        return description;
    }

    /**
     * {@inheritDoc}
     */
    public EnumSet<CODATransition> allowed() {
        return allowed;
    }

}
