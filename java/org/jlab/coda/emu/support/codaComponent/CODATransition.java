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
 *                  <-                          |           ^
 *                 |                            |           |
 *        go       |                            |           |
 *                 |                            |           |
 *                 '->  ACTIVE  --------------->'---------->'
 *
 * __________________________________________________________________
 *
 *  NOTE: DOWNLOADED can always do a download,
 *        CONFIGURED can always do a configure, &
 *        RESET goes from any state to CONFIGURED (to BOOTED if in ERROR or BOOTED state)
 *
 * </pre></code>
 * @author heyes
 * @author timmer
 */
public enum CODATransition {

    /** Configure transition. */
    CONFIGURE("Load the configuration", "CONFIGURED"),
    /** Download transition. */
    DOWNLOAD("Apply the configuration and load", "DOWNLOADED"),
    /** Prestart transition. */
    PRESTART("Prepare to start", "PAUSED"),
    /** Go transition. */
    GO("Start taking data", "ACTIVE"),
    /** End transition. */
    END("End taking data", "DOWNLOADED"),
    /** Pause transition. */
    PAUSE("Pause taking data", "PAUSED"),
    /** Reset transition. */
    RESET("Return to configured state", "CONFIGURED");

    
    /** Description of the transition. */
    private final String description;

    /** CODA run control state entered into if transition succeeded. */
    private final String successState;


    /**
     * Constructor CODATransition creates a new CODATransition instance.
     * Would be nice if 2nd arg was a CODAState object, but that creates
     * a chicken and egg problem when creating these enum objects.
     *
     * @param description of type String
     * @param successState state EMU ends up in if transition successful
     */
    CODATransition(String description, String successState) {
        this.description = description;
        this.successState = successState;
    }

    /**
     * Get the description of this transition.
     */
    public String description() {
        return description;
    }

    /**
     * Returns the CODA run control State (CODAState enum object) upon success of this transition.
     * @return State (CODAState enum object) upon success of this transition
     */
    public State success() {
        return CODAState.get(successState);
    }
}
