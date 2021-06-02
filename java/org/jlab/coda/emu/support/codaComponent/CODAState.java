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

import static org.jlab.coda.emu.support.codaComponent.CODATransition.*;
import java.util.EnumSet;
import java.util.HashMap;

/**
 * This class is an enum which lists all the possible CODA Emu
 * state-machine states. Each of these states has a corresponding set
 * of transitions that are allowed {@link CODATransition}.
 * This is the only class that implements the {@link CODAStateIF} interface.
 *
 * <pre><code>
 *                 *****************
 *                 * State Machine *
 *                 *****************
 * ____________________________________________________
 *   INTERMEDIATE  |                 |   INTERMEDIATE
 *      STATE      |      STATE      |      STATE
 * ________________|_________________|_________________
 *
 *                               (if reset from ERROR or BOOTED)
 *                  &lt;- BOOTED &lt;-----------------------------,
 *                 |                                        |
 *   CONFIGURING   |                                        |
 *                 |               &lt;------------------------|
 *                 '-&gt; CONFIGURED -&gt;-----------------------&gt;|
 *                  &lt;-                                      ^
 *                 |                                        |
 *   DOWNLOADING   |                                        |
 *                 |                                        |
 *                 '-&gt; DOWNLOADED -&gt;-----------------------&gt;|
 *                  &lt;-            &lt;-,&lt;----------,           ^
 *                 |                ^           ^           |
 *                 |                |           |           |
 *   PRESTARTING   |                | ENDING    | ENDING    | reset
 *                 |                |           |           |
 *                 '-&gt; PAUSED -----&gt; -----------|----------&gt;|
 *                  &lt;-                          |           ^
 *                 |                            |           |
 *      GOING      |                            |           |
 *                 |                            |           |
 *                 '-&gt;  ACTIVE  ---------------&gt;'----------&gt;'
 *
 * __________________________________________________________________
 *
 *  NOTE: DOWNLOADED can always do a download,
 *        CONFIGURED can always do a configure, and
 *        RESET goes from any state to CONFIGURED (to BOOTED if in ERROR or BOOTED state)
 *
 * </code></pre>
 *
 * @author heyes
 * @author timmer
 */

public enum CODAState implements CODAStateIF {

    /** BOOTED state. */
    BOOTED("coda component not configured", EnumSet.of(CONFIGURE)),
    
    /** CONFIGURED state. */
    CONFIGURED("configuration loaded", EnumSet.of(CONFIGURE, DOWNLOAD, RESET)),

    /** DOWNLOADED state (same as ENDED). */
    DOWNLOADED("modules loaded", EnumSet.of(DOWNLOAD, PRESTART, RESET)),

    /** PAUSED state (same as PRESTARTED). */
    PAUSED("modules initialized, channels created, ready to go", EnumSet.of(GO, END, RESET)),

    /** ACTIVE state. */
    ACTIVE("taking data", EnumSet.of(PAUSE, END, RESET)),

    // INTERMEDIATE STATES (not used in state machine)

    /** Configuring state - in the process of configuring. */
    CONFIGURING("between booted and configured", EnumSet.noneOf(CODATransition.class)),

    /** Downloading state - in the process of downloading. */
    DOWNLOADING("between configured and downloaded", EnumSet.noneOf(CODATransition.class)),

    /** Prestarting state - in the process of prestarting. */
    PRESTARTING("between downloaded and paused", EnumSet.noneOf(CODATransition.class)),

    /** Activating state - in the process of becoming active. */
    ACTIVATING("between paused and active", EnumSet.noneOf(CODATransition.class)),

    /** Ending state - got END command but no END event yet. */
    ENDING("ending run", EnumSet.noneOf(CODATransition.class)),

    /** Resetting emu - got RESET command. */
    RESETTING("resetting", EnumSet.noneOf(CODATransition.class)),

    // ERROR

    /** ERROR state. */
    ERROR("an error has occurred", EnumSet.noneOf(CODATransition.class));

   //------------------------------------

    /** Description of this state. */
    private final String description;

    /** Set of all transitions allowed out of this state. */
    private final EnumSet<CODATransition> allowed;

    /** Map of CODAState name to an enum. */
    private final static HashMap<String, CODAState> commandTypeToEnumMap = new HashMap<>(12);


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
