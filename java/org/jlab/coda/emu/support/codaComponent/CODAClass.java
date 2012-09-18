/*
 * Copyright (c) 2011, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.codaComponent;


import java.util.HashMap;

/**
 * This class is an enum which lists all the possible CODA class values,
 * which are the types of CODA components such as ROC, EMU, or ER.
 *
 * @author timmer
 */
public enum CODAClass {

    /** Trigger supervisor. */
    TS("trigger supervisor", 1000),

    /** Read out controller. */
    ROC("read out controller", 900),

    /** Data concentrator (first level) type of event builder. */
    DC("data concentrator", 800),

    /** Secondary (second level) type of event builder - to be used with DC's. */
    SEB("event builder", 700),

    /** Primary event builder (one and only one event builder). */
    PEB("event builder", 600),

    /** Analysis application. */
    ANA("analysis application", 500),

    /** Event Recorder. */
    ER("event recorder", 400),

    /** Slow control component. */
    SLC("event recorder", 200),

    /** User component. */
    USR("event recorder", 0),

    /** Event management unit. */
    EMU("event management unit", 0);

    /** Description of the CODA class. */
    private final String description;

    /** Priority of CODA class in run control. */
    private final int priority;

    /** Map containing mapping of string of CODA class name to an enum/command. */
    private static HashMap<String, CODAClass> codaClassToEnumMap = new HashMap<String, CODAClass>();

    // Fill static hashmap after all enum objects created.
    static {
        for (CODAClass item : CODAClass.values()) {
            codaClassToEnumMap.put(item.name(), item);
        }
    }


    /**
     * Constructor CODAClass creates a new CODAClass instance.
     *
     * @param description of type String
     * @param priority default priority such a component has in runcontrol
     */
    CODAClass(String description, int priority) {
        this.priority = priority;
        this.description = description;
    }

    /**
     * Map from type of incoming message from CODA class name to a particular enum.
     * @param s CODA class name.
     * @return associated enum, else null.
     */
    public static CODAClass get(String s) {
        return codaClassToEnumMap.get(s);
    }

    /**
     * Get the description of this transition.
     * @see org.jlab.coda.emu.support.control.Command#description()
     */
    public String description() {
        return description;
    }

    /**
     * Get the default priority associated with this CODA class.
     * @return
     */
    public int getPriority() {
        return priority;
    }

 }