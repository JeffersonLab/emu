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
 * which are the types of CODA components such as ROC, EB, or ER.
 *
 * @author timmer
 */
public enum CODAClass {

    /** Read controller. */
    ROC("read out controller", 1),

    /** Event Builder. */
    EB("event builder", 2),

    /** Event Builder. */
    CDEB("event builder", 2),

    /** Event Recorder. */
    ER("event recorder", 3),

    /** Analysis application. */
    ANA("analysis application", 4);


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