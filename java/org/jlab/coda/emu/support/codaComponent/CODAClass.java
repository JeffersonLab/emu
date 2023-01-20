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

    /** Trigger supervisor. */
    TS("trigger supervisor", 1210),

    /** GT */
    GT("gt", 1110),

    /** FPGA (not used in emu) */
    FPGA("fpga", 960),

    /** Read out controller. */
    ROC("read out controller", 1010),

    /** Data concentrator, first level event builder to be followed by SEB. */
    DC("data concentrator", 910),

    /** Generic representation of either PEBER or SEBER. */
    EBER("event builder and recorder", 810),

    /** Primary event builder and event recorder connected with fifo in one emu - to be used with ROCs. */
    PEBER("primary event builder and recorder", 810),

    /** Secondary event builder and event recorder connected with fifo in one emu - to be used with DCs. */
    SEBER("secondary event builder and recorder", 810),

    /** Secondary event builder - to be used with DC's. */
    SEB("secondary event builder", 610),

    /** Primary event builder (one and only one event builder). */
    PEB("primary event builder", 510),

    /** Secondary time slice aggregator - to be used after PAGG's when streaming.*/
    SAGG("secondary slice aggregator", 660),

    /** Primary time slice aggregator to be used in place of DC or PEB when streaming. */
    PAGG("primary slice aggregator", 560),

    /** Farm Controller. */
    FCS("farm controller", 410),

    /** Event Recorder. */
    ER("event recorder", 310),

    /** Slow control component. */
    SLC("slow control component", 110),

    /** User component. */
    USR("user component", 10),

    /** Event management unit. */
    EMU("event management unit", 0);

    /** Description of the CODA class. */
    private final String description;

    /** Priority of CODA class in run control. */
    private final int priority;

    /** Map containing mapping of string of CODA class name to an enum/command. */
    private static HashMap<String, CODAClass> codaClassToEnumMap = new HashMap<>(11);

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
     * @return description string.
     */
    public String description() {
        return description;
    }

    /**
     * Get the default priority associated with this CODA class.
     * @return priority.
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Is this class representative of an event building emu?
     * @return {@code true} if this class represents an event building emu,
     *         else {@code false}.
     */
    public boolean isEventBuilder() {
        return (this == DC || this == SEB || this == PEB || this == PEBER || this == SEBER);
    }

    /**
     * Is this class representative of a final event building emu (NOT DC)?
     * @return {@code true} if this class represents a final event building emu,
     *         else {@code false}.
     */
    public boolean isFinalEventBuilder() {
        return (this == SEB || this == PEB || this == PEBER || this == SEBER);
    }

    /**
     * Is this class representative of a time slice aggregating emu?
     * @return {@code true} if this class represents an aggregatomg emu,
     *         else {@code false}.
     */
    public boolean isAggregator() {
        return (this == SAGG || this == PAGG);
    }

    /**
     * Is this class representative of a time slice aggregating emu (DC) connected to VTP(s)?
     * @return {@code true} if this class represents an aggregating DC,
     *         else {@code false}.
     */
    public boolean isPrimaryAggregator() { return (this == PAGG); }

    /**
     * Is this class representative of an event recording emu?
     * @return {@code true} if this class represents an event recording emu,
     *         else {@code false}.
     */
    public boolean isEventRecorder() {
        return (this == ER);
    }

}