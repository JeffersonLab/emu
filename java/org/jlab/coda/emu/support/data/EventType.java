/*
 * Copyright (c) 2010, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.data;

/**
 * This enum specifies values associated with CODA event types used in CODA online components.
 * @author timmer
 */
public enum EventType {

    ROC_RAW              (0),
    PHYSICS              (1),
    PARTIAL_PHYSICS      (2),
    DISENTANGLED_PHYSICS (3),
    USER                 (4),
    CONTROL              (5),
    MIXED                (6),
    OTHER                (15);

    private int value;


    /** Fast way to convert integer values into EventType objects. */
    private static EventType[] intToType;


    // Fill array after all enum objects created
    static {
        intToType = new EventType[16];
        for (EventType type : values()) {
            intToType[type.value] = type;
        }
    }


	/**
	 * Obtain the enum from the value.
	 *
	 * @param val the value to match.
	 * @return the matching enum, or <code>null</code>.
	 */
    public static EventType getEventType(int val) {
        if (val > 15 || val < 0) return null;
        return intToType[val];
    }


    /**
     * Obtain the name from the value.
     *
     * @param val the value to match.
     * @return the name, or <code>null</code>.
     */
    public static String getName(int val) {
        if (val > 15 || val < 0) return null;
        EventType type = getEventType(val);
        if (type == null) return null;
        return type.name();
    }


    EventType(int value) {
        this.value = value;
    }


    /**
     * Get the integer value of this enum.
     * @return the integer value of this enum.
     */
    public int getValue() {
        return value;
    }

    /**
     * Is this a mixed event type?
     * @return <code>true</code> if mixed event type, else <code>false</code>
     */
    public boolean isMixed() {
        return (this == MIXED);
    }

    /**
     * Is this a control event type?
     * @return <code>true</code> if control event type, else <code>false</code>
     */
    public boolean isControl() {
        return (this == CONTROL);
    }

    /**
     * Is this a usr or control event type?
     * @return <code>true</code> if user or control event type, else <code>false</code>
     */
    public boolean isUserOrControl() {
        return (this == USER || this == CONTROL);
    }

    /**
     * Is this a data event type?
     * @return <code>true</code> if data event type, else <code>false</code>
     */
    public boolean isROCRaw() {
        return this == ROC_RAW;
    }

    /**
     * Is this a any kind of a physics event type?
     * @return <code>true</code> if any kind of a physics event type, else <code>false</code>
     */
    public boolean isAnyPhysics() {
        return (this == PHYSICS || this == PARTIAL_PHYSICS ||
                this == DISENTANGLED_PHYSICS);
    }

    /**
     * Is this a fully-built, but entangled physics event type?
     * @return <code>true</code> if complete, but entangled physics event type, else <code>false</code>
     */
    public boolean isPhysics() {
        return this == PHYSICS;
    }

    /**
     * Is this a partially-built physics event type?
     * If so, this event did not yet make it through all the layers of event building.
     * @return <code>true</code> if partially-built physics event type, else <code>false</code>
     */
    public boolean isPartialPhysics() {
        return this == PARTIAL_PHYSICS;
    }

    /**
     * Is this a fully-built, disentangled physics event type?
     * @return <code>true</code> if complete and disentangled physics event type, else <code>false</code>
     */
    public boolean isDisentangledPhysics() {
        return this == DISENTANGLED_PHYSICS;
    }

    /**
     * Is this a type appropriate for the event builder?
     * @return <code>true</code> if appropriate for the event builder, else <code>false</code>
     */
    public boolean isEbFriendly() {
        return (isBuildable() || this == CONTROL || this == USER || this == MIXED);
    }

    /**
     * Is this a type buildable by the event builder?
     * @return <code>true</code> if buildable by the event builder, else <code>false</code>
     */
    public boolean isBuildable() {
        return (this == ROC_RAW || this == PHYSICS || this == PARTIAL_PHYSICS);
    }

    /**
     * Is this a user event type?
     * @return <code>true</code> if user event type, else <code>false</code>
     */
    public boolean isUser() {
        return this == USER;
    }



}
