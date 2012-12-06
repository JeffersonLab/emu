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

import java.util.HashMap;

/**
 * This enum specifies values associated with evio event types used in CODA online components.
 * @author timmer
 */
public enum EventType {

    ROC_RAW              (0),
    PHYSICS              (1),
    PARTIAL_PHYSICS      (2),
    DISENTANGLED_PHYSICS (3),
    USER                 (4),
    // Control Event Types:
    CONTROL              (5), // prestart, go, pause or end
    PRESTART             (6),
    GO                   (7),
    PAUSE                (8),
    END                  (9),
    SYNC                (10),
    OTHER               (15);

    private int value;

    /** Faster way to convert integer values into names. */
    private static HashMap<Integer, String> names = new HashMap<Integer, String>(32);

    /** Faster way to convert integer values into EventType objects. */
    private static HashMap<Integer, EventType> types = new HashMap<Integer, EventType>(32);


    // Fill static hashmaps after all enum objects created
    static {
        for (EventType item : EventType.values()) {
            types.put(item.value, item);
            names.put(item.value, item.name());
        }
    }


	/**
	 * Obtain the enum from the value.
	 *
	 * @param val the value to match.
	 * @return the matching enum, or <code>null</code>.
	 */
    public static EventType getEventType(int val) {
        return types.get(val);
    }


    /**
     * Obtain the name from the value.
     *
     * @param val the value to match.
     * @return the name, or <code>null</code>.
     */
    public static String getName(int val) {
        return names.get(val);
    }


    private EventType(int value) {
        this.value   = value;
    }


    /**
     * Get the integer value of this enum.
     * @return the integer value of this enum.
     */
    public int getValue() {
        return value;
    }

    /**
     * Is this a control event type?
     * @return <code>true</code> if control event type, else <code>false</code>
     */
    public boolean isControl() {
        return (this.equals(PRESTART) || this.equals(GO) ||
                this.equals(PAUSE)    || this.equals(END) ||
                this.equals(SYNC)     || this.equals(CONTROL));
    }

    /**
     * Is this a prestart control event type?
     * @return <code>true</code> if prestart control event type, else <code>false</code>
     */
    public boolean isPrestart() {
        return this.equals(PRESTART);
    }

    /**
     * Is this a go control event type?
     * @return <code>true</code> if go control event type, else <code>false</code>
     */
    public boolean isGo() {
        return this.equals(GO);
    }

    /**
     * Is this a pause control event type?
     * @return <code>true</code> if pause control event type, else <code>false</code>
     */
    public boolean isPause() {
        return this.equals(PAUSE);
    }

    /**
     * Is this an end control event type?
     * @return <code>true</code> if end control event type, else <code>false</code>
     */
    public boolean isEnd() {
        return this.equals(END);
    }

    /**
     * Is this an sync control event type?
     * @return <code>true</code> if sync control event type, else <code>false</code>
     */
    public boolean isSync() {
        return this.equals(SYNC);
    }

    /**
     * Is this a data event type?
     * @return <code>true</code> if data event type, else <code>false</code>
     */
    public boolean isROCRaw() {
        return this.equals(ROC_RAW);
    }

    /**
     * Is this a any kind of a physics event type?
     * @return <code>true</code> if any kind of a physics event type, else <code>false</code>
     */
    public boolean isAnyPhysics() {
        return (this.equals(PHYSICS) || this.equals(PARTIAL_PHYSICS) ||
                this.equals(DISENTANGLED_PHYSICS));
    }

    /**
     * Is this a fully-built, but entangled physics event type?
     * @return <code>true</code> if complete, but entangled physics event type, else <code>false</code>
     */
    public boolean isPhysics() {
        return this.equals(PHYSICS);
    }

    /**
     * Is this a partially-built physics event type?
     * If so, this event did not yet make it through all the layers of event building.
     * @return <code>true</code> if partially-built physics event type, else <code>false</code>
     */
    public boolean isPartialPhysics() {
        return this.equals(PARTIAL_PHYSICS);
    }

    /**
     * Is this a fully-built, disentangled physics event type?
     * @return <code>true</code> if complete & disentangled physics event type, else <code>false</code>
     */
    public boolean isDisentangledPhysics() {
        return this.equals(DISENTANGLED_PHYSICS);
    }

    /**
     * Is this a type appropriate for the event builder?
     * @return <code>true</code> if appropriate for the event builder, else <code>false</code>
     */
    public boolean isEbFriendly() {
        return (isBuildable()  || this.equals(USER));
    }

    /**
     * Is this a type buildable by the event builder?
     * @return <code>true</code> if buildable by the event builder, else <code>false</code>
     */
    public boolean isBuildable() {
        return (isControl() || this.equals(ROC_RAW) ||
                this.equals(PHYSICS) || this.equals(PARTIAL_PHYSICS));
    }

    /**
     * Is this a user event type?
     * @return <code>true</code> if user event type, else <code>false</code>
     */
    public boolean isUser() {
        return this.equals(USER);
    }



}
