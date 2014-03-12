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
 * This enum specifies tag values associated with CODA
 * control types used in CODA online components.
 * @author timmer
 */
public enum ControlType {

    SYNC       (0xFFD0),
    PRESTART   (0xFFD1),
    GO         (0xFFD2),
    PAUSE      (0xFFD3),
    END        (0xFFD4),
    ;

    private int value;


    /** Fast way to convert integer values into ControlType objects. */
    private static ControlType[] intToType;


    // Fill array after all enum objects created
    static {
        int index = 0;
        intToType = new ControlType[5];
        for (ControlType type : values()) {
            intToType[index++] = type;
        }
    }


	/**
	 * Obtain the enum from the value.
	 *
	 * @param val the value to match.
	 * @return the matching enum, or <code>null</code>.
	 */
    public static ControlType getControlType(int val) {
        if (val > 0xFFD4 || val < 0xFFD0) return null;
        return intToType[val & 0xf];
    }


    /**
     * Obtain the name from the value.
     *
     * @param val the value to match.
     * @return the name, or <code>null</code>.
     */
    public static String getName(int val) {
        if (val > 0xFFD4 || val < 0xFFD0) return null;
        ControlType type = getControlType(val);
        if (type == null) return null;
        return type.name();
    }


    private ControlType(int value) {
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
     * Is this a control tag of any sort?
     * @param value the tag value to check
     * @return <code>true</code> if control tag or any sort, else <code>false</code>
     */
    public static boolean isControl(int value) {
        return getControlType(value) != null;
    }


    /**
     * Is this a "prestart" control tag?
     * @return <code>true</code> if prestart control tag, else <code>false</code>
     */
    public boolean isPrestart() {
        return this == PRESTART;
    }

    /**
     * Is this a "prestart" control tag?
     * @param value the tag value to check
     * @return <code>true</code> if prestart control tag, else <code>false</code>
     */
    public static boolean isPrestart(int value) {
        ControlType cType = getControlType(value);
        return cType != null && (cType == PRESTART);
    }

    /**
     * Is this a "go" control tag?
     * @return <code>true</code> if go control tag, else <code>false</code>
     */
    public boolean isGo() {
        return this == GO;
    }

    /**
     * Is this a "go" control tag?
     * @param value the tag value to check
     * @return <code>true</code> if go control tag, else <code>false</code>
     */
    public static boolean isGo(int value) {
        ControlType cType = getControlType(value);
        return cType != null && (cType == GO);
    }

    /**
     * Is this a "pause" control tag?
     * @return <code>true</code> if pause control tag, else <code>false</code>
     */
    public boolean isPause() {
        return this.equals(PAUSE);
    }

    /**
     * Is this a "pause" control tag?
     * @param value the tag value to check
     * @return <code>true</code> if pause control tag, else <code>false</code>
     */
    public static boolean isPause(int value) {
        ControlType cType = getControlType(value);
        return cType != null && (cType == PRESTART);
    }

    /**
     * Is this an "end" control tag?
     * @return <code>true</code> if end control tag, else <code>false</code>
     */
    public boolean isEnd() {
        return this == END;
    }

    /**
     * Is this an "end" control tag?
     * @param value the tag value to check
     * @return <code>true</code> if end control tag, else <code>false</code>
     */
    public static boolean isEnd(int value) {
        ControlType cType = getControlType(value);
        return cType != null && (cType == END);
    }

    /**
     * Is this a sync control tag?
     * @return <code>true</code> if sync control tag, else <code>false</code>
     */
    public boolean isSync() {
        return this == SYNC;
    }

    /**
     * Is this an "sync" control tag?
     * @param value the tag value to check
     * @return <code>true</code> if end control tag, else <code>false</code>
     */
    public static boolean isSync(int value) {
        ControlType cType = getControlType(value);
        return cType != null && (cType == SYNC);
    }


}
