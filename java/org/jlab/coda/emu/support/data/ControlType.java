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

    SYNC       (0xFFD0, 0),
    PRESTART   (0xFFD1, 1),
    GO         (0xFFD2, 2),
    PAUSE      (0xFFD3, 3),
    END        (0xFFD4, 4),
    ;

    private int value;
    private int ordinalValue; // enum value can be expressed in 1 byte, useful over the wire


    /** Fast way to convert integer values into ControlType objects. */
    private static ControlType[] intToType;


    // Fill array after all enum objects created
    static {
        intToType = new ControlType[0xf + 1];
        for (ControlType type : values()) {
            intToType[type.ordinalValue] = type;
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
   	 * Obtain the enum from the ordinal value.
   	 *
   	 * @param val the ordinal value to match.
   	 * @return the matching enum, or <code>null</code>.
   	 */
       public static ControlType getControlTypeFromOrdinal(int val) {
           if (val > 4 || val < 0) return null;
           return intToType[val];
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


    private ControlType(int value, int ordinalValue) {
        this.value = value;
        this.ordinalValue = ordinalValue;
    }


    /**
     * Get the integer value of this enum.
     * @return the integer value of this enum.
     */
    public int getValue() {
        return value;
    }


    /**
     * Get the ordinal value of this enum.
     * @return the ordinal value of this enum.
     */
    public int getOrdinalValue() {
        return ordinalValue;
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
