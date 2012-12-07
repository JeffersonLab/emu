package org.jlab.coda.emu.support.data;

import java.util.HashMap;

/**
 * This enum specifies values associated with data acquisition
 * module types used in CODA online components.
 * @author timmer
 */
public enum ModuleType {

    FADC250  (0),
    FADC125  (1),
    TDC      (2);

    private int value;

    /** Faster way to convert integer values into names. */
    private static HashMap<Integer, String> names = new HashMap<Integer, String>(16);

    /** Faster way to convert integer values into ModuleType objects. */
    private static HashMap<Integer, ModuleType> types = new HashMap<Integer, ModuleType>(16);


    // Fill static hashmaps after all enum objects created
    static {
        for (ModuleType item : ModuleType.values()) {
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
    public static ModuleType getTagType(int val) {
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


    private ModuleType(int value) {
        this.value   = value;
    }

    /**
     * Obtain the enum from the value.
     *
     * @param value the value to match.
     * @return the matching enum, or <code>null</code>.
     */
    public static ModuleType getModuleType(int value) {
        ModuleType moduleTypes[] = ModuleType.values();
        for (ModuleType dt : moduleTypes) {
            if (dt.value == value) {
                return dt;
            }
        }
        return null;
    }

    /**
     * Get the integer value of this enum.
     * @return the integer value of this enum.
     */
    public int getValue() {
        return value;
    }


    /**
     * Is this an FADC250 module type?
     * @return <code>true</code> if FADC250 module type, else <code>false</code>
     */
    public boolean isFADC250() {
        return this.equals(FADC250);
    }

    /**
     * Is this an FADC125 module type?
     * @return <code>true</code> if FADC125 module type, else <code>false</code>
     */
    public boolean isFADC125() {
        return this.equals(FADC125);
    }

    /**
     * Is this a TDC module type?
     * @return <code>true</code> if TDC module type, else <code>false</code>
     */
    public boolean isTDC() {
        return this.equals(TDC);
    }



}
