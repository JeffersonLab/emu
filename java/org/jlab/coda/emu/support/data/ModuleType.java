package org.jlab.coda.emu.support.data;


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


    /** Fast way to convert integer values into ModuleType objects. */
    private static ModuleType[] intToType;


    // Fill array after all enum objects created
    static {
        intToType = new ModuleType[3];
        for (ModuleType type : values()) {
            intToType[type.value] = type;
        }
    }


	/**
	 * Obtain the enum from the value.
	 *
	 * @param val the value to match.
	 * @return the matching enum, or <code>null</code>.
	 */
    public static ModuleType getTagType(int val) {
        if (val > 3 || val < 0) return null;
        return intToType[val];
    }


    /**
     * Obtain the name from the value.
     *
     * @param val the value to match.
     * @return the name, or <code>null</code>.
     */
    public static String getName(int val) {
        if (val > 3 || val < 0) return null;
        ModuleType type = getTagType(val);
        if (type == null) return null;
        return type.name();
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
