/*
 * Copyright (c) 2012, Jefferson Science Associates
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
 * This enum specifies values associated with tags used in CODA online components.
 * @author timmer
 */
public enum CODATag {
    // ROC
    RAW_TRIGGER                 (0xFF10),
    RAW_TRIGGER_TS              (0xFF11),

    // Trigger bank
    BUILT_TRIGGER_BANK          (0xFF20),
    BUILT_TRIGGER_TS            (0xFF21),
    BUILT_TRIGGER_RUN           (0xFF22),
    BUILT_TRIGGER_TS_RUN        (0xFF23),
    BUILT_TRIGGER_SPARSIFY      (0xFF24),
    BUILT_TRIGGER_RUN_SPARSIFY  (0xFF25),

    // Physics event
    DISENTANGLED_BANK           (0xFF30),
    BUILT_BY_PEB                (0xFF50),
    BUILT_BY_SEB                (0xFF70),
    ;

    private int value;

    /** Faster way to convert integer values into names. */
    private static HashMap<Integer, String> names = new HashMap<Integer, String>(32);

    /** Faster way to convert integer values into CODATag objects. */
    private static HashMap<Integer, CODATag> tags = new HashMap<Integer, CODATag>(32);


    // Fill static hashmaps after all enum objects created
    static {
        for (CODATag item : CODATag.values()) {
            tags.put(item.value, item);
            names.put(item.value, item.name());
        }
    }


	/**
	 * Obtain the enum from the value.
	 *
	 * @param val the value to match.
	 * @return the matching enum, or <code>null</code>.
	 */
    public static CODATag getTagType(int val) {
        return tags.get(val);
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


    private CODATag(int value) {
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
     * Is this a built trigger tag?
     * @return <code>true</code> if built trigger tag, else <code>false</code>
     */
    public boolean isBuiltTrigger() {
        return (this == BUILT_TRIGGER_BANK     || this == BUILT_TRIGGER_TS ||
                this == BUILT_TRIGGER_RUN      || this == BUILT_TRIGGER_TS_RUN ||
                this == BUILT_TRIGGER_SPARSIFY || this == BUILT_TRIGGER_RUN_SPARSIFY);
    }

    /**
     * Is this a built trigger tag?
     * @param value the tag value to check
     * @return <code>true</code> if built trigger tag, else <code>false</code>
     */
     public static boolean isBuiltTrigger(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == BUILT_TRIGGER_BANK     || cTag == BUILT_TRIGGER_TS ||
                 cTag == BUILT_TRIGGER_RUN      || cTag == BUILT_TRIGGER_TS_RUN ||
                 cTag == BUILT_TRIGGER_SPARSIFY || cTag == BUILT_TRIGGER_RUN_SPARSIFY);
    }

    /**
     * Is this a raw trigger tag?
     * @return <code>true</code> if raw trigger tag, else <code>false</code>
     */
     public boolean isRawTrigger() {
         return (this == RAW_TRIGGER || this == RAW_TRIGGER_TS);
     }

    /**
     * Is this a raw trigger tag?
     * @param value the tag value to check
     * @return <code>true</code> if raw trigger tag, else <code>false</code>
     */
     public static boolean isRawTrigger(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == RAW_TRIGGER || cTag == RAW_TRIGGER_TS);
     }

    /**
     * Is this any kind of a trigger tag?
     * @param value the tag value to check
     * @return <code>true</code> if any kind of trigger tag, else <code>false</code>
     */
     public static boolean isTrigger(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == RAW_TRIGGER            || cTag == RAW_TRIGGER_TS ||
                 cTag == BUILT_TRIGGER_BANK     || cTag == BUILT_TRIGGER_TS ||
                 cTag == BUILT_TRIGGER_RUN      || cTag == BUILT_TRIGGER_TS_RUN ||
                 cTag == BUILT_TRIGGER_SPARSIFY || cTag == BUILT_TRIGGER_RUN_SPARSIFY);
     }

    /**
     * Is this any kind of a trigger tag?
     * @return <code>true</code> if any kind of trigger tag, else <code>false</code>
     */
     public boolean isTrigger() {
         return (this == RAW_TRIGGER            || this == RAW_TRIGGER_TS ||
                 this == BUILT_TRIGGER_BANK     || this == BUILT_TRIGGER_TS ||
                 this == BUILT_TRIGGER_RUN      || this == BUILT_TRIGGER_TS_RUN ||
                 this == BUILT_TRIGGER_SPARSIFY || this == BUILT_TRIGGER_RUN_SPARSIFY);
      }

    /**
     * Does this tag indicate a timestamp is present?
     * @return <code>true</code> if this tag indicates a timestamp exists,
     *          else <code>false</code>
     */
     public boolean hasTimestamp() {
         return (this == RAW_TRIGGER_TS || this == BUILT_TRIGGER_TS ||
                 this == BUILT_TRIGGER_TS_RUN);
     }

    /**
     * Does this tag indicate a timestamp is present?
     * @param value the tag value to check
     * @return <code>true</code> if this tag indicates a timestamp exists,
     *          else <code>false</code>
     */
     public static boolean hasTimestamp(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == RAW_TRIGGER_TS || cTag == BUILT_TRIGGER_TS ||
                 cTag == BUILT_TRIGGER_TS_RUN);
     }

    /**
     * Does this tag indicate run number & type are present?
     * @return <code>true</code> if this tag indicates run number & type exist,
     *          else <code>false</code>
     */
     public boolean hasRunData() {
         return (this == BUILT_TRIGGER_RUN || this == BUILT_TRIGGER_TS_RUN);
     }

    /**
     * Does this tag indicate run number & type are present?
     * @param value the tag value to check
     * @return <code>true</code> if this tag indicates run number & type exist,
     *          else <code>false</code>
     */
     public static boolean hasRunData(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == BUILT_TRIGGER_RUN || cTag == BUILT_TRIGGER_TS_RUN);
     }

    /**
     * Does this tag indicate the trigger bank is sparsified
     * (no timestamps and no roc-specific segments present)?
     * @return <code>true</code> if this tag indicates trigger bank is sparsified,
     *          else <code>false</code>
     */
     public boolean isSparsified() {
         return (this == BUILT_TRIGGER_SPARSIFY || this == BUILT_TRIGGER_RUN_SPARSIFY);
     }

    /**
     * Does this tag indicate the trigger bank is sparsified
     * (no timestamps and no roc-specific segments present)?
     * @param value the tag value to check
     * @return <code>true</code> if this tag indicates trigger bank is sparsified,
     *          else <code>false</code>
     */
     public static boolean isSparsified(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == BUILT_TRIGGER_SPARSIFY || cTag == BUILT_TRIGGER_RUN_SPARSIFY);
     }

}
