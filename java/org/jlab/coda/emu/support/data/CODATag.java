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
    /** Trigger bank from ROC with no timestamps. */
    RAW_TRIGGER                 (0xFF10),
    /** Trigger bank from ROC with 32 bit timestamps. */
    RAW_TRIGGER_TS              (0xFF11),
    /** Trigger bank from ROC with 64 bit timestamps. */
    RAW_TRIGGER_TS_BIG          (0xFF12),

    // Trigger banks with roc-specific data
    /** No timestamp and no run data. */
    BUILT_TRIGGER_BANK          (0xFF20),
    /** Only timestamp data. */
    BUILT_TRIGGER_TS            (0xFF21),
    /** Only run data. */
    BUILT_TRIGGER_RUN           (0xFF22),
    /** Both timestamp and run data. */
    BUILT_TRIGGER_TS_RUN        (0xFF23),

    // Trigger banks without roc-specific data
    /** No timestamp, no run and no roc-specific data. */
    BUILT_TRIGGER_NRSD           (0xFF24),
    /** Timestamp and no roc-specific data. */
    BUILT_TRIGGER_TS_NRSD        (0xFF25),
    /** Run and no roc-specific data. */
    BUILT_TRIGGER_RUN_NRSD       (0xFF26),
    /** Timestamp and run, but no roc-specific data. */
    BUILT_TRIGGER_TS_RUN_NRSD    (0xFF27),

    // Physics event
    DISENTANGLED_BANK           (0xFF30),
    /** Event built by primary event builder. */
    BUILT_BY_PEB                (0xFF50),
    /** Event built by primary event builder in single event mode. */
    BUILT_BY_PEB_IN_SEM         (0xFF51),
    /** Event built by secondary event builder. */
    BUILT_BY_SEB                (0xFF70),
    /** Event built by secondary event builder in single event mode. */
    BUILT_BY_SEB_IN_SEM         (0xFF71),
    ;

    private int value;

    /** Faster way to convert integer values into names. */
    private static HashMap<Integer, String> names = new HashMap<Integer, String>(16);

    /** Faster way to convert integer values into CODATag objects. */
    private static HashMap<Integer, CODATag> tags = new HashMap<Integer, CODATag>(16);


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
                this == BUILT_TRIGGER_NRSD     || this == BUILT_TRIGGER_RUN_NRSD ||
                this == BUILT_TRIGGER_TS_NRSD  || this == BUILT_TRIGGER_TS_RUN_NRSD);
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
                 cTag == BUILT_TRIGGER_NRSD     || cTag == BUILT_TRIGGER_RUN_NRSD ||
                 cTag == BUILT_TRIGGER_TS_NRSD  || cTag == BUILT_TRIGGER_TS_RUN_NRSD);
    }

    /**
     * Is this a raw trigger tag?
     * @return <code>true</code> if raw trigger tag, else <code>false</code>
     */
     public boolean isRawTrigger() {
         return (this == RAW_TRIGGER || this == RAW_TRIGGER_TS || this == RAW_TRIGGER_TS_BIG);
     }

    /**
     * Is this a raw trigger tag?
     * @param value the tag value to check
     * @return <code>true</code> if raw trigger tag, else <code>false</code>
     */
     public static boolean isRawTrigger(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == RAW_TRIGGER || cTag == RAW_TRIGGER_TS || cTag == RAW_TRIGGER_TS_BIG);
     }

    /**
     * Is this any kind of a trigger tag?
     * @param value the tag value to check
     * @return <code>true</code> if any kind of trigger tag, else <code>false</code>
     */
     public static boolean isTrigger(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == RAW_TRIGGER || cTag == RAW_TRIGGER_TS || cTag == RAW_TRIGGER_TS_BIG ||
                 cTag == BUILT_TRIGGER_BANK     || cTag == BUILT_TRIGGER_TS ||
                 cTag == BUILT_TRIGGER_RUN      || cTag == BUILT_TRIGGER_TS_RUN ||
                 cTag == BUILT_TRIGGER_NRSD     || cTag == BUILT_TRIGGER_RUN_NRSD ||
                 cTag == BUILT_TRIGGER_TS_NRSD  || cTag == BUILT_TRIGGER_TS_RUN_NRSD);
     }

    /**
     * Is this any kind of a trigger tag?
     * @return <code>true</code> if any kind of trigger tag, else <code>false</code>
     */
     public boolean isTrigger() {
         return (this == RAW_TRIGGER || this == RAW_TRIGGER_TS || this == RAW_TRIGGER_TS_BIG ||
                 this == BUILT_TRIGGER_BANK     || this == BUILT_TRIGGER_TS ||
                 this == BUILT_TRIGGER_RUN      || this == BUILT_TRIGGER_TS_RUN ||
                 this == BUILT_TRIGGER_NRSD     || this == BUILT_TRIGGER_RUN_NRSD ||
                 this == BUILT_TRIGGER_TS_NRSD  || this == BUILT_TRIGGER_TS_RUN_NRSD);
      }

    /**
     * Does this tag indicate a timestamp is present?
     * @return <code>true</code> if this tag indicates a timestamp exists,
     *          else <code>false</code>
     */
     public boolean hasTimestamp() {
         return (this == RAW_TRIGGER_TS        || this == RAW_TRIGGER_TS_BIG    ||
                 this == BUILT_TRIGGER_TS      || this == BUILT_TRIGGER_TS_RUN  ||
                 this == BUILT_TRIGGER_TS_NRSD || this == BUILT_TRIGGER_TS_RUN_NRSD);
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

         return (cTag == RAW_TRIGGER_TS        || cTag == RAW_TRIGGER_TS_BIG    ||
                 cTag == BUILT_TRIGGER_TS      || cTag == BUILT_TRIGGER_TS_RUN  ||
                 cTag == BUILT_TRIGGER_TS_NRSD || cTag == BUILT_TRIGGER_TS_RUN_NRSD);
     }

    /**
     * Does this tag indicate run number & type are present?
     * @return <code>true</code> if this tag indicates run number & type exist,
     *          else <code>false</code>
     */
     public boolean hasRunData() {
         return (this == BUILT_TRIGGER_RUN      || this == BUILT_TRIGGER_TS_RUN ||
                 this == BUILT_TRIGGER_RUN_NRSD || this == BUILT_TRIGGER_TS_RUN_NRSD);
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

         return (cTag == BUILT_TRIGGER_RUN      || cTag == BUILT_TRIGGER_TS_RUN ||
                 cTag == BUILT_TRIGGER_RUN_NRSD || cTag == BUILT_TRIGGER_TS_RUN_NRSD);
     }


    /**
     * Does this tag indicate the trigger bank has roc-specific data
     * segments present?
     * @return <code>true</code> if this tag indicates trigger bank
     *         has roc-specific data segments present, else <code>false</code>
     */
     public boolean hasRocSpecificData() {
         return !(this == BUILT_TRIGGER_NRSD     || this == BUILT_TRIGGER_TS_NRSD ||
                  this == BUILT_TRIGGER_RUN_NRSD || this == BUILT_TRIGGER_TS_RUN_NRSD);
     }

    /**
     * Does this tag indicate the trigger bank has roc-specific data
     * segments present?
     * @param value the tag value to check
     * @return <code>true</code> if this tag indicates trigger bank
     *         has roc-specific data segments present, else <code>false</code>
     */
     public static boolean hasRocSpecificData(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return !(cTag == BUILT_TRIGGER_NRSD     || cTag == BUILT_TRIGGER_TS_NRSD ||
                  cTag == BUILT_TRIGGER_RUN_NRSD || cTag == BUILT_TRIGGER_TS_RUN_NRSD);
     }


    /**
     * Does this tag indicate the trigger bank was built from event in
     * single event mode?
     * @return <code>true</code> if this tag indicates trigger bank was
     *         built from event in single event mode, else <code>false</code>
     */
     public boolean eventInSEM() {
         return (this == BUILT_BY_PEB_IN_SEM || this == BUILT_BY_SEB_IN_SEM);
     }

    /**
     * Does this tag indicate the trigger bank was built from event in
     * single event mode?
     * @param value the tag value to check
     * @return <code>true</code> if this tag indicates trigger bank was
     *         built from event in single event mode, else <code>false</code>
     */
     public static boolean eventInSEM(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == BUILT_BY_PEB_IN_SEM || cTag == BUILT_BY_SEB_IN_SEM);
     }



//    /**
//     * Does this tag indicate the trigger bank is sparsified
//     * (no timestamps and no roc-specific segments present)?
//     * @return <code>true</code> if this tag indicates trigger bank is sparsified,
//     *          else <code>false</code>
//     */
//     public boolean isSparsified() {
//         return (this == BUILT_TRIGGER_SPARSIFY || this == BUILT_TRIGGER_RUN_SPARSIFY);
//     }
//
//    /**
//     * Does this tag indicate the trigger bank is sparsified
//     * (no timestamps and no roc-specific segments present)?
//     * @param value the tag value to check
//     * @return <code>true</code> if this tag indicates trigger bank is sparsified,
//     *          else <code>false</code>
//     */
//     public static boolean isSparsified(int value) {
//         CODATag cTag = getTagType(value);
//         if (cTag == null) return false;
//
//         return (cTag == BUILT_TRIGGER_SPARSIFY || cTag == BUILT_TRIGGER_RUN_SPARSIFY);
//     }

}
