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
    BUILT_TRIGGER_NRSD          (0xFF24),
    /** Timestamp and no roc-specific data. */
    BUILT_TRIGGER_TS_NRSD       (0xFF25),
    /** Run and no roc-specific data. */
    BUILT_TRIGGER_RUN_NRSD      (0xFF26),
    /** Timestamp and run, but no roc-specific data. */
    BUILT_TRIGGER_TS_RUN_NRSD   (0xFF27),

    // Trigger banks error condition
    /** At least one ROC used in building this trigger bank
     *  has a bad or nonexistent trigger bank of its own. */
    BUILT_TRIGGER_ROC_ERROR     (0xFF4F),

    // Physics event
    /** Event built by primary event builder. */
    BUILT_BY_PEB                (0xFF50),
    /** Event built by primary event builder with sync bit set. */
    BUILT_BY_PEB_SYNC           (0xFF58),
    /** Event built by secondary event builder. */
    BUILT_BY_SEB                (0xFF70),
    /** Event built by secondary event builder with sync bit set. */
    BUILT_BY_SEB_SYNC           (0xFF78),

    //---------------------------------------------
    // Streaming
    //---------------------------------------------

    /** Streaming Info Bank containing timestamp and frame number. */
    STREAMING_SIB                (0xFF30),
    // Physics event
    /** Event built by primary event builder in streaming mode. */
    BUILT_BY_PEB_STREAMING       (0xFF60),
    /** Event built by secondary event builder in streaming mode. */
    BUILT_BY_SEB_STREAMING       (0xFF62),
    /** Event built by DC event builder in streaming mode. */
    BUILT_BY_DC_STREAMING        (0xFF64),

    ;

    private int value;

    /** Fast way to convert integer values into CODATag objects. */
    private static CODATag[] intToType;


    // Fill array after all enum objects created
    static {
        intToType = new CODATag[0xff + 1];
        for (CODATag type : values()) {
            intToType[type.value & 0xff] = type;
        }
    }


	/**
	 * Obtain the enum from the value.
	 *
	 * @param val the value to match.
	 * @return the matching enum, or <code>null</code>.
	 */
    public static CODATag getTagType(int val) {
        if (val > 0xFF71 || val < 0xFF10) return null;
        return intToType[val & 0xff];
    }


    /**
     * Obtain the name from the value.
     *
     * @param val the value to match.
     * @return the name, or <code>null</code>.
     */
    public static String getName(int val) {
        if (val > 0xFF71 || val < 0xFF10) return null;
        CODATag type = getTagType(val);
        if (type == null) return null;
        return type.name();
    }


    CODATag(int value) {
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
     * Values can range from 0xff20 to 0xff4f.
     * Currently only 0xff20 to 0xff27 are used.
     *
     * @return <code>true</code> if built trigger tag, else <code>false</code>
     */
    public boolean isBuiltTrigger() {
        return (value >= 0xff20 && value <= 0xff27);
    }

    /**
     * Is this a built trigger tag?
     * Values can range from 0xff20 to 0xff4f.
     * Currently only 0xff20 to 0xff27 are used.
     *
     * @param value the tag value to check
     * @return <code>true</code> if built trigger tag, else <code>false</code>
     */
     public static boolean isBuiltTrigger(int value) {
         return (value >= 0xff20 && value <= 0xff27);
     }

    /**
     * Is this a raw trigger tag?
     * Values can range from 0xff10 to 0xff1f.
     * Currently only 0xff10 to 0xff12 are used.
     *
     * @return <code>true</code> if raw trigger tag, else <code>false</code>
     */
     public boolean isRawTrigger() {
         return (value >= 0xff10 && value <= 0xff12);
     }

    /**
     * Is this a raw trigger tag?
     * Values can range from 0xff10 to 0xff1f.
     * Currently only 0xff10 to 0xff12 are used.
     *
     * @param value the tag value to check
     * @return <code>true</code> if raw trigger tag, else <code>false</code>
     */
     public static boolean isRawTrigger(int value) {
         return (value >= 0xff10 && value <= 0xff12);
     }

    /**
     * Is this any kind of a trigger tag?
     * Values can range from 0xff10 to 0xff4f.
     * Currently only 0xff10 to 0xff27 are used.
     *
     * @param value the tag value to check
     * @return <code>true</code> if any kind of trigger tag, else <code>false</code>
     */
     public static boolean isTrigger(int value) {
         return (value >= 0xff10 && value <= 0xff27);
     }

    /**
     * Is this any kind of a trigger tag?
     * Values can range from 0xff10 to 0xff4f.
     * Currently only 0xff10 to 0xff27 are used.
     *
     * @return <code>true</code> if any kind of trigger tag, else <code>false</code>
     */
     public boolean isTrigger() {
         return (value >= 0xff10 && value <= 0xff27);
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
         return (value == RAW_TRIGGER_TS.value        || value == RAW_TRIGGER_TS_BIG.value    ||
                 value == BUILT_TRIGGER_TS.value      || value == BUILT_TRIGGER_TS_RUN.value  ||
                 value == BUILT_TRIGGER_TS_NRSD.value || value == BUILT_TRIGGER_TS_RUN_NRSD.value);
     }

    /**
     * Does this tag indicate run number and type are present?
     * @return <code>true</code> if this tag indicates run number and type exist,
     *          else <code>false</code>
     */
     public boolean hasRunData() {
         return (this == BUILT_TRIGGER_RUN      || this == BUILT_TRIGGER_TS_RUN ||
                 this == BUILT_TRIGGER_RUN_NRSD || this == BUILT_TRIGGER_TS_RUN_NRSD);
     }

    /**
     * Does this tag indicate run number and type are present?
     * @param value the tag value to check
     * @return <code>true</code> if this tag indicates run number and type exist,
     *          else <code>false</code>
     */
     public static boolean hasRunData(int value) {
         return (value == BUILT_TRIGGER_RUN.value      || value == BUILT_TRIGGER_TS_RUN.value ||
                 value == BUILT_TRIGGER_RUN_NRSD.value || value == BUILT_TRIGGER_TS_RUN_NRSD.value);
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
         return !(value == BUILT_TRIGGER_NRSD.value     || value == BUILT_TRIGGER_TS_NRSD.value ||
                  value == BUILT_TRIGGER_RUN_NRSD.value || value == BUILT_TRIGGER_TS_RUN_NRSD.value);
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
//         return (value == BUILT_TRIGGER_SPARSIFY.value || value == BUILT_TRIGGER_RUN_SPARSIFY.value);
//     }

    /**
     * Is this any kind of a sync event tag?
     * @param value the tag value to check
     * @return <code>true</code> if any kind of sync event tag, else <code>false</code>
     */
     public static boolean isSyncEVent(int value) {
         return (value == BUILT_BY_PEB_SYNC.value || value == BUILT_BY_SEB_SYNC.value);
     }

    /**
     * Is this any kind of a sync event tag?
     * @return <code>true</code> if any kind of sync event tag, else <code>false</code>
     */
     public boolean isSyncEVent() {
         return (this == BUILT_BY_PEB_SYNC || this == BUILT_BY_SEB_SYNC);
     }


}
