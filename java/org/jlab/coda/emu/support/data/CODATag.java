package org.jlab.coda.emu.support.data;

/**
 * This enum specifies values associated with tags used in CODA online components.
 * @author timmer
 */
public enum CODATag {

    RAW_TRIGGER            (0xFF10),
    RAW_TRIGGER_TS         (0xFF11),
    BUILT_TRIGGER_BANK     (0xFF20),
    BUILT_TRIGGER_TS       (0xFF21),
    BUILT_TRIGGER_RUN      (0xFF22),
    BUILT_TRIGGER_TS_RUN   (0xFF23),
    DISENTANGLED_BANK      (0xFF30),
    ;

    private int value;

    private CODATag(int value) {
        this.value = value;
    }

    /**
     * Obtain the enum from the value.
     *
     * @param value the value to match.
     * @return the matching enum, or <code>null</code>.
     */
    public static CODATag getTagType(int value) {
        CODATag tagTypes[] = CODATag.values();
        for (CODATag dt : tagTypes) {
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
     * Is this a built trigger tag?
     * @return <code>true</code> if built trigger tag, else <code>false</code>
     */
    public boolean isBuiltTrigger() {
        return (this == BUILT_TRIGGER_BANK || this == BUILT_TRIGGER_TS ||
                this == BUILT_TRIGGER_RUN  || this == BUILT_TRIGGER_TS_RUN);
    }

    /**
     * Is this a built trigger tag?
     * @param value the tag value to check
     * @return <code>true</code> if built trigger tag, else <code>false</code>
     */
     public static boolean isBuiltTrigger(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == BUILT_TRIGGER_BANK || cTag == BUILT_TRIGGER_TS ||
                 cTag == BUILT_TRIGGER_RUN  || cTag == BUILT_TRIGGER_TS_RUN);
    }

    /**
     * Is this a raw trigger tag?
     * @return <code>true</code> if raw trigger tag, else <code>false</code>
     */
     public boolean isRawTrigger() {
         return (this == RAW_TRIGGER || this == RAW_TRIGGER_TS);
     }

    /**
     * Is this any kind of a trigger tag?
     * @param value the tag value to check
     * @return <code>true</code> if any kind of trigger tag, else <code>false</code>
     */
     public static boolean isTrigger(int value) {
         CODATag cTag = getTagType(value);
         if (cTag == null) return false;

         return (cTag == RAW_TRIGGER || cTag == RAW_TRIGGER_TS ||
                 cTag == BUILT_TRIGGER_BANK || cTag == BUILT_TRIGGER_TS ||
                 cTag == BUILT_TRIGGER_RUN  || cTag == BUILT_TRIGGER_TS_RUN);
     }

    /**
     * Is this any kind of a trigger tag?
     * @return <code>true</code> if any kind of trigger tag, else <code>false</code>
     */
     public boolean isTrigger() {
         return (this == RAW_TRIGGER || this == RAW_TRIGGER_TS ||
                 this == BUILT_TRIGGER_BANK || this == BUILT_TRIGGER_TS ||
                 this == BUILT_TRIGGER_RUN  || this == BUILT_TRIGGER_TS_RUN);
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

}
