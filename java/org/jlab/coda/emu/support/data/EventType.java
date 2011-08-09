package org.jlab.coda.emu.support.data;

/**
 * This enum specifies values associated with evio event types used in CODA online components.
 * @author timmer
 */
public enum EventType {

    ROC_RAW  (1),
    PHYSICS  (2),
    USER     (3),
    SYNC     (4),
    // Control Event Types:
    PRESTART (5),
    GO       (6),
    PAUSE    (7),
    END      (8);

    private int value;

    private EventType(int value) {
        this.value   = value;
    }

    /**
     * Obtain the enum from the value.
     *
     * @param value the value to match.
     * @return the matching enum, or <code>null</code>.
     */
    public static EventType getEventType(int value) {
        EventType eventypes[] = EventType.values();
        for (EventType dt : eventypes) {
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
     * Is this a control event type?
     * @return <code>true</code> if control event type, else <code>false</code>
     */
    public boolean isControl() {
        return (this.equals(PRESTART) || this.equals(GO) ||
                this.equals(PAUSE)    || this.equals(END) );
    }

    /**
     * Is this a data event type?
     * @return <code>true</code> if data event type, else <code>false</code>
     */
    public boolean isROCRaw() {
        return this.equals(ROC_RAW);
    }

    /**
     * Is this a physics event type?
     * @return <code>true</code> if physics event type, else <code>false</code>
     */
    public boolean isPhysics() {
        return this.equals(PHYSICS);
    }

    /**
     * Is this a user event type?
     * @return <code>true</code> if user event type, else <code>false</code>
     */
    public boolean isUser() {
        return this.equals(USER);
    }

    /**
     * Is this a sync event type?
     * @return <code>true</code> if sync event type, else <code>false</code>
     */
    public boolean isSync() {
        return this.equals(SYNC);
    }


}
