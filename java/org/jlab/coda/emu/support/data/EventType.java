package org.jlab.coda.emu.support.data;

/**
 * This enum specifies values associated with evio event types used in run control components.
 * @author timmer
 */
public enum EventType {

    SYNC     (16, true),
    PRESTART (17, true),
    GO       (18, true),
    PAUSE    (19, true),
    END      (20, true),
    PHYSICS  (0x0C00, false),  // 3072 decimal
    DATA     (0x0C01, false);  // 3073 decimal

    private int value;
    private boolean control;

    private EventType(int value, boolean control) {
        this.value   = value;
        this.control = control;
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
     * Is this a control event type?
     * @return <code>true</code> if control event type, else <code>false</code>
     */
    public boolean isControl() {
        return control;
    }

    /**
     * Is this a data event type?
     * @return <code>true</code> if data event type, else <code>false</code>
     */
    public boolean isData() {
        return this.equals(DATA);
    }

    /**
     * Is this a physics event type?
     * @return <code>true</code> if physics event type, else <code>false</code>
     */
    public boolean isPhysics() {
        return this.equals(PHYSICS);
    }

}
