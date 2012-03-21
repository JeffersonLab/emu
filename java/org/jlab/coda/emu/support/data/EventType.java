package org.jlab.coda.emu.support.data;

/**
 * This enum specifies values associated with evio event types used in CODA online components.
 * @author timmer
 */
public enum EventType {

    ROC_RAW              (0),
    PHYSICS              (1),
    PARTIAL_PHYSICS      (2),
    DISENTANGLED_PHYSICS (3),
    USER                 (4),
    // Control Event Types:
    CONTROL              (5), // prestart, go, pause or end
    PRESTART             (6),
    GO                   (7),
    PAUSE                (8),
    END                  (9),
    OTHER               (15);

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
                this.equals(PAUSE)    || this.equals(END) ||
                this.equals(CONTROL));
    }

    /**
     * Is this a prestart control event type?
     * @return <code>true</code> if prestart control event type, else <code>false</code>
     */
    public boolean isPrestart() {
        return this.equals(PRESTART);
    }

    /**
     * Is this a go control event type?
     * @return <code>true</code> if go control event type, else <code>false</code>
     */
    public boolean isGo() {
        return this.equals(GO);
    }

    /**
     * Is this a pause control event type?
     * @return <code>true</code> if pause control event type, else <code>false</code>
     */
    public boolean isPause() {
        return this.equals(PAUSE);
    }

    /**
     * Is this an end control event type?
     * @return <code>true</code> if end control event type, else <code>false</code>
     */
    public boolean isEnd() {
        return this.equals(END);
    }

    /**
     * Is this a data event type?
     * @return <code>true</code> if data event type, else <code>false</code>
     */
    public boolean isROCRaw() {
        return this.equals(ROC_RAW);
    }

    /**
     * Is this a any kind of a physics event type?
     * @return <code>true</code> if any kind of a physics event type, else <code>false</code>
     */
    public boolean isAnyPhysics() {
        return (this.equals(PHYSICS) || this.equals(PARTIAL_PHYSICS) ||
                this.equals(DISENTANGLED_PHYSICS));
    }

    /**
     * Is this a fully-built, but entangled physics event type?
     * @return <code>true</code> if complete, but entangled physics event type, else <code>false</code>
     */
    public boolean isPhysics() {
        return this.equals(PHYSICS);
    }

    /**
     * Is this a partially-built physics event type?
     * If so, this event did not yet make it through all the layers of event building.
     * @return <code>true</code> if partially-built physics event type, else <code>false</code>
     */
    public boolean isPartialPhysics() {
        return this.equals(PARTIAL_PHYSICS);
    }

    /**
     * Is this a fully-built, disentangled physics event type?
     * @return <code>true</code> if complete & disentangled physics event type, else <code>false</code>
     */
    public boolean isDisentangledPhysics() {
        return this.equals(DISENTANGLED_PHYSICS);
    }

    /**
     * Is this a user event type?
     * @return <code>true</code> if user event type, else <code>false</code>
     */
    public boolean isUser() {
        return this.equals(USER);
    }



}
