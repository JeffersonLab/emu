package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.*;


/**
 * Convenience class designed to keep extra data associated with an evio event.
 *
 * @author: timmer
 * Date: Jan 29, 2010
 */
public class PayloadBank extends EvioEvent implements Attached {

    /** What type of CODA events are contained in this bank (RocRaw, Physics, Control, ...)?
     *  Only one type is stored in one PayloadBank object.
     *  Only one control event is stored in one PayloadBank object. */
    private EventType eventType;

    /** If this is a control event, what type of control is it (SYNC, GO, END, ...)? */
    private ControlType controlType;

    /** If the event type is RocRaw, this is the CODA id of the source. */
    private int sourceId;

    /** The name of the source of these CODA events. */
    private String sourceName;

    /** If the event type is RocRaw or Physics, this is the record id of this CODA events.
     *  The record id is incremented by one for each ET event. Many CODA events (triggers)
     *  may have the same record id. */
    private int recordId;

    /** The number of CODA events (triggers) contained in this evio bank. */
    private int eventCount;

    /** The event number of the first CODA event in this evio bank. */
    private long firstEventNumber;

    /** Is sync bank? from ROC raw data record 4-bit status. */
    private boolean isSync;

    /** Is single event mode? from ROC raw data record 4-bit status. */
    private boolean isSingleEventMode;

    /** Has error? from ROC raw data record 4-bit status. */
    private boolean hasError;

    /** Reserved. from ROC raw data record 4-bit status. */
    private boolean reserved;

    /** Was there an non-fatal error generated while trying to build an event? */
    private boolean nonFatalBuildingError;


    /** Constructor. */
    public PayloadBank() {
        super();
    }

    /**
     * Copy constructor which copies references and doesn't clone.
     * @param bank bank to copy
     */
    public PayloadBank(BaseStructure bank) {
        // copy over all basic, essential components of a bank
        header    = bank.getHeader();
        rawBytes  = bank.getRawBytes();
        byteOrder = bank.getByteOrder();
        children  = bank.getChildren();
        if (children != null && children.size() > 1) isLeaf = false;
    }

    /**
     * This is a general constructor to use for an EvioEvent.
     *
     * @param tag the tag for the event header (which is just a bank header).
     * @param dataType the (enum) data type for the content of the bank.
     * @param num sometimes, but not necessarily, an ordinal enumeration.
     */
    public PayloadBank(int tag, DataType dataType, int num) {
        super(new BankHeader(tag, dataType, num));
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType type) {
        this.eventType = type;
    }

    public ControlType getControlType() {
        return controlType;
    }

    public void setControlType(ControlType type) {
        this.controlType = type;
    }

    public int getSourceId() {
        return sourceId;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public int getRecordId() {
        return recordId;
    }

    public void setRecordId(int recordId) {
        this.recordId = recordId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public int getEventCount() {
        return eventCount;
    }

    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    public long getFirstEventNumber() {
        return firstEventNumber;
    }

    public void setFirstEventNumber(long firstEventNumber) {
        this.firstEventNumber = firstEventNumber;
    }

    public boolean isSync() {
        return isSync;
    }

    public void setSync(boolean sync) {
        isSync = sync;
    }

    public boolean isSingleEventMode() {
        return isSingleEventMode;
    }

    public void setSingleEventMode(boolean singleEventMode) {
        isSingleEventMode = singleEventMode;
    }

    public boolean hasError() {
        return hasError;
    }

    public void setError(boolean hasError) {
        this.hasError = hasError;
    }

    public boolean isReserved() {
        return reserved;
    }

    public void setReserved(boolean reserved) {
        this.reserved = reserved;
    }

    public boolean hasNonFatalBuildingError() {
        return nonFatalBuildingError;
    }

    public void setNonFatalBuildingError(boolean nonFatalBuildingError) {
        this.nonFatalBuildingError = nonFatalBuildingError;
    }


    /**
     * EvioEvent has a clone method. The additional members of this
     * class are primitives/enums so bitwise copies are fine.
     */
    public Object clone() {
        return super.clone();
    }


}
