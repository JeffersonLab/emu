package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.*;


/**
 * Convenience class designed to keep extra data associated with ROC raw bank.
 *
 * @author: timmer
 * Date: Jan 29, 2010
 */
public class PayloadBank extends EvioEvent {

    private EventType type;

    private int sourceId;

    private int recordId;

    private int eventCount;

    private long firstEventNumber;

    private int dataBlockCount;

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


    public PayloadBank() {
        super();
    }

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

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
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

    public int getDataBlockCount() {
        return dataBlockCount;
    }

    public void setDataBlockCount(int dataBlockCount) {
        this.dataBlockCount = dataBlockCount;
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

}
