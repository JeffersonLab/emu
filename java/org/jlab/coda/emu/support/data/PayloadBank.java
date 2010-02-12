package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.*;


/**
 * Convenience class designed to keep extra data associated with ROC raw bank.
 *
 * @author: timmer
 * Date: Jan 29, 2010
 */
public class PayloadBank extends EvioBank {

    private int sourceId;

    private int recordId;

    private int eventCount;

    private int firstEventNumber;

    private int dataBlockCount;

    /** Is sync bank? from ROC raw data record 4-bit status. */
    private boolean isSync;

    /** Is single event mode? from ROC raw data record 4-bit status. */
    private boolean isSingleMode;

    /** Has error? from ROC raw data record 4-bit status. */
    private boolean hasError;

    /** Reserved. from ROC raw data record 4-bit status. */
    private boolean reserved;

    /** Was there an non-fatal error generated while trying to build an event? */
    private boolean nonFatalBuildingError;

    private EvioEvent parsedTriggerBank;


    public PayloadBank() {
        super();
    }

    public PayloadBank(EvioBank bank) {
        // copy over all basic, essential components of a bank
        header    = bank.getHeader();
        rawBytes  = bank.getRawBytes();
        byteOrder = bank.getByteOrder();
        children  = bank.getChildren();
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

    public int getFirstEventNumber() {
        return firstEventNumber;
    }

    public void setFirstEventNumber(int firstEventNumber) {
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

    public boolean isSingleMode() {
        return isSingleMode;
    }

    public void setSingleMode(boolean singleMode) {
        isSingleMode = singleMode;
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

    public EvioEvent getParsedTriggerBank() {
        return parsedTriggerBank;
    }

    public void setParsedTriggerBank(EvioEvent parsedTriggerBank) {
        this.parsedTriggerBank = parsedTriggerBank;
    }


}
