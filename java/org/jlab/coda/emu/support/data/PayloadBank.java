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

    private boolean isSync;

    private boolean isSingleMode;

    private boolean hasError;

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

    public boolean isHasError() {
        return hasError;
    }

    public void setHasError(boolean hasError) {
        this.hasError = hasError;
    }

    public EvioEvent getParsedTriggerBank() {
        return parsedTriggerBank;
    }

    public void setParsedTriggerBank(EvioEvent parsedTriggerBank) {
        this.parsedTriggerBank = parsedTriggerBank;
    }


}
