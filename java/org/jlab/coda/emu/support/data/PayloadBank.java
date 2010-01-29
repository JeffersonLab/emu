package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.EvioBank;
import org.jlab.coda.jevio.EvioEvent;

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
    
    public boolean isSync() {
        return isSync;
    }

    public void setSync(boolean sync) {
        isSync = sync;
    }

}
