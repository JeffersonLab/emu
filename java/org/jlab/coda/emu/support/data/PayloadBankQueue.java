package org.jlab.coda.emu.support.data;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Convenience class designed to keep extra data associated with blocking queue.
 * It is threadsafe.
 *
 * @author: timmer
 * Date: Jan 29, 2010
 */
public class PayloadBankQueue<PayloadBank> extends LinkedBlockingQueue<PayloadBank> {

    private int sourceId;
    
    private int recordId;


    public PayloadBankQueue() {
        super();
    }


    public PayloadBankQueue(int capacity) {
        super(capacity);
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
}
