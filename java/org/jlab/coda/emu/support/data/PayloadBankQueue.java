package org.jlab.coda.emu.support.data;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class designed to keep extra data associated with blocking queue.
 *
 * @author: timmer
 * Date: Jan 29, 2010
 */
public class PayloadBankQueue<E> extends LinkedBlockingQueue<E> {

    private int sourceId;
    
    private int recordId;


    public PayloadBankQueue() {
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
}
