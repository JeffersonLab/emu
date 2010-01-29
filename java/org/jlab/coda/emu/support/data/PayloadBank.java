package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.EvioBank;

/**
 * Created by IntelliJ IDEA.
 * User: timmer
 * Date: Jan 29, 2010
 * Time: 3:29:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class PayloadBank extends EvioBank {

    private int sourceId;

    private int recordId;


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
}
