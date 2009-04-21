package org.jlab.coda.support.data;

import java.nio.IntBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Jan 29, 2009
 * Time: 8:16:58 AM
 */
public class DataTransportRecord extends DataBank {
    private static final int IDBANK_START = DataBank.PAYLOAD_START;
    private static final int IDBANK_LENGTH = 2;
    private static final int RECORD_COUNTER = 2;
    private static final int PAYLOAD_START = IDBANK_START + IDBANK_LENGTH + 1;

    private DataBank recordIDBank;

    private int recordCount = 0;

    public DataTransportRecord(int length) {
        super(length);
    }

    public DataTransportRecord(DataBank db) throws BankFormatException {
        super(db.bbuffer);
        buffer.position(IDBANK_START);

        recordIDBank = new DataBank(buffer.slice());
        //bankCount = recordIDBank.getNumber();
        recordCount = recordIDBank.getData(0);
    }

    public IntBuffer getBuffer() {
        buffer.rewind();
        return buffer;
    }

    public IntBuffer getPayload() {
        buffer.position(PAYLOAD_START);

        return buffer.slice();

    }

    public void setEventCount(int count) {
        recordIDBank.setNumber(count);
    }

    public int getEventCount() {
        return recordIDBank.getNumber();
    }

    public void setRecordID(int theID) {
        buffer.put(RECORD_COUNTER, theID);
    }

    public int getRecordID() {
        return buffer.get(RECORD_COUNTER);
    }

    public void rewind() {
        buffer.rewind();

        buffer.position(PAYLOAD_START);
    }

}
