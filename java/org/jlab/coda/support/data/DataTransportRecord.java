package org.jlab.coda.support.data;

import java.nio.IntBuffer;

/**
 * The Data Transport Record is a bank of banks.
 * The first bank it contains is only a single int which is the Record ID (counter).
 * The following banks are payload banks each of which can be a Physics event or
 * Raw ROC data.
 */
public class DataTransportRecord extends DataBank {

    /** The Record ID bank in the data transport record is the first bank
     *  inside this bank of banks (payload of the bank of banks). This is
     *  the index into the buffer to the start of the Record ID bank. */
    private static final int IDBANK_START = DataBank.PAYLOAD_START;

    /** The Record ID bank is always 2, 32-bit ints long.
     *  One int for header and one for data. */
    private static final int IDBANK_LENGTH = 2;

    // bug bug
    /** Index into buffer to start of the Record ID (counter) value?
     * Shouldn't this be = 4 ???!!! */
    private static final int RECORD_COUNTER = 2;

    /** Index into buffer to start of payload following Record ID bank. */
    private static final int PAYLOAD_START = IDBANK_START + IDBANK_LENGTH + 1;

    /** First bank inside this bank of banks as an object. */
    private DataBank recordIDBank;

    /** Record ID (counter) taken from buffer. */
    private int recordCount = 0;

    /**
     * Constructor to create a DataBank of the given length.
     * @param length length of data bank to be created
     */
    public DataTransportRecord(int length) {
        super(length);
    }

    /**
     * Constructor from an existing DataBank object.
     * @param db DataBank object
     * @throws BankFormatException
     */
    public DataTransportRecord(DataBank db) throws BankFormatException {
        super(db.bbuffer);
        buffer.position(IDBANK_START);

        recordIDBank = new DataBank(buffer.slice());
        //bankCount = recordIDBank.getNumber();
        recordCount = recordIDBank.getData(0);
    }

    /**
     * Get the buffer as a buffer of 32 bit ints.
     * The buffer includes all initial lengths and headers
     * as well as the actual data which follows that.
     *
     * @return the buffer as a buffer of 32 bit ints
     */
    public IntBuffer getBuffer() {
        buffer.rewind();
        return buffer;
    }

    /**
     * Get the buffer NOT including the first 2 32-bit ints (length and header).
     * @return the buffer NOT including the first 2 32-bit ints (length and header)
     */
    public IntBuffer getPayload() {
        buffer.position(PAYLOAD_START);
        return buffer.slice();
    }

    /**
     * Set the event number in the header in the Record ID bank.
     * @param count the event number
     */
    public void setEventCount(int count) {
        recordIDBank.setNumber(count);
    }

    /**
     * Get the event number from the header of the Record ID bank.
     * @return the event number from the header of the Record ID bank
     */
    public int getEventCount() {
        return recordIDBank.getNumber();
    }

    /**
     * Set the Record ID.
     * @param theID Record ID
     */
    public void setRecordID(int theID) {
        buffer.put(RECORD_COUNTER, theID);
    }

    /**
     * Get the Record ID.
     * @return Record ID
     */
    public int getRecordID() {
        return buffer.get(RECORD_COUNTER);
    }

    /**
     * Move the buffer position back to the beginning of the payload.
     */
    public void rewind() {
        // position is set to zero and mark is discarded
        buffer.rewind();
        buffer.position(PAYLOAD_START);
    }

}
