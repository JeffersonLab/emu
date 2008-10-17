/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.support.evio;

/**
 * <pre>Class DataRecord is a class of objects that represent records of data.
 * A data record is an outer wrapper for data transmitted over the network
 * between EMUs and between EMUs and ROCs.
 * <p/>
 * The data is recieved from the network as a byte array. The data is
 * encoded using the EVIO format. This format assumes that the first eight
 * bytes are two 32-bit words, a length word and a bank header word (record
 * header).
 * <p/>
 * The length is the number of 32-bit words in this record.
 * <p/>
 * The record header is an EVIO header with bank type "data record".
 * <p/>
 * The group 32-bit words after the bank header form the data record header.
 * <p/>
 * The data payload follows the data record header.
 *    </pre>
 *
 * @author heyes
 *         Created on Sep 12, 2008
 */
@SuppressWarnings({"WeakerAccess"})
public class DataRecord {
    /** Field bufferLength */
    private int bufferLength;

    /** Field data */
    private byte[] data;

    /** Field RECORD_LENGTH */
    private final static int RECORD_LENGTH = 0;
    /** Field RECORD_HEADER */
    private final static int RECORD_HEADER = 1;
    /** Field RECORD_COUNT */
    private final static int RECORD_COUNT = 2;

    /** Field PAYLOAD */
    public final static int PAYLOAD = 3;

    /**
     * Constructor DataRecord creates a new DataRecord instance.
     *
     * @param db of type byte[]
     * @param l  of type int
     */
    public DataRecord(byte[] db, int l) {
        data = db;
        bufferLength = l;
    }

    /**
     * Constructor DataRecord creates a new DataRecord instance.
     *
     * @param l of type int
     */
    public DataRecord(int l) {
        data = new byte[l];
        bufferLength = l;
    }

    /**
     * Method getData returns the data of this DataRecord object.
     *
     * @return the data (type byte[]) of this DataRecord object.
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Method setData sets the data of this DataRecord object.
     *
     * @param data the data of this DataRecord object.
     */
    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * Method getBufferLength returns the bufferLength of this DataRecord object.
     *
     * @return the bufferLength (type int) of this DataRecord object.
     */
    public int getBufferLength() {
        return bufferLength;
    }

    /**
     * Method setBufferLength sets the bufferLength of this DataRecord object.
     *
     * @param length the bufferLength of this DataRecord object.
     */
    public void setBufferLength(int length) {
        this.bufferLength = length;
    }

    /**
     * Method getData ...
     *
     * @param offset of type int
     * @return int
     */
    private int getData(int offset) {
        int rl = 0;
        int bo = offset << 2;// byte offset
        rl |= data[bo] & 0xFF;
        rl <<= 8;
        rl |= data[1 + bo] & 0xFF;
        rl <<= 8;
        rl |= data[2 + bo] & 0xFF;
        rl <<= 8;
        rl |= data[3 + bo] & 0xFF;
        return rl;
    }

    /**
     * Method setData ...
     *
     * @param offset of type int
     * @param value  of type int
     */
    private void setData(int offset, int value) {

        int v = value;
        int bo = offset << 2; // byte offset
        data[3 + bo] = (byte) (v & 0xFF);
        v >>= 8;
        data[2 + bo] = (byte) (v & 0xFF);
        v >>= 8;
        data[1 + bo] = (byte) (v & 0xFF);
        v >>= 8;
        data[bo] = (byte) (v & 0xFF);
        v >>= 8;
    }

    /**
     * Method getRecordLength returns the recordLength of this DataRecord object.
     *
     * @return the recordLength (type int) of this DataRecord object.
     */
    public int getRecordLength() {
        return getData(RECORD_LENGTH);
    }

    /**
     * Method setRecordLength sets the recordLength of this DataRecord object.
     *
     * @param recordLength the recordLength of this DataRecord object.
     */
    public void setRecordLength(int recordLength) {
        setData(RECORD_LENGTH, recordLength);
    }

    /**
     * Method getRecordCount returns the recordCount of this DataRecord object.
     *
     * @return the recordCount (type int) of this DataRecord object.
     */
    public int getRecordCount() {
        return getData(RECORD_COUNT);
    }

    /**
     * Method setRecordCount sets the recordCount of this DataRecord object.
     *
     * @param recordLength the recordCount of this DataRecord object.
     */
    public void setRecordCount(int recordLength) {
        setData(RECORD_COUNT, recordLength);
    }

    /**
     * Method getRecordHeader returns the recordHeader of this DataRecord object.
     *
     * @return the recordHeader (type int) of this DataRecord object.
     * @throws EVIOHdrException when
     */
    public int getRecordHeader() throws EVIOHdrException {
        int hdr = getData(RECORD_HEADER);

        if ((hdr & 0x0000ff00) != 0x00001000) {
            throw new EVIOHdrException("Record header is not a bank header", hdr);
        }
        return hdr;
    }

    /**
     * Method check ...
     *
     * @throws EVIORecordException when
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    public void check() throws EVIORecordException {
        EVIORecordException e = new EVIORecordException("Record structure check");
        int hdr = getData(RECORD_HEADER);

        if ((hdr & 0x0000ff00) != 0x00001000) {
            Throwable e1 = new EVIOHdrException("Record header is not a bank header", hdr);
            e.addCause(e1);
        }

        if ((hdr & 0xff) != (getRecordCount() & 0xff)) {
            Throwable e1 = new EVIOHdrException("Record count " + (getRecordCount() & 0xff) + "doesn't match short count" + (hdr & 0xff), hdr);
            e.addCause(e1);
        }

        if (e.throwMe()) throw e;
    }

    /**
     * Method setRecordHeader sets the recordHeader of this DataRecord object.
     *
     * @param hdr the recordHeader of this DataRecord object.
     */
    public void setRecordHeader(int hdr) {
        int header = (hdr & 0xffff00ff) | 0x00001000;
        setData(RECORD_HEADER, header);
    }

    /**
     * Method getSourceID returns the sourceID of this DataRecord object.
     *
     * @return the sourceID (type int) of this DataRecord object.
     * @throws EVIOHdrException when
     */
    public int getSourceID() throws EVIOHdrException {
        return (getRecordHeader() & 0xffff0000) >> 16;
    }

    /**
     * Method setSourceID sets the sourceID of this DataRecord object.
     *
     * @param id the sourceID of this DataRecord object.
     */
    public void setSourceID(int id) {
        try {
            setRecordHeader((getRecordHeader() & 0xFFFF) | (id << 16));
        } catch (EVIOHdrException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method getShortCount returns the shortCount of this DataRecord object.
     *
     * @return the shortCount (type int) of this DataRecord object.
     * @throws EVIOHdrException when
     */
    public int getShortCount() throws EVIOHdrException {
        return getRecordHeader() & 0xff;
    }

    /**
     * Method setShortCount sets the shortCount of this DataRecord object.
     *
     * @param co the shortCount of this DataRecord object.
     */
    public void setShortCount(int co) {
        try {
            setRecordHeader((getRecordHeader() & 0xFFFFFF) | (co & 0xff));
        } catch (EVIOHdrException e) {
            e.printStackTrace();
        }
    }

}
