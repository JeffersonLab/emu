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
 * <pre>
 * Class <b>DataBlock </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
@SuppressWarnings({"WeakerAccess"})
public class DataBlock {
    /** Field bufferLength */
    private int bufferLength;

    /** Field data */
    private byte[] data;

    /** Field BLOCK_LENGTH */
    private final static int BLOCK_LENGTH;

    static {
        BLOCK_LENGTH = 0;
    }

    /** Field EVENT_COUNT */
    private final static int EVENT_COUNT = 1;

    /** Field PAYLOAD */
    public final static int PAYLOAD = 8;

    /**
     * Constructor DataBlock creates a new DataBlock instance.
     *
     * @param db of type byte[]
     * @param l  of type int
     */
    public DataBlock(byte[] db, int l) {
        data = db;
        bufferLength = l;
    }

    /**
     * Constructor DataBlock creates a new DataBlock instance.
     *
     * @param l of type int
     */
    public DataBlock(int l) {
        data = new byte[l];
        bufferLength = l;
    }

    /**
     * Method getData returns the data of this DataBlock object.
     *
     * @return the data (type byte[]) of this DataBlock object.
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Method setData sets the data of this DataBlock object.
     *
     * @param data the data of this DataBlock object.
     */
    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * Method getBufferLength returns the bufferLength of this DataBlock object.
     *
     * @return the bufferLength (type int) of this DataBlock object.
     */
    public int getBufferLength() {
        return bufferLength;
    }

    /**
     * Method setBufferLength sets the bufferLength of this DataBlock object.
     *
     * @param length the bufferLength of this DataBlock object.
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
        rl |= data[offset] & 0xFF;
        rl <<= 8;
        rl |= data[1 + offset] & 0xFF;
        rl <<= 8;
        rl |= data[2 + offset] & 0xFF;
        rl <<= 8;
        rl |= data[3 + offset] & 0xFF;
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
        data[3 + offset] = (byte) (v & 0xFF);
        v >>= 8;
        data[2 + offset] = (byte) (v & 0xFF);
        v >>= 8;
        data[1 + offset] = (byte) (v & 0xFF);
        v >>= 8;
        data[offset] = (byte) (v & 0xFF);
        v >>= 8;
    }

    /**
     * Method getRecordLength returns the recordLength of this DataBlock object.
     *
     * @return the recordLength (type int) of this DataBlock object.
     */
    public int getRecordLength() {
        return getData(BLOCK_LENGTH);
    }

    /**
     * Method setRecordLength sets the recordLength of this DataBlock object.
     *
     * @param recordLength the recordLength of this DataBlock object.
     */
    public void setRecordLength(int recordLength) {
        setData(BLOCK_LENGTH, recordLength);
    }

    /**
     * Method getEventCount returns the eventCount of this DataBlock object.
     *
     * @return the eventCount (type int) of this DataBlock object.
     */
    public int getEventCount() {
        return getData(EVENT_COUNT);
    }

    /**
     * Method setEventCount sets the eventCount of this DataBlock object.
     *
     * @param recordLength the eventCount of this DataBlock object.
     */
    public void setEventCount(int recordLength) {
        setData(EVENT_COUNT, recordLength);
    }

}
