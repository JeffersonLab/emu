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
 * Class <b>DataEvent </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
@SuppressWarnings({"WeakerAccess"})
public class DataEvent {
    /** Field bufferLength */
    private int bufferLength;

    /** Field data */
    private byte[] data;

    /** Field EVENT_LENGTH */
    private final static int EVENT_LENGTH = 0;
    /** Field EVENT_HEADER */
    private final static int EVENT_HEADER = 1;
    /** Field PAYLOAD */
    public final static int PAYLOAD = 8;

    /**
     * Constructor DataEvent creates a new DataEvent instance.
     *
     * @param db of type byte[]
     * @param l  of type int
     */
    public DataEvent(byte[] db, int l) {
        data = db;
        bufferLength = l;
    }

    /**
     * Constructor DataEvent creates a new DataEvent instance.
     *
     * @param l of type int
     */
    public DataEvent(int l) {
        data = new byte[l];
        bufferLength = l;
    }

    /**
     * Method getData returns the data of this DataEvent object.
     *
     * @return the data (type byte[]) of this DataEvent object.
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Method setData sets the data of this DataEvent object.
     *
     * @param data the data of this DataEvent object.
     */
    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * Method getBufferLength returns the bufferLength of this DataEvent object.
     *
     * @return the bufferLength (type int) of this DataEvent object.
     */
    public int getBufferLength() {
        return bufferLength;
    }

    /**
     * Method setBufferLength sets the bufferLength of this DataEvent object.
     *
     * @param length the bufferLength of this DataEvent object.
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
     * Method getRecordLength returns the recordLength of this DataEvent object.
     *
     * @return the recordLength (type int) of this DataEvent object.
     */
    public int getRecordLength() {
        return getData(EVENT_LENGTH);
    }

    /**
     * Method setRecordLength sets the recordLength of this DataEvent object.
     *
     * @param recordLength the recordLength of this DataEvent object.
     */
    public void setRecordLength(int recordLength) {
        setData(EVENT_LENGTH, recordLength);
    }

    /**
     * Method getHeader returns the header of this DataEvent object.
     *
     * @return the header (type int) of this DataEvent object.
     */
    public int getHeader() {
        return getData(EVENT_HEADER);
    }

    /**
     * Method setHeader sets the header of this DataEvent object.
     *
     * @param recordLength the header of this DataEvent object.
     */
    public void setHeader(int recordLength) {
        setData(EVENT_HEADER, recordLength);
    }

    /**
     * Method setHeader ...
     *
     * @param trigger of type int
     * @param id      of type int
     * @param n       of type int
     */
    //	#define EMU_EVENT_HDR(t,d,n) ( ((t & 0xffff) << 16) | ((d & 0xff) << 8) | (n &0xff))
    public void setHeader(int trigger, int id, int n) {
        int hdr = ((trigger & 0xffff) << 16) | ((id & 0xff) << 8) | (n & 0xff);
        setData(EVENT_HEADER, hdr);
    }
}
