package org.jlab.coda.support.evio;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 21, 2008
 * Time: 8:35:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class DataImpl implements DataObject {

    protected int start = 0;
    protected int cursor;
    protected int wordsize;

    /** Field data */
    protected byte[] data;
    protected final static int DATA_LENGTH = 0;
    /** Field EVENT_HEADER */
    protected final static int DATA_HEADER = 1;

    protected final static int DATA_PAYLOAD = 2;

    protected final static int EVENT_RECORD_ID = 0x0C00;
    protected final static int DATA_TRANS_RECORD_ID = 0x0C01;
    protected final static int ROC_RAW_RECORD_ID = 0x0C02;

    public DataImpl(int ws, byte[] db, int offs) {
        wordsize = ws;
        data = db;
        start = offs;
        cursor = start + DATA_PAYLOAD * wordsize;
    }

    public DataImpl(int ws, int type, Class dc, byte[] db, int offs) {
        wordsize = ws;
        data = db;
        start = offs;
        cursor = start + DATA_PAYLOAD * wordsize;

        int id = 0x01;
        if (dc.equals(DataBank.class)) {
            id = 0x10;
        } else if (dc.equals(DataSegment.class)) {
            id = 0x20;
        }
        int hdr = ((type & 0xfff) << 16) | ((id & 0xff) << 8);
        setHeader(hdr);
    }

    /**
     * Method getData returns the data of this DataEvent object.
     *
     * @return the data (type byte[]) of this DataEvent object.
     */
    public byte[] getBuffer() {
        return data;
    }

    /**
     * Method setData sets the data of this DataEvent object.
     *
     * @param data the data of this DataEvent object.
     */
    public void setBuffer(byte[] data) {
        this.data = data;
    }

    /**
     * Method getBufferLength returns the bufferLength of this DataEvent object.
     *
     * @return the bufferLength (type int) of this DataEvent object.
     */
    public int getBufferLength() {
        return data.length;
    }

    public int getDataOffset() {
        return cursor;
    }

    /**
     * Method getData ...
     *
     * @param off of type int
     * @return int
     */
    public int getData(int off) {
        int rl = 0;
        int offset = off + start;
        rl |= data[offset] & 0xFF;
        rl <<= 8;
        rl |= data[1 + offset] & 0xFF;

        // need top bytes if wordsize == 4
        if (wordsize == 4) {
            rl <<= 8;
            rl |= data[2 + offset] & 0xFF;
            rl <<= 8;
            rl |= data[3 + offset] & 0xFF;
        }
        return rl;
    }

    /**
     * Method setData ...
     *
     * @param off   of type int
     * @param value of type int
     */
    public void setData(int off, int value) {
        int offset = off + start;
        int v = value;

        // need top bytes if wordsize == 4
        if (wordsize == 4) {
            data[3 + offset] = (byte) (v & 0xFF);
            v >>= 8;
            data[2 + offset] = (byte) (v & 0xFF);
            v >>= 8;
        }

        data[1 + offset] = (byte) (v & 0xFF);
        v >>= 8;
        data[offset] = (byte) (v & 0xFF);
        v >>= 8;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * Method getRecordLength returns the recordLength of this DataEvent object.
     *
     * @return the recordLength (type int) of this DataEvent object.
     */
    public int length() {
        return getData(DATA_LENGTH);
    }

    /**
     * Method setRecordLength sets the recordLength of this DataEvent object.
     *
     * @param length the recordLength of this DataEvent object.
     */
    public void setLength(int length) {
        setData(DATA_LENGTH, length);
    }

    /**
     * Method getHeader returns the header of this DataEvent object.
     *
     * @return the header (type int) of this DataEvent object.
     */
    public int getHeader() {
        return getData(DATA_HEADER);
    }

    /**
     * Method setHeader sets the header of this DataEvent object.
     *
     * @param recordHeader the header of this DataEvent object.
     */
    public void setHeader(int recordHeader) {
        setData(DATA_HEADER, recordHeader);
    }

    public void setN(int n) throws BankFormatException {
        if (wordsize == 2) throw new BankFormatException("Bank method called on a segment");
        int hdr = getHeader();

        hdr = (n & 0xff) | (hdr & 0xffffff00);
    }

    public int getN() throws BankFormatException {
        if (wordsize == 2) throw new BankFormatException("Bank method called on a segment");
        return (getHeader() & 0xff);
    }

    public void setStat(int s) throws BankFormatException {
        if (wordsize == 2) throw new BankFormatException("Bank method called on a segment");
        setHeader((getHeader() & 0x0fffffff) | ((s & 0xf) << 28));
    }

    public int getStat() throws BankFormatException {
        if (wordsize == 2) throw new BankFormatException("Bank method called on a segment");
        return (getHeader() >> 28) & 0xf;
    }

    public void setId(int id) {
        if (wordsize == 4) setHeader((getHeader() & 0x00ff) | ((id & 0xff) << 8));
        else setHeader((getHeader() & 0x00ff) | ((id & 0xff) << 8));
    }

    public int getId() {
        if (wordsize == 4) return (getHeader() >> 8) & 0xff;
        else return (getHeader() >> 8) & 0xff;
    }

    public void setPayloadClass(Class c) {
        if (wordsize == 4) {
            if (c.equals(DataBank.class)) {
                setHeader((getHeader() & 0xffff00ff) | (0x1000));
            }

            if (c.equals(DataSegment.class)) {
                setHeader((getHeader() & 0xffff00ff) | (0x2000));
            } else {
                setHeader((getHeader() & 0xffff00ff) | (0x0100));
            }
        } else {
            if (c.equals(DataBank.class)) {
                setHeader((getHeader() & 0xff00) | (0x10));
            }

            if (c.equals(DataSegment.class)) {
                setHeader((getHeader() & 0xff00) | (0x20));
            } else {
                setHeader((getHeader() & 0xff00) | (0x01));
            }
        }
    }

    public Class getPayloadClass() {
        if (wordsize == 4) {
            if (((getHeader() & 0xff00) >> 8) == 0x10) {
                return DataBank.class;
            }
            if (((getHeader() & 0xff00) >> 8) == 0x20) {
                return DataSegment.class;
            } else {
                return int.class;
            }
        } else {
            if ((getHeader() & 0xff) == 0x10) {
                return DataBank.class;
            }
            if ((getHeader() & 0xff) == 0x20) {
                return DataSegment.class;
            } else {
                return int.class;
            }
        }
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getStart() {
        return start;
    }

    public int getCursor() {
        return cursor;
    }

    public void setCursor(int cursor) {
        this.cursor = cursor;
    }

    public void dumpHeader() {
        System.out.println("hdr for event " + getHeader());
        for (int ix = 0; ix < DATA_PAYLOAD; ix++) {
            System.out.printf("%08X\n", getData(ix));
        }
    }

    /**
     * Method add - copies data from existing bank into this bank
     *
     * @param b
     * @throws BankFormatException
     */
    public void add(DataObject b) throws BankFormatException {
        if (!getPayloadClass().equals(DataBank.class)) throw new BankFormatException("attempt to add a bank to a bank of " + getPayloadClass().getName());
        if ((cursor + (b.length() + 1) * 4) > data.length) throw new BankFormatException("adding bank will cause buffer overflow");

        System.arraycopy(b.getBuffer(), b.getStart(), data, cursor, (b.length() + 1) * 4);
        cursor += (b.length() + 1) * 4;
    }

    public void add(int d) throws BankFormatException {
        if (!getPayloadClass().equals(int.class)) throw new BankFormatException("attempt to add a int to a bank of " + getPayloadClass().getName());
        if ((cursor + 4) > data.length) throw new BankFormatException("adding long will cause buffer overflow");

        setData(cursor, d);
        cursor += 4;
    }
}
