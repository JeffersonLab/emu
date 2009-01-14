package org.jlab.coda.support.evio;

import org.jlab.coda.support.configurer.DataNotFoundException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 11, 2008
 * Time: 1:42:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class DataFile {
    public static final int EVBLOCKSIZE = 8192;

    public static final int EV_VERSION = 2;
    public static final int EV_MAGIC = 0xc0da0100;
    public static final int EV_HDSIZ = 8;
    public static final int EV_HD_BLKSIZ = 0;
    public static final int EV_HD_BLKNUM = 1;
    public static final int EV_HD_HDSIZ = 2;
    public static final int EV_HD_START = 3;
    public static final int EV_HD_USED = 4;
    public static final int EV_HD_VER = 5;
    public static final int EV_HD_RESVD = 6;
    public static final int EV_HD_MAGIC = 7;

    /** Field bufferLength */
    private int bufferLength;

    /** Field data */
    private byte[] data = new byte[EVBLOCKSIZE];

    private byte[] partial = null;
    private int missing = 0;

    private int offset = 0;

    private int left = 0;

    private int size = EVBLOCKSIZE;

    private int blocks = 0;

    private int nevents = 0;

    DataInputStream in = null;

    DataOutputStream out = null;

    public DataFile(DataInputStream in) {
        super();
        this.in = in;
    }

    public DataFile(DataOutputStream out) {
        super();
        this.out = out;
    }

    public DataFile() {
        System.out.println("DataFile constructor");
        setData(EV_HD_BLKSIZ, EVBLOCKSIZE);
        setData(EV_HD_BLKNUM, 0);      // block counter = blocks
        setData(EV_HD_HDSIZ, EV_HDSIZ);
        setData(EV_HD_START, 0);       // start of first full event = partial
        setData(EV_HD_USED, EV_HDSIZ); // amount of space used in the block = offset
        setData(EV_HD_VER, EV_VERSION);
        setData(EV_HD_RESVD, 0); // event number of last event in block = nevents
        setData(EV_HD_MAGIC, EV_MAGIC);
        offset = EV_HDSIZ * 4;
    }

    public int size() {
        return EV_HDSIZ;
    }

    public void write(DataEvent event) throws IOException {
        int toWrite = (event.length() + 1) * 4; // size of payload+length in bytes

        if (EVBLOCKSIZE - offset > toWrite) {
            System.arraycopy(event.getBuffer(), event.getStart(), data, offset, toWrite);
            offset += toWrite;
            nevents++;
        } else {
            System.arraycopy(event.getBuffer(), event.getStart(), data, offset, EVBLOCKSIZE - offset);
            int remainder = toWrite - (EVBLOCKSIZE - offset);
            setData(EV_HD_BLKNUM, blocks++);
            setData(EV_HD_RESVD, nevents++);
            out.write(data, 0, EVBLOCKSIZE);
            offset = EV_HDSIZ * 4;
            if (remainder > 0) System.arraycopy(event.getBuffer(), event.getStart() + toWrite, data, offset, remainder);
            offset += remainder;
            setData(EV_HD_START, offset);
        }
    }

    public void write(DataTransportRecord record) throws IOException, DataNotFoundException {
        int eventCount = record.getEventCount();
        int evoffset = DataTransportRecord.PAYLOAD;
        System.out.println("Record write");
        record.dumpHeader();
        for (int ix = 0; ix < eventCount; ix++) {

            DataEvent ev = new DataEvent(record.getData(), evoffset * 4);

            ev.dumpHeader();
            evoffset += (ev.length() + 1);

            write(ev);
        }
    }

    public DataEvent read() throws IOException {
        int nread = 0;
        DataEvent ev = null;
        if (left == 0) {
            nread = in.read(data, offset, EVBLOCKSIZE - offset);
            if (nread == -1) return ev;
            else {
                left = getData(EV_HD_USED);
                offset = EV_HDSIZ * 4;
            }

        }
        return ev;
    }

    public DataTransportRecord readRecord(int count) throws IOException {
        int counter = 0;
        int totalLength = 0;
        DataEvent[] events = new DataEvent[count];

        for (counter = 0; counter < count; counter++) {
            events[counter] = read();
            if (events[counter] == null) break;
            totalLength += events[counter].length() + 1;
        }

        DataTransportRecord dr = new DataTransportRecord(totalLength, nevents++, counter, 0);
        int evcount = counter;
        for (counter = 0; counter < evcount; counter++) {
            dr.add(events[counter]);
        }
        return dr;
    }

    public void close() throws IOException {
        if ((out != null) && (offset != EV_HDSIZ * 4)) {
            setData(EV_HD_BLKNUM, blocks++);
            setData(EV_HD_RESVD, nevents++);
            out.write(data, 0, EVBLOCKSIZE);
            out.close();
        }

        if (in != null) in.close();
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
}
