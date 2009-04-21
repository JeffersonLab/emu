package org.jlab.coda.support.data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Jan 26, 2009
 * Time: 9:29:42 AM
 */
public class DataBank implements DataBlock {
    ByteBuffer bbuffer;
    IntBuffer buffer;

    private static int bankCtr = 0;
    public static final int BANK_LENGTH = 0;
    public static final int BANK_HEADER = 1;

    private int bankIndex = 0;

    private int bankCount = 0;

    private ArrayList<DataBlock> payloadItems = new ArrayList<DataBlock>();

    public static DataBank map(IntBuffer ib) throws BankFormatException {
        IntBuffer sb = ib.slice();
        DataBank db = new DataBank();

        db.buffer = sb;
        //sb.limit(db.getLength());
        db.decode();
        return db;
    }

    public DataBank() {

    }

    public DataBank(ByteBuffer bufarg) {
        bbuffer = bufarg;
        buffer = bbuffer.asIntBuffer();
        buffer.put(BANK_LENGTH, buffer.capacity() - 1);
    }

    public DataBank(IntBuffer ib) throws BankFormatException {
        this(ib.get(BANK_LENGTH) - 1);
        IntBuffer sb = ib.slice();
        //sb.limit(buffer.limit());

        buffer.put(ib);
        this.decode();
    }

    /**
     * Create a data bank big enough for a payload of length words
     *
     * @param length
     */
    public DataBank(int length) {
        this(ByteBuffer.allocate(4 * (length + 2)));
    }

    public static DataBank read(DataInputStream in) throws IOException, BankFormatException {
        int length = in.readInt();
        DataBank db = new DataBank(length);

        in.readFully(db.bbuffer.array(), 4, length * 4);
        db.decode();
        return db;
    }

    public static void write(DataOutputStream out, DataBank db) throws IOException {
        try {
            out.write(db.bbuffer.array(), 0, (db.getLength() + 1) * 4);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    public static void write(Socket out, DataBank db) throws IOException {
        SocketChannel sc = out.getChannel();
        sc.write(db.bbuffer);

    }

    public int getLength() {
        return buffer.get(BANK_LENGTH);
    }

    public IntBuffer getBuffer() {
        return buffer;
    }

    public int getData(int index) {
        return buffer.get(PAYLOAD_START + index);
    }

    public int getStatus() {
        int hdr = getHeader();
        return (hdr >> 28) & 0xf;
    }

    private int getHeader() {
        return buffer.get(BANK_HEADER);
    }

    public void setStatus(int status) {
        int hdr = getHeader();
        hdr = (hdr & 0x0fffffff) | ((status & 0xf) << 28);

        buffer.put(BANK_HEADER, hdr);
    }

    public int getSourceID() {
        int hdr = getHeader();
        return (hdr >> 16) & 0x0fff;
    }

    public void setSourceID(int sourceID) {
        int hdr = getHeader();
        hdr = (hdr & 0xf000ffff) | ((sourceID & 0x0fff) << 16);
        buffer.put(BANK_HEADER, hdr);
    }

    public int getDataType() {
        int hdr = getHeader();
        return (hdr & 0xff00) >> 8;
    }

    public void setDataType(int dataType) {
        int hdr = getHeader();
        hdr = (hdr & 0xffff00ff) | ((dataType & 0xff) << 8);
        buffer.put(BANK_HEADER, hdr);
    }

    public int getNumber() {
        int hdr = getHeader();
        return (hdr & 0xff);
    }

    public void setNumber(int number) {
        int hdr = getHeader();
        hdr = (hdr & 0xffffff00) | (number & 0xff);
        buffer.put(BANK_HEADER, hdr);
    }

    public void dumpHeader() {
        System.out.printf("len %08x\nHdr %08x\n", buffer.get(BANK_LENGTH), buffer.get(BANK_HEADER));
        for (int ix = 0; ix <= buffer.get(BANK_LENGTH); ix++) {
            System.out.printf("%08x\n", buffer.get(ix));
        }
    }

    public IntBuffer getPayload() {
        buffer.position(PAYLOAD_START);

        return buffer.slice();

    }

    public void decode() throws BankFormatException {

        bankCount = 0;
        if (getDataType() == 0x01) return;
        IntBuffer payload = getPayload();
        payloadItems.clear();
        while (payload.remaining() > 0) {

            DataBlock payloadBank;
            if (getDataType() == 0x10) {
                payloadBank = DataBank.map(payload);
            } else if (getDataType() == 0x20) {
                payloadBank = DataSegment.map(payload);
            } else {
                throw new BankFormatException("Unrecognized data type : " + getDataType());
            }
            payload.position(payload.position() + payloadBank.getLength() + 1);
            payloadItems.add(payloadBank);
        }
        buffer.rewind();

    }

}
