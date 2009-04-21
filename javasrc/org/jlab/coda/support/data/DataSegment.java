package org.jlab.coda.support.data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Jan 26, 2009
 * Time: 9:29:42 AM
 */
public class DataSegment implements DataBlock {
    ByteBuffer bbuffer;
    IntBuffer buffer;

    private static int bankCtr = 0;

    public static final int PAYLOAD_START = 1;

    private int bankIndex = 0;

    private int bankCount = 0;

    private ArrayList<DataBlock> payloadItems = new ArrayList<DataBlock>();

    public static DataSegment map(IntBuffer ib) throws BankFormatException {
        IntBuffer sb = ib.slice();

        DataSegment db = new DataSegment();

        db.buffer = sb;
        sb.limit(db.getLength() + 1);
        db.decode();
        return db;
    }

    public DataSegment() {

    }

    public DataSegment(ByteBuffer bufarg) {
        bbuffer = bufarg;
        buffer = bbuffer.asIntBuffer();
        int hdr = buffer.get(0) & 0xffff0000;
        buffer.put(0, (buffer.limit() & 0xffff) | hdr);
    }

    public DataSegment(IntBuffer ib) throws BankFormatException {
        this(ib.get(0) & 0xffff);

        IntBuffer sb = ib.slice();
        sb.limit(buffer.limit());
        buffer.put(sb);
        this.decode();
    }

    public DataSegment(int length) {
        this(ByteBuffer.allocate(4 * (length + 1)));
        System.out.printf("create data bank, %d\n", (bankCtr++));

        System.out.println("buffer[BANK_LENGTH]" + (buffer.get(0) & 0xffff));
    }

    public static DataSegment read(DataInputStream in) throws IOException, BankFormatException {
        int length = in.readInt();

        DataSegment db = new DataSegment(length);

        in.readFully(db.bbuffer.array(), 4, length * 4);
        db.decode();
        return db;
    }

    public static void write(DataOutputStream out, DataSegment db) throws IOException {
        out.write(db.bbuffer.array());

    }

    public int getLength() {
        return buffer.get(0) & 0xffff;
    }

    public IntBuffer getBuffer() {
        buffer.rewind();
        return buffer;
    }

    public int getData(int index) {
        return buffer.get(PAYLOAD_START + index);
    }

    private int getHeader() {
        return buffer.get(0) >> 16;
    }

    public int getDataType() {
        int hdr = getHeader();
        return hdr & 0xff;
    }

    public void setDataType(int dataType) {
        int len = buffer.get(0) & 0xffff;

        buffer.put(0, (dataType << 16) | len);
    }

    public void dumpHeader() {
        System.out.printf("len %08x\n", buffer.get(0));
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
