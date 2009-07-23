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

    /** This segment's buffer as a ByteBuffer. */
    ByteBuffer bbuffer;

    /** This segment's buffer as an IntBuffer. */
    IntBuffer buffer;

    private static int bankCtr = 0;

    /** Index into the buffer to the second 32 bit int which is the actual data.
     * Unlike a bank, the buffer's first word contains length and header info. */
    public static final int PAYLOAD_START = 1;

    //private int bankIndex = 0;

    private int bankCount = 0;

    /** List of data banks and segments in this segment. */
    private ArrayList<DataBlock> payloadItems = new ArrayList<DataBlock>();

    /**
     * Given a buffer, create a DataSegment object from it.
     * This includes organizing any contained banks and segments
     * into the payloadItems list. Note the buffer in the created
     * DataSegment object is a slice into the argument buffer. Thus
     * changes in either buffer will be visible in both.
     *
     * @param ib given buffer
     * @return DataSegment object
     * @throws BankFormatException if data is not either 32 bit ints, banks, or segments
     */
    public static DataSegment map(IntBuffer ib) throws BankFormatException {
        IntBuffer sb = ib.slice();

        DataSegment db = new DataSegment();

        db.buffer = sb;
        sb.limit(db.getLength() + 1);
        db.decode();
        return db;
    }

    /** No-arg constructor. */
    public DataSegment() {}

    /**
     * Constructor that uses a ByteBuffer as input. Data content is NOT decoded.
     * @param bufarg input buffer
     */
    public DataSegment(ByteBuffer bufarg) {
        bbuffer = bufarg;
        buffer = bbuffer.asIntBuffer();
        // The length (number of 32 bit ints) of the following data,
        // is placed in first word - lowest 16 bits.
        int hdr = buffer.get(0) & 0xffff0000;
        buffer.put(0, (buffer.limit() & 0xffff) | hdr);
    }

    /**
     * Constructor that uses an IntBuffer as input. Data content is decoded.
     * @param ib input buffer
     * @throws BankFormatException
     */
    public DataSegment(IntBuffer ib) throws BankFormatException {
        this(ib.get(0) & 0xffff);

        IntBuffer sb = ib.slice();
        sb.limit(buffer.limit());
        buffer.put(sb);
        this.decode();
    }

    /**
     * Create a data segment big enough for a payload of length words.
     * @param length
     */
    public DataSegment(int length) {
        this(ByteBuffer.allocate(4 * (length + 1)));
        
        System.out.printf("create data bank, %d\n", (bankCtr++));
        System.out.println("buffer[BANK_LENGTH]" + (buffer.get(0) & 0xffff));
    }

    /**
     * Read data from the given input stream and place it into a DataSegment object.
     *
     * @param in input stream
     * @return DataSegment object created from input stream
     * @throws IOException
     * @throws BankFormatException
     */
    public static DataSegment read(DataInputStream in) throws IOException, BankFormatException {
        int length = in.readInt();

        DataSegment db = new DataSegment(length);

        in.readFully(db.bbuffer.array(), 4, length * 4);
        db.decode();
        return db;
    }

    /**
     * Write out the data in a DataSegment object's buffer to the given output stream.
     *
     * @param out output stream
     * @param db DataSegment object to stream
     * @throws IOException
     */
    public static void write(DataOutputStream out, DataSegment db) throws IOException {
        out.write(db.bbuffer.array());

    }

    /**
     * Get the length of the segment including the header
     * but NOT including the first int which is the length
     * (which is being returned here).
     * @return length of the segment
     */
    public int getLength() {
        return buffer.get(0) & 0xffff;
    }

    /**
     * Get the entire buffer as a buffer of 32 bit ints.
     * The buffer includes the header as well as the
     * actual data which follows that.
     *
     * @return the buffer as a buffer of 32 bit ints
     */
    public IntBuffer getBuffer() {
        buffer.rewind();
        return buffer;
    }

    /**
     * Get the 32 bit integer data at the given index.
     * @param index index of desired data
     * @return the 32 bits of data at the given index as an int
     */
    public int getData(int index) {
        return buffer.get(PAYLOAD_START + index);
    }

    /**
     * Get the 16 bit header word.
     * @return the 16 bit header word as an int
     */
    private int getHeader() {
        return buffer.get(0) >>> 16;  // this shift fills zeros in on left side
    }

    /**
     * Get the data type (8 bits) from the header.
     * @return the data type (8 bits) from the header as an int
     */
    public int getDataType() {
        int hdr = getHeader();
        // bug bug of Graham's
        // return hdr & 0xff;
        return (hdr >> 16) & 0xff;
    }

    /**
     * Set the data type (8 bits) in the header.
     * @param dataType data type (lowest 8 bits)
     */
    public void setDataType(int dataType) {
        int len = buffer.get(0) & 0xffff;

        buffer.put(0, (dataType << 16) | len);
    }

    /**
     * Print the first 32 bit int which give the header (most sig. 16 bits)
     * and header (least sig. 16 bits) of the segment.
     */
    public void dumpHeader() {
        System.out.printf("len %08x\n", buffer.get(0));
    }

    /**
     * Get the buffer NOT including the header.
     * @return the buffer NOT including the header
     */
    public IntBuffer getPayload() {
        buffer.position(PAYLOAD_START);

        return buffer.slice();

    }

    /**
     * Takes this buffer and if it's a data bank or data segment,
     * breaks it down into a list of sub banks/segments (each of
     * which may contain sub banks/segments etc).
     *
     * @throws BankFormatException
     */
    public void decode() throws BankFormatException {

        bankCount = 0;

        // if 32 bit int, no decoding necessary
        if (getDataType() == 0x01) return;

        IntBuffer payload = getPayload();
        payloadItems.clear();

        while (payload.remaining() > 0) {

            DataBlock payloadBank;
            // if data bank ...
            if (getDataType() == 0x10) {
                // Take the buffer and create a DataBank from it.
                payloadBank = DataBank.map(payload);
            // else if data segment ...
            } else if (getDataType() == 0x20) {
                // Take the buffer and create a DataSegment from it.
                // The map method calls this function so it's recursive.
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
