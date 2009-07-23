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
 * This class represents a CODA-format bank of data. This bank of data
 * may itself be a data block in another bank. Thus this class implements
 * the block interface.
 */
public class DataBank implements DataBlock {

    /** This bank's buffer as a ByteBuffer. */
    ByteBuffer bbuffer;

    /** This bank's buffer as an IntBuffer. */
    IntBuffer buffer;

    //private static int bankCtr = 0;
    
    /** Index into the buffer to first 32 bit int which is the bank length. */
    public static final int BANK_LENGTH = 0;

    /** Index into the buffer to the second 32 bit int which is the bank header. */
    public static final int BANK_HEADER = 1;

    //private int bankIndex = 0;

    private int bankCount = 0;

    /** List of data banks and segments in this bank. */
    private ArrayList<DataBlock> payloadItems = new ArrayList<DataBlock>();

    /**
     * Given a buffer, create a DataBank object from it.
     * This includes organizing any contained banks and segments
     * into the payloadItems list. Note the buffer in the created
     * DataBank object is a slice into the argument buffer. Thus
     * changes in either buffer will be visible in both.
     *
     * @param ib given buffer
     * @return DataBank object
     * @throws BankFormatException if data is not either 32 bit ints, banks, or segments
     */
    public static DataBank map(IntBuffer ib) throws BankFormatException {
        IntBuffer sb = ib.slice();
        DataBank db = new DataBank();

        db.buffer = sb;
        //sb.limit(db.getLength());
        db.decode();
        return db;
    }

    /** No-arg constructor. */
    public DataBank() { }

    /**
     * Constructor that uses a ByteBuffer as input. Data content is NOT decoded.
     * @param bufarg input buffer
     */
    public DataBank(ByteBuffer bufarg) {
        bbuffer = bufarg;
        buffer = bbuffer.asIntBuffer();
        // first item in buffer is the length (number of 32 bit ints) of the following data
        buffer.put(BANK_LENGTH, buffer.capacity() - 1);
    }

    /**
     * Constructor that uses an IntBuffer as input. Data content is decoded.
     * @param ib input buffer
     * @throws BankFormatException
     */
    public DataBank(IntBuffer ib) throws BankFormatException {
        this(ib.get(BANK_LENGTH) - 1);
        IntBuffer sb = ib.slice();
        //sb.limit(buffer.limit());

        buffer.put(ib);
        this.decode();
    }

    /**
     * Create a data bank big enough for a payload of length words.
     * @param length
     */
    public DataBank(int length) {
        //this(ByteBuffer.allocate(4 * (length + 2)));
        // bug bug: Carl did this, do we really need a direct buffer? (since it's more expensive to create)
        this(ByteBuffer.allocateDirect(4 * (length + 2)));
    }

    /**
     * Read data from the given input stream and place it into a DataBank object.
     *
     * @param in input stream
     * @return DataBank object created from input stream
     * @throws IOException
     * @throws BankFormatException
     */
    public static DataBank read(DataInputStream in) throws IOException, BankFormatException {
        int length = in.readInt();
        DataBank db = new DataBank(length);

        in.readFully(db.bbuffer.array(), 4, length * 4);
        db.decode();
        return db;
    }

    /**
     * Write out the data in a DataBank object's buffer to the given output stream.
     *
     * @param out output stream
     * @param db DataBank object to stream
     * @throws IOException
     */
    public static void write(DataOutputStream out, DataBank db) throws IOException {
        try {
            out.write(db.bbuffer.array(), 0, (db.getLength() + 1) * 4);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    /**
     * Write out the data in a DataBank object's buffer over a TCP socket.
     * Currently there are no usages of this method.
     *
     * @param out output socket
     * @param db DataBank object to stream
     * @throws IOException
     */
    public static void write(Socket out, DataBank db) throws IOException {
        // bug bug: if socket not created with channel, channel will be null !!!
        SocketChannel sc = out.getChannel();
        sc.write(db.bbuffer);

    }

    /**
     * Get the length of the bank including the header
     * but NOT including the first int which is the length
     * (which is being returned here).
     * @return length of the bank
     */
    public int getLength() {
        // returns an int at the given index
        return buffer.get(BANK_LENGTH);
    }

    /**
     * Get the entire buffer as a buffer of 32 bit ints.
     * The buffer includes the first int which is the length
     * and the second int which is the header as well as the
     * actual data which follows that.
     * 
     * @return the buffer as a buffer of 32 bit ints
     */
    public IntBuffer getBuffer() {
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
     * Get the 4 status bits as an int.
     * @return bank status
     */
    public int getStatus() {
        int hdr = getHeader();
        return (hdr >> 28) & 0xf;
    }

    /**
     * Get the 32 bit header word.
     * @return the 32 bit header word as an int
     */
    private int getHeader() {
        return buffer.get(BANK_HEADER);
    }

    /**
     * Set the 4 status bits (4 most significant bits) in the header.
     * @param status status (lowest 4 bits)
     */
    public void setStatus(int status) {
        int hdr = getHeader();
        hdr = (hdr & 0x0fffffff) | ((status & 0xf) << 28);

        buffer.put(BANK_HEADER, hdr);
    }

    /**
     * Get the source (ROC) id number (12 bits) from the header.
     * @return the source (ROC) id number (12 bits) from the header as an int
     */
    public int getSourceID() {
        int hdr = getHeader();
        return (hdr >> 16) & 0x0fff;
    }

    /**
     * Set the source (ROC) id number (12 bits) from the header.
     * @param sourceID  source (ROC) id number (lowest 12 bits)
     */
    public void setSourceID(int sourceID) {
        int hdr = getHeader();
        hdr = (hdr & 0xf000ffff) | ((sourceID & 0x0fff) << 16);
        buffer.put(BANK_HEADER, hdr);
    }

    /**
     * Get the data type (8 bits) from the header.
     * @return the data type (8 bits) from the header as an int
     */
    public int getDataType() {
        int hdr = getHeader();
        return (hdr & 0xff00) >> 8;
    }

    /**
     * Set the data type (8 bits) in the header.
     * @param dataType data type (lowest 8 bits)
     */
    public void setDataType(int dataType) {
        int hdr = getHeader();
        hdr = (hdr & 0xffff00ff) | ((dataType & 0xff) << 8);
        buffer.put(BANK_HEADER, hdr);
    }

    /**
     * Get the lowest 8 bits of the event number (taken from header).
     * @return Get the lowest 8 bits of the event number as an int
     */
    public int getNumber() {
        int hdr = getHeader();
        return (hdr & 0xff);
    }

    /**
     * Set the event number in the header.
     * Use only the lowest 8 bits of the arg.
     * @param number set the event number in the header (lowest 8 bits)
     */
    public void setNumber(int number) {
        int hdr = getHeader();
        hdr = (hdr & 0xffffff00) | (number & 0xff);
        buffer.put(BANK_HEADER, hdr);
    }

    /**
     * Print the first 2 32 bit ints which give the length and header of the bank.
     * Follow that by printing out the whole buffer.
     */
    public void dumpHeader() {
        System.out.printf("len %08x\nHdr %08x\n", buffer.get(BANK_LENGTH), buffer.get(BANK_HEADER));
        for (int ix = 0; ix <= buffer.get(BANK_LENGTH); ix++) {
            System.out.printf("%08x\n", buffer.get(ix));
        }
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
                // The map method calls this function so it's recursive.
                payloadBank = DataBank.map(payload);
            // else if data segment ...
            } else if (getDataType() == 0x20) {
                // take the buffer and create a DataSegment from it
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
