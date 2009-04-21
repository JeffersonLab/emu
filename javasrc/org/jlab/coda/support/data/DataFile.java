package org.jlab.coda.support.data;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

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

    ByteBuffer bbuffer;
    private IntBuffer buffer;

    private byte[] partial = null;
    private int missing = 0;

    private int size = EVBLOCKSIZE;

    private int blocks = 0;

    private int nevents = 0;

    DataInputStream in = null;

    DataOutputStream out = null;

    public DataFile(String fn, boolean read) throws FileNotFoundException {
        init();

        if (read) {
            in = new DataInputStream(new FileInputStream(fn));
            buffer.rewind();
            buffer.limit(0);
        } else {
            out = new DataOutputStream(new FileOutputStream(fn));
        }
    }

    public DataFile(DataInputStream in) {
        init();
        this.in = in;
        buffer.rewind();
        buffer.limit(0);
    }

    public DataFile(DataOutputStream out) {
        init();
        this.out = out;
    }

    public DataFile() {
        init();
    }

    private void init() {
        bbuffer = ByteBuffer.allocate(EVBLOCKSIZE);
        buffer = bbuffer.asIntBuffer();

        buffer.put(EV_HD_BLKSIZ, EVBLOCKSIZE);
        buffer.put(EV_HD_BLKNUM, 0);      // block counter = blocks
        buffer.put(EV_HD_HDSIZ, EV_HDSIZ);
        buffer.put(EV_HD_START, 0);       // start of first full event = partial
        buffer.put(EV_HD_USED, EV_HDSIZ); // amount of space used in the block = offset
        buffer.put(EV_HD_VER, EV_VERSION);
        buffer.put(EV_HD_RESVD, 0); // event number of last event in block = nevents
        buffer.put(EV_HD_MAGIC, EV_MAGIC);
        buffer.position(EV_HDSIZ);

    }

    public void write(DataBank event) throws IOException {
        nevents++;

        event.buffer.rewind();

        while (event.buffer.remaining() > 0) {

            if (buffer.remaining() > event.buffer.remaining()) {
                // event will fit in buffer

                buffer.put(event.buffer);
            } else {
                // event will not fit in buffer so create a temporary IntBuffer to work with
                IntBuffer buf = event.buffer.slice();

                // limit buf to remaining space in buffer
                buf.limit(buffer.remaining());
                event.buffer.position(event.buffer.position() + buffer.remaining());
                buffer.put(buf);

                buffer.put(EV_HD_BLKNUM, blocks++);
                buffer.put(EV_HD_RESVD, nevents);
                buffer.put(EV_HD_USED, buffer.position());

                out.write(bbuffer.array(), 0, EVBLOCKSIZE);

                buffer.rewind();
                buffer.position(EV_HDSIZ);
                while (buffer.remaining() > 0) buffer.put(0);

                buffer.rewind();
                buffer.position(EV_HDSIZ);
                buffer.put(EV_HD_START, 0);

            }
            if ((buffer.position() != EV_HDSIZ) && (buffer.get(EV_HD_START) == 0)) buffer.put(EV_HD_START, buffer.position());

        }
        // rewind the event buffer.
        event.buffer.rewind();
    }

    private void readin() throws IOException {
        int nread = 0;
        nread = in.read(bbuffer.array(), 0, EVBLOCKSIZE);
        if (nread == -1) throw new IOException("read returned -1");
        else {
            buffer.limit(EVBLOCKSIZE / 4);
            buffer.limit(buffer.get(EV_HD_USED));
            buffer.position(EV_HDSIZ);
        }

    }

    public DataBank read() throws IOException, BankFormatException {
        // if the buffer is empty read in a new one.

        if (buffer.remaining() == 0) readin();

        // First word should be an event length
        // Peak at it. buffer.get() would increment position
        // buffer.get(buffer.position()) doesn't.
        int evLen = buffer.get(buffer.position()) + 1;
        int toCopy = evLen;

        // if evLen <= buffer.remaining then just copy the data.
        if (evLen <= buffer.remaining()) {
            IntBuffer tmp = buffer.slice();
            buffer.position(buffer.position() + evLen);
            tmp.limit(toCopy);

            return new DataBank(tmp);
        }

        // if we get here then the remaining data in buffer is
        // less than the event length.

        DataBank event = new DataBank(evLen);

        toCopy -= buffer.remaining();

        event.buffer.put(buffer);

        // buffer is now empty so refill

        readin();

        while (toCopy > 0) {
            if (toCopy <= buffer.remaining()) {
                IntBuffer tmp = buffer.slice();
                buffer.position(buffer.position() + toCopy);
                tmp.limit(toCopy);

                event.buffer.put(tmp);

                return event;
            }

            toCopy -= buffer.remaining();

            event.buffer.put(buffer);

            // buffer is now empty so refill

            readin();
        }

        return event;

    }

    public void close() throws IOException {
        if ((out != null) && (buffer.position() != EV_HDSIZ)) {
            buffer.put(EV_HD_USED, buffer.position());
            buffer.put(EV_HD_BLKNUM, blocks);
            buffer.put(EV_HD_RESVD, nevents);
            print();
            out.write(bbuffer.array(), 0, EVBLOCKSIZE);
            out.close();
        }

        if (in != null) in.close();
    }

    public void print() {
        int ctr = 0;
        int ctr2 = 0;

        buffer.rewind();

        while (buffer.hasRemaining()) {
            System.out.printf("%08x ", buffer.get());

            if (ctr++ == 23) {
                ctr = 0;
                System.out.println("");
            }

            if (ctr2++ == 7) {
                ctr = 0;
                System.out.println("");
            }
        }
        System.out.println("");
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("File name not specified");
            System.exit(1);
        }
        System.out.print("Args ");
        for (int ix = 0; ix < args.length; ix++) {
            System.out.print(args[ix] + ", ");
        }
        System.out.println("");
        boolean read = Boolean.valueOf(args[1]);
        try {
            if (read) {
                DataFile file = new DataFile(args[0], true);
                while (true) {
                    try {
                        DataBank db = file.read();
                        System.out.printf("event %d %08x\n", db.buffer.get(2), db.buffer.get(1));
                    } catch (IOException e) {
                        System.out.println("\nclose file : " + args[0]);
                        break;
                    } catch (BankFormatException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                    //file.print();
                }

            } else {
                DataBank db = new DataBank(10);
                db.dumpHeader();
                db.setDataType(1);

                db.setSourceID(0x234);

                db.setStatus(0xf);

                db.setNumber(0);
                db.buffer.position(3);
                for (int ix = 3; ix <= 11; ix++)
                    db.buffer.put(0xC0DA000 | ix);

                DataFile file = new DataFile(args[0], false);

                for (int ix = 0; ix < 100000; ix++) {
                    db.buffer.put(2, ix);
                    db.setNumber(ix);
                    file.write(db);

                }
                file.close();

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
