package org.jlab.coda.emu.test.blast;

import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.emu.support.data.ByteBufferItem;
import org.jlab.coda.emu.support.data.ByteBufferSupply;
import org.jlab.coda.emu.support.data.CODATag;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.jevio.*;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class copies much from the RocSimulation module.
 * It creates real events which are created and ...
 * Created by timmer on 4/6/17.
 */
public class EmuBlaster2 {

    private String name = "EmuBlaster";

    /** This ROC's id */
    private int id;


    //--------------------------------------
    // Evio event generating stuff
    //--------------------------------------

    /** Id # of event-generating thread starting at 0. */
    private final int generatingThdId;

    /** Record ID of record being created. */
    private int  myRocRecordId;

    /** Event Number of event being created. */
    private long myEventNumber;

    /** Timestamp of event being created. */
    private long timestamp;

    /** Ring buffer containing ByteBuffers - used to hold events for writing. */
    private ByteBufferSupply bbSupply1;

    /** Ring buffer containing ByteBuffers - used to hold events for writing. */
    private ByteBufferSupply bbSupply2;

    /** Use direct ByteBuffer? */
    private boolean direct;

    /** Buffer of an evio event block used as template.
     *  For each event, timestamp and event numbers are different. */
    private ByteBuffer templateBuffer1;
    private ByteBuffer templateBuffer2;


    /** Byte order to evio data written. */
    private ByteOrder outputOrder;

    /** Number of data words in each event. */
    private int generatedDataWords;


    /** Total number of evio events written to the outputs. */
    private long eventCountTotal;

    /** Sum of the sizes, in 32-bit words, of all evio events written to the outputs. */
    private long byteCountTotal;

    /** Number of event producing threads in operation. Each
     *  must match up with its own output channel ring buffer. */
    private int eventProducingThreads;

    /** Keep track of the number of records built in this ROC. Reset at prestart. */
    private volatile int rocRecordId;

    /** Type of trigger sent from trigger supervisor. */
    private int triggerType;

    /** Number of events in each ROC raw record. */
    private int eventBlockSize;

    /** The id of the detector which produced the data in block banks of the ROC raw records. */
    private int detectorId;

    /** Size of a single generated Roc raw event in 32-bit words (including header). */
    private int eventWordSize;

    /** Size of a single generated event in bytes (including header). */
    private int eventSize;

    /** Number of computational loops to act as a delay. */
    private int loops;


    //--------------------------------------
    // Socket stuff
    //--------------------------------------

    /** TCP socket send buffer size in bytes. */
    private int bufferSize = 8192;

    /** TCP socket send buffer size in bytes. */
    private int sendBufferSize = 4*8192;

    private byte[] buffer;

    /** TCP port of Blastee. */
    private int blasteePort = 22333;

    /** Name of host Blastee running on. */
    private String blasteeHost;

    /** If true, set socket option for no-delay on. */
    private boolean noDelay;

    /** Time in seconds to wait for connection to emu server. */
    private int connectTimeout;

    /** Number of sockets used to connect this EmuBlaster to the EmuBlastee. */
    private int socketCount=1;


    //--------------------------------------
    // Evio stuff
    //--------------------------------------

    /** Object to write (marshall) input buffers into larger, output evio buffer (next member). */
    private EventWriterUnsync writer1;
    private EventWriterUnsync writer2;




    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage:\n\n" +
                "   java EmuBlaster\n" +
                "         -h <host>        blastee host\n" +
                "        [-d <buf size>]   writing data buffer size in bytes (8192)\n" +
                "        [-s <buf size>]   TCP send buffer size in bytes (4*8192)\n" +
                "        [-p <port>]       blastee TCP port (22333)\n" +
                "        [-c <sockets>]    number of sockets used\n" +
                "        [-nd]             no-delay socket option on (must be last option specified)\n" +
                "        [-help]           print this message\n");
    }


    /** Run as a stand-alone application. */
    public static void main(String[] args) {
        try {
            EmuBlaster2 blaster = new EmuBlaster2(args);
            blaster.run();
        }
        catch (Exception e) {
            System.out.println(e.toString());
            System.exit(-1);
        }
    }




    public EmuBlaster2(String[] args) {

        // Command line processing
        decodeCommandLine(args);

        // Make a buffer
        buffer = new byte[bufferSize];

        // Pick positive id # based on time for uniqueness
        id = (int)System.currentTimeMillis() & 0x7ffffff;
        rocRecordId = 0;
        generatingThdId = 0;
        eventCountTotal = 0L;
        byteCountTotal  = 0L;
        eventProducingThreads = 1;
        outputOrder = ByteOrder.BIG_ENDIAN;

        connectTimeout = 5; // seconds

        // How many entangled events in one data block?
        eventBlockSize = 20;

        // How many bytes in a single event?
        eventSize = 40;

        // Value for trigger type from trigger supervisor
        triggerType = 15;

        // Id of detector producing data
        detectorId = 111;

        // Set & initialize values
        myRocRecordId = rocRecordId + generatingThdId;
        myEventNumber = 1L + generatingThdId *eventBlockSize;
        timestamp = 0L;

        // Need to coordinate amount of data words
        generatedDataWords = eventBlockSize * eventSize;
//System.out.println("Blaster: generatedDataWords = " + generatedDataWords);


        eventWordSize  = getSingleEventBufferWords(generatedDataWords);
//System.out.println("Blaster: eventWordSize = " + eventWordSize);

        templateBuffer1 = createSingleEventBuffer(generatedDataWords, myEventNumber, timestamp);

//System.out.println("Blaster: templateBuffer pos = " + templateBuffer.position() +
//                    ", lim = " + templateBuffer.limit() + ", cap = " + templateBuffer.capacity());

        // Take this buffer of an evio event (entangled physics events)
        // and use it to fill a given size buffer in evio file format.
        // Create a supply of these buffers - each filled with data.
        // This should simulate what a real emu channel will use.

        // 16 works better than 8 but 32 doesn't help
        bbSupply1 = new ByteBufferSupply(16, bufferSize, outputOrder, direct);

        SocketSender sender1 = new SocketSender(bbSupply1, 1);
        sender1.start();

        if (socketCount == 2) {
            templateBuffer2 = EmuUtilities.deepCopy(templateBuffer1);
            bbSupply2 = new ByteBufferSupply(16, bufferSize, outputOrder, direct);
            SocketSender sender2 = new SocketSender(bbSupply2, 2);
            sender2.start();
        }
    }


    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private void decodeCommandLine(String[] args) {
        // Was sendBufferSize set in command line?
        boolean setSendBufSize = false;

        // loop over all args
        try {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equalsIgnoreCase("-help")) {
                    usage();
                    System.exit(-1);
                }
                else if (args[i].equalsIgnoreCase("-h")) {
                    blasteeHost = args[i + 1];
                    i++;
                }
                else if (args[i].equalsIgnoreCase("-d")) {
                    bufferSize = Integer.parseInt(args[i + 1]);
                    if (bufferSize < 1) {
                        System.out.println("Data buffer size must be > 0");
                        System.exit(1);
                    }
                    i++;
                }
                else if (args[i].equalsIgnoreCase("-s")) {
                    sendBufferSize = Integer.parseInt(args[i + 1]);
                    if (sendBufferSize < 1) {
                        System.out.println("TCP send buffer size must be > 0");
                        System.exit(1);
                    }
                    setSendBufSize = true;
                    i++;
                }
                else if (args[i].equalsIgnoreCase("-p")) {
                    blasteePort = Integer.parseInt(args[i + 1]);
                    if (blasteePort < 1024 || blasteePort > 65535) {
                        System.out.println("Port must be > 1023 & < 65536");
                        System.exit(1);
                    }
                    i++;
                }
                else if (args[i].equalsIgnoreCase("-c")) {
                    socketCount = Integer.parseInt(args[i + 1]);
                    if (socketCount != 1 && socketCount != 2) {
                        System.out.println("Socket count must be 1 or 2");
                    }
                    i++;
                }
                else if (args[i].equalsIgnoreCase("-nd")) {
                    noDelay = true;
                }
                else {
                    usage();
                    System.exit(-1);
                }
            }
        }
        catch (Exception ex) {
            usage();
            System.exit(-1);
        }

        // make sure host is defined
        if (blasteeHost == null) {
            System.out.println("\nNeed host specified on command line");
            usage();
            System.exit(-1);
        }

        // If the sendBufferSize is not explicitly set, make it at least 4x the bufferSize.
        if (!setSendBufSize) {
            sendBufferSize = bufferSize <= 8192 ? 4*8192 : 4*bufferSize;
        }

        return;
    }


    private int getSingleEventBufferWords(int generatedDataWords) {

         int dataWordLength = 1 + generatedDataWords;

         // bank header + bank header +  eventBlockSize segments + data,
         // seg  = (1 seg header + 3 data)
         // data = bank header + int data

         //  totalEventWords = 2 + 2 + eventBlockSize*(1+3) + (2 + data.length);
         return (6 + 4*eventBlockSize + dataWordLength);
    }


    /**
     * Change the first timestamp which is 24 bytes into event in trigger bank.
     * Do this so we can check our decoding in the EmuBlastee.
     * @param buf
     * @param ts
     */
    private void setTimeStamp(ByteBuffer buf, long ts) {
        buf.putLong(24, ts);
    }


    private ByteBuffer createSingleEventBuffer(int generatedDataWords, long eventNumber,
                                              long timestamp ) {
        int writeIndex=0;
        int dataLen = 1 + generatedDataWords;

        // Note: buf limit is set to its capacity in allocate()
        ByteBuffer buf = ByteBuffer.allocate(4*eventWordSize);
        buf.order(outputOrder);

        // Event bank header
        // sync, error, isBigEndian, singleEventMode
        int rocTag = Evio.createCodaTag(false, false, true, false, id);

        // 2nd bank header word = tag << 16 | ((padding & 0x3) << 14) | ((type & 0x3f) << 8) | num
        int secondWord = rocTag << 16 |
                         (DataType.BANK.getValue() << 8) |
                         (eventBlockSize & 0xff);

        buf.putInt(writeIndex, (eventWordSize - 1)); writeIndex += 4;
        buf.putInt(writeIndex, secondWord); writeIndex += 4;


        // Trigger bank header
        secondWord = CODATag.RAW_TRIGGER_TS.getValue() << 16 |
                     (DataType.SEGMENT.getValue() << 8) |
                     (eventBlockSize & 0xff);

        buf.putInt(writeIndex, (eventWordSize - 2 - dataLen - 2 - 1)); writeIndex += 4;
        buf.putInt(writeIndex, secondWord); writeIndex += 4;

        // segment header word = tag << 24 | ((padding << 22) & 0x3) | ((type << 16) & 0x3f) | length
        int segWord = triggerType << 24 | (DataType.UINT32.getValue() << 16) | 3;

        // Add each segment
        for (int i = 0; i < eventBlockSize; i++) {
            buf.putInt(writeIndex, segWord); writeIndex += 4;

            // Generate 3 integers per event (no miscellaneous data)
            buf.putInt(writeIndex, (int) (eventNumber + i)); writeIndex += 4;
            buf.putInt(writeIndex, (int)  timestamp); writeIndex += 4;// low 32 bits
            buf.putInt(writeIndex, (int) (timestamp >>> 32 & 0xFFFF)); writeIndex += 4;// high 16 of 48 bits
            timestamp += 4;
        }


        int dataTag = Evio.createCodaTag(false, false, true, false, detectorId);
        secondWord = dataTag << 16 |
                     (DataType.UINT32.getValue() << 8) |
                     (eventBlockSize & 0xff);

        buf.putInt(writeIndex, dataLen + 1); writeIndex += 4;
        buf.putInt(writeIndex, secondWord); writeIndex += 4;


        // First put in starting event # (32 bits)
        buf.putInt(writeIndex, (int)eventNumber); writeIndex += 4;
        for (int i=0; i < generatedDataWords; i++) {
            // Write a bunch of 1s
            buf.putInt(writeIndex, 1); writeIndex += 4;
        }

        // buf is ready to read
        return buf;
    }


    // Thread that writes into a ring buffer (bbSupply)
    public void run() {

         int  skip=2, printPeriod = 5000;
         long oldVal=0L, totalT=0L, deltaCount, totalCount=0L, bufCounter=0L;
         long t1, t2, deltaT=0L, counter=0L;
         ByteBufferItem bufItem = null;


         try {
             t1 = t2 = System.currentTimeMillis();

             while (true) {
                 // Get buffer from recirculating supply &
                 // put generated events into output channel
                 eventToOutputRing1(bbSupply1.get());
                 byteCountTotal += writer1.getBytesWrittenToBuffer();

                 if (socketCount == 2) {
                     eventToOutputRing2(bbSupply2.get());
                     byteCountTotal += writer2.getBytesWrittenToBuffer();
                 }

                 // Don't get the time each loop since it's quite expensive.
                 //
                 // The following is the equivalent of the mod operation
                 // but is much faster (x mod 2^n == x & (2^n - 1)).
                 // Thus (counter % 256) -> (255 & counter)
                 if ((255 & counter++) == 0) {
                    t2 = System.currentTimeMillis();
                 }
                 deltaT = t2 - t1;

                 if (generatingThdId == 0 && deltaT > printPeriod) {
                     if (skip-- < 1) {
                         totalT += deltaT;
                         deltaCount = byteCountTotal - oldVal;
                         totalCount += deltaCount;

                         System.out.println("EmuBlaster: byte rate = " +
                                            String.format("%.3g", (deltaCount*1000./deltaT) ) +
                                            ",  avg = " +
                                            String.format("%.3g", (totalCount*1000.)/totalT) + " Hz" );
                     }
                     else {
                         System.out.println("EmuBlaster: byte rate = " +
                                            String.format("%.3g", ((byteCountTotal-oldVal)*1000./deltaT) ) + " Hz");
                     }
                     t1 = t2;
                     oldVal = byteCountTotal;
                 }

             }
         }
         catch (Exception e) {
             e.printStackTrace();
             return;
         }
    }


    /**
     * Can use a ByteBufferSupply to act as a mini ring buffer.
     * Take buffer and copy into buffer from the supply.
     *
     * @param item  item containing buffer to be written into by writer1
     */
    private final void eventToOutputRing1(ByteBufferItem item)
            throws EvioException, IOException {

        ByteBuffer destBuf = item.getBuffer();

        // Write evio data from buf into destBuf until desBuf is full
        templateBuffer1.position(0);
        int templateSize = templateBuffer1.remaining();

        if (destBuf.remaining() < templateBuffer1.remaining()) {
            throw new EvioException("specified buffer needs to be bigger (> " +
                                            templateSize + ')');
        }

        try {
            if (writer1 == null) {
                writer1 = new EventWriterUnsync(destBuf);
            }
            else {
                writer1.setBuffer(destBuf);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        while (destBuf.remaining() > templateSize + 64) {
            templateBuffer1.position(0);

            setTimeStamp(templateBuffer1, timestamp);
            timestamp++;

            if (pCounter1++ < 5) {
                Utilities.printBuffer(templateBuffer1, 0, 40, "event 1");
            }

            writer1.writeEvent(templateBuffer1);
        }

        writer1.close();

        bbSupply1.publish(item);
    }

    int pCounter1=0, pCounter2=0;

    /**
     * Can use a ByteBufferSupply to act as a mini ring buffer.
     * Take buffer and copy into buffer from the supply.
     *
     * @param item  item containing buffer to be written into by writer2
     */
    private final void eventToOutputRing2(ByteBufferItem item)
            throws EvioException, IOException {

        ByteBuffer destBuf = item.getBuffer();

        // Write evio data from buf into destBuf until desBuf is full
        templateBuffer2.position(0);
        int templateSize = templateBuffer2.remaining();

        if (destBuf.remaining() < templateBuffer2.remaining()) {
            throw new EvioException("specified buffer needs to be bigger (> " +
                                            templateSize + ')');
        }

        try {
            if (writer2 == null) {
                writer2 = new EventWriterUnsync(destBuf);
            }
            else {
                writer2.setBuffer(destBuf);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        while (destBuf.remaining() > templateSize + 64) {
            templateBuffer2.position(0);

            setTimeStamp(templateBuffer2, timestamp);
            timestamp++;

            if (pCounter2++ < 5) {
                Utilities.printBuffer(templateBuffer2, 0, 40, "event 2");
            }

            writer2.writeEvent(templateBuffer2);
        }

        writer2.close();

        bbSupply2.publish(item);
    }


    /**
     * This class is a separate thread used to write filled data
     * buffers over the emu socket.
     */
    private class SocketSender extends Thread {

        private volatile boolean killThd;
        private ByteBufferSupply bbSupply;
        /** Position relative to the other SocketSender thread,
         *  with 1 = first, 2 = second. */
        private int position;

        public void killThread() {
            killThd = true;
            this.interrupt();
        }

        public SocketSender(ByteBufferSupply bbSupply, int position) {
            this.bbSupply = bbSupply;
            this.position = position;
        }

        /**
         * Send the events currently marshalled into a single buffer.
         * @force if true, force data over socket
         */
        public void run() {
            int i=1;
System.out.println("Starting SocketSender thread, pos = " + position);

            try {

                Socket tcpSocket = new Socket();
                tcpSocket.setTcpNoDelay(noDelay);
                tcpSocket.setSendBufferSize(sendBufferSize);
                tcpSocket.setPerformancePreferences(0,0,1);
                // Connection timeout = 2 sec
                tcpSocket.connect(new InetSocketAddress(blasteeHost, blasteePort), 2000);
                DataOutputStream domainOut = new DataOutputStream(new BufferedOutputStream(tcpSocket.getOutputStream()));

                // Tell EmuBlastee my "name" or id, how many sockets for this one client,
                // and this socket's position (1, 2, ...)
                domainOut.writeInt(id);
                domainOut.writeInt(socketCount);
                domainOut.writeInt(position);

                while (true) {
                    if (killThd) return;

                    // Get a buffer filled by the other thread
                    ByteBufferItem item = bbSupply.consumerGet();  // InterruptedException
                    ByteBuffer buf = item.getBufferAsIs();
                    buf.flip();

                    int binaryLength = buf.remaining();

                    // Type of message is in 1st (lowest) byte.
                    // Source (Emu's EventType) of message is in 2nd byte.
                    //domainOut.writeInt(cMsgConstants.emuEvioFileFormat);
                    //System.out.println("emu client send: cmd int = 0x" + Integer.toHexString(message.getUserInt()));
                    // Total length of binary (not including this int)
                    //domainOut.writeInt(binaryLength);
                    //System.out.println("EmuBlaster: bin len = " + binaryLength);

                    long emuEvioFileFormat = 1L;
                    domainOut.writeLong(emuEvioFileFormat << 32L | (binaryLength & 0xffffffffL));

                    // Write byte array
                    if (binaryLength > 0) {
                        //System.out.println("emu client send: bin len = offset = " + message.getByteArrayOffset());
                        //                    cMsgUtilities.printBuffer(ByteBuffer.wrap(message.getByteArray()),
                        //                                              message.getByteArrayOffset(),
                        //                                              30, "cMsg sending bytes");
                        if (direct) {
                            domainOut.write(ByteDataTransformer.toByteArray(buf));
                        }
                        else {
                            domainOut.write(buf.array(), buf.arrayOffset(), binaryLength);
                        }
                    }

                    domainOut.flush();

                    // Release this buffer so it can be filled again
                    bbSupply.release(item);
                }
            }
            catch (UnsupportedEncodingException e) {
            }
            catch (SocketTimeoutException e) {
                System.out.println("EmuBlaster: connection TIMEOUT");
            }
            catch (InterruptedException e) {
                System.out.println("EmuBlaster: socketSender thread interruped");
            }
            catch (IOException e) {
                System.out.println("Blastee is dead");
                System.exit(-1);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}



