/*
 * Copyright (c) 2019, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.test;


import com.lmax.disruptor.*;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.hipo.CompressionType;
import org.jlab.coda.jevio.*;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * This class is designed to find the bottleneck in using
 * real Hall D data in simulated ROC. It's a hacked-up
 * version of RocSimulation and DataChannelImplEmu.
 *
 * @author timmer
 * (Jun 5, 2019)
 */
public class RealDataTest {

    static int serverPort = 49555;
    static int bufSupplySize = 4096;

    private boolean debug;

    /** Amount of real data bytes in hallDdata array. */
    private int arrayBytes;

    /** Put extracted real data in here. */
    private byte[] hallDdata;

    private int eventWordSize;

    private ByteBuffer templateBuffer;
    private int eventBlockSize = 40;
    private int eventSize = 75;
    private int generatedDataWords = eventBlockSize * eventSize;
    private int generatedDataBytes = 4*generatedDataWords;

    private boolean useRealData = true;

    private int hallDdataPosition = 0;

    private int detectorId = 111;

    private ByteBufferSupply bbSupply;

    /** ID number of this module obtained from config file. */
    private int id = 3;

    private ByteOrder outputOrder = ByteOrder.LITTLE_ENDIAN;

    // Value for trigger type from trigger supervisor
    private int triggerType = 15;
    private int myId = 0;

    /** Total number of evio events written to the outputs. */
    private long eventCountTotal;

    /** Sum of the sizes, in 32-bit words, of all evio events written to the outputs. */
    private long wordCountTotal;

    private EventWriterUnsync writer;
    private DataOutputStream out;

    //--------------------------------------------------------------
    // These represent the internal ring of the output channel
    //--------------------------------------------------------------
    private long nextSequence;
    private long availableSequence ;
    private int outputRingItemCount;

    private RingBuffer<RingItem> ringBufferOut;
    private SequenceBarrier sequenceBarrier;
    private Sequence sequence;




    /**
     * Constructor.
     * @param args program args
     */
    private RealDataTest(String[] args) {
        decodeCommandLine(args);
    }


    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private void decodeCommandLine(String[] args) {

        // loop over all args
        for (int i = 0; i < args.length; i++) {

            if (args[i].equalsIgnoreCase("-h")) {
                usage();
                System.exit(-1);
            }
//            else if (args[i].equalsIgnoreCase("-t")) {
//                String threads = args[i + 1];
//                i++;
//                try {
//                    buildThreadCount = Integer.parseInt(threads);
//                    if (buildThreadCount < 2) {
//                        buildThreadCount = 2;
//                    }
//                }
//                catch (NumberFormatException e) {
//                }
//            }
//            else if (args[i].equalsIgnoreCase("-g")) {
//                global = true;
//            }
//            else if (args[i].equalsIgnoreCase("-v")) {
//                volLong = true;
//            }
            else if (args[i].equalsIgnoreCase("-debug")) {
                debug = true;
            }
            else {
                usage();
                System.exit(-1);
            }
        }
    }


    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage:\n\n" +
            "   java BuildThreadTest\n" +
            "        [-debug]         turn on printout\n" +
            "        [-h]             print this help\n");
    }


    /**
     * Run as a stand-alone application.
     * @param args args
     */
    public static void main(String[] args) {
        try {
            RealDataTest receiver = new RealDataTest(args);
            receiver.run();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /** This method is executed as a thread. */
    public void run() {

        getRealData();

        int myId = 0;
        long myEventNumber = 1L + myId*eventBlockSize;
        long timestamp = myId*4*eventBlockSize;

        eventWordSize  = getSingleEventBufferWords(generatedDataWords);
        //System.out.println("eventWordSize = " + eventWordSize);
        System.out.println("using real data = " + useRealData);

        templateBuffer = createSingleEventBuffer(generatedDataWords, myEventNumber, timestamp);

        writeToBuffer();
    }


    public void writeToBuffer() {

        int skip = 5;
        long oldVal=0L, totalT=0L, totalCount=0L, bufCounter=0L;
        long t1, deltaT, t2;
        ByteBuffer buf;
        ByteBufferItem bufItem;
        boolean copyWholeBuf = true;
        long myEventNumber=1, timestamp=0;

        int counter = 0;

        // Now create a buffer supply
        bbSupply = new ByteBufferSupply(bufSupplySize, 4*eventWordSize, outputOrder, false);
        boolean added = false;

        // Simulates the output channel ring
        setupOutputRingBuffer();

        try {
            
            // internal buffer size in writer
            int containerBufSize = 16000000;
//            ByteBuffer containerBufZero = ByteBuffer.allocate(containerBufSize);
            ByteBuffer containerBuf = ByteBuffer.allocate(containerBufSize);
            containerBuf.order(outputOrder);
            writer = new EventWriterUnsync(containerBuf, 0, 0, null,
                    1, CompressionType.RECORD_UNCOMPRESSED);
            t1 = System.currentTimeMillis();

            // Start up receiving thread
            TestServer server = new TestServer();
            server.start();


            // Start up sending thread
            OutChannel outThread = new OutChannel();
            outThread.start();



            Thread.sleep(1000);

            Socket tcpSocket = new Socket();
            tcpSocket.setTcpNoDelay(true);
            //tcpSocket.setSendBufferSize(2000000);
            //tcpSocket.setPerformancePreferences(0,0,1);

            String dotDecimalAddr = InetAddress.getLocalHost().getHostAddress();
            String hostname = InetAddress.getLocalHost().getHostName();

System.out.println("      Emu connect: try making TCP connection to host = " +
        "127.0.0.1" + "; port = " + serverPort);
            // Don't waste too much time if a connection can't be made, timeout = 5 sec
            tcpSocket.connect(new InetSocketAddress("127.0.0.1", serverPort), 5000);
            out = new DataOutputStream(new BufferedOutputStream(tcpSocket.getOutputStream()));

System.out.println("      Emu connect: connection made!");

            while (true) {

                // Add ROC Raw Records as PayloadBuffer objects
                // Get buffer from recirculating supply.
                bufItem = bbSupply.get();
                buf = bufItem.getBuffer();

                // Some logic to allow us to copy everything into buffer
                // only once. After that, just update it.
                if (copyWholeBuf) {
                    // Only need to do this once too
                    buf.order(outputOrder);

                    if (++bufCounter > bufSupplySize) {
                        copyWholeBuf = false;
                    }
                }

                // Make sure buf has latest timestamp and event # placed in it
                writeEventBuffer(buf, templateBuffer, myEventNumber,
                                 timestamp, false, copyWholeBuf,
                                 generatedDataBytes);

                // Pretend is going to output channel
                eventToOutputRing(buf, bufItem, bbSupply);

                eventCountTotal += eventBlockSize;
                wordCountTotal += eventWordSize;

                myEventNumber += eventBlockSize;
                timestamp += 4 * eventBlockSize;


                if (counter++ % 500000 == 0) {
                    t2 = System.currentTimeMillis();
                    deltaT = t2 - t1;

                    if (skip-- < 1) {
                        totalT += deltaT;
                        totalCount += myEventNumber - oldVal;
                        System.out.println("  Roc mod: event rate = " + String.format("%.3g", ((myEventNumber - oldVal) * 1000. / deltaT)) +
                                " Hz,  avg = " + String.format("%.3g", (totalCount * 1000.) / totalT));
                    } else {
                        System.out.println("  Roc mod: event rate = " + String.format("%.3g", ((myEventNumber - oldVal) * 1000. / deltaT)) + " Hz");
                    }
                    t1 = t2;
                    oldVal = myEventNumber;
                }
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }



    private int getSingleEventBufferWords(int generatedDataWords) {
        int dataWordLength = 1 + generatedDataWords;
        return (6 + 4*eventBlockSize + dataWordLength);
    }


    public static int createCodaTag(boolean sync, boolean error, boolean isBigEndian,
                                    boolean singleEventMode, int id) {
        int status = 0;

        if (sync)            status |= 0x1000;
        if (error)           status |= 0x2000;
        if (isBigEndian)     status |= 0x4000;
        if (singleEventMode) status |= 0x8000;

        return ( status | (id & 0x0fff) );
    }


    /** Setup the output channel ring buffer. */
    void setupOutputRingBuffer() {

        nextSequence = 0L;
        availableSequence = -1L;
        outputRingItemCount = bufSupplySize;

        ringBufferOut =
                createSingleProducer(new RingItemFactory(),
                                     outputRingItemCount,
                                     new SpinCountBackoffWaitStrategy(30000, new LiteBlockingWaitStrategy()));
        //new LiteBlockingWaitStrategy());
        //new YieldingWaitStrategy());

        // One barrier for the ring
        sequenceBarrier = ringBufferOut.newBarrier();
        sequenceBarrier.clearAlert();

        // One sequence for the ring
        sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        ringBufferOut.addGatingSequences(sequence);
        nextSequence = sequence.get() + 1L;
    }


    /**
     * This method places many ROC Raw events with simulated data
     * onto the ring buffer of an output channel.
     *
     * @param buf     the event to place on output channel ring buffer
     * @param item    item corresponding to the buffer allowing buffer to be reused
     * @param bbSupply   ByteBuffer supplier.
     */
    void eventToOutputRing(ByteBuffer buf, ByteBufferItem item, ByteBufferSupply bbSupply) {

        long nextRingItem = ringBufferOut.next();
        RingItem ri = (RingItem) ringBufferOut.get(nextRingItem);
        ri.setBuffer(buf);
        ri.setEventType(EventType.ROC_RAW);
        ri.setControlType(null);
        ri.setSourceName(null);
        ri.setReusableByteBuffer(bbSupply, item);
        ringBufferOut.publish(nextRingItem);
    }


    class OutChannel extends Thread {

        public void run() {

            while (true) {

                RingItem item;

                try  {
                    // Only wait if necessary ...
                    if (availableSequence < nextSequence) {
                        availableSequence = sequenceBarrier.waitFor(nextSequence);
                    }

                    item = ringBufferOut.get(nextSequence);
                    // Get buffer with entangled events in it
                    ByteBuffer buf = item.getBuffer();

                    // Add event to writer's internal buffer
                    boolean added = writer.writeEvent(buf, false);

                    if (!added) {
                        // Event wasn't added since writer's internal buf is full, so reset
                        writer.close();

                        // Type of message over socket is in 1st int, 0 in this test.
                        // Total length of data (not including this int) is in 2nd int
                        out.writeLong((long) (writer.getBytesWrittenToBuffer()));

                        // Write data
                        ByteBuffer containerBuf = writer.getByteBuffer();
                        out.write(containerBuf.array(), 0, writer.getBytesWrittenToBuffer());

                        // Start over
                        containerBuf.clear();
                        writer.setBuffer(containerBuf);
                    }

                    bbSupply.release(item.getByteBufferItem());

                    sequence.set(nextSequence++);
                }
                catch (final Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }



    /**
     * Method to get real data from an existing file of data previously extracted from
     * a Hall D data file.
     * There are 9 files, each containing 16 MB of unique real Hall D data.
     *
     * @return true if hall D data found and available, else false.
     */
    private boolean getRealData() {

        // Try to get the last digit at end of name if any
        int num = 1;

        // First check to see if we already have some data in a file
        String filename = System.getenv("CODA");
        if (filename != null) {
            filename += "/common/bin/hallDdata" + num + ".bin";
        }
        else {
            filename = "/Users/timmer/coda/coda3/common/bin/hallDdata" + num + ".bin";
        }

        try {
            RandomAccessFile file = new RandomAccessFile(filename, "rw");
            arrayBytes = (int) file.length();
            hallDdata = new byte[arrayBytes];
            file.read(hallDdata, 0, arrayBytes);
            file.close();
        }
        catch (Exception e) {
            // No file to be read, so try creating our own
            System.out.println("getRealData: cannot open data file " + filename);
            return false;
        }

        System.out.println("getRealData: successfully read in file " + filename + ", size = " + arrayBytes);
        return true;
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
        int rocTag = createCodaTag(false, false, true, false, id);

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


        int dataTag = createCodaTag(false, false, true, false, detectorId);
        secondWord = dataTag << 16 |
                (DataType.UINT32.getValue() << 8) |
                (eventBlockSize & 0xff);

        buf.putInt(writeIndex, dataLen + 1); writeIndex += 4;
        buf.putInt(writeIndex, secondWord); writeIndex += 4;


        // First put in starting event # (32 bits)
        buf.putInt(writeIndex, (int)eventNumber); writeIndex += 4;
        if (useRealData) {
            // buf has a backing array, so fill it with data the quick way
            int bytes = 4*generatedDataWords;
            if (bytes > arrayBytes) {
                System.out.println("  Roc mod: NEED TO GENERATE MORE REAL DATA, have " + arrayBytes +
                        " but need " + bytes);
            }
            System.arraycopy(hallDdata, hallDdataPosition, buf.array(), writeIndex, bytes);
            hallDdataPosition += bytes;
        }
        else {
            for (int i = 0; i < generatedDataWords; i++) {
                // Write a bunch of 1s
                buf.putInt(writeIndex, 1);
                writeIndex += 4;
            }
        }

        // buf is ready to read
        return buf;
    }



    void writeEventBuffer(ByteBuffer buf, ByteBuffer templateBuf,
                          long eventNumber, long timestamp,
                          boolean syncBit, boolean copy,
                          int generatedDataBytes) {

        // Since we're using recirculating buffers, we do NOT need to copy everything
        // into the buffer each time. Once each of the buffers in the BufferSupply object
        // have been copied into, we only need to change the few places that need updating
        // with event number and timestamp!
        if (copy) {
            // This will be the case if buf is direct
            if (!buf.hasArray()) {
                templateBuf.position(0);
                buf.put(templateBuf);
            }
            else {
                System.arraycopy(templateBuf.array(), 0, buf.array(), 0, templateBuf.limit());
            }
        }

        // Get buf ready to read for output channel
        buf.position(0).limit(templateBuf.limit());

        // Set sync bit in event bank header
        // sync, error, isBigEndian, singleEventMode
        int rocTag = createCodaTag(syncBit, false, true, false, id);

        // 2nd bank header word = tag << 16 | ((padding & 0x3) << 14) | ((type & 0x3f) << 8) | num
        int secondWord = rocTag << 16 |
                (DataType.BANK.getValue() << 8) |
                (eventBlockSize & 0xff);

        buf.putInt(4, secondWord);

        // Skip over 2 bank headers
        int writeIndex = 16;

        // Write event number and timestamp into trigger bank segments
        for (int i = 0; i < eventBlockSize; i++) {
            // Skip segment header
            writeIndex += 4;

            // Generate 3 integers per event (no miscellaneous data)
            buf.putInt(writeIndex, (int) (eventNumber + i)); writeIndex += 4;
            buf.putInt(writeIndex, (int)  timestamp); writeIndex += 4;// low 32 bits
            buf.putInt(writeIndex, (int) (timestamp >>> 32 & 0xFFFF)); writeIndex += 4;// high 16 of 48 bits
            timestamp += 4;
        }

        // Move past data bank header
        writeIndex += 8;

        // Write event number into data bank
        buf.putInt(writeIndex, (int) eventNumber);

        // For testing compression, need to have real data that changes,
        // endianness does not matter.
        // Only copy data into each of the "bufSupplySize" number of events once.
        // Doing this for each event produced every time slows things down too much.
        // Each event has eventBlockSize * eventSize (40*75 = 3000) data bytes.
        // 3k bytes * 4096 events = 12.3MB. This works out nicely since we have
        // retrieved 16MB from a single Hall D data file.
        // However, each Roc has the same data which will lend itself to more compression.
        // So the best thing is for each ROC to have different data.
        //if (false) {
        if (copy && useRealData) {
            // Move to data input position
            writeIndex += 4;

            // Have we run out of data? If so, start over from beginning ...
            if (arrayBytes - hallDdataPosition < generatedDataBytes) {
                hallDdataPosition = 0;
            }

            if (buf.hasArray()) {
                System.arraycopy(hallDdata, hallDdataPosition, buf.array(), writeIndex, generatedDataBytes);
            }
            else {
                buf.position(writeIndex);
                buf.put(hallDdata, hallDdataPosition, generatedDataBytes);
                // Get buf ready to read for output channel
                buf.position(0).limit(templateBuf.limit());

            }

            //System.out.print(".");

            hallDdataPosition += generatedDataBytes;
        }
    }





}


class TestServer extends Thread {

    static int serverPort = 49555;
    static int receiveBufferSize = 1000000;

    public void run()  {

        try {
            // Create server socket at given port
            ServerSocket listeningSocket = new ServerSocket();

            try {
                listeningSocket.setReuseAddress(true);
                // set recv buffer size (must be done BEFORE bind)
                listeningSocket.setReceiveBufferSize(receiveBufferSize);
                // prefer high bandwidth instead of flow latency or short connection times
                listeningSocket.setPerformancePreferences(0, 0, 1);
                listeningSocket.bind(new InetSocketAddress(serverPort));
            }
            catch (IOException ex) {
            }

            while (true) {
                // accept the connection from the client
                Socket socket = listeningSocket.accept();
                socket.setPerformancePreferences(0, 0, 1);

                // Set tcpNoDelay so no packets are delayed
                socket.setTcpNoDelay(true);

                // spawn thread to handle client
                new ClientHandler(socket);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}


/** Class to handle a socket connection to the client. */
class ClientHandler extends Thread {

    /** Socket to client. */
    Socket socket;

    /** Buffered input communication streams for efficiency. */
    BufferedInputStream in;

    /**
     * Constructor.
     * @param socket socket to client
     */
    ClientHandler(Socket socket) {
        this.socket = socket;
        this.start();
    }


    /** This method handles all communication with sender. */
    public void run() {

        System.out.println("Start handling client");

        int bufSize = 16000000;
        long byteCount=0L, messageCount=0L;
        boolean blasteeStop = false;
        //ByteBuffer inputBuf = ByteBuffer.allocate(bufSize);

        EvioNode node;

        ControlType controlType = null;
        EvioNodePool pool = new EvioNodePool(1200);
        EvioCompactReader reader = null;

        long word;
        int cmd, size;

        // Buffered communication streams for efficiency
        DataInputStream in = null;
        try {
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        }
        catch (IOException ex) {
            System.out.println("Sender just quit");
            return;
        }

        // the nodes of each event.
        int numBufs = 32;
        EvioNodePool[] nodePools = new EvioNodePool[numBufs];
        for (int i = 0; i < numBufs; i++) {
            nodePools[i] = new EvioNodePool(3500);
        }

        // Supply of buffer to read data into
        boolean direct = false;
        boolean sequentialRelease = true;
        ByteBufferSupply bbInSupply = new ByteBufferSupply(numBufs, 32,
                                                 ByteOrder.BIG_ENDIAN, direct,
                                                 sequentialRelease, nodePools);

        try {

            while (true) {
                if (blasteeStop) {
                    break;
                }

                // Get buffer from supply in which to read data
                ByteBufferItem item = bbInSupply.get();

                word = in.readLong();
                cmd  = (int) ((word >>> 32) & 0xffL);
                size = (int)   word;   // just truncate for lowest 32 bytes

//                System.out.println(" cmd = " + cmd);
//                System.out.println(" size = " + size);

                item.ensureCapacity(size);
                ByteBuffer buf = item.getBuffer();
                buf.limit(size);

                in.readFully(buf.array(), 0, size);
                //byteCount += size;
                //messageCount++;

                pool.reset();
                if (reader == null) {
                    reader = new EvioCompactReader(buf, pool, false);
                }
                else {
                    reader.setBuffer(buf, pool);
                }

                // If buf contained compressed data
                if (reader.isCompressed()) {
                    // Data may have been uncompressed into a different, larger buffer.
                    // If so, ditch the original and use the new one.
                    ByteBuffer biggerBuf = reader.getByteBuffer();
                    if (biggerBuf != buf) {
                        item.setBuffer(biggerBuf);
                    }
                }


                int eventCount = reader.getEventCount();
                IBlockHeader blockHeader = reader.getFirstBlockHeader();
                EventType eventType = EventType.getEventType(blockHeader.getEventType());

                for (int i = 1; i < eventCount + 1; i++) {

                    //node = reader.getEvent(i);
                    // getScannedEvent will clear child and allNodes lists
                    node = reader.getScannedEvent(i, pool);

                    // Complication: from the ROC, we'll be receiving USER events
                    // mixed in with and labeled as ROC Raw events. Check for that
                    // and fix it.
                    if (eventType == EventType.ROC_RAW) {
                        if (Evio.isUserEvent(node)) {
                            eventType = EventType.USER;
                            System.out.println("USER event from ROC RAW");
                        } else {
                            // Pick this raw data event apart a little
                            if (!node.getDataTypeObj().isBank()) {
                                DataType eventDataType = node.getDataTypeObj();
                                System.out.println("ROC raw record contains " + eventDataType +
                                        " instead of banks (data corruption?)");
                            }
                        }
                    } else if (eventType == EventType.CONTROL) {
                        // Find out exactly what type of control event it is
                        // (May be null if there is an error).
                        controlType = ControlType.getControlType(node.getTag());
                        System.out.println("got " + controlType + " event");
                    } else if (eventType == EventType.USER) {
                        System.out.println("USER event");
                    } else {
                        // Physics or partial physics event must have BANK as data type
                        if (!node.getDataTypeObj().isBank()) {
                            DataType eventDataType = node.getDataTypeObj();
                            System.out.println("physics record contains " + eventDataType +
                                    " instead of banks (data corruption?)");
                        }
                    }
                }

                bbInSupply.release(item);
            }

            // done talking to sender
            socket.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Sender just quit");
    }
}

