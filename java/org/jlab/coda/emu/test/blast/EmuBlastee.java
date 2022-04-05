package org.jlab.coda.emu.test.blast;

import com.lmax.disruptor.*;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.*;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * Created by timmer on 4/10/17.
 */
public class EmuBlastee {

    /** Give each connecting blaster a unique id number. */
    private AtomicInteger clientNumber = new AtomicInteger(1);

    /** Keep track of how many blasters are currently running. */
    private AtomicInteger clientCount = new AtomicInteger(0);

    /** Setting this to true will kill all threads. */
    private volatile boolean killThreads;

    /** Use direct ByteBuffers if true. */
    private boolean direct;

    /** Parse incoming data into evio events in additional thread if true. */
    private boolean parseData;

    /** Contains all ByteBufferSupply objects being used. */
    private ConcurrentHashMap<Integer, ByteBufferSupply> allSupplies =
            new ConcurrentHashMap<>();

    /** Contains all ClientHandler objects being used. */
    private ConcurrentHashMap<Integer, ClientHandler> clients =
            new ConcurrentHashMap<>();

    /** Thread used to clear ByteBufferSupply (ring). Used when not parsing. */
    private ClientMergerForBuffers mergerForBuffersThread;

    /** Thread used to clear ByteBufferSupply and evio data ring. Used when parsing. */
    private ClientMergerForEvio    mergerForEvioThread;

    // Sockets

    /** Read buffer size in bytes. */
    private int bufferSize = 8192;

    /** TCP socket receive buffer size in bytes. */
    private int receiveBufferSize = 4*8192;

    /** TCP port of Blastee. */
    private int blasteePort = 22333;

    /** Number of blocks to read in one loop. */
    private int blockCount = 4;

    /** TCP NO_DELAY on or off. */
    private boolean noDelay;



    // statistics

    /** Time in seconds between statistics printouts. */
    private int timeInterval = 5;

    /** How many messages were received in the last time interval. */
    private long messageCount = 0L;

    /** How many bytes were received in the last time interval. */
    private volatile long byteCount = 0L;

    /** Thread to calculate and print out statistics. */
    private Statistics statThread;




    /** Method to print out correct program command line usage. */
     private static void usage() {
         System.out.println("\nUsage:\n\n" +
             "   java EmuBlastee\n" +
             "        [-d <buf size>]    reading data buffer size in bytes (8192)\n" +
             "        [-r <buf size>]    TCP receive buffer size in bytes (4*8192)\n" +
             "        [-b <blocks>]      number of buffers to read in one loop (4)\n" +
             "        [-p <port>]        TCP port (22333)\n" +
             "        [-t <seconds>]     time between printouts in sec (5)\n" +
             "        [-parse]           parse data into evio events and place in ring\n" +
             "        [-help]            print this message\n");
     }


    /**
     * Run as a stand-alone application.
     * @param args args
     */
     public static void main(String[] args) {
         try {
             EmuBlastee blastee = new EmuBlastee(args);
             blastee.run();
         }
         catch (Exception e) {
             System.out.println(e.toString());
             System.exit(-1);
         }
     }



    /** Stop all communication with Emu domain clients. */
    public void stopServer() {
    }



    /**
     * Constructor.
     * @param args program args
     */
    public EmuBlastee(String[] args) {
        decodeCommandLine(args);
    }


    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private void decodeCommandLine(String[] args) {
        // Was receiveBufferSize set in command line?
        boolean setReceiveBufSize = false;

        try {
            // loop over all args
            for (int i = 0; i < args.length; i++) {
                if (args[i].equalsIgnoreCase("-help")) {
                    usage();
                    System.exit(-1);
                }
                else if (args[i].equalsIgnoreCase("-d")) {
                    bufferSize = Integer.parseInt(args[i + 1]);
                    if (bufferSize < 1) {
                        System.out.println("Data buffer size must be > 0");
                        System.exit(1);
                    }
                    i++;
                }
                else if (args[i].equalsIgnoreCase("-r")) {
                    receiveBufferSize = Integer.parseInt(args[i + 1]);
                    if (receiveBufferSize < 1) {
                        System.out.println("TCP receive buffer size must be > 0");
                        System.exit(1);
                    }
                    setReceiveBufSize = true;
                    i++;
                }
                else if (args[i].equalsIgnoreCase("-b")) {
                    blockCount = Integer.parseInt(args[i + 1]);
                    if (blockCount < 1) {
                        System.out.println("Number of blocks must be > 0");
                        System.exit(1);
                    }
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
                else if (args[i].equalsIgnoreCase("-parse")) {
                    parseData = true;
                }
                else if (args[i].equalsIgnoreCase("-t")) {
                    timeInterval = Integer.parseInt(args[i + 1]);
                    if (timeInterval < 1) {
                        timeInterval = 1;
                    }
                    i++;
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

        // If the receiveBufferSize is not explicitly set, make it at least 4x the bufferSize.
        if (!setReceiveBufSize) {
            receiveBufferSize = bufferSize <= 8192 ?  4*8192 : 4*bufferSize;
        }

        return;
    }


    /**
     * Method to allow connections from EmuBlasters and read their data.
     */
    public void run() {

        // Start statistics thread
        statThread = new Statistics();
        statThread.start();

        // Self-starting, merging thread
        if (parseData) {
            mergerForEvioThread = new ClientMergerForEvio();
        }
        else {
            mergerForBuffersThread = new ClientMergerForBuffers();
        }

        try {
            // Create server socket at given port
            ServerSocket listeningSocket = new ServerSocket();

            listeningSocket.setReuseAddress(true);
            // set recv buffer size (must be done BEFORE bind)
            listeningSocket.setReceiveBufferSize(receiveBufferSize);
            // prefer high bandwidth instead of low latency or short connection times
            listeningSocket.setPerformancePreferences(0,0,1);
            listeningSocket.bind(new InetSocketAddress(blasteePort));

            while (true) {
                // accept the connection from the client
                Socket socket = listeningSocket.accept();

                // spawn thread to handle client
                new ClientHandler(socket);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    /** Class to handle a socket connection to the client of which there may be many. */
    private class ClientHandler extends Thread {

        /** Socket to client. */
        Socket socket;

        /** Buffered input communication streams for efficiency. */
        BufferedInputStream in;

        /** Supply of ByteBuffers and also a ring. */
        ByteBufferSupply bbSupply;

        /** My id number. */
        int me;

        // All ring buffer stuff
        long nextSequence, availableSequence = -1L;
        Sequence sequence;
        SequenceBarrier barrier;
        RingBuffer<RingItem> ringBuffer;




        /**
         * Constructor.
         * @param socket socket channel to client
         */
        ClientHandler(Socket socket) {
            this.socket = socket;
            this.start();
        }


        /** This method handles all communication with EmuBlaster. */
        public void run() {

            int numRead, cmd, size;
            long cmdAndSize;
            boolean blasteeStop = false;
            me = clientNumber.getAndIncrement();
            DataInputStream in = null;
            BufferedInputStream bufferedIn = null;
            SocketChannel channel = null;
            ParsingThread parsingThread = null;

            // For use with direct buffers
            ByteBuffer wordCmdBuf = ByteBuffer.allocate(8);
            IntBuffer ibuf = wordCmdBuf.asIntBuffer();

System.out.println("Start handling client " + me + ", bufferSize = " + bufferSize);

            try {
                channel = socket.getChannel();
                socket.setPerformancePreferences(0,0,1);

                // Set tcpNoDelay so no packets are delayed
                //socket.setTcpNoDelay(noDelay);

                // Set TCP receive buffer size
                if (receiveBufferSize > 0) {
                    socket.setReceiveBufferSize(receiveBufferSize);
                }

                // Use buffered streams for efficiency
                SocketChannel socketChannel = socket.getChannel();
                bufferedIn = new BufferedInputStream(socket.getInputStream());
                in = new DataInputStream(bufferedIn);
            }
            catch (IOException e) {
                e.printStackTrace();
            }

            // Create a ring buffer full of empty ByteBuffer objects
            // in which to copy incoming data from client.
            // NOTE: Using direct buffers works but performance is poor and fluctuates
            // quite a bit in speed.
            bbSupply = new ByteBufferSupply(16, bufferSize,
                                             ByteOrder.BIG_ENDIAN, direct,
                                            true);

            // Create another ring buffer. The above ByteBuffers get
            // parsed into evio eventa & placed into it.
            if (parseData) {
                ringBuffer = createSingleProducer(new RingItemFactory(), 4096, new YieldingWaitStrategy());
                barrier = ringBuffer.newBarrier();
                sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
                ringBuffer.addGatingSequences(sequence);
                nextSequence = sequence.get() + 1;

                clients.put(me, this);

                parsingThread = new ParsingThread();
            }


            // Add to list so merger has access to bbSupply
            allSupplies.put(me, bbSupply);

            // Start thread to parse buffers being put on ring
            //startInputThread();

            clientCount.getAndIncrement();

            // clear stats since we have a new client now
            statThread.clear();



            while (true) {
                if (blasteeStop) {
                    if (parseData) {
                        parsingThread.stop();
                    }
                    break;
                }

                for (int blocks=0; blocks < blockCount; blocks++) {
//System.out.println("Get BB");
                    ByteBufferItem item = null;

                    try {
                        item = bbSupply.get();
                        ByteBuffer buf = item.getBuffer();
                        byte[] array = item.getBuffer().array();

                        if (direct) {
                            channel.read(wordCmdBuf);
                            cmd  = ibuf.get();
                            size = ibuf.get();
                            ibuf.position(0);
                            wordCmdBuf.position(0);
                            buf.limit(size);

                            // Be sure to read everything
                            while (buf.position() < buf.limit()) {
                                channel.read(buf);
                            }
                            buf.flip();
                            buf.position(0).limit(size);
                        }
                        else {
                            cmdAndSize = in.readLong();
                            cmd  = (int) ((cmdAndSize >>> 32) & 0xffL);
                            size = (int)   cmdAndSize;   // just truncate for lowest 32 bytes
                            buf.limit(size);

                            in.readFully(array, 0, size);
                        }

                    }
                    catch (InterruptedException ex) {
                        blasteeStop = true;
                        break;
                    }
                    catch (IOException ex) {
                        blasteeStop = true;
                        break;
                    }

//System.out.println("Publish BB");
                    bbSupply.publish(item);
                    //bbSupply.release(item);

                    byteCount += size + 8;
                    messageCount++;
                }

            }

            // Done talking to Blaster
            try {
                socket.close();
            }
            catch (IOException e) { }

            System.out.println("Blaster " + me + " quitting");

            // One less running blaster
            allSupplies.remove(me);
            clientCount.getAndDecrement();

            if (parseData) {
                // Kill merging thread or it will block on this (soon to be deleted) client
                mergerForEvioThread.stop();
                // Starting another merging thread
                mergerForEvioThread = new ClientMergerForEvio();
            }
            else {
                mergerForBuffersThread.stop();
                mergerForBuffersThread = new ClientMergerForBuffers();
            }

            // Clear stats since we have one less client now
            statThread.clear();
        }



        /** Class to consume all client output buffers from 1 socket, parse them into
         *  smaller evio buffers, and place those smaller buffers onto a ring. */
        private class ParsingThread extends Thread {

            boolean isER = false;

            ParsingThread() {
                this.start();
            }


            /** This method handles all communication with EmuBlaster. */
            public void run() {
                System.out.println("Parsing Thread started");
                long counter=0L;
                try {
                    while (true) {
//                        if ((255 & counter++) == 0) {
//                            System.out.print("+");
//                        }

                        ByteBufferItem item = bbSupply.consumerGet();
                        parseToRing(item);
                        bbSupply.release(item);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }


            /**
             * Parse the buffer into evio bits that get put on a ring.
             * This method contains alot of unnecessary code but it mimics
             * what gets executed in the real emu.
             *
             * @param item ByteBufferSupply item containing buffer to be parsed.
             * @throws IOException
             * @throws EvioException
             */
            private final void parseToRing(ByteBufferItem item) throws IOException, EvioException {

                RingItem ri;
                EvioNode node;
                boolean hasFirstEvent, isUser=false, isStreaming = false;
                boolean dumpData=false, haveInputEndEvent=false;
                ControlType controlType = null;
                EvioCompactReader compactReader = null;
                int id = 0, sourceId = 0;
                long nextRingItem;

                ByteBuffer buf = item.getBuffer();
//System.out.println("p1, buf lim = " + buf.limit() + ", cap = " + buf.capacity());
//Utilities.printBuffer(buf, 0, 100, "Buf");
                try {
                    if (compactReader == null) {
                        compactReader = new EvioCompactReader(buf, false);
                    }
                    else {
                        compactReader.setBuffer(buf);
                    }
                }
                catch (EvioException e) {
                    System.out.println("EmuBlastee: data NOT evio v4 format");
                    throw e;
                }

                // First block header in buffer
                IBlockHeader blockHeader = compactReader.getFirstBlockHeader();
                if (blockHeader.getVersion() < 4) {
    System.out.println("EmuBlastee: data NOT evio v4 format");
                    throw new EvioException("Data not in evio v4 format");
                }

                hasFirstEvent = blockHeader.hasFirstEvent();

                EventType eventType = EventType.getEventType(blockHeader.getEventType());
                if (eventType == null) {
    System.out.println("EmuBlastee: bad format evio block header");
                    throw new EvioException("bad format evio block header");
                }
                int recordId = blockHeader.getNumber();

                // Each PayloadBuffer contains a reference to the buffer it was
                // parsed from (buf).
                // This cannot be released until the module is done with it.
                // Keep track by counting users (# events parsed from same buffer).
                int eventCount = compactReader.getEventCount();
                item.setUsers(eventCount);
//    System.out.println("EmuBlastee: block header, event type " + eventType +
//                       ", recd id = " + recordId + ", event cnt = " + eventCount);

                for (int i = 1; i < eventCount + 1; i++) {
                    if (isER) {
                        // Don't need to parse all bank headers, just top level.
                        node = compactReader.getEvent(i);
                    }
                    else {
                        node = compactReader.getScannedEvent(i);
                    }

                    // Complication: from the ROC, we'll be receiving USER events
                    // mixed in with and labeled as ROC Raw events. Check for that
                    // and fix it.
                    if (eventType == EventType.ROC_RAW) {
                        if (Evio.isUserEvent(node)) {
                            isUser = true;
                            eventType = EventType.USER;
                            if (hasFirstEvent) {
                                System.out.println("EmuBlastee: FIRST event from ROC RAW");
                            }
                            else {
                                System.out.println("EmuBlastee: USER event from ROC RAW");
                            }
                        }
                    }
                    else if (eventType == EventType.CONTROL) {
                        // Find out exactly what type of control event it is
                        // (May be null if there is an error).
                        controlType = ControlType.getControlType(node.getTag());
System.out.println("EmuBlastee: " + controlType + " event from ROC");
                        if (controlType == null) {
                            System.out.println("EmuBlastee: found unidentified control event");
                            throw new EvioException("Found unidentified control event");
                        }
                    }
                    else if (eventType == EventType.USER) {
                        isUser = true;
                        if (hasFirstEvent) {
                            System.out.println("EmuBlastee: FIRST event");
                        }
                        else {
                            System.out.println("EmuBlastee: USER event");
                        }
                    }

                    if (dumpData) {
                        bbSupply.release(item);
                        continue;
                    }

//System.out.println("rb.next()");
                    nextRingItem = ringBuffer.next();
                    ri = ringBuffer.get(nextRingItem);

                    // Set & reset all parameters of the ringItem
                    if (eventType.isBuildable()) {
                        ri.setAll(null, null, node, eventType, controlType,
                                  isUser, hasFirstEvent, isStreaming, id, recordId, sourceId,
                                  node.getNum(), "Blaster", item, bbSupply);
                    }
                    else {
                        ri.setAll(null, null, node, eventType, controlType,
                                  isUser, hasFirstEvent, isStreaming, id, recordId, sourceId,
                                  1, "Blaster", item, bbSupply);
                    }

                    // Only the first event of first block can be "first event"
                    isUser = hasFirstEvent = false;

//System.out.println("rb.publish()");
                    ringBuffer.publish(nextRingItem);

                    // Handle end event ...
                    if (controlType == ControlType.END) {
                        // There should be no more events coming down the pike so
                        // go ahead write out existing events and then shut this
                        // thread down.
System.out.println("EmuBlastee: found END event");
                        haveInputEndEvent = true;
                        break;
                    }
                }
            }
        }
        

    }


    /** Class to consume all client output buffers and merge them into 1 stream. */
    private class ClientMergerForBuffers extends Thread {

        ClientMergerForBuffers() {
            this.start();
        }

        /** This method takes all filled buffers from all supplies and releases them. */
        public void run() {
            try {
                while (true) {
                    for (ByteBufferSupply supply : allSupplies.values()) {
                        ByteBufferItem item = supply.consumerGet();
                        supply.release(item);
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /** Class to consume all client output evio ring buffer items and merge them into 1 stream. */
    private class ClientMergerForEvio extends Thread {

        ClientMergerForEvio() {
            this.start();
        }

        /** This method takes all filled evio buffers from all supplies and releases them. */
        public void run() {
            long counter=0L, availableSequence;
            try {
                while (true) {
                    for (ClientHandler client : clients.values()) {
                        if (client.availableSequence <= client.nextSequence) {
//System.out.println("merger: waitfor ");
                            client.availableSequence = client.barrier.waitFor(client.nextSequence);
                        }

//System.out.println("merger: get ");
                        RingItem ri = client.ringBuffer.get(client.nextSequence);
                        client.sequence.set(client.nextSequence++);

//                        if ((255 & counter++) == 0) {
//                            System.out.print("-");
//                        }
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /** Class to calculate and print statistics. */
    private class Statistics extends Thread {

        private boolean init;
        private long totalBytes, oldVal, totalT, totalCount;
        long localByteCount, t, deltaT, deltaCount;
        private int skip = 2;

        synchronized private void clear() {
            totalBytes    = 0L;
            totalT        = 0L;
            totalCount    = 0L;
            byteCount     = 0L;
            messageCount  = 0L;
            oldVal        = 0L;
            skip          = 2;
            init          = true;
            t = System.currentTimeMillis();
        }

        public void run() {
            
            int sleepTime = 1000*timeInterval;

            clear();

            while (true) {

                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ex) {}

                // Local copy of volatile int
                localByteCount = byteCount;

                deltaT = System.currentTimeMillis() - t;

                if (localByteCount > 0) {

                    if (!init) {
                         totalBytes += localByteCount;
                    }

                    if (skip-- < 1) {
                        totalT += deltaT;
                        deltaCount = totalBytes - oldVal;
                        totalCount += deltaCount;

                        System.out.printf("%3.2e bytes/s in %d sec, %3.2e avg,  Messages: %d Hz,  %d blasters\n",
                                          (deltaCount*1000./deltaT), timeInterval,
                                          ((totalCount*1000.)/totalT), messageCount*1000L/deltaT, clientCount.get());
                    }
                    else {
                        System.out.printf("%3.2e bytes/s in %d sec,  Messages: %d Hz,  %d blasters\n",
                                          (localByteCount*1000./deltaT), timeInterval,
                                          messageCount*1000L/deltaT, clientCount.get());
                    }

                    // Remove effect of printing rate calculations
                    t = System.currentTimeMillis();

                    init = false;
                    oldVal = totalBytes;
                    byteCount = 0L;
                    messageCount = 0L;
                }
                else {
                    System.out.println("No bytes read in the last " + timeInterval + " seconds.");
                    clear();
                }
            }
        }
    }




}
