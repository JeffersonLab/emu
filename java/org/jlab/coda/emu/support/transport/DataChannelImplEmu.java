/*
 * Copyright (c) 2014, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.transport;


import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.*;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

/**
 * This class implement a data channel which
 * gets-data-from/sends-data-to an Emu domain client/server.
 *
 * @author timmer
 * (4/23/2014)
 */
public class DataChannelImplEmu extends DataChannelAdapter {

    /** Data transport subclass object for Emu. */
    private DataTransportImplEmu dataTransportImplEmu;

    /** Do we pause the dataThread? */
    private volatile boolean pause;

    /** Read END event from input ring. */
    private volatile boolean haveInputEndEvent;

    /** Got END command from Run Control. */
    private volatile boolean gotEndCmd;

    /** Got RESET command from Run Control. */
    private volatile boolean gotResetCmd;

    /**
     * If true, dump incoming data immediately so it does not get put on
     * the ring but does get parsed. Use this for testing incoming data rate.
     */
    private boolean dumpData;

    // OUTPUT

    /** Thread used to output data. */
    private DataOutputHelper dataOutputThread;

    /** UDP port of emu domain server. */
    private int sendPort;

    /** TCP send buffer size in bytes. */
    private int tcpSendBuf;

    /** TCP no delay setting. */
    private boolean noDelay;

    /** Time in seconds to wait for connection to emu server. */
    private int connectTimeout;

    /** Subnet client uses to connect to server if possible. */
    private String preferredSubnet;

    /** Coda id of the data source. */
    private int sourceId;

    /** Connection to emu domain server. */
    private cMsg emuDomain;

    /** cMsg message into which out going data is placed in order to be written. */
    private cMsgMessage outGoingMsg = new cMsgMessage();

    // INPUT

    /**
     * Store locally whether this channel's module is an ER or not.
     * If so, don't parse incoming data so deeply - only top bank header.
     */
    private boolean isER;

    /** Thread used to input data. */
    private DataInputHelper dataInputThread;

    /** Data input stream from TCP socket. */
    private DataInputStream in;

    /** TCP receive buffer size in bytes. */
    private int tcpRecvBuf;

    // INPUT & OUTPUT

    /**
     * Biggest chunk of data sent by data producer.
     * Allows good initial value of ByteBuffer size.
     */
    private int maxBufferSize;

    /** Use the evio block header's block number as a record id. */
    private int recordId;

    /** Use direct ByteBuffer? */
    private boolean direct;

    //-------------------------------------------
    // Disruptor (RingBuffer)  Stuff
    //-------------------------------------------
    private long nextRingItem;

    /** Ring buffer holding ByteBuffers when using EvioCompactEvent reader for incoming events. */
    protected ByteBufferSupply bbSupply;


    /**
     * Constructor to create a new DataChannelImplEt instance. Used only by
     * {@link DataTransportImplEt#createChannel(String, Map, boolean, Emu, EmuModule, int)}
     * which is only used during PRESTART in {@link Emu}.
     *
     * @param name         the name of this channel
     * @param transport    the DataTransport object that this channel belongs to
     * @param attributeMap the hashmap of config file attributes for this channel
     * @param input        true if this is an input data channel, otherwise false
     * @param emu          emu this channel belongs to
     * @param module       module this channel belongs to
     * @param outputIndex  order in which module's events will be sent to this
     *                     output channel (0 for first output channel, 1 for next, etc.).
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplEmu(String name, DataTransportImplEmu transport,
                       Map<String, String> attributeMap, boolean input, Emu emu,
                       EmuModule module, int outputIndex)
            throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module, outputIndex);

        dataTransportImplEmu = transport;

        if (input) {
            logger.info("      DataChannel Emu: creating input channel " + name);
        }
        else {
            logger.info("      DataChannel Emu: creating output channel " + name);
        }

        // use direct ByteBuffers or not
        direct = false;
        String attribString = attributeMap.get("direct");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("true") ||
                    attribString.equalsIgnoreCase("on") ||
                    attribString.equalsIgnoreCase("yes")) {
                direct = true;
            }
        }

        // if INPUT channel
        if (input) {
            isER = (emu.getCodaClass() == CODAClass.ER);
            // size of TCP receive buffer (0 means use operating system default)
            tcpRecvBuf = 0;
            attribString = attributeMap.get("recvBuf");
            if (attribString != null) {
                try {
                    tcpRecvBuf = Integer.parseInt(attribString);
                    if (tcpRecvBuf < 0) {
                        tcpRecvBuf = 0;
                    }
                    logger.info("      DataChannel Emu: set recvBuf to " + tcpRecvBuf);
                }
                catch (NumberFormatException e) {}
            }

            // set "data dump" option on
            attribString = attributeMap.get("dump");
            if (attribString != null) {
                if (attribString.equalsIgnoreCase("true") ||
                    attribString.equalsIgnoreCase("on") ||
                    attribString.equalsIgnoreCase("yes")) {
                    dumpData = true;
                }
            }

        }
        // if OUTPUT channel
        else {
            // set TCP_NODELAY option on
            noDelay = false;
            attribString = attributeMap.get("noDelay");
            if (attribString != null) {
                if (attribString.equalsIgnoreCase("true") ||
                    attribString.equalsIgnoreCase("on") ||
                    attribString.equalsIgnoreCase("yes")) {
                    noDelay = true;
                }
            }

            // size of TCP send buffer (0 means use operating system default)
            tcpSendBuf = 0;
            attribString = attributeMap.get("sendBuf");
            if (attribString != null) {
                try {
                    tcpSendBuf = Integer.parseInt(attribString);
                    if (tcpSendBuf < 0) {
                        tcpSendBuf = 0;
                    }
                    logger.info("      DataChannel Emu: set sendBuf to " + tcpSendBuf);
                }
                catch (NumberFormatException e) {}
            }

            // Send port
            sendPort = cMsgNetworkConstants.emuTcpPort;
            attribString = attributeMap.get("port");
            if (attribString != null) {
                try {
                    sendPort = Integer.parseInt(attribString);
                    if (sendPort < 1024 || sendPort > 65535) {
                        sendPort = cMsgNetworkConstants.emuTcpPort;
                    }
                }
                catch (NumberFormatException e) {}
            }
            logger.info("      DataChannel Emu: sending on port " + sendPort);


            // Size of max buffer
            maxBufferSize = 4200000;
            attribString = attributeMap.get("maxBuf");
            if (attribString != null) {
                try {
                    maxBufferSize = Integer.parseInt(attribString);
                    if (maxBufferSize < 0) {
                        maxBufferSize = 4200000;
                    }
                }
                catch (NumberFormatException e) {}
            }
            logger.info("      DataChannel Emu: max buf size = " + maxBufferSize);

            // Emu domain connection timeout in sec
            connectTimeout = -1;
            attribString = attributeMap.get("timeout");
            if (attribString != null) {
                try {
                    connectTimeout = Integer.parseInt(attribString);
                    if (connectTimeout < 0) {
                        connectTimeout = 3;
                    }
                }
                catch (NumberFormatException e) {}
            }
            logger.info("      DataChannel Emu: timeout = " + connectTimeout);

            // Emu domain preferred subnet in dot-decimal format
            preferredSubnet = null;
            attribString = attributeMap.get("subnet");
            if (attribString != null && cMsgUtilities.isDottedDecimal(attribString) == null) {
                preferredSubnet = null;
            }
            logger.info("      DataChannel Emu: over subnet " + preferredSubnet);
        }

        // State after prestart transition -
        // during which this constructor is called
        channelState = CODAState.PAUSED;
    }


    /**
     * Once a client connects to the Emu domain server in the Emu transport object,
     * that socket is passed to this method and a thread is spawned to handle all
     * communications over it. Only used for input channel.
     *
     * @param channel       data input socket/channel
     * @param sourceId      CODA id # of data source
     * @param maxBufferSize biggest chunk of data expected to be sent by data producer
     * @throws IOException  if exception dealing with socket or input stream
     */
    void attachToInput(SocketChannel channel, int sourceId, int maxBufferSize) throws IOException {

        this.sourceId = sourceId;
        this.maxBufferSize = maxBufferSize;

        // Set socket options
        Socket socket = channel.socket();

        // Set TCP receive buffer size
        if (tcpRecvBuf > 0) {
            socket.setReceiveBufferSize(tcpRecvBuf);
        }

        // Use buffered streams for efficiency
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 256000));

        // Create a ring buffer full of empty ByteBuffer objects
        // in which to copy incoming data from client.
        // NOTE: Using direct buffers works but performance is poor and fluctuates
        // quite a bit in speed.
        bbSupply = new ByteBufferSupply(16, maxBufferSize, ByteOrder.BIG_ENDIAN, direct, true);

        System.out.println("      DataChannel Emu in: connection made from " + name);

        // Start thread to handle all socket input
        startInputThread();
    }


    private void openOutputChannel() throws cMsgException {

        // UDL ->  emu://port/expid/destCompName?codaId=id&timeout=sec&bufSize=size&tcpSend=size&noDelay

        // "name" is name of this channel which also happens to be the
        // destination CODA component we want to connect to.
        String udl = "emu://" + sendPort + "/" + emu.getExpid() +
                "/" + name + "?codaId=" + getID();

        if (maxBufferSize > 0) {
            udl += "&bufSize=" + maxBufferSize;
        }
        else {
            udl += "&bufSize=4200000";
        }

        if (connectTimeout > -1) {
            udl += "&timeout=" + connectTimeout;
        }

        if (tcpSendBuf > 0) {
            udl += "&tcpSend=" + tcpSendBuf;
        }

        if (preferredSubnet != null) {
            udl += "&subnet=" + preferredSubnet;
        }

        if (noDelay) {
            udl += "&noDelay";
        }

        emuDomain = new cMsg(udl, name, "emu domain client");
        emuDomain.connect();

        System.out.println("      DataChannel Emu: connected to server w/ UDL = " + udl);

        startOutputThread();
    }


    private void closeOutputChannel() throws cMsgException {
        if (input) return;
        emuDomain.disconnect();
    }


    /** {@inheritDoc} */
    public TransportType getTransportType() {
        return TransportType.EMU;
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        super.prestart();

        if (input) return;
        try {
            openOutputChannel();
        }
        catch (cMsgException e) {
            channelState = CODAState.ERROR;
            emu.setErrorState("      DataChannel Emu out: " + e.getMessage());
            e.printStackTrace();
            throw new CmdExecException(e);
        }

        channelState = CODAState.PAUSED;
    }

    /** {@inheritDoc} */
    public void go() {
        pause = false;
        channelState = CODAState.ACTIVE;
    }

    /** {@inheritDoc} */
    public void pause() {
        pause = true;
        channelState = CODAState.PAUSED;
    }

    /** {@inheritDoc}. Formerly this code was the close() method. */
    public void end() {

        gotEndCmd = true;
        gotResetCmd = false;

        // How long do we wait for each input or output thread
        // to end before we just terminate them?
        long waitTime;

        try {
            waitTime = emu.getEndingTimeLimit();

            if (dataInputThread != null) {
                dataInputThread.join(waitTime);
                dataInputThread.interrupt();
                try {
                    dataInputThread.join(250);
                    if (dataInputThread.isAlive()) {
                        // kill it since we waited as long as possible
                        dataInputThread.stop();
                    }
                }
                catch (InterruptedException e) {}
                dataInputThread = null;

                try {in.close();}
                catch (IOException e) {}
            }

            if (dataOutputThread != null) {
                dataOutputThread.join(waitTime);
                dataOutputThread.interrupt();
                try {
                    dataOutputThread.join(250);
                    if (dataOutputThread.isAlive()) {
                        dataOutputThread.stop();
                    }
                }
                catch (InterruptedException e) {}
                dataOutputThread = null;
            }
        }
        catch (InterruptedException e) {}

        channelState = CODAState.DOWNLOADED;

        try {
            closeOutputChannel();
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }
    }


    /**
     * {@inheritDoc}
     * Reset this channel by interrupting the data sending threads and closing ET system.
     */
    public void reset() {
        gotEndCmd = false;
        gotResetCmd = true;

        if (dataInputThread != null) {
            dataInputThread.interrupt();
            try {
                dataInputThread.join(250);
                if (dataInputThread.isAlive()) {
                    dataInputThread.stop();
                }
            }
            catch (InterruptedException e) {}

            // Close input channel, this should allow it to check value of "gotResetCmd"
            try {in.close();}
            catch (IOException e) {}
        }

        if (dataOutputThread != null) {
            dataOutputThread.interrupt();
            try {
                dataOutputThread.join(250);
                if (dataOutputThread.isAlive()) {
                    dataOutputThread.stop();
                }
            }
            catch (InterruptedException e) {}
        }

        channelState = CODAState.CONFIGURED;
    }


    /**
     * For input channel, start the DataInputHelper thread which takes Evio
     * file-format data, parses it, puts the parsed Evio banks into the ring buffer.
     */
    private final void startInputThread() {
        dataInputThread = new DataInputHelper();
        dataInputThread.start();
        dataInputThread.waitUntilStarted();
    }


    private final void startOutputThread() {
        dataOutputThread = new DataOutputHelper();
        dataOutputThread.start();
        dataOutputThread.waitUntilStarted();
    }


    /**
     * If this is an output channel, it may be blocked on reading from a module
     * because the END event arrived on an unexpected ring
     * (possible if module has more than one event-producing thread
     * AND there is more than one output channel),
     * this method interrupts and allows this channel to read the
     * END event from the proper ring.
     *
     * @param eventIndex index of last buildable event before END event.
     * @param ringIndex  ring to read END event on.
     */
    public void processEnd(long eventIndex, int ringIndex) {

//        super.processEnd(eventIndex, ringIndex);

        eventIndexEnd = eventIndex;
        ringIndexEnd  = ringIndex;

        if (input || !dataOutputThread.isAlive()) {
//logger.debug("      DataChannel Emu out " + outputIndex + ": processEnd(), thread already done");
            return;
        }

        // Don't wait more than 1/2 second
        int loopCount = 20;
        while (dataOutputThread.threadState != ThreadState.DONE && (loopCount-- > 0)) {
            try {
                Thread.sleep(25);
            }
            catch (InterruptedException e) {
                break;
            }
        }

        if (dataOutputThread.threadState == ThreadState.DONE) {
//logger.debug("      DataChannel Emu out " + outputIndex + ": processEnd(), thread done after waiting");
            return;
        }

        // Probably stuck trying to get item from ring buffer,
        // so interrupt it and get it to read the END event from
        // the correct ring.
//logger.debug("      DataChannel Emu out " + outputIndex + ": processEnd(), interrupt thread in state " +
//                     dataOutputThread.threadState);
        dataOutputThread.interrupt();
    }


    /**
     * Class used to get data over network events, parse them into Evio banks,
     * and put them onto a ring buffer.
     */
    private class DataInputHelper extends Thread {

        /** Variable to print messages when paused. */
        private int pauseCounter = 0;

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch latch = new CountDownLatch(1);

        /** Read into ByteBuffers. */
        private EvioCompactReaderUnsync compactReader;


        /** Constructor. */
        DataInputHelper() {
            super(emu.getThreadGroup(), name() + "_data_in");
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {}
        }


        /** {@inheritDoc} */
        @Override
        public void run() {

            // Tell the world I've started
            latch.countDown();

            try {
                long word;
                int cmd, size;
                boolean delay = false;

                while (true) {
                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        return;
                    }

                    if (delay) {
                        Thread.sleep(5);
                        delay = false;
                    }

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
                            logger.warn("      DataChannel Emu in helper: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // First read the command & size with one read, into a long.
                    // These 2, 32-bit ints are sent in network byte order, cmd first.
                    // Reading a long assumes big endian so cmd, which is sent
                    // first, should appear in most significant bytes.
                    word = in.readLong();
                    cmd = (int) ((word >>> 32) & 0xffL);
                    size = (int) word;   // just truncate for lowest 32 bytes

                    // 1st byte has command
                    switch (cmd) {
                        case cMsgConstants.emuEvioFileFormat:
                            handleEvioFileToBuf(size);

                            break;

                        case cMsgConstants.emuEnd:
                            break;

                        default:
                            System.out.println("      DataChannel Emu in: unknown command from Emu client = " + cmd);
                    }

                    if (haveInputEndEvent) {
                        break;
                    }
                }

            }
            catch (InterruptedException e) {
                logger.warn("      DataChannel Emu in: " + name + "  interrupted thd, exiting");
            }
            catch (Exception e) {
                channelState = CODAState.ERROR;
                // If error msg already set, this will not
                // set it again. It will send it to rc.
                emu.setErrorState("DataChannel Emu in: " + e.getMessage());
                logger.warn("      DataChannel Emu in: " + name + " error: " + e.getMessage());
            }
//System.out.println("      DataChannel Emu in: " + name + " end thread");
        }


        private final void handleEvioFileToBuf(int evioBytes) throws IOException, EvioException {

            RingItem ri;
            EvioNode node;
            EventType bankType;
            boolean hasFirstEvent, isUser=false;
            ControlType controlType = null;

            // Get a reusable ByteBuffer, this does an item.buffer.clear()
            ByteBufferItem bbItem = bbSupply.get();

            // If buffer is too small, make a bigger one
            bbItem.ensureCapacity(evioBytes);
            ByteBuffer buf = bbItem.getBuffer();
            buf.limit(evioBytes);

            in.readFully(buf.array(), 0, evioBytes);

            try {
                if (compactReader == null) {
                    compactReader = new EvioCompactReaderUnsync(buf);
                }
                else {
                    compactReader.setBuffer(buf);
                }
            }
            catch (EvioException e) {
                e.printStackTrace();
                errorMsg.compareAndSet(null, "DataChannel Emu in: data NOT evio v4 format");
                throw e;
            }

            // First block header in buffer
            BlockHeaderV4 blockHeader = compactReader.getFirstBlockHeader();
            if (blockHeader.getVersion() < 4) {
                errorMsg.compareAndSet(null, "DataChannel Emu in: data NOT evio v4 format");
                throw new EvioException("Data not in evio v4 format");
            }

            hasFirstEvent = blockHeader.hasFirstEvent();
            EventType eventType = EventType.getEventType(blockHeader.getEventType());
            int recordId = blockHeader.getNumber();

            // Each PayloadBuffer contains a reference to the buffer it was
            // parsed from (buf).
            // This cannot be released until the module is done with it.
            // Keep track by counting users (# events parsed from same buffer).
            int eventCount = compactReader.getEventCount();
            bbItem.setUsers(eventCount);

//logger.info("      DataChannel Emu in: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId + ", event cnt = " + eventCount);

            for (int i = 1; i < eventCount + 1; i++) {
                //System.out.println(name + "->" + i);
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
                bankType = eventType;
                if (eventType == EventType.ROC_RAW) {
                    if (Evio.isUserEvent(node)) {
                        isUser = true;
                        bankType = EventType.USER;
                        if (hasFirstEvent) {
                            logger.info("      DataChannel Emu in: " + name + " FIRST event from ROC RAW");
                        }
                        else {
                            logger.info("      DataChannel Emu in: " + name + " USER event from ROC RAW");
                        }
                    }
//                    else {
//                        logger.info("      DataChannel Emu in: " + name + " ROC RAW event");
//                    }
                }
                else if (eventType == EventType.CONTROL) {
                    // Find out exactly what type of control event it is
                    // (May be null if there is an error).
                    controlType = ControlType.getControlType(node.getTag());
logger.info("      DataChannel Emu in: " + name + " " + controlType + " event from ROC");
                    if (controlType == null) {
                        errorMsg.compareAndSet(null, "DataChannel Emu in: found unidentified control event");
                        throw new EvioException("Found unidentified control event");
                    }
                }
                else if (eventType == EventType.USER) {
                    isUser = true;
                    if (hasFirstEvent) {
//                        logger.info("      DataChannel Emu in: " + name + " FIRST event");
                    }
                    else {
                        logger.info("      DataChannel Emu in: " + name + " USER event");
                    }
                }

                if (dumpData) {
                    bbSupply.release(bbItem);

                    // Handle end event ...
                    if (controlType == ControlType.END) {
                        // There should be no more events coming down the pike so
                        // go ahead write out existing events and then shut this
                        // thread down.
                        logger.info("      DataChannel Emu in: " + name + " found END event");
                        haveInputEndEvent = true;
                        // run callback saying we got end event
                        if (endCallback != null) endCallback.endWait();
                        break;
                    }

                    continue;
                }

                nextRingItem = ringBufferIn.next();
                ri = ringBufferIn.get(nextRingItem);

                // Set & reset all parameters of the ringItem
                if (bankType.isBuildable()) {
                    ri.setAll(null, null, node, bankType, controlType,
                              isUser, hasFirstEvent, id, recordId, sourceId,
                              node.getNum(), name, bbItem, bbSupply);
                }
                else {
                    ri.setAll(null, null, node, bankType, controlType,
                              isUser, hasFirstEvent, id, recordId, sourceId,
                              1, name, bbItem, bbSupply);
                }

                // Only the first event of first block can be "first event"
                isUser = hasFirstEvent = false;

                ringBufferIn.publish(nextRingItem);

                // Handle end event ...
                if (controlType == ControlType.END) {
                    // There should be no more events coming down the pike so
                    // go ahead write out existing events and then shut this
                    // thread down.
                    logger.info("      DataChannel Emu in: " + name + " found END event");
                    haveInputEndEvent = true;
                    // run callback saying we got end event
                    if (endCallback != null) endCallback.endWait();
                    break;
                }
            }
        }

    }




    /**
     * Class used to take Evio banks from ring buffer (placed there by a module),
     * and write them over network to an Emu domain input channel using the Emu
     * domain output channel.
     */
    private class DataOutputHelper extends Thread {

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Let a single waiter know that the main thread has been started. */
        private final CountDownLatch startLatch = new CountDownLatch(1);

        /** Object to write (marshall) input buffers into larger, output evio buffer (next member). */
        private EventWriterUnsync writer;

        /** Buffer to write events into so it can be sent in a cMsg message. */
        private ByteBuffer byteBuffer;

        /** Entry in evio block header. */
        private final BitSet bitInfo = new BitSet(24);

        /** Type of last event written out. */
        private EventType previousEventType;

        /** What state is this thread in? */
        private volatile ThreadState threadState;

        /** Time at which events were sent over socket. */
        private long lastSendTime;


        /** Constructor. */
        DataOutputHelper() {
            super(emu.getThreadGroup(), name() + "_data_out");
            if (direct) {
                byteBuffer = ByteBuffer.allocateDirect(maxBufferSize);
            }
            else {
                byteBuffer = ByteBuffer.allocate(maxBufferSize);
            }
            byteBuffer.order(byteOrder);

            // Create writer to write events into file format
            if (!singleEventOut) {
                try {
                    writer = new EventWriterUnsync(byteBuffer);
                    writer.close();
                }
                catch (EvioException e) {/* never happen */}
            }
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
            }
        }


        /**
         * Send the events currently marshalled into a single buffer.
         * @force if true, force data over socket
         */
        private final void flushEvents(boolean force) throws cMsgException, EvioException {
            writer.close();

            // We must have something to write
            if (writer.getEventsWritten() < 1) {
                return;
            }

            // If we have no more room in buffer, send what we have so far
            outGoingMsg.setUserInt(cMsgConstants.emuEvioFileFormat);
            outGoingMsg.setByteArrayNoCopy(writer.getByteBuffer().array(), 0,
                                           (int) writer.getBytesWrittenToBuffer());
            emuDomain.send(outGoingMsg);

            // Force things out over socket
            if (force) {
                try {
                    emuDomain.flush(0);
                }
                catch (cMsgException e) {
                }
            }

            lastSendTime = emu.getTime();
        }


        /**
         * Write events into internal buffer and, if need be, flush
         * them over socket.
         *
         * @param rItem event to write
         * @throws EmuException if no data to write
         */
        private final void writeEvioData(RingItem rItem)
                throws cMsgException, IOException, EvioException, EmuException {

            int blockNum;
            EventType eType = rItem.getEventType();
            boolean isBuildable = eType.isBuildable();
            int eventsWritten = writer.getEventsWritten();

            // If we're sending out 1 event by itself ...
            if (singleEventOut || !isBuildable) {
//System.out.println("      DataChannel Emu write: type = " + eType);
                // If we already have something stored-up to write, send it out first
                if (eventsWritten > 0 && !writer.isClosed()) {
//System.out.println("      DataChannel Emu write: flush1");
                    flushEvents(false);
                }

                if (isBuildable) {
                    blockNum = recordId++;
                }
                else {
                    blockNum = -1;
                }

                // Write the event ..
                EmuUtilities.setEventType(bitInfo, eType);
                if (rItem.isFirstEvent()) {
                    EmuUtilities.setFirstEvent(bitInfo);
                }
                writer.setBuffer(byteBuffer, bitInfo, blockNum);

                // Unset first event for next round
                EmuUtilities.unsetFirstEvent(bitInfo);

                ByteBuffer buf = rItem.getBuffer();
                if (buf != null) {
                    writer.writeEvent(buf);
                }
                else {
                    EvioNode node = rItem.getNode();
                    if (node != null) {
                        writer.writeEvent(node, false);
                    }
                    else {
                        throw new EmuException("no data to write");
                    }
                }
                rItem.releaseByteBuffer();
//System.out.println("      DataChannel Emu out: flush2");

                // Force over socket if control event
                if (eType.isControl()) {
                    flushEvents(true);
                }
                else {
                    flushEvents(false);
                }
            }
            // If we're marshalling events into a single buffer before sending ...
            else {
//System.out.println("  writeEvioData2: type = " + eType);
                // Following is for testing with C emu producer:
//                boolean a = true;
//                while (a) {
//                    try {
//                        Thread.sleep(3000);
//                    }
//                    catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    System.out.print(".");
//                }

                // If we've already written at least 1 event AND
                // (we have no more room in buffer OR we're changing event types),
                // write what we have.
                if ((eventsWritten > 0 && !writer.isClosed()) &&
                        (!writer.hasRoom(rItem.getTotalBytes()) || previousEventType != eType)) {
//System.out.println("      DataChannel Emu write: flush - no room, diff type");
                    flushEvents(false);
                    // Flush closes the writer so that the next "if" is true
                }

                // Initialize writer if nothing written into buffer yet
                if (eventsWritten < 1 || writer.isClosed()) {
                    // If we're here, we're writing the first event into the buffer.
                    // Make sure there's enough room for at least that one event.
                    if (rItem.getTotalBytes() > byteBuffer.capacity()) {
                        if (direct) {
                            byteBuffer = ByteBuffer.allocateDirect(rItem.getTotalBytes() + 1024);
                        }
                        else {
                            byteBuffer = ByteBuffer.allocate(rItem.getTotalBytes() + 1024);
                        }
                    }

                    // Init writer
                    EmuUtilities.setEventType(bitInfo, eType);
                    writer.setBuffer(byteBuffer, bitInfo, recordId++);
//System.out.println("      DataChannel Emu write: init writer");
                }

//System.out.println("      DataChannel Emu write: write ev into buf");
                // Write the new event ..
                ByteBuffer buf = rItem.getBuffer();
                if (buf != null) {
                    writer.writeEvent(buf);
                }
                else {
                    EvioNode node = rItem.getNode();
                    if (node != null) {
                        writer.writeEvent(node, false);
                    }
                    else {
                        throw new EmuException("no data to write");
                    }
                }

                rItem.releaseByteBuffer();
            }

            previousEventType = eType;
        }


        /** {@inheritDoc} */
        @Override
        public void run() {
//logger.debug("      DataChannel Emu out: started, w/ " + outputRingCount +  " output rings");
            threadState = ThreadState.RUNNING;

            // Tell the world I've started
            startLatch.countDown();
            int counter=1;

            try {
                RingItem ringItem;
                EventType pBankType;
                ControlType pBankControlType;
                boolean gotPrestart = false;

                // Time in milliseconds for writing if time expired
                long timeout = 2000L;
                lastSendTime = System.currentTimeMillis();

                // The 1st event may be a user event or a prestart.
                // After the prestart, the next event may be "go", "end", or a user event.
                // The non-END control events are placed on ring 0 of all output channels.
                // The END event is placed in the ring in which the next data event would
                // have gone. The user events are placed on ring 0 of only the first output
                // channel.

                // Keep reading user & control events (all of which will appear in ring 0)
                // until the 2nd control event (go or end) is read.
                while (true) {
                    // Read next event
                    ringItem = getNextOutputRingItem(0);
                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    // If control event ...
                    if (pBankType == EventType.CONTROL) {
                        // if prestart ..
                        if (pBankControlType == ControlType.PRESTART) {
                            if (gotPrestart) {
                                throw new EmuException("got 2 prestart events");
                            }
logger.debug("      DataChannel Emu out " + outputIndex + ": send prestart event");
                            gotPrestart = true;
                            writeEvioData(ringItem);
                        }
                        else {
                            if (!gotPrestart) {
                                throw new EmuException("prestart, not " + pBankControlType +
                                                               ", must be first control event");
                            }

                            if (pBankControlType != ControlType.GO &&
                                pBankControlType != ControlType.END) {
                                throw new EmuException("second control event must be go or end");
                            }

logger.debug("      DataChannel Emu out " + outputIndex + ": send " + pBankControlType + " event");
                            writeEvioData(ringItem);

                            // Release and go to the next event
                            releaseCurrentAndGoToNextOutputRingItem(0);

                            // Done looking for the 2 control events
                            break;
                        }
                    }
                    // If user event ...
                    else if (pBankType == EventType.USER) {
//logger.debug("      DataChannel Emu out " + outputIndex + ": writing user event");
                        // Write user event
                        writeEvioData(ringItem);
                    }
                    // Only user and control events should come first, so error
                    else {
                        throw new EmuException(pBankType + " type of events must come after go event");
                    }

                    // Keep reading events till we hit go/end
                    releaseCurrentAndGoToNextOutputRingItem(0);
                }


                if (pBankControlType == ControlType.END) {
                    flushEvents(true);
                    logger.debug("      DataChannel Emu out: " + name + " I got END event, quitting");
                    // run callback saying we got end event
                    if (endCallback != null) endCallback.endWait();
                    threadState = ThreadState.DONE;
                    return;
                }

                while (true) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) {
                            try {
                                Thread.sleep(5);
                            }
                            catch (InterruptedException e1) {
                            }
                        }
                        continue;
                    }

                    try {
                        ringItem = getNextOutputRingItem(ringIndex);
                    }
                    catch (InterruptedException e) {
                        threadState = ThreadState.INTERRUPTED;
                        // If we're here we were blocked trying to read the next event.
                        // If there are multiple event building threads in the module,
                        // then the END event may show up in an unexpected ring.
                        // The reason for this is that one thread writes to only one ring.
                        // But since only 1 thread gets the END event, it must write it
                        // into that ring in all output channels whether that ring was
                        // the next place to put a data event or not. Thus it may end up
                        // in a ring which was not the one to be read next.
                        // We've had 1/4 second to read everything else so let's try
                        // reading END from this now-known "unexpected" ring.
                        logger.debug("      DataChannel Emu out " + outputIndex + ": try again, read END from ringIndex " +
                                             ringIndexEnd + " not " + ringIndex);
                        ringItem = getNextOutputRingItem(ringIndexEnd);
                    }

                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    try {
                        writeEvioData(ringItem);
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "Cannot write to file");
                        throw e;
                    }

//logger.debug("      DataChannel Emu out: send seq " + nextSequences[ringIndex] + ", release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);

                    // Do not go to the next ring if we got a control or user event.
                    // All prestart, go, & users go to the first ring. Just keep reading
                    // until we get to a built event. Then start keeping count so
                    // we know when to switch to the next ring.
                    if (outputRingCount > 1 && pBankControlType == null && !pBankType.isUser()) {
                        setNextEventAndRing();
//logger.debug("      DataChannel Emu out, " + name + ": for seq " + nextSequences[ringIndex] + " SWITCH TO ring = " + ringIndex);
                    }

                    if (pBankControlType == ControlType.END) {
                        flushEvents(true);
                        logger.debug("      DataChannel Emu out: " + name + " got END event, quitting");
                        // run callback saying we got end event
                        if (endCallback != null) endCallback.endWait();
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        logger.debug("      DataChannel Emu out: " + name + " got RESET cmd, quitting");
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // Time expired so send out events we have\
//System.out.println("time = " + emu.getTime() + ", lastSendTime = " + lastSendTime);
                    if (emu.getTime() - lastSendTime > timeout) {
                        System.out.println("TIME FLUSH ******************");
                        flushEvents(false);
                    }
                }

            }
            catch (InterruptedException e) {
                logger.warn("      DataChannel Emu out: " + name + "  interrupted thd, exiting");
            }
            catch (Exception e) {
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel cmsg in: " + e.getMessage());
                logger.warn("      DataChannel Emu out : exit thd: " + e.getMessage());
            }
        }

    }

}
