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
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.*;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
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

    /** Coda id of the data source. */
    private int sourceId;

    /** Connection to emu domain server. */
    private cMsg emuDomain;

    /** cMsg message into which out going data is placed in order to be written. */
    private cMsgMessage outGoingMsg = new cMsgMessage();

    // INPUT

    /** Thread used to input data. */
    private DataInputHelper dataInputThread;

    /** Data input stream from TCP socket. */
    private DataInputStream in;

    /** TCP receive buffer size in bytes. */
    private int tcpRecvBuf;

    // INPUT & OUTPUT

    /** Biggest chunk of data sent by data producer.
     *  Allows good initial value of ByteBuffer size.  */
    private int maxBufferSize;

    /** Use the evio block header's block number as a record id. */
    private int recordId;

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
     * @param name          the name of this channel
     * @param transport     the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     * @param emu           emu this channel belongs to
     * @param module        module this channel belongs to
     * @param outputIndex   order in which module's events will be sent to this
     *                      output channel (0 for first output channel, 1 for next, etc.).
     *
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

        // size of TCP send buffer (0 means use operating system default)
        tcpSendBuf = 0;
        String attribString = attributeMap.get("sendBuf");
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

        // set TCP_NODELAY option on
        noDelay = false;
        attribString = attributeMap.get("noDelay");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("true") ||
                attribString.equalsIgnoreCase("on")   ||
                attribString.equalsIgnoreCase("yes"))   {
                noDelay = true;
            }
        }

        // if INPUT channel
        if (input) {


        }
        // if OUTPUT channel
        else {

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
System.out.println("      DataChannel Emu: sending on port " + sendPort);


            // Size of max buffer, input or output
            maxBufferSize = 2100000;
            attribString = attributeMap.get("maxBuf");
            if (attribString != null) {
                try {
                    maxBufferSize = Integer.parseInt(attribString);
                    if (maxBufferSize < 0) {
                        maxBufferSize = 2100000;
                    }
                }
                catch (NumberFormatException e) {}
            }

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

        }

        // State after prestart transition -
        // during which this constructor is called
        state = CODAState.PAUSED;
    }


    /**
     * Once a client connects to the Emu domain server in the Emu transport object,
     * that socket is passed to this method and a thread is spawned to handle all
     * communications over it. Only used for input channel.
     *
     * @param channel
     */
    void attachToInput(SocketChannel channel, int sourceId, int maxBufferSize) throws IOException {
        this.sourceId = sourceId;
        this.maxBufferSize = maxBufferSize;

        // Set socket options
        Socket socket = channel.socket();
        // Set TCP no-delay so no packets are delayed
        socket.setTcpNoDelay(noDelay);
        // Set TCP receive buffer size
        if (tcpRecvBuf > 0) {
            socket.setReceiveBufferSize(tcpRecvBuf);
        }

        // Use buffered streams for efficiency
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 256000));

        // Create a ring buffer full of empty ByteBuffer objects
        // in which to copy incoming data from client.
        bbSupply = new ByteBufferSupply(128, maxBufferSize);

        // Start thread to handle all socket input
        startInputThread();
    }



    private void openOutputChannel() throws cMsgException {

        // UDL ->  emu://port/expid?codaId=id&timeout=sec&bufSize=size&tcpSend=size&noDelay

        String udl = "emu://" + sendPort + "/" +
                emu.getExpid() + "?codaId=" + getID();

        if (maxBufferSize > 0) {
            udl += "&bufSize=" + maxBufferSize;
        }
        else {
            udl += "&bufSize=2100000";
        }

        if (connectTimeout > -1) {
            udl += "&timeout=" + connectTimeout;
        }

        if (tcpSendBuf > 0) {
            udl += "&tcpSend=" + tcpSendBuf;
        }

        if (noDelay) {
            udl += "&noDelay";
        }

        emuDomain = new cMsg(udl, name, "emu domain client");
        emuDomain.connect();
System.out.println("      DataChannel Emu: UDL = " + udl);
        startOutputThread();
    }


    private void closeOutputChannel() throws cMsgException {
        if (input) return;
        emuDomain.disconnect();
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        super.prestart();

        if (input) return;
        try {
            openOutputChannel();
        }
        catch (cMsgException e) {
            e.printStackTrace();
            throw new CmdExecException(e);
        }

        state = CODAState.PAUSED;
    }

    /** {@inheritDoc} */
    public void go() {
        pause = false;
        state = CODAState.ACTIVE;
    }

    /** {@inheritDoc} */
    public void pause() {
        pause = true;
        state = CODAState.PAUSED;
    }

    /** {@inheritDoc}. Formerly this code was the close() method. */
    public void end() {
        logger.warn("      DataChannel Emu: end() " + name);

        gotEndCmd = true;
        gotResetCmd = false;

        // Do NOT interrupt threads which are communicating with the ET server.
        // This will mess up future communications !!!

        // How long do we wait for each input or output thread
        // to end before we just terminate them?
        // The total time for an emu to wait for the END transition
        // is emu.endingTimeLimit. Dividing that by the number of
        // in/output threads is probably a good guess.
        long waitTime;

        // Don't close ET system until helper threads are done
        try {
            waitTime = emu.getEndingTimeLimit();
            if (dataInputThread != null) {
//System.out.println("      DataChannel Emu: try joining input thread ...");
                    dataInputThread.join(waitTime);
                    // kill it if not already dead since we waited as long as possible
                    dataInputThread.interrupt();
//System.out.println("      DataChannel Emu: input thread done");
            }

            if (dataOutputThread != null) {
                waitTime = emu.getEndingTimeLimit();
//System.out.println("      DataChannel Emu: try joining output thread for " + (waitTime/1000) + " sec");
                    dataOutputThread.join(waitTime);
                    // kill everything since we waited as long as possible
                    dataOutputThread.interrupt();
                    dataOutputThread.shutdown();
//System.out.println("      DataChannel Emu: output thread done");
                }
//System.out.println("      DataChannel Emu: all helper thds done");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        state = CODAState.DOWNLOADED;

        try {
            closeOutputChannel();
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }

System.out.println("      DataChannel Emu: end() done");
    }


    /**
     * {@inheritDoc}
     * Reset this channel by interrupting the data sending threads and closing ET system.
     */
    public void reset() {
logger.debug("      DataChannel Emu: reset() " + name);

        gotEndCmd   = false;
        gotResetCmd = true;

        // Don't close ET system until helper threads are done
        if (dataInputThread != null) {
//System.out.println("      DataChannel Emu: interrupt input thread ...");
                dataInputThread.interrupt();
                // Make sure the thread is done, otherwise you risk
                // killing the ET system while a getEvents() call is
                // still in progress (with et-14.0 this is OK).
                // Give it 25% more time than the wait.
                try {dataInputThread.join(400);}  // 625
                catch (InterruptedException e) {}
//System.out.println("      DataChannel Emu: input thread done");
        }

        if (dataOutputThread != null) {
//System.out.println("      DataChannel Emu: interrupt output thread ...");
                dataOutputThread.interrupt();
                dataOutputThread.shutdown();
                // Make sure all threads are done, otherwise you risk
                // killing the ET system while a new/put/dumpEvents() call
                // is still in progress (with et-14.0 this is OK).
                // Give it 25% more time than the wait.
                try {dataOutputThread.join(1000);}
                catch (InterruptedException e) {}
//System.out.println("      DataChannel Emu: output thread done");
        }

        errorMsg.set(null);
        state = CODAState.CONFIGURED;
logger.debug("      DataChannel Emu: reset() " + name + " done");
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
logger.debug("      DataChannel Emu startOutputThread()");
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
logger.debug("      DataChannel Emu out " + outputIndex + ": processEnd(), thread already done");
            return;
        }

        // Don't wait more than 1/2 second
        int loopCount = 20;
        while (dataOutputThread.threadState != ThreadState.DONE && (loopCount-- > 0)) {
            try {
                Thread.sleep(25);
            }
            catch (InterruptedException e) { break; }
        }

        if (dataOutputThread.threadState == ThreadState.DONE) {
logger.debug("      DataChannel Emu out " + outputIndex + ": processEnd(), thread done after waiting");
            return;
        }

        // Probably stuck trying to get item from ring buffer,
        // so interrupt it and get it to read the END event from
        // the correct ring.
logger.debug("      DataChannel Emu out " + outputIndex + ": processEnd(), interrupt thread in state " +
                     dataOutputThread.threadState);
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

        /** Read into EvioEvent objects. */
        private EvioReader eventReader;

        /** Read into ByteBuffers. */
        private EvioCompactReader compactReader;

        /** Is the type of data in ring PayloadBuffer or PayloadBank? */
        private final boolean ringItemIsBuffer;


        /** Constructor. */
        DataInputHelper() {
            super(emu.getThreadGroup(), name() + "_data_in");
System.out.println("      DataChannel Emu in: start EMU input thread");
            // Speed things up with local variable
            ringItemIsBuffer = (ringItemType == ModuleIoType.PayloadBuffer);
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
                int command;
                boolean delay = false;

                while ( true ) {

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

                    // Read the command first
                    command = in.readInt();
//System.out.println("      DataChannel Emu in: cmd = 0x" + Integer.toHexString(command));
//                    Thread.sleep(1000);

                    // 1st byte has command
                    switch (command & 0xff) {
                        case cMsgConstants.emuEvioFileFormat:
                            if  (ringItemIsBuffer) {
//System.out.println("      DataChannel Emu in: event to handleEvioFileToBuf(), name = " + name);
                                handleEvioFileToBuf();
                            }
                            else {
                                handleEvioFileToBank();
                            }

                            break;

                        case cMsgConstants.emuEnd:
System.out.println("      DataChannel Emu in: get emuEnd cmd");
                            break;

                        default:
//System.out.println("      DataChannel Emu in: unknown command from Emu client = " + (command & 0xff));
                    }

                    if (haveInputEndEvent) {
                        break;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Emu in: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                logger.warn("      DataChannel Emu in: " + name + " exit thd: " + e.getMessage());
            }

        }



        private final void handleEvioFileToBank() throws IOException, EvioException {

            EvioEvent event;
            EventType bankType;
            PayloadBank payloadBank;
            ControlType controlType = null;

            // Get a reusable ByteBuffer
            ByteBufferItem bbItem = bbSupply.get();
            ByteBuffer buf = bbItem.getBuffer();

            // Read the length of evio file-format data to come
            int evioBytes = in.readInt();

            // If buffer is too small, make a bigger one
            bbItem.ensureCapacity(evioBytes);

            // Read evio file-format data
            in.readFully(buf.array(), 0, evioBytes);

            try {
                if (eventReader == null) {
                    eventReader = new EvioReader(buf);
                }
                else {
                    eventReader.setBuffer(buf);
                }
            }
            catch (IOException e) {
                errorMsg.compareAndSet(null, "Data is NOT in evio v4 format");
                throw e;
            }
            // Speed things up since no EvioListeners are used - doesn't do much
            eventReader.getParser().setNotificationActive(false);

            // First block header in buffer
            BlockHeaderV4 blockHeader = (BlockHeaderV4)eventReader.getFirstBlockHeader();
            if (blockHeader.getVersion() < 4) {
                errorMsg.compareAndSet(null, "Data is NOT in evio v4 format");
                throw new EvioException("Evio data needs to be written in version 4+ format");
            }

            // eventType may be null if no type info exists in block header.
            // But it should always be there if reading from ROC or DC.
            EventType eventType = EventType.getEventType(blockHeader.getEventType());

            // The recordId associated with each bank is taken from the first
            // evio block header in a single ET data buffer. For a physics or
            // ROC raw type, it should start at zero and increase by one in the
            // first evio block header of the next ET data buffer.
            // There may be multiple banks from the same ET buffer and
            // they will all have the same recordId.
            //
            // Thus, only the first block header # is significant. It is set sequentially
            // by the evWriter object & incremented once per ET event with physics
            // or ROC data (set to -1 for other data types). Copy it into each bank.
            // Even though many banks will have the same number, it should only
            // increment by one. This should work just fine as all evio events in
            // a single ET event should always be there (not possible to skip any)
            // since it is transferred all together.
            //
            // When the QFiller thread of the event builder gets a physics or ROC
            // evio event, it checks to make sure this number is in sequence and
            // prints a warning if it isn't.
            int recordId = blockHeader.getNumber();

//logger.info("      DataChannel Emu in: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId);

//System.out.println("      DataChannel Emu in: parse next event");
            while ((event = eventReader.parseNextEvent()) != null) {
                // Complication: from the ROC, we'll be receiving USER events
                // mixed in with and labeled as ROC Raw events. Check for that
                // and fix it.
                bankType = eventType;
                if (eventType == EventType.ROC_RAW) {
                    if (Evio.isUserEvent(event)) {
                        bankType = EventType.USER;
                    }
                }
                else if (eventType == EventType.CONTROL) {
                    // Find out exactly what type of control event it is
                    // (May be null if there is an error).
                    // TODO: It may NOT be enough just to check the tag
                    controlType = ControlType.getControlType(event.getHeader().getTag());
                    if (controlType == null) {
                        errorMsg.compareAndSet(null, "Found unidentified control event");
                        throw new EvioException("Found unidentified control event");
                    }
                }

                nextRingItem = ringBufferIn.next();
                payloadBank = (PayloadBank) ringBufferIn.get(nextRingItem);

                payloadBank.setEvent(event);
                payloadBank.setEventType(bankType);
                payloadBank.setControlType(controlType);
                payloadBank.setRecordId(recordId);
                payloadBank.setSourceId(sourceId);
                payloadBank.setSourceName(name);
                payloadBank.setEventCount(1);
                payloadBank.matchesId(sourceId == id);

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

            // Have no more need of buffer
            bbSupply.release(bbItem);
        }


        private final void handleEvioFileToBuf() throws IOException, EvioException {

            EvioNode node;
            EventType bankType;
            PayloadBuffer payloadBuffer;
            ControlType controlType = null;

            // Get a reusable ByteBuffer
            ByteBufferItem bbItem = bbSupply.get();
            ByteBuffer buf = bbItem.getBuffer();

            // Read the length of evio file-format data to come
            int evioBytes = in.readInt();
//System.out.println("      DataChannel Emu in: len = " + evioBytes);
            // If buffer is too small, make a bigger one
            bbItem.ensureCapacity(evioBytes);

            // Read evio file-format data
            in.readFully(buf.array(), 0, evioBytes);

            try {
                if (compactReader == null) {
                    compactReader = new EvioCompactReader(buf);
                }
                else {
                    compactReader.setBuffer(buf);
                }
            }
            catch (EvioException e) {
                e.printStackTrace();
                errorMsg.compareAndSet(null, "Data is NOT evio v4 format");
                throw e;
            }

            // First block header in buffer
            BlockHeaderV4 blockHeader = compactReader.getFirstBlockHeader();
            if (blockHeader.getVersion() < 4) {
                errorMsg.compareAndSet(null, "Data is NOT evio v4 format");
                throw new EvioException("Evio data needs to be written in version 4+ format");
            }

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

            for (int i=1; i < eventCount+1; i++) {
                node = compactReader.getScannedEvent(i);

                // Complication: from the ROC, we'll be receiving USER events
                // mixed in with and labeled as ROC Raw events. Check for that
                // and fix it.
                bankType = eventType;
                if (eventType == EventType.ROC_RAW) {
                    if (Evio.isUserEvent(node)) {
                        bankType = EventType.USER;
                    }
                }
                else if (eventType == EventType.CONTROL) {
                    // Find out exactly what type of control event it is
                    // (May be null if there is an error).
                    // TODO: It may NOT be enough just to check the tag
                    controlType = ControlType.getControlType(node.getTag());
                    if (controlType == null) {
                        errorMsg.compareAndSet(null, "Found unidentified control event");
                        throw new EvioException("Found unidentified control event");
                    }
                }

                nextRingItem = ringBufferIn.next();

                payloadBuffer = (PayloadBuffer) ringBufferIn.get(nextRingItem);
                payloadBuffer.setBuffer(node.getStructureBuffer(false));
                payloadBuffer.setEventType(bankType);
                payloadBuffer.setControlType(controlType);
                payloadBuffer.setRecordId(recordId);
                payloadBuffer.setSourceId(sourceId);
                payloadBuffer.setSourceName(name);
                payloadBuffer.setNode(node);
                payloadBuffer.setEventCount(1);
                payloadBuffer.setReusableByteBuffer(bbSupply, bbItem);
                payloadBuffer.matchesId(sourceId == id);

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


    // TODO: most likely needs updating
    /**
     * Class used to take Evio banks from ring buffer (placed there by a module),
     * and write them over network to an Emu domain input channel using the Emu
     * domain output channel. The trick is to have a single thread constantly
     * writing over the network, while another feeds it buffers to write (on a ring).
     * Although this should be faster than the other DataOutputHelper, it's about
     * 4% slower - probably because the bottleneck is elsewhere and this class
     * takes more CPU plus overhead of an extra thread.
     */
    private class DataOutputHelperNew extends Thread {

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Let a single waiter know that the main threads have been started. */
        private final CountDownLatch startLatch = new CountDownLatch(2);

        /** Object to write (marshall) input buffers into larger, output evio buffer (next member). */
        private EventWriter writer;

        /** Buffer to write events into so it can be sent in a cMsg message. */
        private ByteBuffer byteBuffer;

        private final BitSet bitInfo = new BitSet(24);

        private EventType previousEventType;

        private final boolean ringItemIsBuffer;

        // Create a ring buffer full of empty ByteBuffer objects
        // in which place outgoing evio file data.
        private final ByteBufferSupply outBufSupply = new ByteBufferSupply(32, maxBufferSize);

        private ByteBufferItem bufferItem;

        private OutputThread outputThread;

        /** What state is this thread in? */
        private volatile ThreadState threadState;


         /** Constructor. */
        DataOutputHelperNew() {
            super(emu.getThreadGroup(), name() + "_data_out");
            byteBuffer = ByteBuffer.allocate(maxBufferSize);

            // Speed things up with local variable
            ringItemIsBuffer = (ringItemType == ModuleIoType.PayloadBuffer);

            // Create writer to write events into file format
            if (!singleEventOut) {
                try {
                    writer = new EventWriter(byteBuffer);
                    writer.close();
                //  writer = new EventWriter(byteBuffer, 5250, 50000, null, null);
                }
                catch (EvioException e) {/* never happen */}
            }

            outputThread = new OutputThread();
            outputThread.start();
        }



        private class OutputThread extends Thread {

            /** {@inheritDoc} */
            @Override
            public void run() {
                ByteBuffer buf;
                ByteBufferItem item;

                // Tell the world I've started
                startLatch.countDown();

                try {
                    while (true) {
                        item = outBufSupply.consumerGet();

                        buf = item.getBuffer();

                        outGoingMsg.setUserInt(cMsgConstants.emuEvioFileFormat);
                        outGoingMsg.setByteArrayNoCopy(buf.array(), 0, buf.limit());
                        emuDomain.send(outGoingMsg);

                        outBufSupply.consumerRelease(item);
                    }
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {}
        }


        /** Stop all this object's threads. */
        private void shutdown() { }


        /**
         * Send the events currently marshalled into a single buffer.
         * @throws cMsgException
         * @throws EvioException
         */
        private final void flushEvents() throws cMsgException, EvioException{
//System.out.println("      DataChannel Emu out: into flushEvents()");
            writer.close();

            // We must have something to write
            if (writer.getEventsWritten() < 1) {
//System.out.println("      DataChannel Emu out: flushEvents, nothing to write out");
                return;
            }
//System.out.println("      DataChannel Emu out: flushEvents, write event out by publishing");

            // Release buffer used in writer for reuse
            outBufSupply.publish(bufferItem);
        }


        private final void writeEvioData(RingItem rItem, EventType eType)
                                                         throws cMsgException,
                                                                IOException,
                                                                EvioException {
            int blockNum = 0;
            boolean isBuildable = eType.isBuildable();
            int eventsWritten =  writer.getEventsWritten();

            // If we're sending out 1 event by itself ...
            if (singleEventOut || !isBuildable) {
// System.out.println("      DataChannel Emu out: type = " + eType);
                // If we already have something stored-up to write, send it out first
                if (eventsWritten > 0 && !writer.isClosed()) {
//System.out.println("      DataChannel Emu out: flush1");
                    flushEvents();
                }

                if (isBuildable) {
                    blockNum = recordId++;
                }
                else {
                    blockNum = -1;
                }

//System.out.println(""      DataChannel Emu out: get new outBufSupply buf for control/single event");
                bufferItem = outBufSupply.get();

                // Write the event ..
                EmuUtilities.setEventType(bitInfo, eType);
                writer.setBuffer(bufferItem.getBuffer(), bitInfo, blockNum);
                if (ringItemIsBuffer) {
                    writer.writeEvent(rItem.getBuffer());
                }
                else {
                    writer.writeEvent(rItem.getEvent());
                }
                rItem.releaseByteBuffer();
//System.out.println(""      DataChannel Emu out: flush2");
                flushEvents();
            }
            // If we're marshalling events into a single buffer before sending ...
            else {
//System.out.println(""      DataChannel Emu out: type = " + eType);

                // If we've already written at least 1 event AND
                // (we have no more room in buffer OR we're changing event types),
                // write what we have.
                if ((eventsWritten > 0 && !writer.isClosed()) &&
                    (!writer.hasRoom(rItem.getTotalBytes()) || previousEventType != eType)) {
//System.out.println(""      DataChannel Emu out: flush1");
                    flushEvents();
                }

                // Initialize writer if nothing written yet
                if (eventsWritten < 1 || writer.isClosed()) {
//System.out.println(""      DataChannel Emu out: get new outBufSupply buf for marshalled event");
                    bufferItem = outBufSupply.get();

                    // If we're here, we're writing the first event into the buffer.
                    // Make sure there's enough room for at least that one event.
                    if (rItem.getTotalBytes() > bufferItem.getBuffer().capacity()) {
                        bufferItem.ensureCapacity(rItem.getTotalBytes() + 1024);
                    }

                    // Init writer
                    EmuUtilities.setEventType(bitInfo, eType);
                    writer.setBuffer(bufferItem.getBuffer(), bitInfo, recordId++);
//System.out.println(""      DataChannel Emu out: init writer");
                }

//System.out.println(""      DataChannel Emu out: write ev into buf");
                // Write the new event ..
                if (ringItemIsBuffer) {
                    writer.writeEvent(rItem.getBuffer());
                }
                else {
                    writer.writeEvent(rItem.getEvent());
                }

                // Release buffer in channel input ring
                rItem.releaseByteBuffer();
            }

            previousEventType = eType;
        }


        /** {@inheritDoc} */
        @Override
        public void run() {
logger.debug("      DataChannel Emu out: started");

            // Tell the world I've started
            startLatch.countDown();

            try {
                EventType pBankType;
                ControlType pBankControlType;
                RingItem ringItem;

                // First event will be "prestart", by convention in ring 0
                ringItem = getNextOutputRingItem(0);
                writeEvioData(ringItem, ringItem.getEventType());
                releaseCurrentAndGoToNextOutputRingItem(0);
logger.debug("      DataChannel Emu out: sent prestart");

                // First event will be "go", by convention in ring 0
                ringItem = getNextOutputRingItem(0);
                writeEvioData(ringItem, ringItem.getEventType());
                releaseCurrentAndGoToNextOutputRingItem(0);
logger.debug("      DataChannel Emu out: sent go");

                while ( true ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) {
                            try {Thread.sleep(5);}
                            catch (InterruptedException e1) {}
                        }
                        continue;
                    }

                    try {
//logger.debug("      DataChannel Emu out " + outputIndex + ": try getting next buffer from ring");
                        ringItem = getNextOutputRingItem(ringIndex);
//logger.debug("      DataChannel Emu out " + outputIndex + ": got next buffer");
                    }
                    catch (InterruptedException e) {
                        threadState = ThreadState.INTERRUPTED;
                        // If we're here we were blocked trying to read the next
                        // (END) event from the wrong ring. We've had 1/4 second
                        // to read everything else so let's try reading END from
                        // given ring.
System.out.println("      DataChannel Emu out " + outputIndex + ": try again, read END from ringIndex " + ringIndexEnd +
" not " + ringIndex);
                        ringItem = getNextOutputRingItem(ringIndexEnd);
                    }

                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    try {
                        writeEvioData(ringItem, ringItem.getEventType());
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "Cannot write to file");
                        throw e;
                    }

//logger.debug("      DataChannel Emu out: send event " + (nextEvent) + ", release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);

                    // Do not go to the next ring if we got a control or user event.
                    // All prestart, go, & users go to the first ring. Just keep reading
                    // until we get to a built event. Then start keeping count so
                    // we know when to switch to the next ring.
                    if (outputRingCount > 1 && pBankControlType == null &&
                            !pBankType.isUser()) {

                        setNextEventAndRing();
//System.out.println("      DataChannel Emu out, " + name + ": for next ev " + nextEvent + " SWITCH TO ring = " + ringIndex);
                    }

                    if (pBankControlType == ControlType.END) {
                        flushEvents();
System.out.println("      DataChannel Emu out: " + name + " I got END event, quitting");
                        // run callback saying we got end event
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        System.out.println("      DataChannel Emu out: " + name + " got RESET/END cmd, quitting 1");
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Emu out: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
                logger.warn("      DataChannel Emu out : exit thd, " + e.getMessage());
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                e.printStackTrace();
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
        private EventWriter writer;

        /** Buffer to write events into so it can be sent in a cMsg message. */
        private ByteBuffer byteBuffer;

        /** Entry in evio block header. */
        private final BitSet bitInfo = new BitSet(24);

        /** Type of last event written out. */
        private EventType previousEventType;

        /** Is ring item a PayloadBuffer (true) or not? */
        private final boolean ringItemIsBuffer;

        /** What state is this thread in? */
        private volatile ThreadState threadState;


         /** Constructor. */
        DataOutputHelper() {
            super(emu.getThreadGroup(), name() + "_data_out");
            byteBuffer = ByteBuffer.allocate(maxBufferSize);
            byteBuffer.order(byteOrder);

            // Speed things up with local variable
            ringItemIsBuffer = (ringItemType == ModuleIoType.PayloadBuffer);

            // Create writer to write events into file format
            if (!singleEventOut) {
                try {
                    writer = new EventWriter(byteBuffer);
                    writer.close();
                //  writer = new EventWriter(byteBuffer, 5250, 50000, null, null);
                }
                catch (EvioException e) {/* never happen */}
            }
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {}
        }


        /** Stop all this object's threads. */
        private void shutdown() { }


        /**
         * Send the events currently marshalled into a single buffer.
         * @throws cMsgException
         * @throws EvioException
         */
        private final void flushEvents() throws cMsgException, EvioException {
//System.out.println("\n      DataChannel Emu out: into flushEvents()\n");
            writer.close();

            // We must have something to write
            if (writer.getEventsWritten() < 1) {
//System.out.println("      DataChannel Emu out: flushEvents, nothing to write out");
                return;
            }
//System.out.println("      DataChannel Emu out: flushEvents, write event out");
            // If we have no more room in buffer, send what we have so far
            outGoingMsg.setUserInt(cMsgConstants.emuEvioFileFormat);
            outGoingMsg.setByteArrayNoCopy(writer.getByteBuffer().array(), 0,
                                           (int) writer.getBytesWrittenToBuffer());
            emuDomain.send(outGoingMsg);
        }


        private final void writeEvioData(RingItem rItem, EventType eType)
                                                         throws cMsgException,
                                                                IOException,
                                                                EvioException {
            int blockNum = 0;
            boolean isBuildable = eType.isBuildable();
            int eventsWritten = writer.getEventsWritten();

            // If we're sending out 1 event by itself ...
            if (singleEventOut || !isBuildable) {
// System.out.println("      DataChannel Emu out: type = " + eType);
                // If we already have something stored-up to write, send it out first
                if (eventsWritten > 0 && !writer.isClosed()) {
//System.out.println("      DataChannel Emu out: flush1");
                    flushEvents();
                }

                if (isBuildable) {
                    blockNum = recordId++;
                }
                else {
                    blockNum = -1;
                }

//System.out.println("      DataChannel Emu out:: write ev into buf");
                // Write the event ..
                EmuUtilities.setEventType(bitInfo, eType);
                writer.setBuffer(byteBuffer, bitInfo, blockNum);
                if (ringItemIsBuffer) {
                    writer.writeEvent(rItem.getBuffer());
                }
                else {
                    writer.writeEvent(rItem.getEvent());
                }
                rItem.releaseByteBuffer();
//System.out.println("      DataChannel Emu out: flush2");
                flushEvents();
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
//System.out.println("      DataChannel Emu out: flush1");
                    flushEvents();
                }

                // Initialize writer if nothing written into buffer yet
                if (eventsWritten < 1 || writer.isClosed()) {
                    // If we're here, we're writing the first event into the buffer.
                    // Make sure there's enough room for at least that one event.
                    if (rItem.getTotalBytes() > byteBuffer.capacity()) {
                        byteBuffer = ByteBuffer.allocate(rItem.getTotalBytes() + 1024);
                    }

                    // Init writer
                    EmuUtilities.setEventType(bitInfo, eType);
                    writer.setBuffer(byteBuffer, bitInfo, recordId++);
//System.out.println("      DataChannel Emu out: init writer");
                }

//System.out.println("      DataChannel Emu out: write ev into buf");
                // Write the new event ..
                if (ringItemIsBuffer) {
                    writer.writeEvent(rItem.getBuffer());
                }
                else {
                    writer.writeEvent(rItem.getEvent());
                }
//System.out.println("      DataChannel Emu out: writeEvioData(), release buf");
                rItem.releaseByteBuffer();
            }

            previousEventType = eType;
        }


        /** {@inheritDoc} */
        @Override
        public void run() {
logger.debug("      DataChannel Emu out: started, w/ " + outputRingCount +  " output rings");
            threadState = ThreadState.RUNNING;

            // Tell the world I've started
            startLatch.countDown();

            try {
                EventType pBankType;
                ControlType pBankControlType;
                RingItem ringItem;

                // First event will be "prestart", by convention in ring 0
                ringItem = getNextOutputRingItem(0);
                writeEvioData(ringItem, ringItem.getEventType());
                releaseCurrentAndGoToNextOutputRingItem(0);
logger.debug("      DataChannel Emu out: sent prestart");

                // First event will be "go", by convention in ring 0
                ringItem = getNextOutputRingItem(0);
                writeEvioData(ringItem, ringItem.getEventType());
                releaseCurrentAndGoToNextOutputRingItem(0);
logger.debug("      DataChannel Emu out: sent go");

                while ( true ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) {
                            try {Thread.sleep(5);}
                            catch (InterruptedException e1) {}
                        }
                        continue;
                    }

                    try {
//logger.debug("      DataChannel Emu out " + outputIndex + ": try getting next buffer from ring");
                        ringItem = getNextOutputRingItem(ringIndex);
//logger.debug("      DataChannel Emu out " + outputIndex + ": got next buffer");
                    }
                    catch (InterruptedException e) {
                        threadState = ThreadState.INTERRUPTED;
                        // If we're here we were blocked trying to read the next
                        // (END) event from the wrong ring. We've had 1/4 second
                        // to read everything else so let's try reading END from
                        // given ring.
System.out.println("      DataChannel Emu out " + outputIndex + ": try again, read END from ringIndex " + ringIndexEnd +
" not " + ringIndex);
                        ringItem = getNextOutputRingItem(ringIndexEnd);
                    }

                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    try {
                        writeEvioData(ringItem, ringItem.getEventType());
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "Cannot write to file");
                        throw e;
                    }

//logger.debug("      DataChannel Emu out: send event " + (nextEvent) + ", release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);

                    // Do not go to the next ring if we got a control or user event.
                    // All prestart, go, & users go to the first ring. Just keep reading
                    // until we get to a built event. Then start keeping count so
                    // we know when to switch to the next ring.
                    if (outputRingCount > 1 && pBankControlType == null &&
                            !pBankType.isUser()) {

                        setNextEventAndRing();
//System.out.println("      DataChannel Emu out, " + name + ": for next ev " + nextEvent + " SWITCH TO ring = " + ringIndex);
                    }

                    if (pBankControlType == ControlType.END) {
                        flushEvents();
System.out.println("      DataChannel Emu out: " + name + " I got END event, quitting");
                        // run callback saying we got end event
                        if (endCallback != null) endCallback.endWait();
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
System.out.println("      DataChannel Emu out: " + name + " got RESET/END cmd, quitting 1");
                        threadState = ThreadState.DONE;
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Emu out: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
                logger.warn("      DataChannel Emu out : exit thd: " + e.getMessage());
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                //e.printStackTrace();
            }

        }

    }



}
