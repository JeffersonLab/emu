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


import com.lmax.disruptor.*;
import com.lmax.disruptor.TimeoutException;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
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

    /** Read END event from input queue. */
    private volatile boolean haveInputEndEvent;

    /** Read END event from output queue. */
//    private volatile boolean haveOutputEndEvent;

    /** Got END command from Run Control. */
    private volatile boolean gotEndCmd;

    /** Got RESET command from Run Control. */
    private volatile boolean gotResetCmd;

    // OUTPUT

    /** Thread used to output data. */
    private DataOutputHelper dataOutputThread;

    /** Is the EMU using this ET output channel as a simulated ROC ? */
    private boolean isROC;

    /** Is the EMU using this ET output channel as an event builder? */
    private boolean isEB;

    /** Is the EMU using this ET output channel as the last level event builder? */
    private boolean isFinalEB;

    private boolean marshallOutput;

    private int sendPort;

    // INPUT

    /** Thread used to input data. */
    private DataInputHelper dataInputThread;


    private int ringChunk;



    private SocketChannel channel;
    private DataInputStream in;
    private DataOutputStream out;
    private int maxBufferSize;
    private boolean isBigEndian;
    private int sourceId;
    private int tcpRecvBuf;
    private int tcpSendBuf;
    private boolean noDelay;
    private cMsg emuDomain;
    private cMsgMessage outGoingMsg = new cMsgMessage();

    //-------------------------------------------
    // Disruptor (RingBuffer)  Stuff
    //-------------------------------------------
    private long nextRingItem;

    /** Ring buffer holding ByteBuffers when using EvioCompactEvent reader for incoming events. */
    protected ByteBufferSupply bbSupply;

    private long[] nextSequences;
    private long[] availableSequences;
    private int rbIndex;





    /**
     * Constructor to create a new DataChannelImplEt instance. Used only by
     * {@link org.jlab.coda.emu.support.transport.DataTransportImplEt#createChannel(String, java.util.Map, boolean, org.jlab.coda.emu.Emu, org.jlab.coda.emu.EmuModule)}
     * which is only used during PRESTART in {@link org.jlab.coda.emu.Emu}.
     *
     * @param name          the name of this channel
     * @param transport     the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     * @param emu           emu this channel belongs to
     * @param module        module this channel belongs to
     *
     * @throws org.jlab.coda.emu.support.transport.DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplEmu(String name, DataTransportImplEmu transport,
                       Map<String, String> attributeMap, boolean input, Emu emu,
                       EmuModule module)
        throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module);

        dataTransportImplEmu = transport;

        if (input) {
logger.info("      DataChannel Emu : creating input channel " + name);
        }
        else {
logger.info("      DataChannel Emu : creating output channel " + name);
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
logger.info("      DataChannel Emu : set sendBuf to " + tcpSendBuf);
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
logger.info("      DataChannel Emu : set recvBuf to " + tcpRecvBuf);
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

            ringChunk = 1;
            attribString = attributeMap.get("ringChunk");
            if (attribString != null) {
                try {
                    ringChunk = Integer.parseInt(attribString);
                    if (ringChunk < 1) {
                        ringChunk = 1;
                    }
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
System.out.println("Sending on port " + sendPort);

            // Do we send out individual events or do we
            // marshall them into one evio-file-format buffer?
            marshallOutput = false;
            attribString = attributeMap.get("marshall");
            if (attribString != null) {
                if (attribString.equalsIgnoreCase("true") ||
                        attribString.equalsIgnoreCase("on")   ||
                        attribString.equalsIgnoreCase("yes"))   {
                    marshallOutput = true;
                }
            }


            // Size of max buffer
//TODO: fix this
            maxBufferSize = 1000;
            attribString = attributeMap.get("maxBuf");
            if (attribString != null) {
                try {
                    maxBufferSize = Integer.parseInt(attribString);
                    if (maxBufferSize < 0) {
                        maxBufferSize = 20000;
                    }
                }
                catch (NumberFormatException e) {}
            }

            // Create a ring buffer full of empty ByteBuffer objects
            // in which to copy outgoing data.
//TODO: How do we set the number of buffers???
            bbSupply = new ByteBufferSupply(256, maxBufferSize);

            nextSequences = new long[ringCount];
            availableSequences = new long[ringCount];
            Arrays.fill(availableSequences, -1L);

            for (int i=0; i < ringCount; i++) {
                nextSequences[i] = sequences[i].get() + 1L;
            }

            // If this is an event building EMU, set the control array
            // for each outgoing ET buffer.
            CODAClass emuClass = emu.getCodaClass();
            isEB = emuClass.isEventBuilder();
            isROC = emuClass == CODAClass.ROC;
            if (isEB || isROC) {
                // Is this the last level event builder (not a DC)?
                // In this case, we want the first control word to indicate
                // what type of event is being sent.
                //
                // Control events will be received and dealt with by the FCS
                // (Farm Control Supervisor).
                isFinalEB = (emuClass == CODAClass.PEB || emuClass == CODAClass.SEB);
                // The value of control[0] will be set in the DataOutputHelper
            }
        }

        // State after prestart transition -
        // during which this constructor is called
        state = CODAState.PAUSED;
    }


    /**
     * Once a client connects to the Emu domain server in the Emu transport object,
     * that socket is passed to this method and a thread is spawned to handle all
     * communications over it.
     *
     * @param channel
     */
    void attachToInput(SocketChannel channel, int sourceId, int maxBufferSize, int isBigEndian) throws IOException {
        this.channel = channel;
        this.sourceId = sourceId;
        this.maxBufferSize = maxBufferSize;
        this.isBigEndian = isBigEndian == 1;

        // Set socket options
        Socket socket = channel.socket();
        // Set TCP no-delay so no packets are delayed
        socket.setTcpNoDelay(noDelay);
        // Set TCP receive buffer size
        if (tcpRecvBuf > 0) {
            socket.setReceiveBufferSize(tcpRecvBuf);
        }

        // Use buffered streams for efficiency
        in  = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 256000));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 1024));

        // Create a ring buffer full of empty ByteBuffer objects
        // in which to copy incoming data from client.
        bbSupply = new ByteBufferSupply(256, maxBufferSize);

        // Start thread to handle all socket input
        startInputThread();
    }



    private void openOutputChannel() throws cMsgException {

        // UDL ->  emu://port/expid?myCodaId=id&bufSize=size&tcpSend=size&noDelay

        String udl = "emu://" + sendPort + "/" +
                emu.getExpid() + "?myCodaId=" + getID();
        if (maxBufferSize > 0) {
            udl += "&bufSize=" + maxBufferSize;
        }
        if (tcpSendBuf > 0) {
            udl += "&tcpSend=" + tcpSendBuf;
        }
        if (noDelay) {
            udl += "&noDelay";
        }
        emuDomain = new cMsg(udl, name, "emu domain client");
// TODO: Put timeout here!!!
        emuDomain.connect();
System.out.println("UDL = " + udl);
        startOutputThread();
    }


    private void closeOutputChannel() throws cMsgException {
        emuDomain.disconnect();
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        if (input) return;
        try {
            openOutputChannel();
        }
        catch (cMsgException e) {
            e.printStackTrace();
            throw new CmdExecException(e);
        }
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
        logger.warn("      DataChannel Emu end() : " + name + " - end threads & close ET system");

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
//System.out.println("      DataChannelImplEmu.end : waiting for helper threads to end ...");
            if (dataInputThread != null) {
//System.out.println("        try joining input thread #" + i + " ...");
                    dataInputThread.join(waitTime);
                    // kill it if not already dead since we waited as long as possible
                    dataInputThread.interrupt();
//System.out.println("        in thread done");
            }

            if (dataOutputThread != null) {
                waitTime = emu.getEndingTimeLimit();
//System.out.println("        try joining output thread #" + i + " for " + (waitTime/1000) + " sec");
                    dataOutputThread.join(waitTime);
                    // kill everything since we waited as long as possible
                    dataOutputThread.interrupt();
                    dataOutputThread.shutdown();
//System.out.println("        out thread done");
                }
//System.out.println("      helper thds done");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        state = CODAState.DOWNLOADED;
System.out.println("      end() is done");

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
logger.debug("      DataChannel Emu reset() : " + name + " channel, in threads = 1");

        gotEndCmd   = false;
        gotResetCmd = true;

        // Don't close ET system until helper threads are done
        if (dataInputThread != null) {
//System.out.println("        interrupt input thread #" + i + " ...");
                dataInputThread.interrupt();
                // Make sure the thread is done, otherwise you risk
                // killing the ET system while a getEvents() call is
                // still in progress (with et-14.0 this is OK).
                // Give it 25% more time than the wait.
                try {dataInputThread.join(400);}  // 625
                catch (InterruptedException e) {}
//System.out.println("        input thread done");
        }

        if (dataOutputThread != null) {
//System.out.println("        interrupt output thread #" + i + " ...");
                dataOutputThread.interrupt();
                dataOutputThread.shutdown();
                // Make sure all threads are done, otherwise you risk
                // killing the ET system while a new/put/dumpEvents() call
                // is still in progress (with et-14.0 this is OK).
                // Give it 25% more time than the wait.
                try {dataOutputThread.join(1000);}
                catch (InterruptedException e) {}
//System.out.println("        output thread done");
        }

        errorMsg.set(null);
        state = CODAState.CONFIGURED;
logger.debug("      DataChannel Emu reset() : " + name + " - done");
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
     * Class used to get data over network events, parse them into Evio banks,
     * and put them onto a ring buffer.
     */
    private class DataInputHelper extends Thread {

        /** Variable to print messages when paused. */
        private int pauseCounter = 0;

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch latch = new CountDownLatch(1);


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

                   // 1st byte has command
                    switch (command & 0xf) {
                        case cMsgConstants.emuEvioFile:
                            if (queueItemType == QueueItemType.PayloadBank) {
                                handleEvioFileToBank();
                            }
                            else if  (queueItemType == QueueItemType.PayloadBuffer) {
                                handleEvioFileToBuf();
                            }

                            break;

                        case cMsgConstants.emuEvioEvent:
                            // EventType in 2nd byte
                            EventType etype   = EventType.getEventType(command >> 8 & 0xf);
                            // ControlType in 3rd byte
                            ControlType ctype = ControlType.getControlTypeFromOrdinal(command >> 16 & 0xf);
//System.out.println("      DataChannel Emu in helper: type ordinal val = " + (command >> 8 & 0xf) +
//                           ", cmd ordinal val = " + (command >> 16 & 0xf));

                            handleEvioEvent(etype, ctype);
                            break;

                        case cMsgConstants.emuEnd:
                            break;

                        default:
                            System.out.println("Unknown command from Emu client");
                    }

                    if (haveInputEndEvent) {
                        break;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Emu in helper: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                e.printStackTrace();
                logger.warn("      DataChannel Emu in helper: " + name + " exit thd: " + e.getMessage());
            }

        }



        /**
         * Handle single incoming evio event over socket.
         * Event is not in evio file format, just comes by itself.
         * This method is only used when sending from RocSimulation module
         * and "Q" type is PayloadBuffer.
         *
         * @param eventType      type of event to parse
         * @param controlType    type of control event to parse
         * @throws IOException   failure reading socket
         * @throws EvioException failure parsing evio data
         */
        private final void handleEvioEvent(EventType eventType, ControlType controlType)
                                    throws IOException, EvioException {

            int recordId = 0;

            // Read the length of evio file-format data to come
            int evioBytes = in.readInt();
            if (evioBytes > maxBufferSize) {
                // TODO: throw an exception here
            }

//System.out.println("      DataChannel Emu in helper: wait for next ring buf for writing, bytes = " + evioBytes);
            nextRingItem = ringBufferIn.next();
//System.out.println("      DataChannel Emu in helper: Got sequence " + nextRingItem);

            PayloadBuffer payloadBuffer = (PayloadBuffer) ringBufferIn.get(nextRingItem);
            byte[] array;
            ByteBuffer buf = payloadBuffer.getBuffer();
            if (buf == null || buf.capacity() < evioBytes) {
                array = new byte[evioBytes];
                buf = ByteBuffer.wrap(array);
            }
            else {
                array = buf.array();
            }

            // Read evio file-format data
            in.readFully(array, 0, evioBytes);
//TODO: should not have to make a new reader each time
            EvioCompactStructureHandler eventReader = new EvioCompactStructureHandler(buf, DataType.BANK);
            EvioNode node = eventReader.getScannedStructure();

//logger.info("      DataChannel Emu in helper: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId);

            // Complication: from the ROC, we'll be receiving USER events
            // mixed in with and labeled as ROC Raw events. Check for that
            // and fix it.
            EventType bankType = eventType;
            if (eventType == EventType.ROC_RAW) {
                if (Evio.isUserEvent(node)) {
                    bankType = EventType.USER;
                }
            }

            payloadBuffer.setBuffer(node.getStructureBuffer(false));
            payloadBuffer.setNode(node);
            payloadBuffer.setEventType(bankType);
            payloadBuffer.setControlType(controlType);
            payloadBuffer.setRecordId(recordId);
            payloadBuffer.setSourceId(sourceId);
            payloadBuffer.setSourceName(name);
//            payloadBuffer.setSequence(nextRingItem);
            payloadBuffer.matchesId(sourceId == id);


            ringBufferIn.publish(nextRingItem);
//System.out.println("      DataChannel Emu in helper: published sequence " + nextRingItem);

            // Handle END event
            if (controlType == ControlType.END) {
                logger.info("      DataChannel Emu in helper: " + name + " found END event");
                haveInputEndEvent = true;
                // Run callback saying we got end event
                if (endCallback != null) endCallback.endWait();
            }
        }



        private final void handleEvioFileToBank() throws IOException, EvioException {

            EvioEvent event;
            EventType bankType;
            PayloadBank payloadBank;
            ControlType controlType = null;

            // Get a reusable ByteBuffer
            logger.debug("      DataChannel Emu in helper: get buffer from ring");
            ByteBufferItem  bbItem = bbSupply.get();
            ByteBuffer buf = bbItem.getBuffer();

            // Read the length of evio file-format data to come
            int evioBytes = in.readInt();

            // If buffer is too small, make a bigger one
            if (evioBytes > bbItem.getBufferSize()) {
                buf = ByteBuffer.allocate(evioBytes);
                bbItem.setBuffer(buf);
            }

            // Read evio file-format data
            in.readFully(buf.array(), 0, evioBytes);

            EvioReader reader;
            try {
                reader = new EvioReader(buf);
            }
            catch (IOException e) {
                errorMsg.compareAndSet(null, "Data is NOT in evio v4 format");
                throw e;
            }
            // Speed things up since no EvioListeners are used - doesn't do much
            reader.getParser().setNotificationActive(false);

            // First block header in buffer
            BlockHeaderV4 blockHeader = (BlockHeaderV4)reader.getFirstBlockHeader();
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

//logger.info("      DataChannel Emu in helper: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId);

//System.out.println("      DataChannel Emu in helper: parse next event");
            while ((event = reader.parseNextEvent()) != null) {
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

//System.out.println("      DataChannel Emu in helper: wait for next ring buf for writing");
                nextRingItem = ringBufferIn.next();
//System.out.println("      DataChannel Emu in helper: Got sequence " + nextRingItem);
                payloadBank = (PayloadBank) ringBufferIn.get(nextRingItem);

                payloadBank.setEvent(event);
                payloadBank.setEventType(bankType);
                payloadBank.setControlType(controlType);
                payloadBank.setRecordId(recordId);
                payloadBank.setSourceId(sourceId);
                payloadBank.setSourceName(name);
//                payloadBank.setSequence(nextRingItem);
                payloadBank.matchesId(sourceId == id);

                ringBufferIn.publish(nextRingItem);
//System.out.println("      DataChannel Emu in helper: published sequence " + nextRingItem);

                // Handle end event ...
                if (controlType == ControlType.END) {
                    // There should be no more events coming down the pike so
                    // go ahead write out existing events and then shut this
                    // thread down.
                    logger.info("      DataChannel Emu in helper: " + name + " found END event");
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

            // If buffer is too small, make a bigger one
            if (evioBytes > bbItem.getBufferSize()) {
                buf = ByteBuffer.allocate(evioBytes);
//System.out.println("RESETTTTTTTTTTTTTTTTTT  SIZEEEEEEEEEEEEEEEEEEEE");
                bbItem.setBuffer(buf);
            }

            // Read evio file-format data
            in.readFully(buf.array(), 0, evioBytes);

            EvioCompactReader compactReader;
            try {
                compactReader = new EvioCompactReader(buf);
            }
            catch (EvioException e) {
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

//logger.info("      DataChannel Emu in helper: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId + ", event cnt = " + eventCount);

            for (int i=1; i < eventCount+1; i++) {
//System.out.println("      DataChannel Emu in helper: get next event");
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

//System.out.println("      DataChannel Emu in helper: wait for next ring buf for writing");
                nextRingItem = ringBufferIn.next();
//System.out.println("      DataChannel Emu in helper: Got sequence " + nextRingItem);
                payloadBuffer = (PayloadBuffer) ringBufferIn.get(nextRingItem);
                payloadBuffer.setBuffer(node.getStructureBuffer(false));
                payloadBuffer.setEventType(bankType);
                payloadBuffer.setControlType(controlType);
                payloadBuffer.setRecordId(recordId);
                payloadBuffer.setSourceId(sourceId);
                payloadBuffer.setSourceName(name);
                payloadBuffer.setNode(node);
//                payloadBuffer.setSequence(nextRingItem);
                payloadBuffer.setReusableByteBuffer(bbSupply, bbItem);
                payloadBuffer.matchesId(sourceId == id);

                ringBufferIn.publish(nextRingItem);
//System.out.println("      DataChannel Emu in helper: published sequence " + nextRingItem);

                // Handle end event ...
                if (controlType == ControlType.END) {
                    // There should be no more events coming down the pike so
                    // go ahead write out existing events and then shut this
                    // thread down.
                    logger.info("      DataChannel Emu in helper: " + name + " found END event");
                    haveInputEndEvent = true;
                    // run callback saying we got end event
                    if (endCallback != null) endCallback.endWait();
                    break;
                }
            }
        }

    }


    /**
     * Class used to take Evio banks from ring buffer, write them over
     * network to an Emu using the Emu domain input channel.
     */
    private class DataOutputHelper extends Thread {

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch startLatch = new CountDownLatch(1);

        private EventWriter writer;

        private ByteBuffer byteBuffer;




         /** Constructor. */
        DataOutputHelper() {
            super(emu.getThreadGroup(), name() + "_data_out");
            byteBuffer = ByteBuffer.allocate(maxBufferSize);
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
            if (!marshallOutput) {
                return;
            }
            // If we have no more room in buffer, send what we have so far
            writer.close();
            outGoingMsg.setUserInt(cMsgConstants.emuEvioFile);
            outGoingMsg.setByteArray(byteBuffer.array(), 0,
                                     (int) writer.getBytesWrittenToBuffer());
            emuDomain.send(outGoingMsg);
            // Reset writer ..
            writer.setBuffer(byteBuffer);
        }


        /**
         * Gets the next ring buffer item placed there by the last module.
         * Only call from one thread. MUST be followed by call to
         * {@link #releaseOutputRingItem(int)} AFTER the returned item
         * is used or nothing will work right.
         *
         * @param ringIndex ring buffer to take item from
         * @return next ring buffer item
         */
        private RingItem getNextOutputRingItem(int ringIndex)
                throws InterruptedException, EmuException {

            RingItem item = null;
//            System.out.println("getNextOutputRingITem: index = " + ringIndex);
//            System.out.println("                     : availableSequences = " + availableSequences[ringIndex]);
//            System.out.println("                     : nextSequences = " + nextSequences[ringIndex]);

            try  {
                // Only wait for read-volatile-memory if necessary ...
                if (availableSequences[ringIndex] < nextSequences[ringIndex]) {
    //System.out.println("getNextOutputRingITem: WAITING");
                    availableSequences[ringIndex] = sequenceBarriers[ringIndex].waitFor(nextSequences[ringIndex]);
                }
    //System.out.println("getNextOutputRingITem: available seq = " + availableSequences[ringIndex]);

                item = ringBuffers[ringIndex].get(nextSequences[ringIndex]);
    //System.out.println("getNextOutputRingITem: got seq = " + nextSequences[ringIndex]);
//System.out.println("Got ring item " + item.getRecordId());
            }
            catch (final TimeoutException ex) {
                /* never happen since we don't use timeout wait strategy */
                ex.printStackTrace();
            }
            catch (final AlertException ex) {
                ex.printStackTrace();
                throw new EmuException("Ring buffer error",ex);
            }

            return item;
        }


        /**
         * Releases the item obtained by calling {@link #getNextOutputRingItem(int)},
         * so that it may be reused for writing into by the last module.
         * @param ringIndex ring buffer to release item to
         */
        private void releaseOutputRingItem(int ringIndex) {
            sequences[ringIndex].set(nextSequences[ringIndex]);
            nextSequences[ringIndex]++;
        }



        private final void writeEvioData(PayloadBuffer buffer, EventType eType) throws cMsgException,
                                                                IOException,
                                                                EvioException {

            // If we're marshalling events into a single buffer before sending ...
            if (marshallOutput && !eType.isControl()) {
                // If we have no more room in buffer ...
                if (!writer.hasRoom(buffer.getTotalBytes())) {
                    flushEvents();
                }

                // Write the new event ..
                writer.writeEvent(buffer.getBuffer());
                buffer.releaseByteBuffer();
            }
            else {
                // Encode some info about this event into an integer
                int cmd = cMsgConstants.emuEvioEvent | (buffer.getEventType().getValue() << 8);
                ControlType cType = buffer.getControlType();
                if (cType != null)  {
                    cmd |= (cType.getOrdinalValue() << 16);
                }

                outGoingMsg.setUserInt(cmd);
                outGoingMsg.setByteArray(buffer.getBuffer().array(), 0, buffer.getTotalBytes());
                emuDomain.send(outGoingMsg);
            }
        }


        private final void writeEvioData(PayloadBank bank, EventType eType) throws cMsgException,
                                                                  IOException,
                                                                  EvioException {
            if (marshallOutput && !eType.isControl()) {
//System.out.println("writeEvioData: total bytes = " + bank.getTotalBytes() + ", has room = " +
//                           writer.hasRoom(bank.getTotalBytes()));
                if (!writer.hasRoom(bank.getTotalBytes())) {
                    flushEvents();
                }

                writer.writeEvent(bank.getEvent());
            }
            else {
                // Encode some info about this event into an integer
                //System.out.println("writeEvioData: bank ev type = " + bank.getEventType());
                int cmd = cMsgConstants.emuEvioEvent | (bank.getEventType().getValue() << 8);
                ControlType cType = bank.getControlType();
                if (cType != null)  {
                    //System.out.println("writeEvioData: control ev type = " + cType + ", ordinal val = " + cType.getOrdinalValue());
                    cmd |= (cType.getOrdinalValue() << 16);
                }

                outGoingMsg.setUserInt(cmd);
                ByteBuffer buf = ByteBuffer.allocate(bank.getTotalBytes());
                bank.getEvent().write(buf);
                outGoingMsg.setByteArray(buf.array());
                emuDomain.send(outGoingMsg);
            }
        }



        /** {@inheritDoc} */
        @Override
        public void run() {
logger.debug("      DataChannel Emu out helper: started");

            // Tell the world I've started
            startLatch.countDown();

            try {
                PayloadBank pBank;
                PayloadBuffer pBuffer;
                RingItem ringItem;
                int ringChunkCounter = ringChunk;

                // Create writer to write events into file format
                if (marshallOutput) {
                    writer = new EventWriter(byteBuffer);
                }

                // First event will be "prestart", by convention in ring 0
                ringItem = getNextOutputRingItem(0);
                if (queueItemType == QueueItemType.PayloadBank) {
                    pBank = (PayloadBank)ringItem;
                    writeEvioData(pBank, ringItem.getEventType());
                }
                else if  (queueItemType == QueueItemType.PayloadBuffer) {
                    pBuffer = (PayloadBuffer)ringItem;
                    writeEvioData(pBuffer, ringItem.getEventType());
                }
                releaseOutputRingItem(0);

                // First event will be "go", by convention in ring 0
                ringItem = getNextOutputRingItem(0);
                if (queueItemType == QueueItemType.PayloadBank) {
                    pBank = (PayloadBank)ringItem;
                    writeEvioData(pBank, ringItem.getEventType());
                }
                else if  (queueItemType == QueueItemType.PayloadBuffer) {
                    pBuffer = (PayloadBuffer)ringItem;
                    writeEvioData(pBuffer, ringItem.getEventType());
                }
                releaseOutputRingItem(0);

                while ( true ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) {
                            try {Thread.sleep(5);}
                            catch (InterruptedException e1) {}
                        }
                        continue;
                    }

//logger.debug("      DataChannel Emu out helper: get next buffer from ring");
                    ringItem = getNextOutputRingItem(rbIndex);
                    ControlType pBankControlType = ringItem.getControlType();

                    if (queueItemType == QueueItemType.PayloadBank) {
                        pBank = (PayloadBank)ringItem;
                        // Write over socket ...
//logger.debug("      DataChannel Emu out helper: write bank over net");
                        writeEvioData(pBank, ringItem.getEventType());
                    }
                    else if  (queueItemType == QueueItemType.PayloadBuffer) {
                        pBuffer = (PayloadBuffer)ringItem;
                        // Write over socket ...
                        writeEvioData(pBuffer, ringItem.getEventType());
                    }

//logger.debug("      DataChannel Emu out helper: release ring item");
                    releaseOutputRingItem(rbIndex);
                    if (--ringChunkCounter < 1) {
                        rbIndex = ++rbIndex % ringCount;
                        ringChunkCounter = ringChunk;
//                        System.out.println("switch ring to "+ rbIndex);
                    }
                    else {
//                        System.out.println(""+ ringChunkCounter);
                    }

                    if (pBankControlType == ControlType.END) {
                        flushEvents();
System.out.println("      DataChannel Emu out helper: " + name + " I got END event, quitting");
                        // run callback saying we got end event
                        if (endCallback != null) endCallback.endWait();
                        break;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd || gotEndCmd) {
                        System.out.println("      DataChannel Emu out helper: " + name + " got RESET/END cmd, quitting 1");
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Emu out helper: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
                logger.warn("      DataChannel Emu out helper : exit thd: " + e.getMessage());
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                e.printStackTrace();
            }

        }


    }



}