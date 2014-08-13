/*
 * Copyright (c) 2009, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.jevio.*;


import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.*;
import java.util.Map;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class implement a data channel which gets data from
 * or sends data to a cMsg server.
 *
 * @author timmer
 * (Dec 2, 2009)
 */
public class DataChannelImplCmsg extends DataChannelAdapter {

    /** Field transport */
    private final DataTransportImplCmsg dataTransportImplCmsg;

    /** Subject of either subscription or outgoing messages. */
    private String subject;

    /** Type of either subscription or outgoing messages. */
    private String type;

    /** Do we pause the dataThread? */
    private boolean pause;

    /** Enforce evio block header numbers to be sequential? */
    private boolean blockNumberChecking;

    /** Read END event from input queue. */
    private volatile boolean haveInputEndEvent;

    /** Read END event from output queue. */
    private volatile boolean haveOutputEndEvent;

    /** Got END command from Run Control. */
    private volatile boolean gotEndCmd;

    /** Got RESET command from Run Control. */
    private volatile boolean gotResetCmd;

    // INPUT

    /** cMsg subscription for receiving messages with data. */
    private cMsgSubscriptionHandle sub;

    // OUTPUT

    /** Number of writing threads to ask for in copying data from banks to cMsg messages. */
    private int writeThreadCount;

    /** Array of threads used to output data. */
    private DataOutputHelper dataOutputThread;


    /**
     * Fill up a message with banks until it reaches this size limit in bytes
     * before another is used. Unless you have one big one which gets sent by itself.
     */
    private int outputSizeLimit = 256000;

    /** Fill up a message with at most this number of banks before another is used. */
    private int outputCountLimit = 10000;


// TODO: does this need to be reset? I think so ...
    /** Use the evio block header's block number as a record id. */
    private int recordId;

    //-------------------------------------------
    // Disruptor (RingBuffer)  Stuff
    //-------------------------------------------

//    /** Ring buffer holding ByteBuffers when using EvioCompactEvent reader for incoming events. */
//    protected ByteBufferSupply bbSupply;   // input

    private int rbIndex;



    private final void messageToBank(cMsgMessage msg) throws IOException, EvioException {

//logger.info("      DataChannel cmsg in helper: " + name + ": got BANK message in callback");

        byte[] data = msg.getByteArray();
        if (data == null) {
            errorMsg.compareAndSet(null, "cMsg message data has no data");
            throw new EvioException("cMsg message data has no data");
        }

        ByteBuffer buf = ByteBuffer.wrap(data);

        try {
            EvioReader eventReader = new EvioReader(buf, blockNumberChecking);

            // Speed things up since no EvioListeners are used - doesn't do much
            eventReader.getParser().setNotificationActive(false);

            // First block header in buffer
            IBlockHeader blockHeader = eventReader.getFirstBlockHeader();
            int evioVersion = blockHeader.getVersion();
            if (evioVersion < 4) {
                errorMsg.compareAndSet(null, "Evio data needs to be written in version 4+ format");
                throw new EvioException("Evio data needs to be written in version 4+ format");
            }
            BlockHeaderV4 header4 = (BlockHeaderV4)blockHeader;
            // eventType may be null if no type info exists in block header.
            // But it should always be there if reading from ROC or DC.
            EventType eventType = EventType.getEventType(header4.getEventType());
            ControlType controlType = null;
            int sourceId = header4.getReserved1();

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
            int recordId = header4.getNumber();

//logger.info("      DataChannel cmsg in helper: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId);

//System.out.println("      DataChannel cmsg in helper: parse next event");
            EvioEvent event;
            EventType bankType;
            long nextRingItem;

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
                    controlType = ControlType.getControlType(event.getHeader().getTag());
                    if (controlType == null) {
                        errorMsg.compareAndSet(null, "Found unidentified control event");
                        throw new EvioException("Found unidentified control event");
                    }
                }

                nextRingItem = ringBufferIn.next();
                RingItem ringItem = ringBufferIn.get(nextRingItem);

                ringItem.setEvent(event);
                ringItem.setEventType(bankType);
                ringItem.setControlType(controlType);
                ringItem.setRecordId(recordId);
                ringItem.setSourceId(sourceId);
                ringItem.setSourceName(name);
                ringItem.setEventCount(1);
                ringItem.matchesId(sourceId == id);

                ringBufferIn.publish(nextRingItem);

                // Handle end event ...
                if (controlType == ControlType.END) {
                    // There should be no more events coming down the pike so
                    // go ahead write out existing events and then shut this
                    // thread down.
                    logger.info("      DataChannel cmsg in helper: " + name + " found END event");
                    haveInputEndEvent = true;
                    // run callback saying we got end event
                    if (endCallback != null) endCallback.endWait();
                    return;
                }
            }
        }
        catch (Exception e) {
            // If we haven't yet set the cause of error, do so now & inform run control
            errorMsg.compareAndSet(null, "cMsg message data has invalid format");

            // set state
            state = CODAState.ERROR;
            emu.sendStatusMessage();
        }
    }


    private final void messageToBuf(cMsgMessage msg) throws IOException, EvioException {

        byte[] data = msg.getByteArray();
        if (data == null) {
            errorMsg.compareAndSet(null, "cMsg message data has no data");
            throw new EvioException("cMsg message data has no data");
        }

        ByteBuffer buf = ByteBuffer.wrap(data);

        try {
            EvioCompactReader compactReader = new EvioCompactReader(buf);

            BlockHeaderV4 blockHeader = compactReader.getFirstBlockHeader();
            if (blockHeader.getVersion() < 4) {
                errorMsg.compareAndSet(null, "Data is NOT evio v4 format");
                throw new EvioException("Evio data needs to be written in version 4+ format");
            }

            EventType eventType = EventType.getEventType(blockHeader.getEventType());
            int recordId = blockHeader.getNumber();
            int sourceId = blockHeader.getReserved1();
            int eventCount = compactReader.getEventCount();
//logger.info("      DataChannel cmsg in helper: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId + ", event cnt = " + eventCount);

            EvioNode node;
            EventType bankType;
            ControlType controlType = null;
            long nextRingItem;

            for (int i=1; i < eventCount+1; i++) {
                node = compactReader.getScannedEvent(i);

                bankType = eventType;
                if (eventType == EventType.ROC_RAW) {
                    if (Evio.isUserEvent(node)) {
                        bankType = EventType.USER;
                    }
                }
                else if (eventType == EventType.CONTROL) {
                    controlType = ControlType.getControlType(node.getTag());
                    if (controlType == null) {
                        errorMsg.compareAndSet(null, "Found unidentified control event");
                        throw new EvioException("Found unidentified control event");
                    }
                }

                nextRingItem = ringBufferIn.next();
                RingItem ringItem = ringBufferIn.get(nextRingItem);

                ringItem.setNode(node);
                ringItem.setBuffer(node.getStructureBuffer(false));
                ringItem.setBuffer(buf);
                ringItem.setEventType(bankType);
                ringItem.setControlType(controlType);
                ringItem.setRecordId(recordId);
                ringItem.setSourceId(sourceId);
                ringItem.setSourceName(name);
                ringItem.setEventCount(1);
                ringItem.matchesId(sourceId == id);

                ringBufferIn.publish(nextRingItem);

                if (controlType == ControlType.END) {
                    logger.info("      DataChannel Emu in helper: " + name + " found END event");
                    haveInputEndEvent = true;
                    if (endCallback != null) endCallback.endWait();
                    return;
                }
            }
        }
        catch (EvioException e) {
            e.printStackTrace();
            errorMsg.compareAndSet(null, "Data is NOT evio v4 format");
            throw e;
        }

    }


    /**
     * This class defines the callback to be run when a message matching the subscription arrives.
     */
    class ReceiveMsgCallback extends cMsgCallbackAdapter {

        /** Callback method definition. */
        public void callback(cMsgMessage msg, Object userObject) {

//logger.info("      DataChannel cmsg in helper: " + name + ": got message in callback");
            try {
                if (ringItemType == ModuleIoType.PayloadBank) {
                    messageToBank(msg);
                }
                else if (ringItemType == ModuleIoType.PayloadBuffer) {
                    messageToBuf(msg);
                }
            }
            catch (Exception e) {

                e.printStackTrace();


                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, "cMsg message data has invalid format");

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();
            }
        }

        // Define "getMaximumCueSize" to set max number of unprocessed messages kept locally
        // before things "back up" (potentially slowing or stopping senders of messages of
        // this subject and type). Default = 1000.
    }


    /**
     * Constructor to create a new DataChannelImplCmsg instance. Used only by
     * {@link DataTransportImplCmsg#createChannel(String, Map, boolean, Emu, EmuModule)}
     * which is only used during PRESTART in the EmuModuleFactory.
     *
     * @param name          the name of this channel
     * @param transport     the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     * @param emu           emu this channel belongs to
     * @param module        module this channel belongs to
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplCmsg(String name, DataTransportImplCmsg transport,
                        Map<String, String> attributeMap, boolean input, Emu emu,
                        EmuModule module)
                throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module);

        dataTransportImplCmsg = transport;


        // Set option whether or not to enforce evio
        // block header numbers to be sequential
        String attribString = attributeMap.get("blockNumCheck");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("true") ||
                attribString.equalsIgnoreCase("on")   ||
                attribString.equalsIgnoreCase("yes"))   {
                blockNumberChecking = true;
            }
        }

        // Set subject & type for either subscription (incoming msgs) or for outgoing msgs.
        // Use any defined in config file else use defaults.
        subject = attributeMap.get("subject");
        if (subject == null) subject = name;

        type = attributeMap.get("type");
        if (type == null) type = "data";

        if (input) {
            try {
                // create subscription for receiving messages containing data
                ReceiveMsgCallback cb = new ReceiveMsgCallback();
System.out.println("\n\nDataChannel: subscribe to subject = " + subject + ", type = " + type + "\n\n");
                sub = dataTransportImplCmsg.getCmsgConnection().subscribe(subject, type, cb, null);
            }
            catch (cMsgException e) {
                logger.info("      DataChannelImplCmsg.const : " + e.getMessage());
                throw new DataTransportException(e);
            }
        }
        else {
            // Tell emu what that output name is for stat reporting
            emu.setOutputDestination("cMsg");

            // How may cMsg message buffer filling threads for each data output thread?
            writeThreadCount = 1;
            attribString = attributeMap.get("wthreads");
            if (attribString != null) {
                try {
                    writeThreadCount = Integer.parseInt(attribString);
                    if (writeThreadCount <  1) writeThreadCount = 1;
                    if (writeThreadCount > 10) writeThreadCount = 10;
                }
                catch (NumberFormatException e) {}
            }
logger.info("      DataChannel cMsg : write threads = " + writeThreadCount);

            dataOutputThread = new DataOutputHelper(emu.getThreadGroup(), name() + " data out");
            dataOutputThread.start();
            dataOutputThread.waitUntilStarted();
        }
    }


    /**
     * Method to print out the bank for diagnostic purposes.
     * @param bank bank to print out
     * @param bankName name of bank for printout
     */
    private void printBank(EvioBank bank, String bankName) {
        try {
            StringWriter sw2 = new StringWriter(1000);
            XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
            bank.toXML(xmlWriter);
            if (bankName == null) {
                System.out.println("bank:\n" + sw2.toString());
            }
            else {
                System.out.println("bank " + bankName + ":\n" + sw2.toString());
            }
        }
        catch (XMLStreamException e) {
            e.printStackTrace();
        }
    }


    /** {@inheritDoc} */
    public void go() {
        if (input) {
            sub.restart();
        }
        else {
            pause = false;
        }
    }


    /** {@inheritDoc} */
    public void pause() {
        if (input) {
            sub.pause();
        }
        else {
            pause = true;
        }
    }


    /** {@inheritDoc} */
    public void end() {

        logger.warn("      DataChannel cMsg close() : " + name + " - closing this channel");

        gotEndCmd = true;
        gotResetCmd = false;

        // Do NOT interrupt threads which are communicating with the cMsg server.
        // This will mess up future communications !!!

        // How long do we wait for each output thread
        // to end before we just terminate them?
        // The total time for an emu to wait for the END transition
        // is emu.endingTimeLimit. Dividing that by the number of
        // output threads is probably a good guess.
        long waitTime;

        // Don't unsubscribe until helper threads are done
        try {
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

        // At this point all threads should be done
        if (sub != null) {
            try {
                dataTransportImplCmsg.getCmsgConnection().unsubscribe(sub);
            } catch (cMsgException e) {/* ignore */}
        }

        errorMsg.set(null);
        state = CODAState.CONFIGURED;
//logger.debug("      DataChannel cMsg reset() : " + name + " - done");
    }


    /**
     * {@inheritDoc}.
     * Reset this channel by interrupting the message sending threads and unsubscribing.
     */
    public void reset() {
logger.debug("      DataChannel cMsg reset() : " + name + " - resetting this channel");

        gotEndCmd   = false;
        gotResetCmd = true;

        // Don't unsubscribe until helper threads are done
        if (dataOutputThread != null) {
//System.out.println("        interrupt output thread #" + i + " ...");
            dataOutputThread.interrupt();
            dataOutputThread.shutdown();
            // Make sure all threads are done.
            try {
                dataOutputThread.join(1000);}
            catch (InterruptedException e) {}
//System.out.println("        output thread done");
        }

        // At this point all threads should be done
        if (sub != null) {
            try {
                dataTransportImplCmsg.getCmsgConnection().unsubscribe(sub);
            } catch (cMsgException e) {/* ignore */}
        }

        errorMsg.set(null);
        state = CODAState.CONFIGURED;

//logger.debug("      DataChannel cMsg reset() : " + name + " - done");
    }


    /**
     * Start the startOutputHelper thread which takes a bank from
     * the queue, puts it in a message, and sends it.
     */
    private void startOutputHelper() {
        dataOutputThread = new DataOutputHelper(emu.getThreadGroup(),
                                                           name() + " data out");
    }





    /**
     * Class used to take Evio banks from Q, write them into cMsg messages.
     */
    private class DataOutputHelperNew extends Thread {

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch startLatch = new CountDownLatch(1);

        private ByteBufferSupply bufferSupply;

        private MessageWritingThread msgWritingThread;

        private final cMsg serverConnection;

        /** Object for writing banks into message's data buffer. */
        private EventWriter evWriter;



         /** Constructor. */
        DataOutputHelperNew(ThreadGroup group, String name) {
            super(group, name);

            bufferSupply = new ByteBufferSupply(1024, outputSizeLimit);
            serverConnection = dataTransportImplCmsg.getCmsgConnection();
            msgWritingThread = new MessageWritingThread();
            msgWritingThread.start();
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
            }
        }


        /** Stop all this object's threads. */
        private void shutdown() {
        }


        /**
         * Thread to write cMsg messages.
         */
        class MessageWritingThread extends Thread {

            private final int endian;
            private final cMsgMessage message;


            MessageWritingThread() {
                endian = byteOrder == ByteOrder.BIG_ENDIAN ? cMsgConstants.endianBig :
                                                             cMsgConstants.endianLittle;
                message = new cMsgMessage();
                message.setSubject(subject);
                message.setType(type);
                try { message.setByteArrayEndian(endian); }
                catch (cMsgException e) {/* never happen */}
            }


            @Override
            public void run() {
                ByteBuffer buf;
                ByteBufferItem item;

                try {
                    while(true) {
                        item = bufferSupply.consumerGet();
                        if (item != null) {
                            // Put data into cMsg message
                            buf = item.getBuffer();
                            message.setByteArrayNoCopy(buf.array(), 0, buf.limit());
                            // Send msg
                            serverConnection.send(message);
                            bufferSupply.consumerRelease(item);
                        }
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
            }

        }




        /**
         * Encode the event type into the bit info word
         * which will be in each evio block header.
         *
         * @param bSet bit set which will become part of the bit info word
         * @param type event type to be encoded
         */
        void setEventType(BitSet bSet, int type) {
            // check args
            if (type < 0) type = 0;
            else if (type > 15) type = 15;

//            if (bSet == null || bSet.size() < 6) {
//                return;
//            }

            bSet.clear();

            // do the encoding
            for (int i=2; i < 6; i++) {
                bSet.set(i, ((type >>> i - 2) & 0x1) > 0);
            }
        }


        @Override
        public void run() {

            // Tell the world I've started
            startLatch.countDown();

            try {
                RingItem ringItem;
                ByteBuffer buffer;
                ByteBufferItem bufferItem;
                int pBankSize=0, listTotalSizeMax, eventCount;

                EventType previousType, pBanktype = null;
                ControlType pBankControlType = null;
                BitSet bitInfo = new BitSet(24);

                // Place to store a bank off the ring buf when
                // already have enough for one message to be filled.
                RingItem firstBankFromRing = null;

                // RocSimulation generates "ringChunk" sequential events at once,
                // so, a single ring will have ringChunk sequential events together.
                // Take this into account when reading from multiple rings.
                // We must get the output order right.
                int ringChunkCounter = outputRingChunk;

                // Create an array of objects produced by preceding module
                RingItem[] bankArray = new RingItem[outputCountLimit];
                int bankArrayCount = 0;


                while ( serverConnection.isConnected() ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) Thread.sleep(5);
                        continue;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        shutdown();
                        return;
                    }

                    // First, clear array (rather the index)
                    bankArrayCount = 0;

                    // Init variables
                    eventCount = 0;
                    listTotalSizeMax = 32;
                    previousType = null;

                    // Grab a bank to put into a cMsg buffer,
                    // checking occasionally to see if we got an
                    // RESET command or someone found an END event.
                    while (!gotResetCmd) {
// System.out.println("      DataChannel cMsg out helper: try getting ring item");
                        // Get bank off of ring ...
                        if (firstBankFromRing == null) {
                            ringItem  = getNextOutputRingItem(rbIndex);
//System.out.println("      DataChannel cMsg out helper: GOT ring item");

                            pBanktype = ringItem.getEventType();
                            pBankSize = ringItem.getTotalBytes();
                            pBankControlType = ringItem.getControlType();
                            eventCount++;
                            // Assume worst case (largest size) of one block header/bank
                            listTotalSizeMax += pBankSize + 32;
                        }
                        // Unless we already did so in the previous loop
                        else {
                            ringItem = firstBankFromRing;
                            firstBankFromRing = null;
                            listTotalSizeMax = pBankSize + 64;
                            // Set recordId depending on what type this bank is
                            if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
                                recordId++;
                            }
                            else {
                                recordId = -1;
                            }
                        }

                        // This the first time through the while loop
                        if (previousType == null) {
                            // Add bank to the array since there's always room for one
                            bankArray[bankArrayCount++] = ringItem;

                            // Set recordId depending on what type this bank is
                            if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
                                recordId++;
                            }
                            else {
                                recordId = -1;
                            }
                        }
                        // Do we only want 1 evio event / message?
                        // Is this bank a diff type as previous bank?
                        // Will it not fit into the message target size?
                        // Will it be > the target number of banks per message?
                        // In all these cases start using a new list.
                        else if (singleEventOut ||
                                (previousType != pBanktype) ||
                                (listTotalSizeMax >= outputSizeLimit) ||
                                (bankArrayCount + 1 > outputCountLimit)) {
//                            (bankList.size() + 1 > outputCountLimit)) {

                            // Store final value of events' maximum possible size
                            listTotalSizeMax -= pBankSize - 32;

                            // Be sure to store what we just
                            // pulled off the ring to be the next bank!
                            firstBankFromRing = ringItem;

                            // Write this list of events out
                            break;
                        }
                        // It's OK to add this bank to the existing list.
                        else {
                            // Add bank to list since there's room and it's the right type
//                            bankList.add(ringItem);
                            bankArray[bankArrayCount++] = ringItem;
                        }

                        // Set this for next round
                        previousType = pBanktype;

                        // Not an "END" event
                        ringItem.setAttachment(Boolean.FALSE);

                        // Get ready to retrieve next ring item
                        gotoNextRingItem(rbIndex);

                        // If control event, quit loop and write what we have
                        if (pBankControlType != null) {
System.out.println("      DataChannel cMsg out helper: SEND CONTROL RIGHT THROUGH: " + pBankControlType);

                            // Look for END event and mark it in attachment
                            if (pBankControlType == ControlType.END) {
                                ringItem.setAttachment(Boolean.TRUE);
                                haveOutputEndEvent = true;
System.out.println("      DataChannel cMsg out helper: " + name + " I got END event, quitting 2");
                                // run callback saying we got end event
                                if (endCallback != null) endCallback.endWait();
                            }

                            // Write this control event out
                            break;
                        }

                        // Be careful not to use up all the events in the output
                        // ring buffer before writing (& freeing up) some.
                        if (eventCount >= outputRingItemCount /2) {
                            break;
                        }
                    } // while no reset ...

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        shutdown();
                        return;
                    }

                    // TODO: ??
                    if (bankArrayCount < 1) continue;

//                    if (bankList.size() < 1) {
//                        continue;
//                    }

                    // Grab an empty byte buffer (will eventually
                    // become the byte array of a cMsg message).
                    bufferItem = bufferSupply.get();
                    // Make sure it's big enough
                    bufferItem.ensureCapacity(listTotalSizeMax);

                    buffer = bufferItem.getBuffer();
                    buffer.order(byteOrder);

                    // Encode the event type into bits
//                    setEventType(bitInfo, bankList.get(0).getEventType().getValue());
                    setEventType(bitInfo, bankArray[0].getEventType().getValue());

                    // Create object to write evio banks into message buffer
                    if (evWriter == null) {
                        evWriter = new EventWriter(buffer, 550000, 200, null, bitInfo, emu.getCodaid());
                    }
                    else {
                        evWriter.setBuffer(buffer, bitInfo);
                    }
                    evWriter.setStartingBlockNumber(recordId);

                    // Write banks into message buffer
                    if (ringItemType == ModuleIoType.PayloadBank) {
//                        for (RingItem ri : bankList) {
//                            evWriter.writeEvent(ri.getEvent());
//                            ri.releaseByteBuffer();
//                        }
                        for (int i=0; i < bankArrayCount; i++) {
                            evWriter.writeEvent(bankArray[i].getEvent());
                            bankArray[i].releaseByteBuffer();
                        }
                    }
                    else {
//                        for (RingItem ri : bankList) {
//                            evWriter.writeEvent(ri.getBuffer());
//                            ri.releaseByteBuffer();
//                        }
                        for (int i=0; i < bankArrayCount; i++) {
                            evWriter.writeEvent(bankArray[i].getBuffer());
                            bankArray[i].releaseByteBuffer();
                        }
                    }

                    evWriter.close();
                    buffer.flip();

                    // Handle END event ...
//                    for (RingItem ri : bankList) {
//                        if (ri.getAttachment() == Boolean.TRUE) {
//                            // There should be no more events coming down the pike so
//                            // go ahead write out events and then shut this thread down.
//                            break;
//                        }
//                    }
                    for (int i=0; i < bankArrayCount; i++) {
                        if (bankArray[i].getAttachment() == Boolean.TRUE) {
                            // There should be no more events coming down the pike so
                            // go ahead write out events and then shut this thread down.
                            break;
                        }
                    }

                    // Make buffer available for writing thread to use
                    bufferSupply.publish(bufferItem);

//System.out.println("      DataChannel cMsg: ring release " + rbIndex);
                    releaseOutputRingItem(rbIndex);

                    if (--ringChunkCounter < 1) {
                        rbIndex = ++rbIndex % outputRingCount;
                        ringChunkCounter = outputRingChunk;
                    }

                    if (haveOutputEndEvent) {
System.out.println("      DataChannel cMsg out helper: " + name + " some thd got END event, quitting 4");
                        shutdown();
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel cMsg out helper: " + name + "  interrupted thd, exiting");

            // only cMsgException thrown
            } catch (Exception e) {
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                e.printStackTrace();
                logger.warn("      DataChannel cMsg out helper: " + name + " exit thd: " + e.getMessage());
            }

        }


    }







    /**
     * Class used to take Evio banks from Q, write them into cMsg messages.
     */
    private class DataOutputHelper extends Thread {

        /** Used to sync things before putting new cMsg messages. */
        private CountDownLatch latch;

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Thread pool for writing Evio banks into new cMsg messages. */
        private ExecutorService writeThreadPool;

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch startLatch = new CountDownLatch(1);



         /** Constructor. */
        DataOutputHelper(ThreadGroup group, String name) {
            super(group, name);

            try {
                // Thread pool with "writeThreadCount" number of threads & queue.
                writeThreadPool = Executors.newFixedThreadPool(writeThreadCount);
            }
            catch (Exception e) {
                e.printStackTrace();
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


        /** Stop all this object's threads. */
        private void shutdown() {
            // Cancel queued jobs and call interrupt on executing threads
            writeThreadPool.shutdown();

            // Only wait for threads to terminate if shutting
            // down gracefully for an END command.
            if (gotEndCmd) {
                try { writeThreadPool.awaitTermination(100L, TimeUnit.MILLISECONDS); }
                catch (InterruptedException e) {}
            }
        }


        /**
         * This method is used to send an array of cMsg messages to an cMsg server.
         * It allows coordination between multiple DataOutputHelper threads so that
         * event order is preserved.
         *
         * @param msgs           the cMsg messages to send to cMsg server
         * @param messages2Write number of messages to write
         *
         * @throws InterruptedException if wait interrupted
         * @throws IOException cMsg communication error
         * @throws cMsgException problems sending message(s) to cMsg server
         */
        private void writeMessages(cMsgMessage[] msgs, int messages2Write)
                throws InterruptedException, cMsgException {

//System.out.println("singlethreaded put: array len = " + msgs.length + ", send " + messages2Write +
// " # of messages to cMsg server");
                for (cMsgMessage msg : msgs) {
                    dataTransportImplCmsg.getCmsgConnection().send(msg);
                    if (--messages2Write < 1)  break;
                }
        }


        /** {@inheritDoc} */
        @Override
        public void run() {

            // Tell the world I've started
            startLatch.countDown();

            try {
                EventType previousType, pBanktype;
                ControlType pBankControlType;
                ArrayList<RingItem> bankList;
                RingItem ringItem;
                int nextMsgListIndex, thisMsgListIndex, pBankSize, listTotalSizeMax;
                EvWriter[] writers = new EvWriter[writeThreadCount];
                // Place to store a bank off the queue for the next message out
                RingItem firstBankFromQueue = null;

                int eventCount, messages2Write;
                int[] recordIds = new int[writeThreadCount];
                int[] bankListSize = new int[writeThreadCount];
                cMsgMessage[] msgs = new cMsgMessage[writeThreadCount];

                // RocSimulation generates "ringChunk" sequential events at once,
                // so, a single ring will have ringChunk sequential events together.
                // Take this into account when reading from multiple rings.
                // We must get the output order right.
                int ringChunkCounter = outputRingChunk;

                // Create an array of lists of PayloadBank objects by 2-step
                // initialization to avoid "generic array creation" error.
                // Create one list for each write thread.
                ArrayList<RingItem>[] bankListArray = new ArrayList[writeThreadCount];
                for (int i=0; i < writeThreadCount; i++) {
                    bankListArray[i] = new ArrayList<RingItem>();
                    msgs[i] = new cMsgMessage();
                    msgs[i].setSubject(subject);
                    msgs[i].setType(type);
                }


                while ( dataTransportImplCmsg.getCmsgConnection().isConnected() ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) Thread.sleep(5);
                        continue;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        shutdown();
                        return;
                    }

                    // First, clear all the lists of banks we need -
                    // one list for each write thread.
                    for (int j=0; j < writeThreadCount; j++) {
                        bankListArray[j].clear();
                    }

                    // Init variables
                    eventCount = 0;
                    messages2Write = 0;
                    nextMsgListIndex = thisMsgListIndex = 0;
                    listTotalSizeMax = 32;
                    previousType = null;
                    bankList = bankListArray[nextMsgListIndex];

                    // Grab a bank to put into a cMsg buffer,
                    // checking occasionally to see if we got an
                    // RESET command or someone found an END event.
                    do {
// System.out.println("      DataChannel cMsg out helper: try getting ring item");
                        // Get bank off of Q, unless we already did so in a previous loop
                        if (firstBankFromQueue != null) {
                            ringItem = firstBankFromQueue;
                            firstBankFromQueue = null;
                        }
                        else {
                            ringItem = getNextOutputRingItem(rbIndex);
                        }
//System.out.println("      DataChannel cMsg out helper: GOT ring item");

                        eventCount++;

                        pBanktype = ringItem.getEventType();
                        pBankSize = ringItem.getTotalBytes();
                        pBankControlType = ringItem.getControlType();

                        // Assume worst case of one block header/bank
                        listTotalSizeMax += pBankSize + 32;

                        // This the first time through the while loop
                        if (previousType == null) {
                            // Add bank to the list since there's always room for one
                            bankList.add(ringItem);

                            // First time through loop nextMessageIndex = thisMessageIndex,
                            // at least until it gets incremented below.
                            //
                            // Set recordId depending on what type this bank is
                            if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
                                recordIds[thisMsgListIndex] = recordId++;
                            }
                            else {
                                recordIds[thisMsgListIndex] = -1;
                            }

                            // Keep track of list's maximum possible size
                            bankListSize[thisMsgListIndex] = listTotalSizeMax;

                            // Index of next list
                            nextMsgListIndex++;
                        }
                        // Is this bank a diff type as previous bank?
                        // Will it not fit into the target size per message?
                        // Will it be > the target number of banks per message?
                        // In all these cases start using a new list.
                        else if (singleEventOut ||
                                 (previousType != pBanktype) ||
                                 (listTotalSizeMax >= outputSizeLimit) ||
                                 (bankList.size() + 1 > outputCountLimit)) {

                            // Store final value of previous list's maximum possible size
                            bankListSize[thisMsgListIndex] = listTotalSizeMax - pBankSize - 32;

                            // If we've already used up the max number of messages,
                            // write things out first. Be sure to store what we just
                            // pulled off the Q to be the next bank!
                            if (nextMsgListIndex >= writeThreadCount) {
//                                        System.out.println("Already used " +
//                                                            nextMessageIndex + " messages for " + writeThreadCount +
//                                                            " write threads, store bank for next round");
                                firstBankFromQueue = ringItem;
                                break;
                            }

                            // Get new list
                            bankList = bankListArray[nextMsgListIndex];
                            // Add bank to new list
                            bankList.add(ringItem);
                            // Size of new list (64 -> take ending header into account)
                            bankListSize[nextMsgListIndex] = listTotalSizeMax = pBankSize + 64;

                            // Set recordId depending on what type this bank is
                            if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
                                recordIds[nextMsgListIndex] = recordId++;
                            }
                            else {
                                recordIds[nextMsgListIndex] = -1;
                            }

                            // Index of this & next lists
                            thisMsgListIndex++;
                            nextMsgListIndex++;
                        }
                        // It's OK to add this bank to the existing list.
                        else {
                            // Add bank to list since there's room and it's the right type
                            bankList.add(ringItem);
                            // Keep track of list's maximum possible size
                            bankListSize[thisMsgListIndex] = listTotalSizeMax;
                        }

                        // Set this for next round
                        previousType = pBanktype;
                        ringItem.setAttachment(Boolean.FALSE);

                        gotoNextRingItem(rbIndex);

                        // If control event, quit loop and write what we have
                        if (pBankControlType != null) {
System.out.println("      DataChannel cMsg out helper: SEND CONTROL RIGHT THROUGH: " + pBankControlType);

                            // Look for END event and mark it in attachment
                            if (pBankControlType == ControlType.END) {
                                ringItem.setAttachment(Boolean.TRUE);
                                haveOutputEndEvent = true;
System.out.println("      DataChannel cMsg out helper: " + name + " I got END event, quitting 2");
                                // run callback saying we got end event
                                if (endCallback != null) endCallback.endWait();
                            }

                            break;
                        }

                        // Be careful not to use up all the events in the output
                        // ring buffer before writing (& freeing up) some.
                        if (eventCount >= outputRingItemCount /2) {
                            break;
                        }

                    } while (!gotResetCmd && (thisMsgListIndex < writeThreadCount));

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        shutdown();
                        return;
                    }

//                    if (writeThreadCount > 1 && nextMsgListIndex > 1) {
                        latch = new CountDownLatch(nextMsgListIndex);
//                    }

                    // For each cMsg message that can be filled with something ...
                    for (int i=0; i < nextMsgListIndex; i++) {
                        // Get one of the list of banks to put into this cMsg message
                        bankList = bankListArray[i];
//System.out.println("  Looking at msg list " + i + ", with " + bankList.size() +
//                        " # of events in it");
                        if (bankList.size() < 1) {
                            continue;
                        }


                        // Write banks' data into ET buffer in separate thread.
                        // Do not recreate writer object if not necessary.
                        if (writers[i] == null) {
                            writers[i] = new EvWriter(bankList, msgs[i],
                                                      bankListSize[i], recordIds[i]);
                        }
                        else {
                            writers[i].setupWriter(bankList, msgs[i],
                                                   bankListSize[i], recordIds[i]);
                        }
                        writeThreadPool.execute(writers[i]);

                        // Keep track of how many messages we want to write
                        messages2Write++;

                        // Handle END event ...
                        for (RingItem ri : bankList) {
                            if (ri.getAttachment() == Boolean.TRUE) {
                                // There should be no more events coming down the pike so
                                // go ahead write out events and then shut this thread down.
                                break;
                            }
                        }
                    }

                    // Wait for all events to finish processing
//                    if (writeThreadCount > 1 && nextMsgListIndex > 1) {
                        latch.await();
//                    }

                    try {
//System.out.println("      DataChannel cMsg: write " + messages2Write + " messages");
                        // Write cMsg messages after gathering them all
                        writeMessages(msgs, messages2Write);
                    }
                    catch (cMsgException e) {
                        errorMsg.compareAndSet(null, "Cannot communicate with cMsg server");
                        throw e;
                    }

//System.out.println("      DataChannel cMsg: ring release " + rbIndex);
                    releaseOutputRingItem(rbIndex);

                    if (--ringChunkCounter < 1) {
                        rbIndex = ++rbIndex % outputRingCount;
                        ringChunkCounter = outputRingChunk;
                    }

                    if (haveOutputEndEvent) {
System.out.println("      DataChannel cMsg out helper: " + name + " some thd got END event, quitting 4");
                        shutdown();
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel cMsg out helper: " + name + "  interrupted thd, exiting");

            // only cMsgException thrown
            } catch (Exception e) {
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                e.printStackTrace();
                logger.warn("      DataChannel cMsg out helper: " + name + " exit thd: " + e.getMessage());
            }

        }



        /**
         * This class is designed to write an evio bank's
         * contents into a cMsg message by way of a thread pool.
         */
        private class EvWriter implements Runnable {

            /** List of evio banks to write. */
            private ArrayList<RingItem> bankList;

            /** cMsg message's data buffer. */
            private ByteBuffer buffer;

            /** Object for writing banks into message's data buffer. */
            private EventWriter evWriter;

            /** Message to send with bank data inside. */
            private cMsgMessage msg;


            /**
             * Encode the event type into the bit info word
             * which will be in each evio block header.
             *
             * @param bSet bit set which will become part of the bit info word
             * @param type event type to be encoded
             */
            void setEventType(BitSet bSet, int type) {
                // check args
                if (type < 0) type = 0;
                else if (type > 15) type = 15;

                if (bSet == null || bSet.size() < 6) {
                    return;
                }
                // do the encoding
                for (int i=2; i < 6; i++) {
                    bSet.set(i, ((type >>> i - 2) & 0x1) > 0);
                }
            }


            /**
             * Constructor.
             * @param bankList list of banks to be written into a single cMsg message
             * @param msg cMsg message in which to write the list of banks
             * @param bankByteSize total size of the banks in bytes <b>including block headers</b>
             * @param myRecordId value of starting block header's block number
             */
            EvWriter(ArrayList<RingItem> bankList, cMsgMessage msg,
                     int bankByteSize, int myRecordId) {
                setupWriter(bankList, msg, bankByteSize, myRecordId);
            }

            /**
             * Create and/or setup the object to write evio events into cmsg buffer.
             *
             * @param bankList list of banks to be written into a single cMsg message
             * @param msg cMsg message in which to write the list of banks
             * @param bankByteSize total size of the banks in bytes <b>including block headers</b>
             * @param myRecordId value of starting block header's block number
             */
            void setupWriter(ArrayList<RingItem> bankList, cMsgMessage msg,
                     int bankByteSize, int myRecordId) {

                this.msg = msg;
                this.bankList = bankList;

                // Need to account for block headers + a little extra just in case

// TODO: use bufferSupply here!
                buffer = ByteBuffer.allocate(bankByteSize);
                buffer.order(byteOrder);

                // Encode the event type into bits
                BitSet bitInfo = new BitSet(24);
                setEventType(bitInfo, bankList.get(0).getEventType().getValue());

                try {
                    // Create object to write evio banks into message buffer
                    if (evWriter == null) {
                        evWriter = new EventWriter(buffer, 550000, 200, null, bitInfo, emu.getCodaid());
                    }
                    else {
                        evWriter.setBuffer(buffer, bitInfo);
                    }
                    evWriter.setStartingBlockNumber(myRecordId);
                }
                catch (EvioException e) {e.printStackTrace();/* never happen */}
            }


            /**
             * {@inheritDoc}<p>
             * Write bank into cMsg message buffer.
             */
            public void run() {
                try {
                    // Write banks into message buffer
                    if (ringItemType == ModuleIoType.PayloadBank) {
                        for (RingItem ri : bankList) {
                            evWriter.writeEvent(ri.getEvent());
                            ri.releaseByteBuffer();
                        }
                    }
                    else {
                        for (RingItem ri : bankList) {
                            evWriter.writeEvent(ri.getBuffer());
                            ri.releaseByteBuffer();
                        }
                    }

                    evWriter.close();
                    buffer.flip();

                    // Put data into cMsg message
                    msg.setByteArrayNoCopy(buffer.array(), 0, buffer.limit());
                    msg.setByteArrayEndian(byteOrder == ByteOrder.BIG_ENDIAN ? cMsgConstants.endianBig :
                                                                               cMsgConstants.endianLittle);


                    // Tell the DataOutputHelper thread that we're done
                    //if (writeThreadCount > 1)
                        latch.countDown();
                }
                catch (EvioException e) {
                    e.printStackTrace();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
