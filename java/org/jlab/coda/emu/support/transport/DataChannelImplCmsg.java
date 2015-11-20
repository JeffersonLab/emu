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
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.jevio.*;


import java.io.IOException;
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

    /** Read END event from output ring. */
    private volatile boolean haveOutputEndEvent;

    /** Got END command from Run Control. */
    private volatile boolean gotEndCmd;

    /** Got RESET command from Run Control. */
    private volatile boolean gotResetCmd;

    // INPUT

    /** Store locally whether this channel's module is an ER or not.
      * If so, don't parse incoming data so deeply - only top bank header. */
    private boolean isER;

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
    private int outputCountLimit = 1000;

    /** Use the evio block header's block number as a record id. */
    private int recordId;


    /**
     * Convert incoming msg into EvioNode objects.
     * @param msg incoming msg
     */
    private final void messageToBuf(cMsgMessage msg) {

        try {
            byte[] data = msg.getByteArray();
            if (data == null) {
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel cmsg in: cMsg message has no data");
                return;
            }

            ByteBuffer buf = ByteBuffer.wrap(data);
//        Utilities.printBuffer(buf, 0, data.length/4, "buf, control?");

            EvioCompactReader compactReader = new EvioCompactReader(buf);

            BlockHeaderV4 blockHeader = compactReader.getFirstBlockHeader();
            if (blockHeader.getVersion() < 4) {
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel cmsg in: data NOT evio v4 format");
                return;
            }

            boolean hasFirstEvent = blockHeader.hasFirstEvent();
            EventType eventType = EventType.getEventType(blockHeader.getEventType());
            int recordId = blockHeader.getNumber();
            int sourceId = blockHeader.getReserved1();
            int eventCount = compactReader.getEventCount();
//logger.info("      DataChannel cmsg in: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId + ", event cnt = " + eventCount);

            EvioNode node;
            EventType bankType;
            ControlType controlType = null;
            long nextRingItem;

            for (int i=1; i < eventCount+1; i++) {
                if (isER) {
                    // Don't need to parse all bank headers, just top level.
                    node = compactReader.getEvent(i);
                }
                else {
                    node = compactReader.getScannedEvent(i);
                }

                bankType = eventType;
                if (eventType == EventType.ROC_RAW) {
                    if (Evio.isUserEvent(node)) {
                        bankType = EventType.USER;
                    }
                }
                else if (eventType == EventType.CONTROL) {
                    controlType = ControlType.getControlType(node.getTag());
                    if (controlType == null) {
                        channelState = CODAState.ERROR;
                        emu.setErrorState("DataChannel cmsg in: found unidentified control event");
                        return;
                    }
                }

                nextRingItem = ringBufferIn.next();
                RingItem ringItem = ringBufferIn.get(nextRingItem);

                ringItem.setNode(node);
//                ringItem.setBuffer(node.getStructureBuffer(false));
                ringItem.setEventType(bankType);
                ringItem.setControlType(controlType);
                ringItem.isFirstEvent(hasFirstEvent);
                ringItem.setRecordId(recordId);
                ringItem.setSourceId(sourceId);
                ringItem.setSourceName(name);
                ringItem.matchesId(sourceId == id);
                // Set the event count properly for blocked events
                if (bankType.isBuildable()) {
                    ringItem.setEventCount(node.getNum());
                }
                else {
                    ringItem.setEventCount(1);
                }

                ringBufferIn.publish(nextRingItem);

                if (controlType == ControlType.END) {
                    logger.info("      DataChannel Emu in: " + name + " found END event");
                    if (endCallback != null) endCallback.endWait();
                    return;
                }
            }
        }
        catch (Exception e) {
            channelState = CODAState.ERROR;
            emu.setErrorState("DataChannel cmsg in: " + e.getMessage());
        }
    }


    /**
     * This class defines the callback to be run when a message matching the subscription arrives.
     */
    class ReceiveMsgCallback extends cMsgCallbackAdapter {

        /** Callback method definition. */
        public void callback(cMsgMessage msg, Object userObject) {
                messageToBuf(msg);
        }

        public int getMaximumQueueSize() { return 100000; }
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
//logger.debug("      DataChannel cmsg out " + outputIndex + ": processEnd(), thread already done");
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
//logger.debug("      DataChannel cmsg out " + outputIndex + ": processEnd(), thread done after waiting");
            return;
        }

        // Probably stuck trying to get item from ring buffer,
        // so interrupt it and get it to read the END event from
        // the correct ring.
//logger.debug("      DataChannel cmsg out " + outputIndex + ": processEnd(), interrupt thread in state " +
//                     dataOutputThread.threadState);
        dataOutputThread.interrupt();
    }



    /**
     * Constructor to create a new DataChannelImplCmsg instance. Used only by
     * {@link DataTransportImplCmsg#createChannel(String, Map, boolean, Emu, EmuModule, int)}
     * which is only used during PRESTART in the EmuModuleFactory.
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
    DataChannelImplCmsg(String name, DataTransportImplCmsg transport,
                        Map<String, String> attributeMap, boolean input, Emu emu,
                        EmuModule module, int outputIndex)
                throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module, outputIndex);

        dataTransportImplCmsg = transport;

        // Set subject & type for either subscription (incoming msgs) or for outgoing msgs.
        // Use any defined in config file else use defaults.
        subject = attributeMap.get("subject");
        if (subject == null) subject = name;

        type = attributeMap.get("type");
        if (type == null) type = "data";

        if (input) {
            isER = (emu.getCodaClass() == CODAClass.ER);
            try {
                // create subscription for receiving messages containing data
                ReceiveMsgCallback cb = new ReceiveMsgCallback();
System.out.println("      DataChannel cmsg: subscribe to subject = " + subject + ", type = " + type + "\n\n");
                sub = dataTransportImplCmsg.getCmsgConnection().subscribe(subject, type, cb, null);
            }
            catch (cMsgException e) {
logger.info("      DataChannel cmsg: " + e.getMessage());
                throw new DataTransportException(e);
            }
        }
        else {
            // Tell emu what that output name is for stat reporting
            emu.setOutputDestination("cMsg");

            // How may cMsg message buffer filling threads the data output thread?
            writeThreadCount = 1;
            String attribString = attributeMap.get("wthreads");
            if (attribString != null) {
                try {
                    writeThreadCount = Integer.parseInt(attribString);
                    if (writeThreadCount <  1) writeThreadCount = 1;
                    if (writeThreadCount > 10) writeThreadCount = 10;
                }
                catch (NumberFormatException e) {}
            }
logger.info("      DataChannel cmsg: write threads = " + writeThreadCount);

            dataOutputThread = new DataOutputHelper(emu.getThreadGroup(), name() + " data out");
            dataOutputThread.start();
            dataOutputThread.waitUntilStarted();
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
logger.warn("      DataChannel cmsg: end() " + name);

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
//System.out.println("      DataChannel cmsg: try joining output thread for " + (waitTime/1000) + " sec");
                dataOutputThread.join(waitTime);
                // kill everything since we waited as long as possible
                dataOutputThread.interrupt();
                dataOutputThread.shutdown();
//System.out.println("      DataChannel cmsg: output thread done");
            }
//System.out.println("      DataChannel cmsg: all helper thds done");
        }
        catch (InterruptedException e) {}

        // At this point all threads should be done
        if (sub != null) {
            try {
                dataTransportImplCmsg.getCmsgConnection().unsubscribe(sub);
            } catch (cMsgException e) {/* ignore */}
        }

        errorMsg.set(null);
        channelState = CODAState.CONFIGURED;
logger.debug("      DataChannel cmsg: end() " + name + " done");
    }


    /**
     * {@inheritDoc}.
     * Reset this channel by interrupting the message sending threads and unsubscribing.
     */
    public void reset() {
logger.debug("      DataChannel cmsg: reset() " + name);

        gotEndCmd   = false;
        gotResetCmd = true;

        // Don't unsubscribe until helper threads are done
        if (dataOutputThread != null) {
//System.out.println("      DataChannel cmsg: interrupt output thread ...");
            dataOutputThread.interrupt();
            dataOutputThread.shutdown();
            // Make sure all threads are done.
            try {
                dataOutputThread.join(1000);}
            catch (InterruptedException e) {}
//System.out.println("      DataChannel cmsg: output thread done");
        }

        // At this point all threads should be done
        if (sub != null) {
            try {
                dataTransportImplCmsg.getCmsgConnection().unsubscribe(sub);
            } catch (cMsgException e) {/* ignore */}
        }

        errorMsg.set(null);
        channelState = CODAState.CONFIGURED;

logger.debug("      DataChannel cmsg: reset() " + name + " done");
    }



    /**
     * Class used to take Evio banks from ring, write them into cMsg messages.
     */
    private class DataOutputHelper extends Thread {

        /** Used to sync things before putting new ET events. */
        private Phaser phaser;

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Thread pool for writing Evio banks into new cMsg messages. */
        private ExecutorService writeThreadPool;

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch startLatch = new CountDownLatch(1);

        /** What state is this thread in? */
        private volatile ThreadState threadState;



         /** Constructor. */
        DataOutputHelper(ThreadGroup group, String name) {
            super(group, name);

            try {
                // Thread pool with "writeThreadCount" number of threads
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
         * This method is used to send an array of cMsg messages to a cMsg server.
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

//System.out.println("      DataChannel cmsg out: array len = " + msgs.length + ", send " + messages2Write +
// " # of messages to cMsg server");
            for (cMsgMessage msg : msgs) {
                dataTransportImplCmsg.getCmsgConnection().send(msg);
                if (--messages2Write < 1)  break;
            }
        }


        /** {@inheritDoc} */
        @Override
        public void run() {

            threadState = ThreadState.RUNNING;

            // Tell the world I've started
            startLatch.countDown();

            try {
                EventType previousType, pBanktype;
                ControlType pBankControlType;
                ArrayList<RingItem> bankList;
                RingItem ringItem;
                int nextMsgListIndex, thisMsgListIndex, pBankSize, listTotalSizeMax;
                EvWriter[] writers = new EvWriter[writeThreadCount];
                // Place to store a bank off the ring for the next message out
                RingItem firstBankFromRing = null;

                // Time in milliseconds for writing if time expired
                long startTime, timeout = 2000L;

                int eventCount, messages2Write;
                int[] recordIds = new int[writeThreadCount];
                int[] bankListSize = new int[writeThreadCount];
                cMsgMessage[] msgs = new cMsgMessage[writeThreadCount];

                // Create an array of lists of RingItem objects by 2-step
                // initialization to avoid "generic array creation" error.
                // Create one list for each write thread.
                ArrayList<RingItem>[] bankListArray = new ArrayList[writeThreadCount];
                for (int i=0; i < writeThreadCount; i++) {
                    bankListArray[i] = new ArrayList<RingItem>();
                    msgs[i] = new cMsgMessage();
                    msgs[i].setSubject(subject);
                    msgs[i].setType(type);
                }

                // Always start out reading prestart & go events from ring 0
                int outputRingIndex=0;

                phaser = new Phaser(1);

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

                    // Index into bankListArray of next bankList to use.
                    nextMsgListIndex = 0;
                    // Index into bankListArray of current bankList
                    thisMsgListIndex = 0;
                    bankList = bankListArray[thisMsgListIndex];

                    listTotalSizeMax = 32;
                    // EventType of events contained in the previous list
                    previousType = null;

                    // Set time of entering do-loop
                    startTime = System.currentTimeMillis();

                    // Grab a bank to put into a cMsg buffer,
                    // checking occasionally to see if we got an
                    // RESET command or someone found an END event.
                    do {
// System.out.println("      DataChannel cmsg out: try getting ring item");
                        // Get bank off of Q, unless we already did so in a previous loop
                        if (firstBankFromRing != null) {
                            ringItem = firstBankFromRing;
                            firstBankFromRing = null;
                        }
                        else {
//System.out.print("      DataChannel cmsg out: get next buffer from ring ... ");
                            try {
                                ringItem = getNextOutputRingItem(outputRingIndex);
                            }
                            catch (InterruptedException e) {
                                threadState = ThreadState.INTERRUPTED;
                                // If we're here we were blocked trying to read the next
                                // (END) event from the wrong ring. We've had 1/4 second
                                // to read everything else so let's try reading END from
                                // given ring.
System.out.println("      DataChannel cmsg out: try again, read END from ringIndex " + ringIndexEnd +
                   " not " + outputRingIndex);
                                ringItem = getNextOutputRingItem(ringIndexEnd);
                            }
                        }

//System.out.println("      DataChannel cmsg out: GOT ring item");

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
                                firstBankFromRing = ringItem;
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

                        gotoNextRingItem(outputRingIndex);

                        // If control event, quit loop and write what we have
                        if (pBankControlType != null) {
                            // Look for END event and mark it in attachment
                            if (pBankControlType == ControlType.END) {
                                ringItem.setAttachment(Boolean.TRUE);
                                haveOutputEndEvent = true;
System.out.println("      DataChannel cmsg out " + outputIndex + ": I got END event, quitting 2, byteOrder = " +
ringItem.getByteOrder());
                                // run callback saying we got end event
                                if (endCallback != null) endCallback.endWait();
                            }
                            else if (pBankControlType == ControlType.PRESTART) {
System.out.println("      DataChannel cmsg out " + outputIndex + ": have PRESTART, ringIndex = " + outputRingIndex);
                            }
                            else if (pBankControlType == ControlType.GO) {
System.out.println("      DataChannel cmsg out " + outputIndex + ": have GO, ringIndex = " + outputRingIndex);
                                // If the module has multiple build threads, then it's possible
                                // that the first buildable event (next one in this case)
                                // will NOT come on ring 0. Make sure we're looking for it
                                // on the right ring. It was set to the correct value in
                                // DataChannelAdapter.prestart().
                                outputRingIndex = ringIndex;
                            }

                            break;
                        }

                        // Do not go to the next ring if we got a control (previously taken
                        // care of) or user event.
                        // All prestart, go, & users go to the first ring. Just keep reading
                        // until we get to a buildable event. Then start keeping count so
                        // we know when to switch to the next ring.
                        if (outputRingCount > 1 && !pBanktype.isUser()) {
                            outputRingIndex = setNextEventAndRing();
//System.out.println("      DataChannel cmsg out " + outputIndex + ": for next ev " + nextEvent +
//                   " SWITCH TO ring = " + outputRingIndex);
                        }

                        // Be careful not to use up all the events in the output
                        // ring buffer before writing some (& freeing them up).
                        // Also write what we have if time (2 sec) has expired.
                        if ((eventCount >= outputRingItemCount*3/4) ||
                            (emu.getTime() - startTime > timeout)) {
//                            if (emu.getTime() - startTime > timeout) {
//                                System.out.println("TIME FLUSH ******************");
//                            }
                            break;
                        }

                    } while (!gotResetCmd && (nextMsgListIndex < writeThreadCount));

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        shutdown();
                        return;
                    }

                    phaser.bulkRegister(nextMsgListIndex);

                    // For each cMsg message that can be filled with something ...
                    for (int i=0; i < nextMsgListIndex; i++) {
                        // Get one of the list of banks to put into this cMsg message
                        bankList = bankListArray[i];
//System.out.println("      DataChannel cmsg: looking at msg list " + i + ", with " + bankList.size() +
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

//                        // Handle END event ...
//                        for (RingItem ri : bankList) {
//                            if (ri.getAttachment() == Boolean.TRUE) {
//                                // There should be no more events coming down the pike so
//                                // go ahead write out events and then shut this thread down.
//                                break;
//                            }
//                        }
                    }

                    // Wait for all events to finish processing
                    phaser.arriveAndAwaitAdvance();

                    try {
//System.out.println("      DataChannel cmsg out: write " + messages2Write + " messages");
                        // Write cMsg messages after gathering them all
                        writeMessages(msgs, messages2Write);
                    }
                    catch (cMsgException e) {
                        errorMsg.compareAndSet(null, "Cannot communicate with cMsg server");
                        throw e;
                    }

                    // FREE UP ring buffer items for reuse.
                    // If we did NOT read from a particular ring, there is still no
                    // problem since its sequence was never increased and we only
                    // end up releasing something already released.
                    for (int i=0; i < outputRingCount; i++) {
                        releaseOutputRingItem(i);
                    }

                    if (haveOutputEndEvent) {
System.out.println("      DataChannel cmsg out: " + name + " some thd got END event, quitting 4");
                        shutdown();
                        threadState = ThreadState.DONE;
                        return;
                    }
                }
            }
            catch (InterruptedException e) {
logger.warn("      DataChannel cmsg out: " + name + "  interrupted thd, exiting");
            }
            catch (Exception e) {
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel cmsg out: " + e.getMessage());
logger.warn("      DataChannel cmsg out: " + name + " exit thd: " + e.getMessage());
            }

            threadState = ThreadState.DONE;
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
                EmuUtilities.setEventType(bitInfo, bankList.get(0).getEventType());
                if (bankList.get(0).isFirstEvent()) {
                    EmuUtilities.setFirstEvent(bitInfo);
                }

                try {
                    // Create object to write evio banks into message buffer
                    if (evWriter == null) {
                        evWriter = new EventWriter(buffer, 550000, 200, null, bitInfo,
                                                   emu.getCodaid(), myRecordId);
                    }
                    else {
                        evWriter.setBuffer(buffer, bitInfo, myRecordId);
                    }
                }
                catch (EvioException e) {/* never happen */}
            }


            /**
             * {@inheritDoc}<p>
             * Write bank into cMsg message buffer.
             */
            public void run() {
                try {
                    // Write banks into message buffer
                    EvioNode node;
                    ByteBuffer buf;

                    for (RingItem ri : bankList) {
                        buf  = ri.getBuffer();
                        node = ri.getNode();
                        if (buf != null) {
                            evWriter.writeEvent(buf);
                        }
                        else if (node != null) {
                            evWriter.writeEvent(ri.getNode(), false);
                        }
                        ri.releaseByteBuffer();
                    }

                    evWriter.close();
                    buffer.flip();

                    // Put data into cMsg message
//System.out.println("      DataChannel cmsg out: put evio into cMsg msg: limit = " + buffer.limit());
//Utilities.printBuffer(buffer, 0, buffer.limit()/4, " control event out");
                    msg.setByteArrayNoCopy(buffer.array(), 0, buffer.limit());
                    msg.setByteArrayEndian(byteOrder == ByteOrder.BIG_ENDIAN ? cMsgConstants.endianBig :
                                                                               cMsgConstants.endianLittle);

                    // Tell the DataOutputHelper thread that we're done
                    phaser.arriveAndDeregister();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    channelState = CODAState.ERROR;
                    emu.setErrorState("DataChannel cmsg out: " + e.getMessage());
                }
            }
        }
    }

}
