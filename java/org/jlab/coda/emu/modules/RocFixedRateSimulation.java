/*
 * Copyright (c) 2011, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.modules;

import com.lmax.disruptor.RingBuffer;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODAStateIF;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.CompactEventBuilder;
import org.jlab.coda.jevio.DataType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

/**
 * This class simulates a Roc. It is a module which can use multiple threads
 * to create events and send them to a single output channel.<p>
 * This ROC works with TsFixedRateSimulation class to establish a fixed data
 * rate for all simulated ROCs together. Total rate is set in TsFixedRateSimulation.
 * @author timmer
 * (2011)
 */
public class RocFixedRateSimulation extends ModuleAdapter {

    /** Keep track of the number of records built in this ROC. Reset at prestart. */
    private volatile int rocRecordId;

    /** Threads used for generating events. */
    private EventGeneratingThread[] eventGeneratingThreads;

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

    /** Number of Roc raw events to produce before syncing with other Rocs. */
    private int syncCount;

    /** Number of computational loops to act as a delay. */
    private int loops;

    /** Number of ByteBuffers in each EventGeneratingThread. */
    private int bufSupplySize;

    private int eventsPerBuffer;
    private int generatedDataWords;
    private double bufsPerSec;

    //----------------------------------------------------
    // Members used to synchronize all fake Rocs to each other which allows run to
    // end properly. I.e., they all produce the same number of buildable events.
    //----------------------------------------------------
    /** Is this ROC to be synced with others? */
    private boolean synced;

    /** Connection to platform's cMsg name server. */
    private cMsg cMsgServer;

    /** Message to send TS saying that we finished our loops. */
    private cMsgMessage syncMessage;

    /** Message to send TS with our event size in bytes. */
    private cMsgMessage initMessage;

    /** Object to handle callback subscription. */
    private cMsgSubscriptionHandle cmsgSubHandle;

    /** Synchronization primitive for initializing run. */
    private CountDownLatch initLatch;

    /** Synchronization primitive for producing events. */
    private Phaser syncPhaser;

    /** Synchronization primitive for END cmd. */
    private Phaser endPhaser;

    /** Flag used to stop event production. */
    private volatile boolean timeToEnd;

    /** Flag saying we got the END command. */
    private volatile boolean gotEndCommand;

    /** Flag saying we got the END command. */
    private volatile boolean gotResetCommand;

    /** Flag saying we got the GO command. */
    private volatile boolean gotGoCommand;


    /**
     * Callback to be run when a message from fake TS
     * arrives, allowing all ROCs to sync up. We subscribe
     * once to subject = "sync" and type = "ROC".
     */
    private class SyncCallback extends cMsgCallbackAdapter {
        public void callback(cMsgMessage msg, Object userObject) {
System.out.println("callback: got msg from fake TS");
            int endIt = msg.getUserInt();
            if (endIt > 0) {
                // Signal to finish end() method and it will
                // also quit event-producing thread.
//System.out.println("callback: ARRIVE -> END IT");
                timeToEnd = true;
                endPhaser.arriveAndDeregister();
            }

            // Signal for event-producing loop to continue
            syncPhaser.arrive();
        }
    }

    /**
     * Callback to be run when a message from fake TS
     * arrives, telling us how many events we need to put in the 4MB buffer.
     * We subscribe once to subject = "init" and type = "ROC".
     */
    private class InitCallback extends cMsgCallbackAdapter {
        public void callback(cMsgMessage msg, Object userObject) {
            eventsPerBuffer = msg.getUserInt();
            cMsgPayloadItem item = msg.getPayloadItem("bufsPerSec");
            bufsPerSec = 1;
            try {
                bufsPerSec = item.getDouble();
            }
            catch (cMsgException e) {
                e.printStackTrace();
            }
System.out.println("callback: got init return msg from TS, events/buf = " + eventsPerBuffer +
                   ", bus/sec = " + bufsPerSec);

            // The number of bufs/sec needs to be passed down to the output channel(s)
            for (DataChannel chan : outputChannels) {
                chan.regulateOutputBufferRate(eventsPerBuffer, bufsPerSec);
            }

            // Signal for GO to continue
            initLatch.countDown();
        }
    }

    /** Instantiate a callback to use in subscription. */
    private SyncCallback syncCallback = new SyncCallback();

    /** Instantiate a callback to use in subscription. */
    private InitCallback initCallback = new InitCallback();

    //----------------------------------------------------


    /**
     * Create a User event with a ByteBuffer which is ready to read.
     *
     * @param order byte order in which to write event into buffer
     * @param isFirstEvent true if event is to be a "first event",
     *                     that is, written as the first event in each file split
     * @return created PayloadBuffer object containing User event in byte buffer
     */
    static private PayloadBuffer createUserBuffer(ByteOrder order, boolean isFirstEvent, int val) {
        try {
            CompactEventBuilder builder = new CompactEventBuilder(44, order);

            // Create a single array of integers which is the bank data
            // The user event looks like a Roc Raw event but with num = 0.
            // tag=rocID, num=0 (indicates user event), type=bank
            //builder.openBank(1, 0, DataType.BANK);
            builder.openBank(2, 0, DataType.INT32);
            builder.addIntData(new int[]{val});
            builder.closeAll();
            ByteBuffer bb = builder.getBuffer();

            PayloadBuffer pBuf = new PayloadBuffer(bb);  // Ready to read buffer
            // User events from the ROC come as type ROC RAW but with num = 0
//            if (isFirstEvent) {
                pBuf.setEventType(EventType.USER);
//            }
//            else {
//                pBuf.setEventType(EventType.ROC_RAW);
//            }
            if (isFirstEvent) {
                pBuf.isFirstEvent(true);
            }

            return pBuf;
        }
        catch (Exception e) {/* never happen */}

        return null;
    }



    /**
     * Constructor RocSimulation creates a simulated ROC instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public RocFixedRateSimulation(String name, Map<String, String> attributeMap, Emu emu) {

        super(name, attributeMap, emu);

        // Fill out message to send to fake TS
        syncMessage = new cMsgMessage();
        syncMessage.setSubject("syncFromRoc");
        syncMessage.setType(emu.name());
        try {  syncMessage.setHistoryLengthMax(0); }
        catch (cMsgException e) {/* never happen */}

        String s;

        // Value for trigger type from trigger supervisor
        triggerType = 15;
        try { triggerType = Integer.parseInt(attributeMap.get("triggerType")); }
        catch (NumberFormatException e) { /* defaults to 15 */ }
        if (triggerType <  0) triggerType = 0;
        else if (triggerType > 15) triggerType = 15;

        // Id of detector producing data
        detectorId = 111;
        try { detectorId = Integer.parseInt(attributeMap.get("detectorId")); }
        catch (NumberFormatException e) { /* defaults to 111 */ }
        if (detectorId < 0) detectorId = 0;

        // How many entangled events in one data block?
        eventBlockSize = 40;
        try { eventBlockSize = Integer.parseInt(attributeMap.get("blockSize")); }
        catch (NumberFormatException e) { /* defaults to 1 */ }
        if (eventBlockSize <   1) eventBlockSize = 1;
        else if (eventBlockSize > 255) eventBlockSize = 255;

        // How many WORDS in a single event?
        eventSize = 75;  // 300 bytes
        try { eventSize = Integer.parseInt(attributeMap.get("eventSize")); }
        catch (NumberFormatException e) {}
        if (eventSize < 1) eventSize = 1;

        // How many loops to constitute a delay?
        loops = 0;
        try { loops = Integer.parseInt(attributeMap.get("loops")); }
        catch (NumberFormatException e) { /* defaults to 0 */ }
        if (loops < 1) loops = 0;

        // How many iterations (writes of an entangled block of evio events)
        // before syncing fake ROCs together?
        syncCount = 1000000;
        try { syncCount = Integer.parseInt(attributeMap.get("syncCount")); }
        catch (NumberFormatException e) { /* defaults to 100k */ }
        if (syncCount < 10) syncCount = 10;

        // Event generating threads
        eventGeneratingThreads = new EventGeneratingThread[eventProducingThreads];

        // Is this ROC to be synced with others?
        synced = true;
        s = attributeMap.get("sync");
        if (s != null) {
            if (s.equalsIgnoreCase("false") ||
                s.equalsIgnoreCase("off")   ||
                s.equalsIgnoreCase("no"))   {
                synced = false;
            }
        }

        // Need to coordinate amount of data words
        generatedDataWords = eventBlockSize * eventSize;
System.out.println("  Roc mod: generatedDataWords = " + generatedDataWords);

        eventWordSize  = getSingleEventBufferWords(generatedDataWords);
System.out.println("  Roc mod: eventWordSize = " + eventWordSize);

        // Fill out message to send to fake TS
        initMessage = new cMsgMessage();
        initMessage.setSubject("initFromRoc");
        initMessage.setType(emu.name());
        initMessage.setUserInt(4*eventWordSize);
        try {  initMessage.setHistoryLengthMax(0); }
        catch (cMsgException e) {/* never happen */}

        // the module sets the type of CODA class it is.
        emu.setCodaClass(CODAClass.ROC);
    }


    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {}

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getInputChannels() {return null;}

    /** {@inheritDoc} */
    public void clearChannels() {outputChannels.clear();}

    /** {@inheritDoc} */
    public int getInternalRingCount() {return bufSupplySize;}

    //---------------------------------------
    // Threads
    //---------------------------------------


    /** End all threads because a END cmd or END event came through.  */
    private void endThreads() {
        // The order in which these threads are shutdown does(should) not matter.
        // Transport objects should already have been shutdown followed by this module.
        if (RateCalculator != null) RateCalculator.interrupt();
        RateCalculator = null;

        for (EventGeneratingThread thd : eventGeneratingThreads) {
            if (thd != null) {
                try {
//System.out.println("  Roc mod: endThreads() event generating thd, try interrupting " + thd.getName());
                    thd.interrupt();
                    thd.join();
//System.out.println("  Roc mod: endThreads() event generating thd, joined " + thd.getName());
                }
                catch (InterruptedException e) {
                }
            }
        }
//System.out.println("  Roc mod: endThreads() DONE");
    }


    /** Kill all threads immediately because a RESET cmd came through.  */
    private void killThreads() {
//System.out.println("  Roc mod: start killThreads()");
        // The order in which these threads are shutdown does(should) not matter.
        // Transport objects should already have been shutdown followed by this module.
        if (RateCalculator != null) {
//System.out.println("  Roc mod: interrupt rate calc thread");
            RateCalculator.interrupt();
        }
        RateCalculator = null;

        for (EventGeneratingThread thd : eventGeneratingThreads) {
            if (thd != null) {
                // Kill this thread with deprecated stop method because it can easily
                // block on the uninterruptible rb.next() method call and RESET never
                // completes. First give it a chance to end gracefully.
                thd.interrupt();
//System.out.println("  Roc mod: interrupt event generating thread");
                try {
                    thd.join(1000);
                }
                catch (InterruptedException e) {}
            }
        }
//System.out.println("  Roc mod: done killThreads()");
    }


    //---------------------------------------


    /**
     * This method is called by a running EventGeneratingThread.
     * It generates many ROC Raw events in it with simulated data,
     * and places them onto the ring buffer of an output channel.
     *
     * @param ringNum the id number of the output channel ring buffer
     * @param buf     the event to place on output channel ring buffer
     * @param item    item corresponding to the buffer allowing buffer to be reused
     */
    void eventToOutputRing(int ringNum, ByteBuffer buf,
                                   ByteBufferItem item, ByteBufferSupply bbSupply) {

        if (outputChannelCount < 1) {
            bbSupply.release(item);
            return;
        }

        // TODO: assumes only one output channel ...
        RingBuffer rb = outputChannels.get(0).getRingBuffersOut()[ringNum];

//System.out.println("  Roc mod: wait for next ring buf for writing");
        long nextRingItem = rb.next();

//System.out.println("  Roc mod: got sequence " + nextRingItem);
        RingItem ri = (RingItem) rb.get(nextRingItem);
        ri.setBuffer(buf);
        ri.setEventType(EventType.ROC_RAW);
        ri.setControlType(null);
        ri.setSourceName(null);
        ri.setReusableByteBuffer(bbSupply, item);

//System.out.println("  Roc mod: published ring item #" + nextRingItem + " to ring " + ringNum);
        rb.publish(nextRingItem);
    }


    private void sendMsgToSynchronizer(boolean gotEnd) throws cMsgException {
        if (gotEnd) {
            syncMessage.setUserInt(1);
//System.out.println("  Roc mod: sent \"got End cmd\"");
        }
        else {
            syncMessage.setUserInt(0);
//System.out.println("  Roc mod: send \"no End received\"");
        }
        cMsgServer.send(syncMessage);
    }


    private int getSingleEventBufferWords(int generatedDataWords) {

        int dataWordLength = 1 + generatedDataWords;

        // bank header + bank header +  eventBlockSize segments + data,
        // seg  = (1 seg header + 3 data)
        // data = bank header + int data

        //  totalEventWords = 2 + 2 + eventBlockSize*(1+3) + (2 + data.length);
        return (6 + 4*eventBlockSize + dataWordLength);
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

    //TODO: 1st
    void writeEventBuffer(ByteBuffer buf, ByteBuffer templateBuf,
                                   long eventNumber, long timestamp, boolean copy) {

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
    }


    /**
     * This thread generates events with junk data in it (all zeros except first word which
     * is the event number).
     * It is started by the GO transition and runs while the state of the module is ACTIVE.
     * <p/>
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * selects an output by taking the next one from a simple iterator. This thread then creates
     * data transport records with payload banks containing ROC raw records and places them on the
     * output DataChannel.
     * <p/>
     */
    class EventGeneratingThread extends Thread {

        private final int myId;
        private int  myRocRecordId;
        private long myEventNumber, timestamp;
        /** Ring buffer containing ByteBuffers - used to hold events for writing. */
        private ByteBufferSupply bbSupply;
        private ByteBuffer templateBuffer;


        EventGeneratingThread(int id, ThreadGroup group, String name) {
            super(group, name);
            this.myId = id;

            // Set & initialize values
            myRocRecordId = rocRecordId + myId;
            myEventNumber = 1L + myId*eventBlockSize;
            timestamp = myId*4*eventBlockSize;

            // Need to coordinate amount of data words
            // generatedDataWords & eventWordSize are calculated in constructor

            templateBuffer = createSingleEventBuffer(generatedDataWords, myEventNumber, timestamp);

System.out.println("  Roc mod: start With (id=" + myId + "):\n    record id = " + myRocRecordId +
                           ", ev # = " +myEventNumber + ", ts = " + timestamp +
                           ", blockSize = " + eventBlockSize);
        }


        public void run() {

            int  i,j,k=0;
            int  skip=3,  userEventLoop = syncCount;
            long oldVal=0L, totalT=0L, totalCount=0L, bufCounter=0L;
            long t1, deltaT, t2;
            ByteBuffer buf;
            ByteBufferItem bufItem;
            boolean copyWholeBuf = true;

//            long switchCounterPt = 4200;

            // We need for the # of buffers in our bbSupply object to be >=
            // the # of ring buffer slots in the output channel or we can get
            // a deadlock. Although we get the value from the first channel's
            // first ring, it's the same for all output channels.
            if (outputChannelCount > 0) {
                bufSupplySize = outputChannels.get(0).getRingBuffersOut()[0].getBufferSize();
            }
            else {
                bufSupplySize = 4096;
            }

            // Now create our own buffer supply to match
            bbSupply = new ByteBufferSupply(bufSupplySize, 4*eventWordSize, ByteOrder.BIG_ENDIAN, false);
            System.out.println("  Roc mod: " + bufSupplySize + " buffers of byte size = " + (4*eventWordSize));

            try {
                t1 = System.currentTimeMillis();

                // Use dummy arg that's overwritten later
                boolean noBuildableEvents = false;

                System.out.println("SETTING loops to " + loops);

                while (moduleState == CODAState.ACTIVE || paused) {

                    if (gotResetCommand) {
                        return;
                    }

                    if (noBuildableEvents) {
                        Thread.sleep(250);
                    }
                    else {
                        // Add ROC Raw Records as PayloadBuffer objects
                        // Get buffer from recirculating supply
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
//System.out.println("  Roc mod: write event");

                        writeEventBuffer(buf, templateBuffer, myEventNumber,
                                         timestamp, copyWholeBuf);


                        // Put generated events into output channel
                        eventToOutputRing(myId, buf, bufItem, bbSupply);

//                        // Delay things
//                        for (j=0; j < loops; j++) {
//                            for (i = 0; i < 15; i++) {
//                                k = k % 3;
//                            }
//                        }

//                        if (switchCounterPt-- < 0) {
//                            Thread.sleep(1000);
//                        }

                        eventCountTotal += eventBlockSize;
                        wordCountTotal  += eventWordSize;

                        myEventNumber += eventProducingThreads*eventBlockSize;
                        timestamp     += 4*eventProducingThreads*eventBlockSize;
                        myRocRecordId += eventProducingThreads;
//System.out.println("  Roc mod: next (id=" + myId + "):\n           record id = " + myRocRecordId +
//                           ", ev # = " +myEventNumber + ", ts = " + timestamp);

//                        Thread.sleep(1);

//                        if (userEventLoop == userEventLoopMax - 10) {
//System.out.println("  Roc mod: INSERT USER EVENT");
//                            eventToOutputChannel(Evio.createUserBuffer(outputOrder), 0, 0);
//                        }

                        // Do the requisite number of iterations before syncing up
                        if (synced && --userEventLoop < 1) {
                            // Did we receive the END command yet? ("moduleState" is volatile)
                            if (moduleState == CODAState.DOWNLOADED) {
                                // END command has arrived
//System.out.println("  Roc mod: end has arrived");
                                gotEndCommand = true;
                            }

                            // Send message to synchronizer that we're waiting
                            // whether or not we got the END command. Only
                            // want 1 msg sent, so have the first thread do it.
                            if (myId == 0) {
                                sendMsgToSynchronizer(gotEndCommand);
                            }

                            // Wait for synchronizer's response before continuing
//System.out.println("  Roc mod: phaser await advance, ev count = " + eventCountTotal);
                            syncPhaser.arriveAndAwaitAdvance();
//System.out.println("  Roc mod: phaser PAST advance, ev count = " + eventCountTotal);

                            // Every ROC has received the END command and completed the
                            // same number of iterations, therefore it's time to quit.
                            if (timeToEnd) {
//System.out.println("  Roc mod: arrive, SYNC told me to quit");
                                endPhaser.arriveAndDeregister();
                                return;
                            }

                            // For better testing change synCount by 1 each time
                            userEventLoop = ++syncCount;
                        }
                    }

                    t2 = emu.getTime();
                    deltaT = t2 - t1;

                    if (myId == 0 && deltaT > 2000) {
                        if (skip-- < 1) {
                            totalT += deltaT;
                            totalCount += myEventNumber-oldVal;
                            System.out.println("  Roc mod: event rate = " + String.format("%.3g", ((myEventNumber-oldVal)*1000./deltaT) ) +
                                                       " Hz,  avg = " + String.format("%.3g", (totalCount*1000.)/totalT));
                        }
                        else {
                            System.out.println("  Roc mod: event rate = " + String.format("%.3g", ((myEventNumber-oldVal)*1000./deltaT) ) + " Hz");
                        }
                        t1 = t2;
                        oldVal = myEventNumber;
                    }

                }
            }
            catch (InterruptedException e) {
                // End or Reset most likely
            }
            catch (Exception e) {
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());
                moduleState = CODAState.ERROR;
                emu.sendStatusMessage();
                return;
            }
        }

    }


    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        gotResetCommand = true;
System.out.println("  Roc mod: reset()");
        Date theDate = new Date();
        CODAStateIF previousState = moduleState;
        moduleState = CODAState.CONFIGURED;

        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;

        // rb.next() can block in endThreads() when doing a RESET.
        // So just kill threads by force instead of being nice about it.
        killThreads();

        paused = false;

        if (previousState.equals(CODAState.ACTIVE)) {
            // set end-of-run time in local XML config / debug GUI
            try {
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            }
            catch (DataNotFoundException e) {}
        }
    }


    /** {@inheritDoc} */
    public void end() throws CmdExecException {
        paused = false;
        gotEndCommand = true;

        // We're the only module, and the END event has made it through
        endCallback.endWait();

        // Skip over this if not synced or go never received
        if (gotGoCommand && synced) {
            // Wait until all threads are done writing events
//System.out.println("  Roc mod: end(), endPhaser block here");
            try {
                endPhaser.awaitAdvanceInterruptibly(endPhaser.arrive());
            }
            catch (InterruptedException e) {
//System.out.println("  Roc mod: end(), endPhaser interrupted");
                return;
            }
//System.out.println("  Roc mod: end(), past endPhaser");
        }

        // Put this line down here so we don't pop out of event-generating
        // loop before END is properly dealt with.
        moduleState = CODAState.DOWNLOADED;

        if (synced) {
            // Unsubscribe
            try {
                if (cmsgSubHandle != null) {
                    cMsgServer.unsubscribe(cmsgSubHandle);
                    cmsgSubHandle = null;
                }
            } catch (cMsgException e) {}
        }

        // set end-of-run time in local XML config / debug GUI
        try {
            // Set end-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}

        killThreads();

        // Putting the following 2 calls right after setting moduleState
        // results in the END event not making it through the output channel on occasion.
        // This is probably because there is a race condition as the killThreads()
        // may not have yet stopped the event producing threads from inserting events
        // while the eventToOutputChannel call (see below) was doing the same thing
        // simultaneously.

        // Put in END event
        PayloadBuffer pBuf = Evio.createControlBuffer(ControlType.END, 0, 0,
                                                      (int)eventCountTotal, 0,
                                                      outputOrder, false);
        // Send to first ring on ALL channels
        for (int i=0; i < outputChannelCount; i++) {
            if (i > 0) {
                // copy buffer and use that
                pBuf = new PayloadBuffer(pBuf);
            }
            eventToOutputChannel(pBuf, i, 0);
System.out.println("  Roc mod: inserted END event to channel " + i);
        }
    }


    /** {@inheritDoc} */
    public void prestart() {

System.out.println("  Roc mod: PRESTART");
        moduleState = CODAState.PAUSED;

        // Reset some variables
        gotGoCommand = gotEndCommand = gotResetCommand = false;
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;
        rocRecordId = 0;

        initLatch = new CountDownLatch(1);
        // Subscribe to cMsg server for ROC initialization purposes
        System.out.println("  Roc mod: init subscribe");
        cMsgServer = emu.getCmsgPortal().getCmsgServer();
        try {
            cmsgSubHandle = cMsgServer.subscribe("init", "ROC", initCallback, null);
        }
        catch (cMsgException e) {/* never happen */}


        if (synced) {
            syncPhaser = new Phaser(eventProducingThreads + 1);
            endPhaser = new Phaser(eventProducingThreads + 2);
            timeToEnd = false;
            gotEndCommand = false;
        }

        // create threads objects (but don't start them yet)
        RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), emu.name()+":watcher");

        for (int i=0; i < eventProducingThreads; i++) {
            eventGeneratingThreads[i] = new EventGeneratingThread(i, emu.getThreadGroup(),
                                                                  emu.name()+":generator");
        }

        boolean sendUser = true;

//        // Send user events right before prestart
//        if (sendUser && emu.name().equals("Roc1")) {
//            // Put in User events
//            System.out.println("  Roc mod: write USER event for Roc1");
//            PayloadBuffer pBuf = createUserBuffer(outputOrder, false, 1);
//            eventToOutputChannel(pBuf, 0, 0);
//
//            System.out.println("  Roc mod: write FIRST event for Roc1");
//            pBuf = createUserBuffer(outputOrder, true, 2);
//            eventToOutputChannel(pBuf, 0, 0);
//
//            System.out.println("  Roc mod: write USER event for Roc1");
//            pBuf = createUserBuffer(outputOrder, false, 3);
//            eventToOutputChannel(pBuf, 0, 0);

//            for (int i=0; i < 8200; i++) {
//                System.out.println("  Roc mod: write FIRST event for Roc1");
//                PayloadBuffer pBuf = createUserBuffer(outputOrder, true, i);
//                eventToOutputChannel(pBuf, 0, 0);
//
//            }

//            eventCountTotal += 8200 + 0;
//            wordCountTotal  += (8200 + 0)*7;
//        }


        // Create PRESTART event
        PayloadBuffer pBuf = Evio.createControlBuffer(ControlType.PRESTART, emu.getRunNumber(),
                                                      emu.getRunTypeId(), 0, 0,
                                                      outputOrder, false);
        // Send to first ring on ALL channels
        for (int i=0; i < outputChannelCount; i++) {
            // Copy buffer and use that
            PayloadBuffer pBuf2 = new PayloadBuffer(pBuf);
            eventToOutputChannel(pBuf2, i, 0);
System.out.println("  Roc mod: inserted PRESTART event to channel " + i);
        }

//        // Send more user events right after prestart
//        if (sendUser && emu.name().equals("Roc1")) {
//            // Put in User events
//            System.out.println("  Roc mod: write USER event after prestart for Roc1");
//            PayloadBuffer pBuf = createUserBuffer(outputOrder, false, 5);
//            eventToOutputChannel(pBuf, 0, 0);
//
//            System.out.println("  Roc mod: write FIRST event after prestart for Roc1");
//            pBuf = createUserBuffer(outputOrder, true, 6);
//            eventToOutputChannel(pBuf, 0, 0);
//
//            System.out.println("  Roc mod: write USER event after prestart for Roc1");
//            pBuf = createUserBuffer(outputOrder, false, 7);
//            eventToOutputChannel(pBuf, 0, 0);
//
//            eventCountTotal += 3;
//            wordCountTotal  += 21;
//        }



        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}

        // Subscribe to cMsg server for ROC synchronization purposes
        if (synced) {
            cMsgServer = emu.getCmsgPortal().getCmsgServer();
            try {
                cmsgSubHandle = cMsgServer.subscribe("sync", "ROC", syncCallback, null);
            }
            catch (cMsgException e) {/* never happen */}
        }
    }


    /** {@inheritDoc} */
    public void go() {
        gotGoCommand = true;

        if (moduleState == CODAState.ACTIVE) {
//System.out.println("  Roc mod: we must have hit go after PAUSE");
        }

        // Since TS gets prestarted AFTER the ROC, need to wait until GO
        // to send init msg subscribed to by TS in its PRESTART.
        System.out.println("  Roc mod: init send to sub =  " + initMessage.getSubject());
        // Send init message to fake TS
        try {
            cMsgServer.send(initMessage);
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }

        System.out.println("  Roc mod: await init response from TS");
        try {
            initLatch.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("  Roc mod: past init");

        // Create GO event
        PayloadBuffer pBuf = Evio.createControlBuffer(ControlType.GO, 0, 0,
                                                      (int) eventCountTotal, 0,
                                                      outputOrder, false);
        // Send to first ring on ALL channels
        for (int i=0; i < outputChannelCount; i++) {
            // Copy buffer and use that
            PayloadBuffer pBuf2 = new PayloadBuffer(pBuf);
            eventToOutputChannel(pBuf2, i, 0);
System.out.println("  Roc mod: inserted GO event to channel " + i);
        }

//        try {
//            Thread.sleep(500);
//        }
//        catch (InterruptedException e) {}

        moduleState = CODAState.ACTIVE;

        // start up all threads
        if (RateCalculator == null) {
            RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), emu.name()+":watcher");
        }

        if (RateCalculator.getState() == Thread.State.NEW) {
            RateCalculator.start();
        }

        for (int i=0; i < eventProducingThreads; i++) {
            if (eventGeneratingThreads[i] == null) {
                eventGeneratingThreads[i] = new EventGeneratingThread(i, emu.getThreadGroup(),
                                                                      emu.name()+":generator");
            }

//System.out.println("  Roc mod: event generating thread " + eventGeneratingThreads[i].getName() + " isAlive = " +
//                           eventGeneratingThreads[i].isAlive());
            if (eventGeneratingThreads[i].getState() == Thread.State.NEW) {
//System.out.println("  Roc mod: starting event generating thread");
                eventGeneratingThreads[i].start();
            }
        }

        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}

    }


}