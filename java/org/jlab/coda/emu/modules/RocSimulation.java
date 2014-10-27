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
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.CompactEventBuilder;
import org.jlab.coda.jevio.DataType;
import org.jlab.coda.jevio.EvioException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Phaser;

/**
 * This class simulates a Roc. It is a module which uses a single thread
 * to create events and send them to a single output channel.<p>
 * TODO: ET buffers have the number of events in them which varies from ROC to ROC.
 * @author timmer
 * (2011)
 */
public class RocSimulation extends ModuleAdapter {

    /** Keep track of the number of records built in this ROC. Reset at prestart. */
    private volatile int rocRecordId;

    /** Threads used for generating events. */
    private EventGeneratingThread[] eventGeneratingThreads;

    /** Type of trigger sent from trigger supervisor. */
    private int triggerType;

    /** Is this ROC in single event mode? */
    private boolean isSingleEventMode;

    /** Number of events in each ROC raw record. */
    private int eventBlockSize;

    /** The id of the detector which produced the data in block banks of the ROC raw records. */
    private int detectorId;

    /** Size of a single generated Roc raw event in 32-bit words (including header). */
    private int eventWordSize;

    //----------------------------------------------------
    // Members for keeping statistics
    //----------------------------------------------------
    private volatile boolean killThreads;

    /** Used in debugging output to track each time an array of evio events is generated. */
    static int jobNumber;

    //----------------------------------------------------
    // Members used to synchronize all fake Rocs to each other which allows run to
    // end properly. I.e., they all produce the same number of buildable events.
    //----------------------------------------------------
    /** Is this ROC to be synced with others? */
    private boolean synced;

    /** Connection to platform's cMsg name server. */
    private cMsg cMsgServer;

    /** Message to send synchronizer saying that we finished our loops. */
    private cMsgMessage message;

    /** Object to handle callback subscription. */
    private cMsgSubscriptionHandle cmsgSubHandle;

    /** Synchronization primitive for producing events. */
    private Phaser phaser;

    /** Synchronization primitive for END cmd. */
    private Phaser endPhaser;

    /** Flag used to stop event production. */
    private volatile boolean timeToEnd;

    /** Flag saying we got the END command. */
    private volatile boolean gotEndCommand;


    /** Callback to be run when a message from synchronizer
     *  arrives, allowing all ROCs to sync up. */
    private class SyncCallback extends cMsgCallbackAdapter {
        public void callback(cMsgMessage msg, Object userObject) {
            int endIt = msg.getUserInt();
            if (endIt > 0) {
                // Signal to finish end() method and it will
                // also quit event-producing thread.
//System.out.println("ARRIVE -> END IT");
                timeToEnd = true;
                endPhaser.arriveAndDeregister();
            }

            // Signal for event-producing loop to continue
            phaser.arrive();
        }
    }

    /** Instantiate a callback to use in subscription. */
    private SyncCallback callback = new SyncCallback();

    //----------------------------------------------------



    /**
     * Constructor RocSimulation creates a simulated ROC instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public RocSimulation(String name, Map<String, String> attributeMap, Emu emu) {

        super(name, attributeMap, emu);

        // Fill out message to send to synchronizer
        message = new cMsgMessage();
        message.setSubject("syncFromRoc");
        message.setType(emu.name());

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
        eventBlockSize = 2;
        try { eventBlockSize = Integer.parseInt(attributeMap.get("blockSize")); }
        catch (NumberFormatException e) { /* defaults to 1 */ }
        if (eventBlockSize <   1) eventBlockSize = 1;
        else if (eventBlockSize > 255) eventBlockSize = 255;

        // Event generating threads
        eventGeneratingThreads = new EventGeneratingThread[eventProducingThreads];

        // Is this ROC to be synced with others?
        synced = false;
        s = attributeMap.get("sync");
        if (s != null) {
            if (s.equalsIgnoreCase("true") ||
                s.equalsIgnoreCase("on")   ||
                s.equalsIgnoreCase("yes"))   {
                synced = true;
            }
        }

        // Is this ROC in single-event-mode?
        s = attributeMap.get("SEMode");
        if (s != null) {
            if (s.equalsIgnoreCase("true") ||
                s.equalsIgnoreCase("on")   ||
                s.equalsIgnoreCase("yes"))   {
                isSingleEventMode = true;
            }
        }

        if (isSingleEventMode) {
            eventBlockSize = 1;
        }

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
    public ModuleIoType getOutputRingItemType() {return ModuleIoType.PayloadBuffer;}

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
        System.out.println("Start killThreads()");
        // The order in which these threads are shutdown does(should) not matter.
        // Transport objects should already have been shutdown followed by this module.
        if (RateCalculator != null) RateCalculator.interrupt();
        RateCalculator = null;

        for (EventGeneratingThread thd : eventGeneratingThreads) {
            if (thd != null) {
                // Kill this thread with deprecated stop method because it can easily
                // block on the uninterruptible rb.next() method call and RESET never
                // completes. First give it a chance to end gracefully.
                thd.interrupt();
                try {
                    thd.join(250);
                    if (thd.isAlive()) {
                        thd.stop();
                    }
                }
                catch (InterruptedException e) {}
            }
        }
        System.out.println("Done killThreads()");
    }


    //---------------------------------------


    /**
     * This method is called by a running EventGeneratingThread.
     * It generates many ROC Raw events in it with simulated
     * FADC250 data, and places them onto the ring buffer of an output channel.
     *
     * @param ringNum the id number of the output channel ring buffer
     * @param buf     the event to place on output channel ring buffer
     * @param item    item corresponding to the buffer allowing buffer to be reused
     */
    private void eventToOutputRing(int ringNum, ByteBuffer buf,
                                   ByteBufferItem item, ByteBufferSupply bbSupply) {

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

//System.out.println("  Roc mod: published record id " + rrId + " to ring " + ringNum);
        rb.publish(nextRingItem);
    }


    private void sendMsgToSynchronizer(boolean gotEnd) throws cMsgException {
        if (gotEnd) {
            message.setUserInt(1);
//System.out.println("  Roc mod: sent \"got End cmd\"");
        }
        else {
            message.setUserInt(0);
//System.out.println("  Roc mod: send \"no End received\"");
        }
        cMsgServer.send(message);
    }


    /**
     * This thread generates events with simulated FADC250 data in it.
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
        private CompactEventBuilder builder;
        /** Ring buffer containing ByteBuffers - used to hold events for writing. */
        private ByteBufferSupply bbSupply;
        // Number of data words in each event
        private int generatedDataWords;



        EventGeneratingThread(int id, ThreadGroup group, String name) {
            super(group, name);
            this.myId = id;

            // Need to coordinate amount of data words
            generatedDataWords = eventBlockSize * 40;

            PayloadBank[] evs = Evio.createRocDataEvents(id, triggerType,
                                                         detectorId, 0,
                                                         0, eventBlockSize,
                                                         0L, 0,  2, generatedDataWords,
                                                         isSingleEventMode);

            eventWordSize = evs[0].getHeader().getLength() + 1;
System.out.println("  Roc mod: each generated event size = " + (4*eventWordSize) +
                   " bytes, words = " + (eventWordSize) + ", tag = " +
                   evs[0].getHeader().getTag() + ", num = " + evs[0].getHeader().getNumber());

            // Set & initialize values
            myRocRecordId = rocRecordId + myId;
            myEventNumber = 1L + myId*eventBlockSize;
            timestamp = myId*4*eventBlockSize;

System.out.println("  Roc mod: start With (id=" + myId + "):\n    record id = " + myRocRecordId +
                           ", ev # = " +myEventNumber + ", ts = " + timestamp +
                           ", blockSize = " + eventBlockSize);

            bbSupply = new ByteBufferSupply(32768, 4*eventWordSize);
        }




        public void run() {

            int  userEventLoopMax=10000, eventNumber;
            int  index, status=0, skip=3,  userEventLoop = userEventLoopMax;
            long oldVal=0L, totalT=0L, totalCount=0l;
            long now, deltaT, start_time = System.currentTimeMillis();
            ByteBuffer buf = ByteBuffer.allocate(8);
            ByteBufferItem bufItem = null;
            int[] segmentData = new int[3];

            // Determine data size of events right here
            int[] data;
            if (isSingleEventMode) {
                data = new int[3 + generatedDataWords];
            }
            else {
                data = new int[1 + generatedDataWords];
            }


            try {

                // Use dummy arg that's overwritten later
                builder = new CompactEventBuilder(buf);

                boolean noBuildableEvents = false;

                while (state == CODAState.ACTIVE || paused) {

                    if (noBuildableEvents) {
                        Thread.sleep(250);
                    }
                    else {


                        try {
                            // Add ROC Raw Records as PayloadBuffer objects
                            if (isSingleEventMode) {
                                // nothing right now
                            }
                            else {
                                bufItem = bbSupply.get();
                                buf = bufItem.getBuffer();
                                buf.order(outputOrder);

                                eventNumber = (int) myEventNumber;

                                // Create a ROC Raw Data Record event/bank
                                // with eventBlockSize physics events in it.
                                builder.setBuffer(buf);

                                int rocTag = Evio.createCodaTag(status, id);
                                builder.openBank(rocTag, eventBlockSize, DataType.BANK);

                                // Create the trigger bank (of segments)
                                builder.openBank(CODATag.RAW_TRIGGER_TS.getValue(), eventBlockSize, DataType.SEGMENT);


                                for (int i = 0; i < eventBlockSize; i++) {
                                    // Each segment contains eventNumber & timestamp of corresponding event in data bank
                                    builder.openSegment(triggerType, DataType.UINT32);
                                    // Generate 3 segments per event (no miscellaneous data)
                                    segmentData[0] = eventNumber + i;
                                    segmentData[1] = (int)  timestamp; // low 32 bits
                                    segmentData[2] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
                                    timestamp += 4;

                                    builder.addIntData(segmentData);
                                    builder.closeStructure();
                                }
                                // Close trigger bank
                                builder.closeStructure();

                                // Create a single data block bank
                                index = 0;

                                // First put in starting event # (32 bits)
                                data[index++] = eventNumber;

                                // if single event mode, put in timestamp
                                if (isSingleEventMode) {
                                    data[index++] = (int)  timestamp; // low 32 bits
                                    data[index++] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
                                }

                                for (int i=0; i < generatedDataWords; i++) {
                                    data[index+i] = i;
                                }

                                int dataTag = Evio.createCodaTag(status, detectorId);
                                builder.openBank(dataTag, eventBlockSize, DataType.UINT32);
                                builder.addIntData(data);

                                builder.closeAll();
                            }
                        }
                        catch (EvioException e) { /* should not happen */ }


                        // Put generated events into output channel
                        eventToOutputRing(myId, buf, bufItem, bbSupply);


                        eventCountTotal += eventBlockSize;
                        wordCountTotal  += eventWordSize;

                        myEventNumber += eventProducingThreads*eventBlockSize;
                        timestamp     += 4*eventProducingThreads*eventBlockSize;
                        myRocRecordId += eventProducingThreads;
//System.out.println("  Roc mod: next (id=" + myId + "):\n    record id = " + myRocRecordId +
//                           ", ev # = " +myEventNumber + ", ts = " + timestamp);

//                        Thread.sleep(1);

//                        if (userEventLoop == userEventLoopMax - 10) {
//System.out.println("  Roc mod: INSERT USER EVENT");
//                            eventToOutputChannel(Evio.createUserBuffer(outputOrder), 0, 0);
//                        }

                        // Do the requisite number of iterations before syncing up
                        if (synced && userEventLoop-- < 1) {
                            // Did we receive the END command yet? ("state" is volatile)
                            if (state == CODAState.DOWNLOADED) {
                                // END command has arrived
//System.out.println("  Roc mod: end has arrived");
                                gotEndCommand = true;
                            }

                            // Send message to synchronizer that we're waiting
                            // and whether or not we got the END command. Only
                            // want 1 msg sent, so have the first thread do it.
                            if (myId == 0) {
                                sendMsgToSynchronizer(gotEndCommand);
                            }

                            // Wait for synchronizer's response before continuing
//System.out.println("  Roc mod: phaser await advance");
                            phaser.arriveAndAwaitAdvance();

                            // Every ROC has received the END command and completed the
                            // same number of iterations, therefore it's time to quit.
                            if (timeToEnd) {
//System.out.println("  Roc mod: SYNC told me to quit");
                                endPhaser.arriveAndDeregister();
                                return;
                            }

                            userEventLoop = userEventLoopMax;
                        }
                    }

                    now = System.currentTimeMillis();
                    deltaT = now - start_time;

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
                        start_time = now;
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
                state = CODAState.ERROR;
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
        Date theDate = new Date();
        State previousState = state;
        state = CODAState.CONFIGURED;

        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;

        // rb.next() can block in endThreads() when doin a RESET.
        // So just kill threads by force instead of bein nice about it.
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

        if (synced) {
            // Wait until loop is done writing events
//System.out.println("  Roc mod: end(), endPhaser block here");
            endPhaser.arriveAndAwaitAdvance();
//System.out.println("  Roc mod: end(), past endPhaser");
        }

        // Put this line down here so we don't pop out of event-generating
        // loop before END is properly dealt with.
        state = CODAState.DOWNLOADED;

        // Put in END event
        PayloadBuffer pBuf = Evio.createControlBuffer(ControlType.END, 0, 0,
                                                      (int)eventCountTotal, 0,
                                                      outputOrder);
        // Send to first ring
        eventToOutputChannel(pBuf, 0, 0);

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
    }


    /** {@inheritDoc} */
    public void prestart() {

        state = CODAState.PAUSED;

        // Reset some variables
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;
        rocRecordId = 0;

        if (synced) {
            phaser = new Phaser(eventProducingThreads + 1);
            endPhaser = new Phaser(2);
            timeToEnd = false;
            gotEndCommand = false;
        }

        // create threads objects (but don't start them yet)
        RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), name+":watcher");

        for (int i=0; i < eventProducingThreads; i++) {
            eventGeneratingThreads[i] = new EventGeneratingThread(i, emu.getThreadGroup(),
                                                                  name+":generator");
        }

        // Put in PRESTART event
        PayloadBuffer pBuf = Evio.createControlBuffer(ControlType.PRESTART, emu.getRunNumber(),
                                                      emu.getRunTypeId(), 0, 0,
                                                      outputOrder);
        // Send to first ring
        eventToOutputChannel(pBuf, 0, 0);
System.out.println("  Roc mod: insert PRESTART event");

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}

        // Subscribe to cMsg server for ROC synchronization purposes
        if (synced) {
            cMsgServer = emu.getCmsgPortal().getCmsgServer();
            try {
                System.out.println("  Roc mod: subscribe()" );
                cmsgSubHandle = cMsgServer.subscribe("sync", "ROC", callback, null);
            }
            catch (cMsgException e) {/* never happen */}
        }
    }


    /** {@inheritDoc} */
    public void go() {
        if (state == CODAState.ACTIVE) {
//System.out.println("  Roc mod: we must have hit go after PAUSE");
        }

        // Put in GO event
        PayloadBuffer pBuf = Evio.createControlBuffer(ControlType.GO, 0, 0,
                                                      (int) eventCountTotal, 0,
                                                      outputOrder);
        eventToOutputChannel(pBuf, 0, 0);
System.out.println("  Roc mod: insert GO event");

//        try {
//            Thread.sleep(500);
//        }
//        catch (InterruptedException e) {}

        state = CODAState.ACTIVE;

        // start up all threads
        if (RateCalculator == null) {
            RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), name+":watcher");
        }

        if (RateCalculator.getState() == Thread.State.NEW) {
            RateCalculator.start();
        }

        for (int i=0; i < eventProducingThreads; i++) {
            if (eventGeneratingThreads[i] == null) {
                eventGeneratingThreads[i] = new EventGeneratingThread(i, emu.getThreadGroup(),
                                                                      name+":generator");
            }

//System.out.println("  Roc mod: event generating thread " + eventGeneratingThreads[i].getName() + " isAlive = " +
//                    eventGeneratingThread.isAlive());
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