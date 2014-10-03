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
import org.jlab.coda.emu.*;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;

import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.nio.ByteBuffer;
import java.util.*;
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

    /**
     * Number of Evio events generated & sent before an END event is sent.
     * Value of 0 means don't end any END events automatically.
     */
    private int endLimit;

    /** Size of a single generated Roc raw event in 32-bit words (including header). */
    private int eventWordSize;

    //----------------------------------------------------
    // Members for keeping statistics
    //----------------------------------------------------
    /** The number of the event to be assigned to that which is built next. */
    private long eventNumber;

    /** The number of the last event that this ROC created. */
    private long lastEventNumberCreated;

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

        // Number of Roc raw events generated by one call to data generating method
        outputRingChunk = 10;

        // How many events to generate before sending END event, defaults to 0 (no limit)
        endLimit = 0;
        try { endLimit = Integer.parseInt(attributeMap.get("end")); }
        catch (NumberFormatException e) { /* defaults to 0 */ }
        if (endLimit < 0) endLimit = 0;

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
     * @param bufs    the events to place on output channel ring buffer
     * @param items   item corresponding to the buffer allowing buffer to be reused
     */
    private void eventToOutputRing(int ringNum, ByteBuffer[] bufs,
                                   ByteBufferItem[] items, ByteBufferSupply bbSupply) {

        int index = 0;
        for (ByteBuffer buf : bufs) {
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
            ri.setReusableByteBuffer(bbSupply, items[index++]);

//System.out.println("  Roc mod: published record id " + rrId + " to ring " + ringNum);
            rb.publish(nextRingItem);
        }
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


        EventGeneratingThread(int id, ThreadGroup group, String name) {
            super(group, name);
            this.myId = id;

            // Found out how many events are generated per method call, and the event size
            PayloadBank[] evs = Evio.createRocDataEvents(id, triggerType,
                                                         detectorId, 0,
                                                         0, eventBlockSize,
                                                         0L, 0,  2,
                                                         isSingleEventMode);

            eventWordSize = evs[0].getHeader().getLength() + 1;
System.out.println("  Roc mod: each generated event size = " + (4*eventWordSize) +
                   " bytes, words = " + (eventWordSize) + ", tag = " +
                   evs[0].getHeader().getTag() + ", num = " + evs[0].getHeader().getNumber());

            // Set & initialize values
            myRocRecordId = rocRecordId + myId;
            myEventNumber = eventNumber + myId*eventBlockSize*outputRingChunk;
            timestamp = myId*4*eventBlockSize*outputRingChunk;

System.out.println("  Roc mod: start With (id=" + myId + "):\n    record id = " + myRocRecordId +
                           ", ev # = " +myEventNumber + ", ts = " + timestamp);

            bbSupply = new ByteBufferSupply(32768, 4*eventWordSize);
        }




        public void run() {

            boolean sentOneAlready = false;
            int  userEventLoopMax=1000;
            int  status=0, skip=3,  userEventLoop = userEventLoopMax;
            long oldVal=0L, totalT=0L, totalCount=0l;
            long now, deltaT, start_time = System.currentTimeMillis();
            ByteBuffer[] evs;
            ByteBuffer buf = ByteBuffer.allocate(8);
            ByteBufferItem[] items = new ByteBufferItem[outputRingChunk];

            try {

                // Use dummy arg that's overwritten later
                builder = new CompactEventBuilder(buf);

                boolean noBuildableEvents = false;

                while (state == CODAState.ACTIVE || paused) {

                    if (noBuildableEvents) {
                        Thread.sleep(250);
                    }
                    else {
                        if (sentOneAlready && (endLimit > 0) && (myEventNumber + outputRingChunk > endLimit)) {
                            System.out.println("  Roc mod: hit event number limit of " + endLimit + ", quitting");

                            // Put in END event
                            System.out.println("  Roc mod: insert END event");
                            ByteBuffer controlBuf = Evio.createControlBuffer(ControlType.END, 0, 0,
                                                                             (int) eventCountTotal, 0,
                                                                             outputOrder);
                            PayloadBuffer pBuf = new PayloadBuffer(controlBuf);
                            pBuf.setEventType(EventType.CONTROL);
                            pBuf.setControlType(ControlType.END);
                            eventToOutputChannel(pBuf, 0, myId);
                            if (endCallback != null) endCallback.endWait();

                            return;
                        }
                        sentOneAlready = true;

                        // Get "outputRingChunk" number of ByteBuffer objects to write events into
                        evs = Evio.createRocDataEventsFast(id, triggerType,
                                                           detectorId, status,
                                                           (int) myEventNumber, eventBlockSize,
                                                           timestamp,
                                                           outputRingChunk,
                                                           isSingleEventMode,
                                                           bbSupply, builder,
                                                           items, outputOrder);

                        // Put generated events into output channel
                        eventToOutputRing(myId, evs, items, bbSupply);
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
                            // and whether or not we got the END command.
                            sendMsgToSynchronizer(gotEndCommand);

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

                        eventCountTotal += outputRingChunk * eventBlockSize;
                        wordCountTotal  += outputRingChunk * eventWordSize;

                        myEventNumber += eventProducingThreads*eventBlockSize*outputRingChunk;
                        timestamp     += 4*eventProducingThreads*eventBlockSize*outputRingChunk;
                        myRocRecordId += eventProducingThreads;
//System.out.println("  Roc mod: next (id=" + myId + "):\n    record id = " + myRocRecordId +
//                           ", ev # = " +myEventNumber + ", ts = " + timestamp);
//
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
        ByteBuffer controlBuf = Evio.createControlBuffer(ControlType.END, 0, 0,
                                                         (int)eventCountTotal, 0,
                                                         outputOrder);
        PayloadBuffer pBuf = new PayloadBuffer(controlBuf);
        pBuf.setEventType(EventType.CONTROL);
        pBuf.setControlType(ControlType.END);
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
        eventNumber = 1L;
        lastEventNumberCreated = 0L;

        if (synced) {
            phaser = new Phaser(2);
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
        ByteBuffer controlBuf = Evio.createControlBuffer(ControlType.PRESTART, emu.getRunNumber(),
                                                         emu.getRunTypeId(), 0, 0,
                                                         outputOrder);
        PayloadBuffer pBuf = new PayloadBuffer(controlBuf);
        pBuf.setEventType(EventType.CONTROL);
        pBuf.setControlType(ControlType.PRESTART);
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
        ByteBuffer controlBuf = Evio.createControlBuffer(ControlType.GO, 0, 0,
                                                         (int) eventCountTotal, 0,
                                                         outputOrder);
        PayloadBuffer pBuf = new PayloadBuffer(controlBuf);
        pBuf.setEventType(EventType.CONTROL);
        pBuf.setControlType(ControlType.GO);
        eventToOutputChannel(pBuf, 0, 0);
System.out.println("  Roc mod: insert GO event");

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