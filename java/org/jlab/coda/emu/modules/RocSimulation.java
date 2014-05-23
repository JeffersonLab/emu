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

    /** Number of Roc raw events generated by one call to data generating method. */
    private int numEvents;

    /** Size of a single generated Roc raw event in 32-bit words (including header). */
    private int eventWordSize;

    // The following members are for keeping statistics
    /** The number of the event to be assigned to that which is built next. */
    private long eventNumber;

    /** The number of the last event that this ROC created. */
    private long lastEventNumberCreated;

    /** Used in debugging output to track each time an array of evio events is generated. */
    static int jobNumber;



    /**
     * Constructor RocSimulation creates a simulated ROC instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public RocSimulation(String name, Map<String, String> attributeMap, Emu emu) {

        super(name, attributeMap, emu);

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

        numEvents = 10;

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
    public QueueItemType getOutputQueueItemType() {return QueueItemType.PayloadBuffer;}

    //---------------------------------------
    // Threads
    //---------------------------------------


    /** End all threads because a RESET/END cmd or END event came through.  */
    private void endThreads() {
        // The order in which these threads are shutdown does(should) not matter.
        // Transport objects should already have been shutdown followed by this module.
        if (RateCalculator != null) RateCalculator.interrupt();
        RateCalculator = null;

        for (EventGeneratingThread thd : eventGeneratingThreads) {
            if (thd != null) {
                try {
                    // Kill this thread before thread pool threads to avoid exception.
    //System.out.println("          RocSim endThreads: try joining ev-gen thread ...");
                    thd.join();
    //System.out.println("          RocSim endThreads: done");
                }
                catch (InterruptedException e) {}

            }
            thd = null;
        }
    }



    /**
     * This method is called by a DataGenerateJob running in a thread from a pool.
     * It generates many ROC Raw events in it with simulated
     * FADC250 data, and places them onto the ring buffer of an output channel.
     *
     * @param ringNum the id number of the output channel ring buffer
     * @param bank    the event to place on output channel ring buffer
     * @throws InterruptedException if put or wait interrupted
     */
    private void eventToOutputRing(int ringNum, PayloadBuffer bank) throws InterruptedException {

        // Place Data Transport Record on output ring buffer
//System.out.println("eventToOutput_RING: set event type");

        RingBuffer rb = outputChannels.get(0).getRingBuffers()[ringNum];

//System.out.println("     : wait for next ring buf for writing");
        long nextRingItem = rb.next();
//System.out.println("     : Get sequence " + nextRingItem);
        PayloadBuffer pb = (PayloadBuffer) rb.get(nextRingItem);
        pb.setBuffer(bank.getBuffer());
        pb.setEventType(bank.getEventType());
//System.out.println("     : set event type to " + bank.getEventType());
        pb.setControlType(bank.getControlType());
        pb.setSourceName(bank.getSourceName());
        pb.setRecordId(bank.getRecordId());

        rb.publish(nextRingItem);
    }


    /**
     * This method is called by a DataGenerateJob running in a thread from a pool.
     * It generates many ROC Raw events in it with simulated
     * FADC250 data, and places them onto the ring buffer of an output channel.
     *
     * @param ringNum the id number of the output channel ring buffer
     * @param bufs    the events to place on output channel ring buffer
     * @param items   item corresponding to the buffer allowing buffer to be reused
     * @throws InterruptedException if put or wait interrupted
     */
    private void eventToOutputRing(int ringNum, ByteBuffer[] bufs,
                                   ByteBufferItem[] items, ByteBufferSupply bbSupply) throws InterruptedException {

        int index = 0;
        for (ByteBuffer buf : bufs) {
            RingBuffer rb = outputChannels.get(0).getRingBuffers()[ringNum];

//System.out.println("     : wait for next ring buf for writing");
            long nextRingItem = rb.next();
//System.out.println("     : Got sequence " + nextRingItem);
            PayloadBuffer pb = (PayloadBuffer) rb.get(nextRingItem);
            pb.setBuffer(buf);
            pb.setEventType(EventType.ROC_RAW);
            pb.setControlType(null);
            pb.setSourceName(null);
            pb.setReusableByteBuffer(bbSupply, items[index++]);

//System.out.println("published : record id " + rrId + " to ring " + ringNum);
            rb.publish(nextRingItem);
        }
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
            numEvents = 2;
            PayloadBank[] evs = Evio.createRocDataEvents(id, triggerType,
                                                         detectorId, 0,
                                                         0, eventBlockSize,
                                                         0L, 0,
                                                         numEvents,
                                                         isSingleEventMode);

            eventWordSize = evs[0].getHeader().getLength() + 1;
//System.out.println("ROCSim: each generated event size = " + (4*eventWordSize) +
//                   " bytes, words = " + (eventWordSize) + ", tag = " +
//                   evs[0].getHeader().getTag() + ", num = " + evs[0].getHeader().getNumber());

            // Set & initialize values
            numEvents = 10;
            myRocRecordId = rocRecordId + myId;
            myEventNumber = eventNumber + myId*eventBlockSize*numEvents;
            timestamp = myId*4*eventBlockSize*numEvents;

System.out.println("\n\nStart With (id=" + myId + "):\n    record id = " + myRocRecordId +
                           ", ev # = " +myEventNumber + ", ts = " + timestamp);

            bbSupply = new ByteBufferSupply(8192, 4*eventWordSize);
        }




        public void run() {

            boolean sentOneAlready = false;
            int  status=0, skip=3;
            long oldVal=0L, totalT=0L, totalCount=0l;
            long now, deltaT, start_time = System.currentTimeMillis();
            ByteBuffer[] evs;
            ByteBuffer buf = ByteBuffer.allocate(8);
            ByteBufferItem[] items = new ByteBufferItem[numEvents];

            try {

                // Use dummy arg that's overwritten later
                builder = new CompactEventBuilder(buf);

                while (state == CODAState.ACTIVE || paused) {

                    if (sentOneAlready && (endLimit > 0) && (myEventNumber + numEvents > endLimit)) {
                        System.out.println("\nRocSim: hit event number limit of " + endLimit + ", quitting\n");

                        // Put in END event
                        try {
                            System.out.println("          RocSim: Putting in END control event");
                            ByteBuffer controlBuf = Evio.createControlBuffer(ControlType.END, 0, 0,
                                                                             (int) eventCountTotal, 0);
                            PayloadBuffer pBuf = new PayloadBuffer(controlBuf);
                            pBuf.setEventType(EventType.CONTROL);
                            pBuf.setControlType(ControlType.END);
                            eventToOutputRing(myId, pBuf);
                            if (endCallback != null) endCallback.endWait();
                        }
                        catch (InterruptedException e) {}
                        catch (EvioException e) {/* never happen */}

                        return;
                    }
                    sentOneAlready = true;

                    // Get "numEvents" number of ByteBuffer objects to write events into
                    evs = Evio.createRocDataEventsFast(id, triggerType,
                                                       detectorId, status,
                                                       (int) myEventNumber, eventBlockSize,
                                                       timestamp,
                                                       numEvents,
                                                       isSingleEventMode,
                                                       bbSupply, builder, items);

                    // Put generated events into output channel
                    eventToOutputRing(myId, evs, items, bbSupply);

                    // stats // TODO: problem with multiple threads writing to these stat values
                    //assert(evs.length == numEvents);
                    eventCountTotal += numEvents * eventBlockSize;
                    wordCountTotal  += numEvents * eventWordSize;

                    myEventNumber += eventProducingThreads*eventBlockSize*numEvents;
                    timestamp     += 4*eventProducingThreads*eventBlockSize*numEvents;
                    myRocRecordId += eventProducingThreads;
//System.out.println("Next (id=" + myId + "):\n    record id = " + myRocRecordId +
//                           ", ev # = " +myEventNumber + ", ts = " + timestamp);
//
//                    Thread.sleep(20);

                    now = System.currentTimeMillis();
                    deltaT = now - start_time;

                    if (myId == 0 && deltaT > 2000) {
                        if (skip-- < 1) {
                            totalT += deltaT;
                            totalCount += myEventNumber-oldVal;
                            System.out.println("event rate = " + String.format("%.3g", ((myEventNumber-oldVal)*1000./deltaT) ) +
                                                       " Hz,  avg = " + String.format("%.3g", (totalCount*1000.)/totalT));
                        }
                        else {
                            System.out.println("event rate = " + String.format("%.3g", ((myEventNumber-oldVal)*1000./deltaT) ) + " Hz");
                        }
                        start_time = now;
                        oldVal = myEventNumber;
                    }

                }
            }
            catch (InterruptedException e) {}
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

        endThreads();

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
        state = CODAState.DOWNLOADED;

        endThreads();

        paused = false;

//            // Put in END event
//            try {
//System.out.println("          RocSim: Putting in END control event");
//                EvioEvent controlEvent = Evio.createControlEvent(ControlType.END, 0, 0,
//                                                                 (int)eventCountTotal, 0);
//                PayloadBank bank = new PayloadBank(controlEvent);
//                bank.setEventType(EventType.CONTROL);
//                bank.setControlType(ControlType.END);
//                outputChannels.get(0).getQueue().put(new RingItem(bank));
//                if (endCallback != null) endCallback.endWait();
//            }
//            catch (InterruptedException e) {}
//            catch (EvioException e) {/* never happen */}

        // set end-of-run time in local XML config / debug GUI
        try {
            // Set end-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
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

        // create threads objects (but don't start them yet)
        RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), name+":watcher");

        for (int i=0; i < eventProducingThreads; i++) {
            eventGeneratingThreads[i] = new EventGeneratingThread(i, emu.getThreadGroup(),
                                                                  name+":generator");
        }

        // Put in PRESTART event
        try {
            ByteBuffer controlBuf = Evio.createControlBuffer(ControlType.PRESTART, emu.getRunNumber(),
                                                             emu.getRunTypeId(), 0, 0);
            PayloadBuffer pBuf = new PayloadBuffer(controlBuf);
            pBuf.setEventType(EventType.CONTROL);
            pBuf.setControlType(ControlType.PRESTART);
            // Send to first ring
            eventToOutputRing(0, pBuf);
System.out.println("          RocSim: Put in PRESTART control event");
        }
        catch (InterruptedException e) {}
        catch (EvioException e) {/* never happen */}

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}
    }


    /** {@inheritDoc} */
    public void go() {
        if (state == CODAState.ACTIVE) {
//System.out.println("          RocSim: We musta hit go after PAUSE");
        }

        // Put in GO event
        try {
            ByteBuffer controlBuf = Evio.createControlBuffer(ControlType.GO, 0, 0,
                                                             (int) eventCountTotal, 0);
            PayloadBuffer pBuf = new PayloadBuffer(controlBuf);
            pBuf.setEventType(EventType.CONTROL);
            pBuf.setControlType(ControlType.GO);
            eventToOutputRing(0, pBuf);
System.out.println("          RocSim: Put in GO control event");
        }
        catch (InterruptedException e) {}
        catch (EvioException e) {/* never happen */}

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

//System.out.println("ROC: event generating thread " + eventGeneratingThreads[i].getName() + " isAlive = " +
//                    eventGeneratingThread.isAlive());
            if (eventGeneratingThreads[i].getState() == Thread.State.NEW) {
    //System.out.println("starting event generating thread");
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