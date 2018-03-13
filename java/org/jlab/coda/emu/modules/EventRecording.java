/*
 * Copyright (c) 2012, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.modules;

import com.lmax.disruptor.*;
import org.jlab.coda.emu.*;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODAStateIF;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.emu.support.transport.TransportType;

import java.util.*;

/**
 * <pre><code>
 *
 *
 *
 *                Ring Buffer (single producer, lock free)
 *                   ____
 *                 /  |  \
 *         ^      /1 _|_ 2\
 *         |     |__/   \__|
 *     Producer->|6 |   | 3|
 *               |__|___|__|
 *                \ 5 | 4 / <-- Recording Thread
 *                 \__|__/         |
 *                                 V
 *
 *
 * Actual input channel ring buffer has thousands of events (not 6).
 * The producer is a single input channel which reads incoming data,
 * parses it and places it into the ring buffer.
 * *
 *  Input Channels
 *  (evio bank              RB1  RB2
 *   ring buffer)            |    |
 *                           |    |
 *                           V    V
 *  1 RecordingThread:         RT1
 *  Grab 1 event &              |
 *  place in all module's       |
 *  output channels             |
 *                              |
 *                              V
 * Output Channel(s):       OC1, OC2, ...
 * (1 ring buffer for
 *  each channel)
 *
 * </code></pre><p>
 *
 * This class is the event recording module. It has one recording thread.
 * This thread exists to take buffers of Evio banks off of the input channels.
 * A copy of each user and control event is placed into all of the output channels.
 * The physics events, on the other hand, are split between the output channels
 * on a round-robin basis. If no output channels are defined in the config file,
 * this module discards all events.<p>
 *
 * Things get trickier with multiple input channels. There should never be more
 * than 2 input channels in which case one must be an emu socket and the other an
 * ET channel. The emu socket is assumed to carry the main flow of physics events.
 * Any et input channel is assumed to carry user events and is given a lower priority.
 * This means reading from it should never block.
 *
 * @author timmer
 * (2012)
 */
public class EventRecording extends ModuleAdapter {

    /** Thread used to record events. */
    private Thread recordingThread;

    private DataChannel mainInputChannel;
    private DataChannel   etInputChannel;
    private DataChannel   etOutputChannel;
    private DataChannel[] fileOutputChannels;
    private RingItem[]    outputEvents;

    private int fileOutChannelCount;
    private int etOutChannelCount;
    private int mainIndex, etIndex, etOutIndex;

    private boolean singleInput, singleOutput;

   /** END event detected by one of the recording threads. */
    private volatile boolean haveEndEvent;

    /** Maximum time in milliseconds to wait when commanded to END but no END event received. */
    private long endingTimeLimit = 30000;

    /** ET output channel's prescale value. */
    private int prescale;

    // ---------------------------------------------------

    /** If {@code true}, get debug print out. */
    private boolean debug = false;

    //-------------------------------------------
    // Disruptor (RingBuffer)  stuff
    //-------------------------------------------

    /** One RingBuffer. */
    private RingBuffer<RingItem>[] ringBuffersIn;

    /** Size of RingBuffer for input channel. */
    private int[] ringBufferSizes;

    /** One sequence for recording thread. */
    public Sequence[] sequencesIn;

    /** All recording threads share one barrier. */
    public SequenceBarrier[] barriersIn;


    /**
     * Constructor creates a new EventRecording instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public EventRecording(String name, Map<String, String> attributeMap, Emu emu) {

        super(name, attributeMap, emu);

        // At this point there is no need to go beyond 1 event recording thread,
        // although the ER was originally written to handle multiple threads.
        // Currently, however, due to recent changes in which "first events"
        // arriving prior to a prestart are recorded after, only 1 recording
        // thread can be run without breaking this feature.
    }


    /** {@inheritDoc} */
    public void clearChannels() {
        inputChannels.clear();
        outputChannels.clear();
    }


    //---------------------------------------
    // Start and end threads
    //---------------------------------------


    /**
     * Method to start threads for stats, filling Qs, and recording events.
     * It creates these threads if they don't exist yet.
     */
    private void startThreads() {
        if (RateCalculator != null) {
            RateCalculator.interrupt();
        }
        RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), name+":watcher");

        if (RateCalculator.getState() == Thread.State.NEW) {
            RateCalculator.start();
        }

//        if (singleInput && singleOutput) {
//            recordingThread = new RecordingThreadOneToOne(emu.getThreadGroup(), name+":recorder");
//        }
//        else {
            recordingThread = new RecordingThreadTwoToMany(emu.getThreadGroup(), name + ":recorder");
//        }
        recordingThread.start();
    }


    /**
     * Interrupt record thread because an END cmd/event or RESET cmd came through.
     * The record thread calling this method is not interrupted.
     *
     * @param end if <code>true</code> called from end(), else called from reset()
     */
    private void interruptThreads(boolean end) {
        // Although the emu's end() method checks to see if the END event has made it
        // all the way through, it gives up if it takes longer than about 30 seconds
        // at each channel or module.
        // Check again if END event has arrived.
        if (end && !haveEndEvent) {
System.out.println("  ER mod: will end thread but no END event!");
            moduleState = CODAState.ERROR;
            emu.setErrorState("ER will end thread but no END event");
        }

        if (RateCalculator != null) {
            RateCalculator.interrupt();
        }

        if (recordingThread != null) {
            recordingThread.interrupt();
        }
    }


    /**
     * Try joining record thread, up to 1 sec.
     */
    private void joinThreads() {
        if (RateCalculator != null) {
            try {
                RateCalculator.join(1000);
            }
            catch (InterruptedException e) {}
        }

        if (recordingThread != null) {
            try {
                recordingThread.join(1000);
            }
            catch (InterruptedException e) {}
        }
    }


    /**
     * This thread is started by the GO transition and runs while the state of the module is ACTIVE.
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * pulls one bank off the input DataChannel. The bank is copied and placed in each output
     * channel. The count of outgoing banks and the count of data words are incremented.
     *
     * This class is written so that there must only be one RecordingThread.
     * It also takes any events arriving prior to prestart and throws them away unless it's
     * a "first event" (user type) in which case the very last "first event" gets passed on
     * to the output channel(s).
     */
    private class RecordingThreadOneToOne extends Thread {

        RecordingThreadOneToOne(ThreadGroup group, String name) {
            super(group, name);
        }


        @Override
        public void run() {

            RingItem    ringItem    = null;
            ControlType controlType = null;
            long t1, t2, counter = 0L;
            final long timeBetweenSamples = 500; // sample every 1/2 sec
            int totalNumberEvents=1, wordCount=0, firstEventsWords=0;
            ArrayList<RingItem> firstEvents = new ArrayList<>(4);
            boolean gotBank, gotPrestart=false, isPrestart=false;

            // Ring Buffer stuff
            // Available sequence (largest index of items desired)
            long availableSequence = -2L;
            // Next sequence (index of next item desired)
            long nextSequence = sequencesIn[0].get() + 1L;

            // Beginning time for sampling control
            t1 = System.currentTimeMillis();

//            int printCounter=0;

            while (moduleState == CODAState.ACTIVE || paused) {

                try {
                    gotBank = false;

                    // Will BLOCK here waiting for item if none available
                    // Only wait or read-volatile-memory if necessary ...
                    if (availableSequence < nextSequence) {
                        // Available sequence may be larger than what we desired
//System.out.println("  ER mod: wait for seq " + nextSequence);
                        availableSequence = barriersIn[0].waitFor(nextSequence);
                    }

                    // scale from 0% to 100% of ring buffer size
                    t2 = emu.getTime();
                    if (t2-t1 > timeBetweenSamples) {
                        //inputChanLevels[0] = ((int)(availableSequence - nextSequence) + 1)*100/ringBufferSize;
                        //inputChanLevels[0] = ((int)(ringBufferIn.getCursor() - nextSequence) + 1)*100/ringBufferSize;

                        inputChanLevels[0] = ((int)(ringBuffersIn[0].getCursor() -
                                                    ringBuffersIn[0].getMinimumGatingSequence()) + 1)*100/ ringBufferSizes[0];
                        if (inputChanLevels[0] > 100) {
                            System.out.println("INPUT CHANNEL LEVEL IS TOO HIGH = " + inputChanLevels[0]);
                        }
                        //if (printCounter++ % 100000 == 0) {
                        //    System.out.println("in level = " + inputChanLevels[0]);
                        //}
                        t1 = t2;
                    }

                    while (nextSequence <= availableSequence) {

                        // Get item from input channel
                        ringItem = ringBuffersIn[0].get(nextSequence);
                        wordCount = ringItem.getNode().getLength() + 1;
                        controlType = ringItem.getControlType();
                        totalNumberEvents = ringItem.getEventCount();

//                        // Code for testing changing input/output channel fill levels.
//                        // TODO: Comment out when finished testing!!!
//                        if (counter++ % 1000 == 0) {
//                            Thread.sleep(1);
//                        }

                        // Look at control events ...
                        if (controlType != null) {
//System.out.println("  ER mod: got control event, " + controlType);
                            // Looking for prestart
                            if (controlType.isPrestart()) {
                                prestartCallback.endWait();
                                if (gotPrestart) {
                                    throw new EmuException("got 2 prestart events");
                                }
                                isPrestart = gotPrestart = true;
                                wordCount = 5 + firstEventsWords;
                                totalNumberEvents = 1 + firstEvents.size();
                            }
                            else if (!gotPrestart) {
                                throw new EmuException("prestart, not " + controlType +
                                                       ", must be first control event");
                            }
                            else if (controlType != ControlType.GO && controlType != ControlType.END) {
                                throw new EmuException("second control event must be go or end");
                            }
                        }

                        // If we haven't gotten the prestart event ...
                        if (!gotPrestart) {
                            // Throw away all events except any "first events"
                            if (!ringItem.isFirstEvent()) {
System.out.println("  ER mod: THROWING AWAY event of type " + ringItem.getEventType() + " which arrived before PRESTART event");
                                // Release ByteBuffer used by item since it will NOT
                                // be sent to output channel where this is normally done.
                                ringItem.releaseByteBuffer();
                            }
                            else {
                                // Store first events until prestart is received, then write.
                                //
                                // We do NOT, however, want to leave them in the byte buffers
                                // provided in the ET input channel which are obtained from a
                                // ByteBufferSupply and are thus part of a ring buffer.
                                // There are a limited number of these and if not released immediately
                                // here, these buffers may all get used up - bringing things to a
                                // grinding halt in the ET input channel.
                                //
                                // We also do not want to increase "nextSequence" since if we keep
                                // over 4096, the same ringItems will be reused when rb.get() is
                                // called.
                                //
                                // Solution is to copy the ringItem right now and release the
                                // original ringItem and the buffer from the supply.


                                // Cloning the ringItem makes a copy of the ByteBuffer it contains
                                RingItem newRingItem = (PayloadBuffer)((PayloadBuffer)ringItem).clone();

                                // If however, the data was NOT contained in a ByteBuffer but in
                                // an EvioNode instead, copy that data ...
                                if (ringItem.getBuffer() == null) {
                                    // Get a copy of the node data into the buffer
                                    newRingItem.setBuffer(ringItem.getNode().getStructureBuffer(true));
                                    newRingItem.setNode(null);
                                }

                                // Release old stuff
                                ringItem.setNode(null);
                                ringItem.setBuffer(null);
                                ringItem.releaseByteBuffer();

System.out.println("  ER mod: STORE \"first event\" of type " + ringItem.getEventType() + " which arrived before PRESTART event");
                                // Copy new stuff into list
                                firstEvents.add(newRingItem);
                                firstEventsWords += wordCount;
                            }

                            // Release the ring buffer slot of input channel for re-use.
                            // This is fine since we copied the ringItem and released the
                            // original data.

                            sequencesIn[0].set(nextSequence++);

                            continue;
                        }

//System.out.println("  ER mod: accept item " + nextSequence + ", type " + ringItem.getEventType());
                        gotBank = true;
                        break;
                    }

                    if (!gotBank) {
                        continue;
                    }

                    if (outputChannelCount > 0) {
                        // If multiple output channels, we must copy the ringItem.
                        // Make sure the buffer being used is not prematurely released
                        // for reuse. Do this by increasing the # of buffer users.
                        if (outputChannelCount > 1) {
                            ringItem.getByteBufferItem().addUsers(outputChannelCount - 1);
                        }

                        // Prestart event is a special case as there may be "first events"
                        // which preceded it but now must come after.
                        if (isPrestart) {
//System.out.println("  ER mod: sending PRESTART to out chan");
                            // Place prestart event on first output channel
                            eventToOutputChannel(ringItem, 0, 0);

                            // Now place "first events" on the first channel
                            for (RingItem ri : firstEvents) {
System.out.println("  ER mod: sending \"first event\" to out chan");
                                eventToOutputChannel(ri, 0, 0);
                            }

                            // Copy each event and place one on each additional output channel
                            for (int j=1; j < outputChannelCount; j++) {
                                PayloadBuffer bb = new PayloadBuffer((PayloadBuffer)ringItem);
                                eventToOutputChannel(bb, j, 0);

                                for (RingItem ri : firstEvents) {
                                    bb = new PayloadBuffer((PayloadBuffer)ri);
                                    eventToOutputChannel(bb, j, 0);
                                }
                            }
                            isPrestart = false;
                        }
                        else {
                            // Place event on first output channel
//System.out.println("  ER mod: call eventToOutputChannel()");
                            eventToOutputChannel(ringItem, 0, 0);

                            // Copy event and place one on each additional output channel
                            for (int j = 1; j < outputChannelCount; j++) {
                                // This copy shares the underlying buffer / buffer item / buffer supply
                                PayloadBuffer bb = new PayloadBuffer((PayloadBuffer) ringItem);
                                ByteBufferItem item = bb.getByteBufferItem();
                                // Already done above, right???
                                //if (item != null) item.addUsers(1);
                                eventToOutputChannel(bb, j, 0);
                            }
                        }
                    }

                    eventCountTotal += totalNumberEvents;
                    wordCountTotal += wordCount;

                    // If END event, interrupt other record threads then quit this one.
                    if (controlType == ControlType.END) {
logger.info("  ER mod: found END event");
                        haveEndEvent = true;
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }

                    // Do NOT release the reusable ByteBuffer back to its supply.
                    // It was passed on to the ring buffer of the output channel.
                    // It's that channel that will release the buffer when it's done
                    // writing it to file or wherever.
                    // But if NO output, it needs to be freed now.
                    if (outputChannelCount < 1) {
                        // We want to release "first event" buffers before prestart - each in the order
                        // obtained from the channel. That's because, for the ER with one file output
                        // channel, the buffer supply is not synchronized and relies on the ER to
                        // release all buffers from the ring sequentially. This is designed to keep
                        // things fast.
                        if (isPrestart) {
                            for (RingItem ri : firstEvents) {
                                ri.releaseByteBuffer();
                            }
                            isPrestart = false;
                            firstEvents.clear();
                        }
                        ringItem.releaseByteBuffer();
                    }

                    // Release the ring buffer slot of input channel for re-use,
                    // which is fine even if we haven't released the data (in the input channel
                    // byte buffer supply item). That's because when the slot is reused, it will
                    // use a different buffer item from that supply.
                    sequencesIn[0].set(nextSequence++);

                }
                catch (InterruptedException e) {
System.out.println("  ER mod: INTERRUPTED recording thread " + Thread.currentThread().getName());
                    return;
                }
                catch (AlertException e) {
System.out.println("  ER mod: ring buf alert");
                    // If we haven't yet set the cause of error, do so now & inform run control
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ER ring buf alert");
                    return;
                }
                catch (TimeoutException e) {
System.out.println("  ER mod: ring buf timeout");
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ER ring buf timeout");
                    return;
                }
                catch (Exception e) {
System.out.println("  ER mod: MAJOR ERROR recording event: " + e.getMessage());
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ER MAJOR ERROR recording event: " + e.getMessage());
                    return;
                }
            }
if (debug) System.out.println("  ER mod: recording thread ending");
        }

    }


    /**
     * This thread is started by the GO transition and runs while the state of the module is ACTIVE.
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * pulls one bank off the input DataChannel. The bank is copied and placed in each output
     * channel. The count of outgoing banks and the count of data words are incremented.
     *
     * This class is written so that there must only be one RecordingThread.
     * It also takes any events arriving prior to prestart and throws them away unless it's
     * a "first event" (user type) in which case the very last "first event" gets passed on
     * to the output channel(s).
     */
    private class RecordingThreadTwoToMany extends Thread {

        RecordingThreadTwoToMany(ThreadGroup group, String name) {
            super(group, name);
        }


        @Override
        public void run() {

            RingItem    ringItem    = null;
            ControlType controlType = null;
            long t1, t2, counter = 0L;
            final long timeBetweenSamples = 500; // sample every 1/2 sec
            int totalNumberEvents=1, wordCount=0, firstEventsWords=0;
            RingItem firstEvent = null;
            boolean gotBank, gotPrestart=false, isPrestart=false, mainItem=true;
            boolean isUser=false, isControl=false, isFirst=false;
            EventType pBankType = null;
            int fileIndex=0;
            long physicsEventCounter=0;

            // Ring Buffer stuff, 1 input buffer for et & the other for emu
            long mainAvailableSequence = -2L;
            long mainNextSequence = sequencesIn[mainIndex].get() + 1L;

            long etAvailableSequence = -2L;
            long etNextSequence = sequencesIn[etIndex].get() + 1L;

            // Beginning time for sampling control
            t1 = System.currentTimeMillis();

            while (moduleState == CODAState.ACTIVE || paused) {

                try {
                    gotBank = false;

                    // Will BLOCK here waiting for item if none available
                    if (mainAvailableSequence < mainNextSequence) {
                        mainAvailableSequence = barriersIn[mainIndex].waitFor(mainNextSequence);
//System.out.println("  ER mod: available seq " + mainAvailableSequence);
                    }

                    // Non-blockingly check the (secondary) ET system
                    if (!singleInput && (etAvailableSequence < etNextSequence)) {
                        // Before we wait, check to see if there's anything to wait for ...
                        if (ringBuffersIn[etIndex].getCursor() >= etNextSequence) {
//System.out.println("Get event from ET ...");
                            etAvailableSequence = barriersIn[etIndex].waitFor(etNextSequence);
                        }
                    }

                    // Statistics
                    t2 = emu.getTime();
                    if (t2-t1 > timeBetweenSamples) {
                        // Scale from 0% to 100% of ring buffer size
                        inputChanLevels[mainIndex] = ((int)(ringBuffersIn[mainIndex].getCursor() -
                                                    ringBuffersIn[mainIndex].getMinimumGatingSequence()) + 1)*100 /
                                                    ringBufferSizes[mainIndex];

                        inputChanLevels[etIndex] = ((int)(ringBuffersIn[etIndex].getCursor() -
                                                    ringBuffersIn[etIndex].getMinimumGatingSequence()) + 1)*100 /
                                                    ringBufferSizes[etIndex];
                        t1 = t2;
                    }

//                    while (mainNextSequence <= mainAvailableSequence) {

                    while (mainNextSequence <= mainAvailableSequence ||
                             etNextSequence <=   etAvailableSequence ) {

                        // Get item from input channel.
                        // Deal with all secondary ET (user) events first since they
                        // may come before prestart.
                        if (etNextSequence <= etAvailableSequence) {
                            ringItem = ringBuffersIn[etIndex].get(etNextSequence);
                            mainItem = false;
                        }
                        else {
                            ringItem = ringBuffersIn[mainIndex].get(mainNextSequence);
                            mainItem = true;
                        }

                        wordCount = ringItem.getNode().getLength() + 1;
                        controlType = ringItem.getControlType();
                        totalNumberEvents = ringItem.getEventCount();
                        pBankType = ringItem.getEventType();
                        isControl = pBankType.isControl();
                        isUser    = pBankType.isUser();
                        isFirst   = ringItem.isFirstEvent();

//                        // Code for testing changing input/output channel fill levels.
//                        // TODO: Comment out when finished testing!!!
//                        if (counter++ % 1000 == 0) {
//                            Thread.sleep(1);
//                        }

                        // Look at control events ...
                        if (isControl) {

                            // Accept control events only from main channel
                            if (!mainItem) {
System.out.println("  ER mod: reject " + controlType + " event from ET input channel, release seq " + etNextSequence);
                                ringItem.releaseByteBuffer();
                                sequencesIn[etIndex].set(etNextSequence++);
                                continue;
                            }

System.out.println("  ER mod: got control event, " + controlType);
                            // Looking for prestart
                            if (controlType.isPrestart()) {
                                prestartCallback.endWait();
                                if (gotPrestart) {
                                    throw new EmuException("got 2 prestart events");
                                }
                                isPrestart = gotPrestart = true;
                                wordCount = 5 + firstEventsWords;
                                totalNumberEvents = 1;
                                if (firstEvent != null) totalNumberEvents++;
                            }
                            else if (!gotPrestart) {
                                throw new EmuException("prestart, not " + controlType +
                                                       ", must be first control event");
                            }
                            else if (controlType != ControlType.GO && controlType != ControlType.END) {
                                throw new EmuException("second control event must be go or end");
                            }
                        }

                        // If we haven't gotten the prestart event ...
                        if (!gotPrestart) {
                            // Throw away all events except any "first events"
                            if (ringItem.isFirstEvent()) {
                                // Store the latest first event until prestart is received, then write.
                                //
                                // We do NOT, however, want to leave it in the byte buffer
                                // provided in the ET input channel which are obtained from a
                                // ByteBufferSupply and are thus part of a ring buffer.
                                // There are a limited number of these and should be released immediately.
                                // Solution is to copy the ringItem right now and release the
                                // original ringItem and the buffer from the supply.

                                // Cloning the ringItem makes a copy of the ByteBuffer it contains
                                firstEvent = (PayloadBuffer)((PayloadBuffer)ringItem).clone();
                                firstEventsWords = wordCount;

                                // If however, the data was NOT contained in a ByteBuffer but in
                                // an EvioNode instead, copy that data ...
                                if (firstEvent.getBuffer() == null) {
                                    // Get a copy of the node data into the buffer
                                    firstEvent.setBuffer(ringItem.getNode().getStructureBuffer(true));
                                    firstEvent.setNode(null);
                                }
System.out.println("  ER mod: SET \"first event\" of type " + ringItem.getEventType() + " which arrived before PRESTART event");
                            }
                            else {
System.out.println("  ER mod: THROWING AWAY event of type " + ringItem.getEventType() + " which arrived before PRESTART event");
                            }

                            // Release ByteBuffer used by item since it will NOT
                            // be sent to output channel where this is normally done.
                            // Will either be thrown away (not first event) or copied.
                            ringItem.releaseByteBuffer();

                            // Release the ring buffer slot of input channel for re-use.
                            // This is fine since we copied the ringItem and released the
                            // original data.
                            if (mainItem) {
                                sequencesIn[mainIndex].set(mainNextSequence++);
                            }
                            else {
                                sequencesIn[etIndex].set(etNextSequence++);
                            }

                            continue;
                        }

//System.out.println("  ER mod: accept item " + emuNextSequence + '/' + etNextSequence +
//                   ", type " + ringItem.getEventType());
                        gotBank = true;
                        break;
                    }

                    if (!gotBank) {
                        continue;
                    }

                    if (outputChannelCount > 0) {

                        if (isControl || isFirst) {
                            // Since control events & BOR event(s) need to be duplicated and sent
                            // over all output channels, and since they are contained in an
                            // EvioNode object, it's easiest to completely copy them into a
                            // new ByteBuffer. Thus, we don't need to mess with increasing
                            // the number of users of the buffer from the original supply.

                            // Avoid writing an event in an output channel while simultaneously
                            // copying it here for putting into another channel. You'll end up
                            // copying a buffer possibly while its position and limit are being changed.
                            // NOT A GOOD IDEA! So do all copying first.
                            outputEvents[0] = ringItem;
                            for (int i=1; i < outputChannelCount; i++) {
                                outputEvents[i] = new PayloadBuffer((PayloadBuffer)ringItem);
                            }

                            // Now place one on each output channel
                            for (int j=0; j < outputChannelCount; j++) {
System.out.println("  ER mod: writing control/first (seq " + mainNextSequence +
                   '/' + etNextSequence + ") to channel " + fileOutputChannels[j].name());
                                eventToOutputChannel(outputEvents[j], j, 0);
                            }

                            // Prestart event is a special case as there may be a "first" event
                            // which preceded it but now must come after ... for each channel.
                            // This "first" or Beginning-of-run (BOR) event will be sent over
                            // all channels as opposed to the normal user events.
                            if (isPrestart) {
                                if (firstEvent != null) {
                                    // Copy first event
                                    outputEvents[0] = firstEvent;
                                    for (int i = 1; i < outputChannelCount; i++) {
                                        outputEvents[i] = new PayloadBuffer((PayloadBuffer) firstEvent);
                                    }

                                    // Place one on each output channel
                                    for (int j = 0; j < outputChannelCount; j++) {
System.out.println("  ER mod: sending first event to chan " + outputChannels.get(j).name());
                                        eventToOutputChannel(outputEvents[j], j, 0);
                                    }
                                }
                                isPrestart = false;
                            }
                        }
                        // Non-BOR user event here
                        else if (isUser) {
System.out.println("  ER mod: writing user (seq " + mainNextSequence + '/' + etNextSequence + ')');
                            // Put user events into 1 channel
                            
                            // By default make it the first file channel.
                            // If none, then the ET channel.
                            if (fileOutChannelCount > 0) {
                                eventToOutputChannel(ringItem, fileOutputChannels[0], 0);
                            }
                            else if (etOutChannelCount > 0) {
                                eventToOutputChannel(ringItem, etOutputChannel, 0);
                            }
                        }
                        // Physics event here
                        else {
                            // Any ET channel will receive a prescaled # of events.
                            if (etOutChannelCount > 0 && (physicsEventCounter++ % prescale == 0)) {
                                // Copy item
                                PayloadBuffer bb = new PayloadBuffer((PayloadBuffer) ringItem);
                                ByteBufferItem item = bb.getByteBufferItem();
                                // Write to ET system
                                eventToOutputChannel(bb, etOutputChannel, 0);
                            }
//System.out.println("  ER mod: writing ev (seq " + emuNextSequence +
//                   ") to file channel " + fileOutputChannels[fileIndex].name());
                            // Split physics events round-robin between file channels
                            eventToOutputChannel(ringItem, fileOutputChannels[fileIndex], 0);

                            // Index to next file channel
                            fileIndex = (fileIndex + 1) % fileOutChannelCount;
                        }
                    }

                    eventCountTotal += totalNumberEvents;
                    wordCountTotal += wordCount;

                    // If END event, interrupt other record threads then quit this one.
                    if (controlType == ControlType.END) {
logger.info("  ER mod: found END event");
                        haveEndEvent = true;
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }

                    // Do NOT release the reusable ByteBuffer back to its supply.
                    // It was passed on to the ring buffer of the output channel.
                    // It's that channel that will release the buffer when it's done
                    // writing it to file or wherever.
                    // But if NO output, it needs to be freed now.
                    // The first event was already copied and freed.
                    if (outputChannelCount < 1) {
                        isPrestart = false;
                        firstEvent = null;
                        ringItem.releaseByteBuffer();
                    }

                    // Release the ring buffer slot of input channel for re-use,
                    // which is fine even if we haven't released the data (in the input channel
                    // byte buffer supply item). That's because when the slot is reused, it will
                    // use a different buffer item from that supply.
                    if (mainItem) {
                        sequencesIn[mainIndex].set(mainNextSequence++);
                    }
                    else {
                        sequencesIn[etIndex].set(etNextSequence++);
                    }

                }
                catch (InterruptedException e) {
System.out.println("  ER mod: INTERRUPTED recording thread " + Thread.currentThread().getName());
                    return;
                }
                catch (AlertException e) {
System.out.println("  ER mod: ring buf alert");
                    // If we haven't yet set the cause of error, do so now & inform run control
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ER ring buf alert");
                    return;
                }
                catch (TimeoutException e) {
System.out.println("  ER mod: ring buf timeout");
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ER ring buf timeout");
                    return;
                }
                catch (Exception e) {
                    e.printStackTrace();
System.out.println("  ER mod: MAJOR ERROR recording event: " + e.getMessage());
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ER MAJOR ERROR recording event: " + e.getMessage());
                    return;
                }
            }
if (debug) System.out.println("  ER mod: recording thread ending");
        }

    }



    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        Date theDate = new Date();
        CODAStateIF previousState = moduleState;
        moduleState = CODAState.CONFIGURED;

        // Threads must be ended
        interruptThreads(false);
        joinThreads();
        // Probably won't be blocked writing to the file in a non-interrptible way
        //stopBlockingThreads();

        RateCalculator  = null;
        recordingThread = null;

        paused = false;

        if (previousState.equals(CODAState.ACTIVE)) {
            try {
                // Set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            }
            catch (DataNotFoundException e) {}
        }
    }


    /** {@inheritDoc} */
    public void go() {
        moduleState = CODAState.ACTIVE;
        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }


    /** {@inheritDoc} */
    public void end() {
        moduleState = CODAState.DOWNLOADED;

        // Recording thread should already be ended by END event.
        // If not, wait for it 1 sec.
        interruptThreads(true);
        joinThreads();

        RateCalculator = null;
        recordingThread = null;

        paused = false;

        try {
            // Set end-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {

        moduleState = CODAState.PAUSED;
        paused = true;

        // Make sure we have 1 or 2 input channels
        if (inputChannelCount != 1 && inputChannelCount != 2) {
            moduleState = CODAState.ERROR;
            emu.setErrorState("ER must have at least 1 and no more than 2 input channels, not " + inputChannelCount);
            return;
        }

        // Help to direct logic in recording thread
        if (inputChannels.size()  == 1) singleInput  = true;
        if (outputChannels.size() == 1) singleOutput = true;

        // Place to put ring level stats
        inputChanLevels  = new int[inputChannelCount];
        inputChanNames   = new String[inputChannelCount];
        outputChanLevels = new int[outputChannelCount];
        outputChanNames  = new String[outputChannelCount];

        // Initialize variables
        int indx = 0;
        mainIndex = etIndex = etOutIndex = 0;
        mainInputChannel = etInputChannel = null;
        fileOutChannelCount = etOutChannelCount = 0;

        //------------------------------------------------
        // Disruptor (RingBuffer) stuff for channels
        //------------------------------------------------
        ringBuffersIn    = new RingBuffer[inputChannelCount];
        ringBufferSizes  = new int[inputChannelCount];
        sequencesIn      = new Sequence[inputChannelCount];
        barriersIn       = new SequenceBarrier[inputChannelCount];
        outputEvents     = new RingItem[outputChannelCount];


        try {

            for (DataChannel ch : inputChannels) {

                if (ch.getTransportType() == TransportType.ET) {
                    // If there is a single input, ET is the main one
                    if (singleInput) {
                        mainInputChannel = ch;
                        mainIndex = indx;
                    }
                    // If there are multiple inputs, ET is not the main one, emu socket is.
                    else {
                        etInputChannel = ch;
                        etIndex = indx;
                        // Only expecting user events. Not expecting END event on this
                        // channel so go ahead and say it already got it so END
                        // transition will not fail.
                        ch.getEndCallback().endWait();
                    }
                }
                else {
                    mainInputChannel = ch;
                    mainIndex = indx;
                }

                // Channel names for easy gathering of stats
                inputChanNames[indx] = ch.name();
                // Get input channels' ring buffers
                ringBuffersIn[indx] = ch.getRingBufferIn();
                // Have ring sizes handy for calculations
                ringBufferSizes[indx] = ringBuffersIn[indx].getBufferSize();

                // We have 1 sequence for the recording thread for each input channel
                sequencesIn[indx] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
                // This sequence is the last consumer before producer comes along
                ringBuffersIn[indx].addGatingSequences(sequencesIn[indx]);
                // We have 1 barrier for recording thread
                barriersIn[indx] = ringBuffersIn[indx].newBarrier();

                indx++;
            }

            // Check to make sure we have proper input channels
            if (inputChannelCount == 2 && (etInputChannel == null || mainInputChannel == null)) {
                throw new CmdExecException("For 2 input channels, must have 1 ET & 1 EMU");
            }

            // What kind of output channels do we have?
//System.out.println("  ER mod: prestart(): output chan count = " + outputChannels.size());
            indx = 0;
            for (DataChannel ch : outputChannels) {
                if (ch.getTransportType() == TransportType.ET) {
                    etOutputChannel = ch;
                    etOutChannelCount++;
                    etOutIndex = indx;
                    prescale = ch.getPrescale();
                }
                else if (ch.getTransportType() == TransportType.FILE) {
                    fileOutChannelCount++;
                }
                outputChanNames[indx++] = ch.name();
//System.out.println("  ER mod: prestart(): out chan name = " + ch.name());
            }

            if (fileOutChannelCount + etOutChannelCount != outputChannelCount) {
                throw new CmdExecException("Allow only ET and File output channels");
            }
            else if (etOutChannelCount > 1) {
                throw new CmdExecException("Allow only 1 ET output channel");
            }

            // Store all file output channels together in 1 array
            if (fileOutChannelCount > 0) {
                fileOutputChannels = new DataChannel[fileOutChannelCount];
                indx = 0;
                for (DataChannel ch : outputChannels) {
                    if (ch.getTransportType() == TransportType.FILE) {
                        fileOutputChannels[indx++] = ch;
                    }
                }
            }

            // Reset some variables
            eventRate = wordRate = 0F;
            eventCountTotal = wordCountTotal = 0L;
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Create & start threads
        startThreads();

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}
    }

}