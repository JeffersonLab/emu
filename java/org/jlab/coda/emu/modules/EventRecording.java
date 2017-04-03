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
 *                \ 5 | 4 / <-- Recording Threads 1, 2, ... M
 *                 \__|__/         |
 *                                 V
 *
 *
 * Actual input channel ring buffer has thousands of events (not 6).
 * The producer is a single input channel which reads incoming data,
 * parses it and places it into the ring buffer.
 * *
 *  1 Input Channel
 *  (evio bank              RB1
 *   ring buffer)            |
 *                           |
 *                           V
 *  1 RecordingThread:      RT1
 *  Grab 1 event &           |
 *  place in all module's    |
 *  output channels          |
 *                           |
 *                           V
 * Output Channel(s):    OC1, OC2, ...
 * (1 ring buffer for
 *  each channel)
 *
 * </code></pre><p>
 *
 * This class is the event recording module. It has one recording thread.
 * This thread exists for the purpose of taking buffers of Evio banks off
 * of the 1 input channel and placing a copy of each bank into all of
 * the output channels. If no output channels are defined in the config file,
 * this module discards all events.
 *
 * @author timmer
 * (2012)
 */
public class EventRecording extends ModuleAdapter {

    /** There should only be one input DataChannel. */
    private DataChannel inputChannel;

    /** Thread used to record events. */
    private RecordingThread recordingThread;

   /** END event detected by one of the recording threads. */
    private volatile boolean haveEndEvent;

    /** Maximum time in milliseconds to wait when commanded to END but no END event received. */
    private long endingTimeLimit = 30000;

    // ---------------------------------------------------

    /** If {@code true}, get debug print out. */
    private boolean debug = false;

    //-------------------------------------------
    // Disruptor (RingBuffer)  stuff
    //-------------------------------------------

    /** One RingBuffer. */
    private RingBuffer<RingItem> ringBufferIn;

    /** Size of RingBuffer for input channel. */
    private int ringBufferSize;

    /** One sequence for recording thread. */
    public Sequence sequenceIn;

    /** All recording threads share one barrier. */
    public SequenceBarrier barrierIn;


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
    public void addInputChannels(ArrayList<DataChannel> input_channels) {
        super.addInputChannels(input_channels);
        if (inputChannels.size() > 0) {
            inputChannel = inputChannels.get(0);
        }
    }


    /**
     * Get the one input channel in use.
     * @return  the one input channel in use.
     */
    public DataChannel getInputChannel() {return inputChannel;}


    /** {@inheritDoc} */
    public void clearChannels() {
        inputChannels.clear();
        outputChannels.clear();
        inputChannel = null;
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

        recordingThread = new RecordingThread(emu.getThreadGroup(), name+":recorder");
        recordingThread.start();
    }


    /**
     * End record thread because an END cmd or event came through.
     * The record thread calling this method is not interrupted.
     *
     * @param wait if <code>true</code> check if END event has arrived and
     *             if not, wait up to endingTimeLimit.
     */
    private void endRecordThread(boolean wait) {

        if (wait && !haveEndEvent) {
            long startTime = System.currentTimeMillis();

            // Wait up to endingTimeLimit millisec for events to
            // be processed & END event to arrive, then proceed
            while (!haveEndEvent && (System.currentTimeMillis() - startTime < endingTimeLimit)) {
                try {Thread.sleep(200);}
                catch (InterruptedException e) {}
            }

            if (!haveEndEvent) {
System.out.println("  ER mod: will end thread but no END event!");
                moduleState = CODAState.ERROR;
                emu.setErrorState("ER will end thread but no END event");
            }
        }

        // NOTE: the EMU calls this ER module's end() and reset()
        // methods which, in turn, call this method.
        if (recordingThread != null) {
            recordingThread.interrupt();
            try {
                recordingThread.join(250);
                if (recordingThread.isAlive()) {
                    recordingThread.stop();
                }
            }
            catch (InterruptedException e) {
            }
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
    private class RecordingThread extends Thread {

        RecordingThread(ThreadGroup group, String name) {
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
            long nextSequence = sequenceIn.get() + 1L;

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
                        availableSequence = barrierIn.waitFor(nextSequence);
                    }

                    // scale from 0% to 100% of ring buffer size
                    t2 = emu.getTime();
                    if (t2-t1 > timeBetweenSamples) {
                        //inputChanLevels[0] = ((int)(availableSequence - nextSequence) + 1)*100/ringBufferSize;
                        //inputChanLevels[0] = ((int)(ringBufferIn.getCursor() - nextSequence) + 1)*100/ringBufferSize;

                        inputChanLevels[0] = ((int)(ringBufferIn.getCursor() -
                                                    ringBufferIn.getMinimumGatingSequence()) + 1)*100/ringBufferSize;
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
                        ringItem = ringBufferIn.get(nextSequence);
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
//System.out.println("  ER mod: THROWING AWAY event of type " + ringItem.getEventType());
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
                                }

                                // Release old stuff
                                ringItem.setNode(null);
                                ringItem.setBuffer(null);
                                ringItem.releaseByteBuffer();

//System.out.println("  ER mod: STORE \"first event\" of type " + ringItem.getEventType());
                                // Copy new stuff into list
                                firstEvents.add(newRingItem);
                                firstEventsWords += wordCount;
                            }

                            // Release the ring buffer slot of input channel for re-use.
                            // This is fine since we copied the ringItem and released the
                            // original data.

                            sequenceIn.set(nextSequence++);

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
//System.out.println("  ER mod: sending \"first event\" to out chan");
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
                                PayloadBuffer bb = new PayloadBuffer((PayloadBuffer) ringItem);
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
                    sequenceIn.set(nextSequence++);

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


//    /**
//     * This thread is started by the GO transition and runs while the state of the module is ACTIVE.
//     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
//     * pulls one bank off the input DataChannel. The bank is copied and placed in each output
//     * channel. The count of outgoing banks and the count of data words are incremented.
//     * This class is written to be able to run more than 1 RecordingThread at once.
//     */
//    private class RecordingThread extends Thread {
//
//        RecordingThread(ThreadGroup group, String name) {
//            super(group, name);
//        }
//
//
//        @Override
//        public void run() {
//
//            int totalNumberEvents=1, wordCount=0;
//            RingItem    ringItem    = null;
//            ControlType controlType = null;
//
//            boolean gotBank;
//
//            boolean takeRingStats = false;               n
//            takeRingStats = true;
//
//            // Ring Buffer stuff
//            availableSequence = -2L;
//            nextSequence = sequenceIn.get() + 1L;
//
//
//            while (moduleState == CODAState.ACTIVE || paused) {
//
//                try {
//                    gotBank = false;
//
//                    // Will BLOCK here waiting for item if none available
//                    // Only wait or read-volatile-memory if necessary ...
//                    if (availableSequence < nextSequence) {
//                        // Available sequence may be larger than what we desired
////System.out.println("  ER mod: " + order + ", wait for seq " + nextSequence);
//                        availableSequence = barrierIn.waitFor(nextSequence);
////System.out.println("  ER mod: " + order + ", got seq " + availableSequence);
//                        if (takeRingStats) {
//                            // scale from 0% to 100% of ring buffer size
//                            inputChanLevels[0] = ((int)(availableSequence - nextSequence) + 1)*100/ringBufferSize;
////                            if (i==0 && printCounter++ % 10000000 == 0) {
////                                System.out.print(inputChanLevel[0] + "\n");
////                            }
//                        }
//                    }
//
//                    while (nextSequence <= availableSequence) {
//                        ringItem = ringBufferIn.get(nextSequence);
//                        wordCount = ringItem.getNode().getLength() + 1;
//                        controlType = ringItem.getControlType();
//                        totalNumberEvents = ringItem.getEventCount();
//
////System.out.println("  ER mod: " + order + ", accept item " + nextSequence + ", type " + ringItem.getEventType());
//                            if (ringItem.getEventType() == EventType.CONTROL) {
//System.out.println("  ER mod: got control event, " + ringItem.getControlType());
//                            }
//                            gotBank = true;
//                            break;
//                    }
//
//                    if (!gotBank) {
////System.out.println("  ER mod: " + order + ", don't have bank, continue");
//                        continue;
//                    }
////                    else {
////System.out.println("  ER mod: " + order + ", GOT bank, out chan count = " + outputChannelCount);
////                    }
//
//                    if (outputChannelCount > 0) {
//                        // If multiple output channels, we must copy the ringItem.
//                        // Make sure the buffer being used is not prematurely released
//                        // for reuse. Do this by increasing the # of buffer users.
//                        if (outputChannelCount > 1) {
//                            ringItem.getByteBufferItem().addUsers(outputChannelCount - 1);
//                        }
//
//                        // Place event on first output channel
////System.out.println("  ER mod: " + order + ", call eventToOutputChannel()");
//                        eventToOutputChannel(ringItem, 0, 0);
//
//                        // Copy event and place one on each additional output channel
//                        for (int j=1; j < outputChannelCount; j++) {
//                            PayloadBuffer bb = new PayloadBuffer((PayloadBuffer)ringItem);
//                            eventToOutputChannel(bb, j, 0);
//                        }
//                    }
//
//                    eventCountTotal += totalNumberEvents;
//                    wordCountTotal  += wordCount;
//
//                    // If END event, interrupt other record threads then quit this one.
//                    if (controlType == ControlType.END) {
//System.out.println("  ER mod: found END event");
//                        haveEndEvent = true;
//                        if (endCallback != null) endCallback.endWait();
//                        return;
//                    }
//
//                    // Do NOT release the reusable ByteBuffer back to its supply.
//                    // It was passed on to the input ring buffer of the output channel.
//                    // It's that channel that will release the buffer when it's done
//                    // writing it to file or wherever.
//                    // But if NO output, it needs to be freed.
//                    if (outputChannelCount < 1) {
//                        ringItem.releaseByteBuffer();
//                    }
//
//                    // Release the events back to the ring buffer for re-use
//                    sequenceIn.set(nextSequence++);
//
//                }
//                catch (InterruptedException e) {
//if (debug) System.out.println("  ER mod: INTERRUPTED recording thread " + Thread.currentThread().getName());
//                    return;
//                }
//                catch (AlertException e) {
//if (debug) System.out.println("  ER mod: ring buf alert");
//                    // If we haven't yet set the cause of error, do so now & inform run control
//                    moduleState = CODAState.ERROR;
//                    emu.setErrorState("ER ring buf alert");
//                    return;
//                }
//                catch (TimeoutException e) {
//if (debug) System.out.println("  ER mod: ring buf timeout");
//                    moduleState = CODAState.ERROR;
//                    emu.setErrorState("ER ring buf timeout");
//                    return;
//                }
//                catch (Exception e) {
//if (debug) System.out.println("  ER mod: MAJOR ERROR recording event: " + e.getMessage());
//                    moduleState = CODAState.ERROR;
//                    emu.setErrorState("ER MAJOR ERROR recording event: " + e.getMessage());
//                    return;
//                }
//            }
//System.out.println("  ER mod: recording thread ending");
//        }
//
//    }



    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        Date theDate = new Date();
        CODAStateIF previousState = moduleState;
        moduleState = CODAState.CONFIGURED;

        if (RateCalculator != null) RateCalculator.interrupt();

        // Recording thread must be immediately ended
        endRecordThread(false);

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

        // The order in which these thread are shutdown does(should) not matter.
        // Rocs should already have been shutdown, followed by the input transports,
        // followed by this module (followed by the output transports).
        if (RateCalculator != null) RateCalculator.interrupt();

        // Recording thread should already be ended by END event.
        // If not, wait for it 1/4 sec.
        endRecordThread(true);

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

        // Make sure we have only one input channel
        if (inputChannels.size() != 1) {
            moduleState = CODAState.ERROR;
            emu.setErrorState("ER does not have exactly 1 input channel");
            return;
        }

        //------------------------------------------------
        // Disruptor (RingBuffer) stuff for input channels
        //------------------------------------------------

        // Place to put ring level stats
        inputChanLevels  = new int[1];
        outputChanLevels = new int[outputChannelCount];

        // channel name for easy gathering of stats
        inputChanNames = new String[1];
        inputChanNames[0] = inputChannel.name();

        int indx = 0;
        outputChanNames = new String[outputChannelCount];
        for (DataChannel ch : outputChannels) {
            outputChanNames[indx++] = ch.name();
        }

        // Get input channel's ring buffer
        ringBufferIn = inputChannels.get(0).getRingBufferIn();

        // Have ring sizes handy for calculations
        ringBufferSize = ringBufferIn.getBufferSize();

        // We have 1 sequence for the recording thread
        sequenceIn = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

        // This sequence is the last consumer before producer comes along
        ringBufferIn.addGatingSequences(sequenceIn);

        // We have 1 barrier for recording thread
        barrierIn = ringBufferIn.newBarrier();


        // How many output channels do we have?
//            outputChannelCount = outputChannels.size();

        // Reset some variables
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;

        // Create & start threads
        startThreads();

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}
    }

}