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
 *
 * Recording threads are in no particular order.
 * The producer will only take slots that the recording threads have finished using.
 *
 * 1 Input Channel
 * (evio bank               RB1
 *  ring buffer)             |\\
 *                           | \ \
 *                           |  \ \
 *                           |   \ \
 *                           |    \  \
 *                           |     \   \
 *                           |      \    \
 *                           |       \     \
 *                           |        \      \
 *                           V        V       V
 *  RecordingThreads:       RT1      RT2      RTM
 *  Grab 1 event &           |        |        |
 *  place in all module's    |        |        |
 *  output channels          |        |        |
 *                           |        |        |
 *                           \        |       /
 *                            \       |      /
 *                             V      V     V
 * Output Channel(s):    OC1: RB1    RB2   RBM
 * (1 ring buffer for    OC2: RB1    RB2   RBM  ...
 *  each recording thd
 *  in each channel)
 *
 *
 *  M = 1 by default
 * </code></pre><p>
 *
 * This class is the event recording module. It is a multithreaded module which can have
 * several recording threads. Each of these threads exists for the purpose of taking
 * Evio banks off of the 1 input channel and placing a copy of each bank into all of
 * the output channels. If no output channels are defined in the config file,
 * this module discards all events.
 *
 * @author timmer
 * (2012)
 */
public class EventRecording extends ModuleAdapter {

    /** There should only be one input DataChannel. */
    private DataChannel inputChannel;

    /** The number of RecordThread objects. */
    private int recordingThreadCount;

    /** Container for threads used to record events. */
    private LinkedList<RecordingThread> recordingThreadList = new LinkedList<RecordingThread>();

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

    /** One sequence per recording thread. */
    public Sequence[] sequenceIn;

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

        // default to 1 event recording thread
        recordingThreadCount = eventProducingThreads;
System.out.println("  ER mod: " + recordingThreadCount + " # recording threads");
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

        recordingThreadList.clear();
        for (int i=0; i < recordingThreadCount; i++) {
            RecordingThread thd = new RecordingThread(i, emu.getThreadGroup(), name+":recorder"+i);
            recordingThreadList.add(thd);
            thd.start();
        }
    }


    /**
     * End all record threads because an END cmd or event came through.
     * The record thread calling this method is not interrupted.
     *
     * @param thisThread the record thread calling this method; if null,
     *                   all record threads are interrupted
     * @param wait if <code>true</code> check if END event has arrived and
     *             if all the Qs are empty, if not, wait up to 1/5 second.
     */
    private void endRecordThreads(RecordingThread thisThread, boolean wait) {

        if (wait) {
            // Look to see if anything still on the input channel Q
            long startTime = System.currentTimeMillis();
// TODO: fix this
//            boolean haveUnprocessedEvents = channelQ.size() > 0;
            boolean haveUnprocessedEvents = true;

            // Wait up to endingTimeLimit millisec for events to
            // be processed & END event to arrive, then proceed
            while ((haveUnprocessedEvents || !haveEndEvent) &&
                   (System.currentTimeMillis() - startTime < endingTimeLimit)) {
                try {Thread.sleep(200);}
                catch (InterruptedException e) {}
// TODO: fix this
//                haveUnprocessedEvents = channelQ.size() > 0;
                haveUnprocessedEvents = false;
            }

            if (haveUnprocessedEvents || !haveEndEvent) {
if (debug) System.out.println("  ER mod: will end threads but no END event or ring not empty!");
                moduleState = CODAState.ERROR;
                emu.setErrorState("ER will end threads but no END event or ring not empty");
            }
        }

        // NOTE: the EMU calls this ER module's end() and reset()
        // methods which, in turn, call this method. In this case,
        // all recording threads will be interrupted in the following code.

        // Interrupt all recording threads except the one calling this method
        for (Thread thd : recordingThreadList) {
            if (thd == thisThread) continue;
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


    //---------------------------------------
    // Threads
    //---------------------------------------


    /**
     * This thread is started by the GO transition and runs while the state of the module is ACTIVE.
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * pulls one bank off the input DataChannel. The bank is copied and placed in each output
     * channel. The count of outgoing banks and the count of data words are incremented.
     */
    private class RecordingThread extends Thread {

        /** The order of this recording thread, relative to the other recording threads,
          * starting at zero. */
        private final int order;

        /** The total number of recording threads. */
        private final int rtCount;

        // RingBuffer Stuff

        /** 1 sequence for this particular recording thread. */
        private Sequence sequence;
        /** Available sequence (largest index of items desired). */
        private long availableSequence;
        /** Next sequence (index of next item desired). */
        private long nextSequence;



        RecordingThread(int order, ThreadGroup group, String name) {
            super(group, name);
            this.order = order;
            rtCount = recordingThreadCount;
        }


        @Override
        public void run() {

            int totalNumberEvents=1, wordCount=0;
            RingItem    ringItem    = null;
            ControlType controlType = null;

            int skipCounter = order + 1;
            boolean gotBank;

            boolean takeRingStats = false;
            if (order == 0) {
                takeRingStats = true;
            }

            // Ring Buffer stuff
            availableSequence = -2L;
            sequence = sequenceIn[order];
            nextSequence = sequence.get() + 1L;


            while (moduleState == CODAState.ACTIVE || paused) {

                try {
                    gotBank = false;

                    // Will BLOCK here waiting for item if none available
                    // Only wait or read-volatile-memory if necessary ...
                    if (availableSequence < nextSequence) {
                        // Available sequence may be larger than what we desired
//System.out.println("  ER mod: " + order + ", wait for seq " + nextSequence);
                        availableSequence = barrierIn.waitFor(nextSequence);
//System.out.println("  ER mod: " + order + ", got seq " + availableSequence);
                        if (takeRingStats) {
                            // scale from 0% to 100% of ring buffer size
                            inputChanLevels[0] = ((int)(availableSequence - nextSequence) + 1)*100/ringBufferSize;
//                            if (i==0 && printCounter++ % 10000000 == 0) {
//                                System.out.print(inputChanLevel[0] + "\n");
//                            }
                        }
                    }

                    while (nextSequence <= availableSequence) {
                        ringItem = ringBufferIn.get(nextSequence);
                        wordCount = ringItem.getNode().getLength() + 1;
                        controlType = ringItem.getControlType();
                        totalNumberEvents = ringItem.getEventCount();

                        // Skip over events being recorded by other recording threads
                        if (skipCounter - 1 > 0)  {
//System.out.println("  ER mod: " + order + ", skip " + nextSequence);
                            nextSequence++;
                            skipCounter--;
                        }
                        // Found a bank, so do something with it (skipCounter[i] - 1 == 0)
                        else {
//System.out.println("  ER mod: " + order + ", accept item " + nextSequence + ", type " + ringItem.getEventType());
                            if (ringItem.getEventType() == EventType.CONTROL) {
System.out.println("  ER mod: " + order + ", got control event, " + ringItem.getControlType());
                            }
                            gotBank = true;
                            skipCounter = rtCount;
                            break;
                        }
                    }

                    if (!gotBank) {
//System.out.println("  ER mod: " + order + ", don't have bank, continue");
                        continue;
                    }
//                    else {
//System.out.println("  ER mod: " + order + ", GOT bank, out chan count = " + outputChannelCount);
//                    }

                    if (outputChannelCount > 0) {
                        // If multiple output channels, we must copy the ringItem.
                        // Make sure the buffer being used is not prematurely released
                        // for reuse. Do this by increasing the # of buffer users.
                        if (outputChannelCount > 1) {
                            ringItem.getByteBufferItem().addUsers(outputChannelCount - 1);
                        }

                        // Place event on first output channel
//System.out.println("  ER mod: " + order + ", call eventToOutputChannel()");
                        eventToOutputChannel(ringItem, 0, order);

                        // Copy event and place one on each additional output channel
                        for (int j=1; j < outputChannelCount; j++) {
                            PayloadBuffer bb = new PayloadBuffer((PayloadBuffer)ringItem);
                            eventToOutputChannel(bb, j, order);
                        }
                    }

                    eventCountTotal += totalNumberEvents;
                    wordCountTotal  += wordCount;

                    // If END event, interrupt other record threads then quit this one.
                    if (controlType == ControlType.END) {
System.out.println("  ER mod: found END event");
                        haveEndEvent = true;
                        endRecordThreads(this, false);
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }

                    // Do NOT release the reusable ByteBuffer back to its supply.
                    // It was passed on to the input ring buffer of the output channel.
                    // It's that channel that will release the buffer when it's done
                    // writing it to file or wherever.
                    // But if NO output, it needs to be freed.
                    if (outputChannelCount < 1) {
                        ringItem.releaseByteBuffer();
                    }

                    // Release the events back to the ring buffer for re-use
                    sequence.set(nextSequence++);

                }
                catch (InterruptedException e) {
if (debug) System.out.println("  ER mod: INTERRUPTED recording thread " + Thread.currentThread().getName());
                    return;
                }
                catch (AlertException e) {
if (debug) System.out.println("  ER mod: ring buf alert");
                    // If we haven't yet set the cause of error, do so now & inform run control
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ER ring buf alert");
                    return;
                }
                catch (TimeoutException e) {
if (debug) System.out.println("  ER mod: ring buf timeout");
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ER ring buf timeout");
                    return;
                }
                catch (Exception e) {
if (debug) System.out.println("  ER mod: MAJOR ERROR recording event: " + e.getMessage());
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ER MAJOR ERROR recording event: " + e.getMessage());
                    return;
                }
            }
System.out.println("  ER mod: recording thread ending");
        }

    }


    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        Date theDate = new Date();
        org.jlab.coda.emu.support.codaComponent.State previousState = moduleState;
        moduleState = CODAState.CONFIGURED;

        if (RateCalculator != null) RateCalculator.interrupt();

        // Recording threads must be immediately ended
        endRecordThreads(null, false);

        RateCalculator = null;
        recordingThreadList.clear();

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

        // Recording threads should already be ended by END event
        endRecordThreads(null, true);

        RateCalculator = null;
        recordingThreadList.clear();

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

        // 1 sequence per recording thread
        sequenceIn = new Sequence[recordingThreadCount];

        // Place to put ring level stats
        inputChanLevels = new int[1];

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

        // For each recording thread ...
        for (int j=0; j < recordingThreadCount; j++) {
            // We have 1 sequence for each recording thread
            sequenceIn[j] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

            // This sequence may be the last consumer before producer comes along
            ringBufferIn.addGatingSequences(sequenceIn[j]);
        }

        // We have 1 barrier (shared by recording threads)
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