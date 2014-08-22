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
 * Actual input channel ring buffer has 2048 events (not 6).
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

    /** Type of object to expect for input. */
    private ModuleIoType inputType = ModuleIoType.PayloadBuffer;

    /** Type of object to place on output channels. */
    private ModuleIoType outputType = ModuleIoType.PayloadBuffer;

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

    /** Number of output channels. */
    private int outputChannelCount;

    //-------------------------------------------
    // Disruptor (RingBuffer)  stuff
    //-------------------------------------------

    /** One RingBuffer. */
    private RingBuffer<RingItem> ringBufferIn;

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
System.out.println("EventRecording constructor: " + recordingThreadCount +
                           " # recording threads");

        // Does this module accurately represent the whole EMU's stats?
        String str = attributeMap.get("statistics");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                representStatistics = true;
            }
        }
    }


    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {
        if (input_channels == null) return;
        this.inputChannels.addAll(input_channels);
        if (inputChannels.size() > 0) {
            inputChannel = inputChannels.get(0);
        }
    }

    /** {@inheritDoc} */
    public void addOutputChannels(ArrayList<DataChannel> output_channels) {
        if (output_channels == null) return;
        this.outputChannels.addAll(output_channels);
        outputChannelCount = outputChannels.size();
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

    /** {@inheritDoc} */
    public ModuleIoType getInputRingItemType() {return inputType;}

    /** {@inheritDoc} */
    public ModuleIoType getOutputRingItemType() {return outputType;}


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
     *             if all the Qs are empty, if not, wait up to 1/2 second.
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
if (debug) System.out.println("endRecordThreads: will end threads but no END event or Q not empty!!!");
                errorMsg.compareAndSet(null, "ending threads but no END event or Q not empty");
                state = CODAState.ERROR;
                emu.sendStatusMessage();
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
     * This method is used to place an item onto a specified ring buffer of a
     * single, specified output channel.
     *
     * @param bankOut    the built/control/user event to place on output channel
     * @param ringNum    which output channel ring buffer to place item on
     * @param channelNum which output channel to place item on
     */
    private void dataToOutputChannel(RingItem bankOut, int ringNum, int channelNum) {

        // Have output channels?
        if (outputChannelCount < 1) {
            return;
        }

        RingBuffer rb = outputChannels.get(channelNum).getRingBuffersOut()[ringNum];
        long nextRingItem = rb.next();

        RingItem ri = (RingItem) rb.get(nextRingItem);
        ri.setBuffer(bankOut.getBuffer());
        ri.setEventType(bankOut.getEventType());
        ri.setControlType(bankOut.getControlType());
        ri.setSourceName(bankOut.getSourceName());
        ri.setAttachment(bankOut.getAttachment());
        ri.setReusableByteBuffer(bankOut.getByteBufferSupply(),
                                 bankOut.getByteBufferItem());

        rb.publish(nextRingItem);
    }



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
            RingItem      ringItem     = null;
            PayloadBuffer recordingBuf = null;
            ControlType   controlType  = null;

            int skipCounter = order + 1;
            boolean gotBank;

            // Ring Buffer stuff
            availableSequence = -2L;
            sequence = sequenceIn[order];
            nextSequence  = sequence.get() + 1L;


            while (state == CODAState.ACTIVE || paused) {

                try {
                    gotBank = false;

                    // Will BLOCK here waiting for item if none available
                    // Only wait or read-volatile-memory if necessary ...
                    if (availableSequence < nextSequence) {
                        // Available sequence may be larger than what we desired
                        availableSequence = barrierIn.waitFor(nextSequence);
                    }

                    while (nextSequence <= availableSequence) {
                        ringItem = ringBufferIn.get(nextSequence);
                        controlType = ringItem.getControlType();
                        totalNumberEvents = ringItem.getEventCount();

                        recordingBuf = (PayloadBuffer)ringItem;
                        wordCount = recordingBuf.getNode().getLength() + 1;

                        // Skip over events being recorded by other recording threads
                        if (skipCounter - 1 > 0)  {
                            nextSequence++;
                            skipCounter--;
                        }
                        // Found a bank, so do something with it (skipCounter[i] - 1 == 0)
                        else {
//System.out.println("btThread " + order + ": accept item " + nextSequence + ", type " + ringItem.getEventType());
                            if (ringItem.getEventType() == EventType.CONTROL) {
                                System.out.println("          : " + ringItem.getControlType());
                            }
                            gotBank = true;
                            skipCounter = rtCount;
                            break;
                        }
                    }

                    if (!gotBank) {
                        continue;
                    }

                    if (outputChannelCount > 0) {
                        // Place event on first output channel
                        dataToOutputChannel(recordingBuf, order, 0);

                        // Copy event and place one on each additional output channel
                        for (int j=1; j < outputChannelCount; j++) {
                            PayloadBuffer bb = new PayloadBuffer(recordingBuf);
                            dataToOutputChannel(bb, order, j);
                        }
                    }

                    // If END event, interrupt other record threads then quit this one.
                    if (controlType == ControlType.END) {
                        System.out.println("Found END event in record thread");
                        haveEndEvent = true;
                        endRecordThreads(this, false);
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }

                    eventCountTotal += totalNumberEvents;
                    wordCountTotal  += wordCount;

                    // Release the reusable ByteBuffers back to their supply
                    ringItem.releaseByteBuffer();

                    // Release the events back to the ring buffer for re-use
                    sequence.set(nextSequence++);

                }
                catch (InterruptedException e) {
                    if (debug) System.out.println("INTERRUPTED thread " + Thread.currentThread().getName());
                    return;
                }
                catch (AlertException e) {
                    if (debug) System.out.println("Ring buf alert, " + Thread.currentThread().getName());
                    return;
                }
                catch (TimeoutException e) {
                    if (debug) System.out.println("Ring buf timeout, " + Thread.currentThread().getName());
                    return;
                }
            }

System.out.println("recording thread is ending !!!");
        }

    }


    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        Date theDate = new Date();
        org.jlab.coda.emu.support.codaComponent.State previousState = state;
        state = CODAState.CONFIGURED;

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
        state = CODAState.ACTIVE;
        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }


    /** {@inheritDoc} */
    public void end() {
        state = CODAState.DOWNLOADED;

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

        state = CODAState.PAUSED;
        paused = true;

        // Make sure we have only one input channel
        if (inputChannels.size() != 1) {
            state = CODAState.ERROR;
            return;
        }

        //------------------------------------------------
        // Disruptor (RingBuffer) stuff for input channels
        //------------------------------------------------

        // 1 sequence per recording thread
        sequenceIn = new Sequence[recordingThreadCount];

        // Get input channel's ring buffer
        ringBufferIn = inputChannels.get(0).getRingBufferIn();

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