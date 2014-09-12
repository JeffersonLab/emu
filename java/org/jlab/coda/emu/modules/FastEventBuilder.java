/*
 * Copyright (c) 2010, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000 Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.modules;

import com.lmax.disruptor.*;
import com.lmax.disruptor.TimeoutException;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <pre><code>
 *
 *
 *                Ring Buffer (single producer, lock free)
 *                   ____
 *                 /  |  \
 *         ^      /1 _|_ 2\  <---- Build Threads 1-M
 *         |     |__/   \__|               |
 *     Producer->|6 |   | 3|               V
 *               |__|___|__|
 *                \ 5 | 4 / <---- Pre-Processing Thread
 *                 \__|__/                 |
 *                                         V
 *
 *
 * Actual input channel ring buffers have thousands of events (not 6).
 * The producer is a single input channel which reads incoming data,
 * parses it and places it into the ring buffer.
 *
 * The leading consumer is the pre-processing thread.
 * All build threads come after the pre-processing thread in no particular order.
 * The producer will only take slots that the build threads have finished using.
 *
 * N Input Channels
 * (evio bank             RB1_      RB2_ ...  RBN_
 *  ring buffers)         |  |      |  |      |  |
 *                        |  |      |  |      |  |
 *                        V  |      V  |      V  |
 * Pre-Processing thds:  PP1 |     PP2 |     PPN |
 *  Check evio bank          |         |         |
 *  for good event type      |         |         |
 *  and format               |         |         |
 *                           V         V         V
 *                           |        /         /       _
 *                           |      /        /       /
 *                           |    /        /        /
 *                           |  /       /         <   Crossbar of
 *                           | /      /            \  Connections
 *                           |/    /                \
 *                           |  /                    \
 *                           |/       |       |       -
 *                           V        V       V
 *  BuildingThreads:        BT1      BT2      BTM
 *  Grab 1 bank from         |        |        |
 *  each ring,               |        |        |
 *  build event, &           |        |        |
 *  place in                 |        |        |
 *  output channel           \        |       /
 *                            \       |      /
 *                             V      V     V
 * Output Channel(s):    OC1: RB1    RB2   RBM
 * (1 ring buffer for    OC2: RB1    RB2   RBM  ...
 *  each build thread
 *  in each channel)
 *
 *
 *  M != N in general
 *  M = 2 by default
 *
 * </code></pre><p>
 *
 * This class is the event building module. It is a multi-threaded module which has 1
 * Pre-Processing thread per input channel. Each of these threads exists for the sole purpose
 * of examining Evio banks from 1 input channel and seeing if they are in the proper format
 * (ROC Raw, Physics, Control, User). They throw an exception for any banks that are not in
 * the proper format and place any User events in the first output channel.
 *
 * After pre-processing, each BuildingThread - of which there may be any number - takes
 * one bank from each ring buffer (and therefore input channel), skipping every Mth,
 * and builds them into a single event. The built event is placed in a ring buffer of
 * an output channel. This is by round robin if more than one channel or on all output channels
 * if a control event. If this EB is a DC and has multiple SEBs as output channels,
 * then the output is more complex - sebChunk number of contiguous events go to one channel
 * before being switched to the next channel. Each output channel has the same number of ring buffers
 * as build threads. This avoids any contention & locking while writing. Each build thread
 * only writes to a fixed, single ring buffer of each output channel. It is the job of each
 * output channel to merge the contents of their rings into a single, ordered output stream.<p>
 *
 * NOTE: When building, any Control events must appear on each channel in the same order.
 * If not, an exception may be thrown. If so, the Control event is passed along to all output channels.
 * If no output channels are defined in the config file, this module builds, but discards all events.
 */
public class FastEventBuilder extends ModuleAdapter {

    /** The number of BuildingThread objects. */
    private int buildingThreadCount;

    /** Container for threads used to build events. */
    private ArrayList<BuildingThread> buildingThreadList = new ArrayList<BuildingThread>();

    /**
     * Threads (one for each input channel) used to take Evio data from
     * input channels and check them for proper format.
     */
    private Thread preProcessors[];

    /** Maximum time in milliseconds to wait when commanded to END but no END event received. */
    private long endingTimeLimit = 30000;

    /** The number of the experimental run. */
    private int runNumber;

    /** The number of the experimental run's configuration. */
    private int runTypeId;

    /** The eventNumber value when the last sync event arrived. */
    private volatile long eventNumberAtLastSync;

    /** If <code>true</code>, get debug print out. */
    private boolean debug = false;

    // ---------------------------------------------------
    // Configuration parameters
    // ---------------------------------------------------

    /** If <code>true</code>, check timestamps for consistency. */
    private boolean checkTimestamps;

    /**
     * The maximum difference in ticks for timestamps for a single event before
     * an error condition is flagged. Only used if {@link #checkTimestamps} is
     * <code>true</code>.
     */
    private int timestampSlop;

    /**
     * If true, swap data if necessary when building events.
     * Assume data is all 32 bit integers.
     */
    private boolean swapData;

    /** If true, include run number & type in built trigger bank. */
    private boolean includeRunData;

    /** If true, do not include empty roc-specific segments in trigger bank. */
    private boolean sparsify;

    // ---------------------------------------------------

    /** Object used to start all event building after go event received. */
    private CountDownLatch waitForGo;

    /** Object used to start all build threads looking
     * for go event after prestart event received. */
    private CountDownLatch waitForPrestart;

    /** Have complete END event (on all input channels)
     *  detected by one of the building threads. */
    private volatile boolean haveEndEvent;

    /** Synchronize between build threads who will write GO. */
    private final AtomicBoolean firstToGetGo = new AtomicBoolean(false);

    /** Synchronize between build threads who will write END. */
    private final AtomicBoolean firstToGetEnd = new AtomicBoolean(false);

    /** Synchronize between build threads who will write PRESTART. */
    private final AtomicBoolean firstToGetPrestart = new AtomicBoolean(false);


    //-------------------------------------------
    // Disruptor (RingBuffer)  stuff
    //-------------------------------------------

    /** One RingBuffer per input channel (references to channels' rings). */
    private RingBuffer<RingItem>[] ringBuffersIn;

    /** One pre-processing sequence for each input channel. */
    private Sequence[] preBuildSequence;

    /** One pre-processing barrier for each input channel. */
    private SequenceBarrier[] preBuildBarrier;

    /** For each input channel, 1 sequence per build thread. */
    private Sequence[][] buildSequenceIn;

    /** For each input channel, all build threads share one barrier. */
    private SequenceBarrier[] buildBarrierIn;




    /**
     * Constructor creates a new EventBuilding instance.
     *
     * @param name         name of module
     * @param attributeMap map containing attributes of module
     * @param emu          emu which created this module
     */
    public FastEventBuilder(String name, Map<String, String> attributeMap, Emu emu) {
        super(name, attributeMap, emu);

        // Set number of building threads
        buildingThreadCount = eventProducingThreads;
System.out.println("  EB mod: " + buildingThreadCount +
                           " number of event building threads");
        // If # build threads not explicitly set in config, make it 2
        // which seems to perform the best.
        if (!epThreadsSetInConfig) {
            buildingThreadCount = eventProducingThreads = 1;
        }

        // default is to swap data if necessary -
        // assume 32 bit ints
        swapData = true;
        String str = attributeMap.get("swap");
        if (str != null) {
            if (str.equalsIgnoreCase("false") ||
                str.equalsIgnoreCase("off")   ||
                str.equalsIgnoreCase("no"))   {
                swapData = false;
            }
        }

        // default is NOT to include run number & type in built trigger bank
        str = attributeMap.get("runData");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("in")   ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                includeRunData = true;
            }
        }

        // default is NOT to sparsify roc-specific segments in trigger bank
        sparsify = false;
        str = attributeMap.get("sparsify");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                sparsify = true;
            }
        }

        // default is to check timestamp consistency
        checkTimestamps = true;
        str = attributeMap.get("tsCheck");
        if (str != null) {
            if (str.equalsIgnoreCase("false") ||
                str.equalsIgnoreCase("off")   ||
                str.equalsIgnoreCase("no"))   {
                checkTimestamps = false;
            }
        }

        // default to 2 clock ticks
        timestampSlop = 2;
        try {
            timestampSlop = Integer.parseInt(attributeMap.get("tsSlop"));
        }
        catch (NumberFormatException e) {}
    }



    /** {@inheritDoc} */
    public ModuleIoType getInputRingItemType() {return ModuleIoType.PayloadBuffer;}

    /** {@inheritDoc} */
    public ModuleIoType getOutputRingItemType() {return ModuleIoType.PayloadBuffer;}



    /**
     * This class takes RingItems from a RingBuffer (an input channel, eg. ROC),
     * and processes them making sure each item is a valid, evio, DAQ event.
     * All other types of events are ignored.
     * Nothing in this class depends on single event mode status.
     */
    private class PreProcessor extends Thread {

        private long nextSequence;

        private final Sequence sequence;
        private final SequenceBarrier barrier;
        private final RingBuffer<RingItem> ringBuffer;
        private final DataChannel channel;

        PreProcessor(RingBuffer<RingItem> ringBuffer,  SequenceBarrier barrier,
                     Sequence sequence, DataChannel channel,
                     ThreadGroup group, String name) {

            super(group, name);

            this.channel = channel;
            this.barrier = barrier;
            this.sequence = sequence;
            this.ringBuffer = ringBuffer;

            nextSequence = sequence.get() + 1L;
        }

        @Override
        public void run() {
            RingItem pBuf;

            while (state == CODAState.ACTIVE || paused) {
                try {

                    while (state == CODAState.ACTIVE || paused) {
//System.out.println("  EB mod: wait for Seq = " + nextSequence + " in pre-processing");
                        final long availableSequence = barrier.waitFor(nextSequence);

//System.out.println("  EB mod: available Seq = " + availableSequence + " in pre-processing");
                        while (nextSequence <= availableSequence) {
                            pBuf = ringBuffer.get(nextSequence);
                            Evio.checkPayload((PayloadBuffer)pBuf, channel);

                            // Take user event and place on output channel
                            if (pBuf.getEventType().isUser()) {
System.out.println("  EB mod: got user event in pre-processing");

                                // Swap headers, NOT DATA, if necessary
                                if (outputOrder != pBuf.getByteOrder()) {
                                    try {
System.out.println("  EB mod: swap user event headers");
                                        ByteDataTransformer.swapEvent(pBuf.getBuffer(), pBuf.getBuffer(),
                                                                      0, 0, false, null);
                                    }
                                    catch (EvioException e) {/* should never happen */ }
                                }

                                // Send it on.
                                // User events are thrown away if no output channels
                                // since this event builder does nothing with them.
                                // User events go into the first ring of the first channel.
                                eventToOutputChannel(pBuf, 0, 0);
                            }

                            nextSequence++;
//System.out.println("  EB mod:PreProcessing: next Seq = " + nextSequence + " in pre-processing");
                        }

//System.out.println("  EB mod:PreProcessing: set Seq = " + availableSequence + " in pre-processing");
                        sequence.set(availableSequence);
                    }

                } catch (AlertException e) {
                    // don't know what this means
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    // won't happen
                } catch (EmuException e) {
                    // EmuException from Evio.checkPayload() if
                    // Roc raw or physics banks are in the wrong format
if (debug) System.out.println("  EB mod: Roc raw or physics event in wrong format");
                    errorMsg.compareAndSet(null, "Roc raw or physics banks are in the wrong format");
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();
                    return;
                } catch (InterruptedException e) {
                    return;
                }
            }

        }
    }


    /**
     * This method is used to place an item onto a ring buffer of an output channel.
     *
     * @param banksOut array of built/control/user events to place on output channel
     * @param ringNum the ring buffer to put it on (starting at 0)
     * @throws InterruptedException if wait, put, or take interrupted
     */
    private void bankToOutputChannel(List<RingItem> banksOut, int ringNum, int channelNum)
                    throws InterruptedException {

        // Have output channels?
        if (outputChannelCount < 1) {
            for (RingItem bank : banksOut) {
                bank.getByteBufferSupply().release(bank.getByteBufferItem());
            }
            return;
        }

        RingBuffer rb = outputChannels.get(channelNum).getRingBuffersOut()[ringNum];

//System.out.println(  EB mod: wait for next ring buf for writing");
        long lastRingItem = rb.next(banksOut.size());
        long ringItem = lastRingItem - banksOut.size() + 1;
//System.out.println("  EB mod: Got last sequence " + lastRingItem);

        for (int i = 0; i < banksOut.size(); i++) {
            ringItem += i;
            PayloadBuffer pb = (PayloadBuffer) rb.get(ringItem);
            pb.setBuffer     (banksOut.get(i).getBuffer());
            pb.setEventType  (banksOut.get(i).getEventType());
            pb.setControlType(banksOut.get(i).getControlType());
            pb.setSourceName (banksOut.get(i).getSourceName());
            pb.setAttachment (banksOut.get(i).getAttachment());
            pb.setReusableByteBuffer(banksOut.get(i).getByteBufferSupply(),
                                     banksOut.get(i).getByteBufferItem());

//System.out.println("  EB mod: published item " + ringItem + " to ring " + ringNum);
            rb.publish(ringItem);
        }
    }


    /**
     * This method is called by a build thread and is used to place
     * a bank onto the ring buffer of an output channel.
     *
     * @param ringNum the id number of the output channel ring buffer
     * @param buf     the event to place on output channel ring buffer
     * @param item    item corresponding to the buffer allowing buffer to be reused
     * @throws InterruptedException if put or wait interrupted
     */
    private void eventToOutputRing(int ringNum, int channelNum, ByteBuffer buf, EventType eventType,
                                   ByteBufferItem item, ByteBufferSupply bbSupply)
            throws InterruptedException {

        // Have output channels?
        if (outputChannelCount < 1) {
            bbSupply.release(item);
            return;
        }

        RingBuffer rb = outputChannels.get(channelNum).getRingBuffersOut()[ringNum];

//System.out.println("  EB mod: wait for next ring buf for writing");
        long nextRingItem = rb.next();
//System.out.println("  EB mod: Got sequence " + nextRingItem);
        RingItem ri = (RingItem) rb.get(nextRingItem);
        ri.setBuffer(buf);
        ri.setEventType(eventType);
        ri.setControlType(null);
        ri.setSourceName(null);
        ri.setReusableByteBuffer(bbSupply, item);

//System.out.println("  EB mod: published to ring " + ringNum);
        rb.publish(nextRingItem);
    }


    /**
     * This method looks for either a prestart or go event in all the
     * input channels' ring buffers.
     *
     * @param sequences     one sequence per ring buffer per build thread
     * @param barriers      one barrier per ring buffer
     * @param nextSequences one "index" per ring buffer per build thread to
     *                      keep track of which event each thread is at
     *                      in each ring buffer
     *
     * @return a copy of the control event found
     * @throws EmuException if got non-control or non-prestart/go event
     * @throws InterruptedException if taking of event off of Q is interrupted
     */
    private PayloadBuffer gotAllControlEvents(Sequence[] sequences,
                                              SequenceBarrier barriers[],
                                              long nextSequences[])
            throws EmuException, InterruptedException {

        boolean isPrestart = true;
        PayloadBuffer[] buildingBanks = new PayloadBuffer[inputChannelCount];

        // First thing we do is look for the go/prestart event and pass it on
        // Grab one control event from each ring buffer.
        for (int i=0; i < inputChannelCount; i++) {
            try  {
                final long availableSequence = barriers[i].waitFor(nextSequences[i]);
//System.out.println("  EB mod: gotAllControlEvents: available Seq = " + availableSequence);

                buildingBanks[i] = (PayloadBuffer) ringBuffersIn[i].get(nextSequences[i]);
                ControlType cType = buildingBanks[i].getControlType();
//System.out.println("  EB mod: gotAllControlEvents: Seq[" + i + "] = " + nextSequences[i] + " has control type " + cType);
                if (cType == null) {
                    throw new EmuException("Expecting control event, got something else");
                }

                if (cType.isGo()) {
                    isPrestart = false;
                }
                else if (!cType.isPrestart()) {
                    Utilities.printBuffer(buildingBanks[i].getBuffer(), 0, 10, "Bad go/prestart");
                    throw new EmuException("Expecting go or prestart, got " + cType);
                }

//System.out.println("  EB mod: gotAllControlEvents: set Seq[" + i + "] = " + nextSequences[i]);
                sequences[i].set(nextSequences[i]);

                nextSequences[i]++;
//System.out.println("  EB mod: gotAllControlEvents: next Seq[" + i + "] = " + nextSequences[i]);
            }
            catch (final TimeoutException e) {
                e.printStackTrace();
                throw new EmuException("Cannot get control event", e);
            }
            catch (final AlertException e) {
                e.printStackTrace();
                throw new EmuException("Cannot get control event", e);
            }
        }

        // Throw exception if inconsistent
        Evio.gotConsistentControlEvents(buildingBanks, runNumber, runTypeId);

        if (isPrestart) {
System.out.println("  EB mod: have consistent PRESTART event(s)");
        }
        else {
System.out.println("  EB mod: have consistent GO event(s)");
        }

        return buildingBanks[0];
    }


    /**
     * This method writes the given control event into all the output channels.
     *
     * @param controlEvent control event to be written to output channels.
     * @param isPrestart   {@code true} if prestart event being written, else go event
     * @throws InterruptedException if writing of event to output channels is interrupted
     */
    private void controlToOutputAsync(PayloadBuffer controlEvent, boolean isPrestart)
            throws InterruptedException {

        // Put 1 event on each output Q
        if (outputChannelCount > 0) {
            // Take one of the go/prestart events and update
            // it with the latest event builder data.
            Evio.updateControlEvent(controlEvent, runNumber,
                                    runTypeId, (int)eventCountTotal, 0, outputOrder);

            // Place event on first output channel
            eventToOutputChannel(controlEvent, 0, 0);

            // Copy the newly-updated event & place on each
            // of the other output channels.
            for (int j = 1; j < outputChannelCount; j++) {
                // Copy control event
                PayloadBuffer bb = new PayloadBuffer(controlEvent);
                // Write to other output channels
                eventToOutputChannel(bb, j, 0);
            }
        }

        if (isPrestart) {
            System.out.println("  EB mod: wrote PRESTART from build thread");
        }
        else {
            System.out.println("  EB mod: wrote GO from build thread");
        }
    }


    /**
     * This thread is started by the GO transition and runs while the state of the module is ACTIVE.
     * <p/>
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * selects an output by taking the next one from a simple iterator. The thread then pulls
     * one DataBank off each input DataChannel and stores them in an ArrayList.
     * <p/>
     * An empty buffer big enough to store all of the banks pulled off the inputs is created.
     * The incoming banks from the ArrayList are built into a new bank.
     * The count of outgoing banks and the count of data words are incremented.
     * If this module has outputs, built banks are placed on an output DataChannel.
     */
    class BuildingThread extends Thread {

        /** The total number of build threads. */
        private final int btCount;

        /** The order of this build thread, relative to the other build threads,
          * starting at zero. */
        private final int btIndex;

        // Stuff needed to direct built events to proper output channel(s)

        /** Number (index) of the current, sequential-between-all-built-thds,
         * built event produced from this EMU.
         * 1st build thread starts at 0, 2nd starts at 1, etc.
         * 1st build thread's 2nd event is btCount, 2nd thread's 2nd event is btCount + 1, etc.*/
        private long evIndex = 0;

        /** If each output (SEB) channel gets sebChunk contiguous events, then this is the
         * index of that group or chunk being currently written by this EMU.
         * evGroupIndex = evIndex/sebChunk. */
        private long evGroupIndex = 0;

        /** Which channel does this thread currently output to, starting at 0?
         * outputChannelIndex = evGroupIndex % outputChannelCount.*/
        private int outputChannelIndex = 0;

        // RingBuffer Stuff

        /** 1 sequence for each input channel in this particular build thread. */
        private Sequence[] buildSequences;
        /** Array of available sequences (largest index of items desired), one per input channel. */
        private long availableSequences[];
        /** Array of next sequences (index of next item desired), one per input channel. */
        private long nextSequences[];



        BuildingThread(int btIndex, ThreadGroup group, String name) {
            super(group, name);
            this.btIndex = btIndex;
            evIndex = btIndex;
            btCount = buildingThreadCount;
System.out.println("  EB mod: create Build-Thread with index " + btIndex);
        }


        @Override
        public void run() {

            // Create a reusable supply of ByteBuffer objects
            // for writing built physics events into.
            ByteBufferSupply bbSupply = new ByteBufferSupply(8192, 2000, outputOrder);

            // Object for building physics events in a ByteBuffer
            CompactEventBuilder builder = null;
            try {
                // Internal buffer of 8 bytes will be overwritten later
                builder = new CompactEventBuilder(8, outputOrder, true);
            }
            catch (EvioException e) {/*never happen */}


            // Skipping events is necessary if there are multiple build threads.
            // This is the way of dividing up events among build threads without
            // contention and mutex locking.
            //
            // For example, with 3 build threads:
            // the 1st build thread gets item #1, the 2nd item #2, and the 3rd item #3.
            // In the next round,
            // the 1st thread gets item #4, the 2nd item #5, and the 3rd, item #6, etc.
            //
            // Remember that in the rings, each consumer (sequence) sees all items and must
            // therefore skip over those that the other build threads are working on.
            // One complication is the presence of user events. These must be skipped over
            // and completely ignored by all build threads.
            //
            // Easiest to implement this with one counter per input channel.
            int[] skipCounter = new int[inputChannelCount];
            Arrays.fill(skipCounter, btIndex + 1);

             // Initialize
            int     totalNumberEvents=1;
            long    firstEventNumber=1;
            boolean haveEnd;
            boolean firstToFindEnd;
            boolean nonFatalError;
            boolean havePhysicsEvents;
            boolean gotFirstBuildEvent;
            boolean gotBank;
            boolean isEventNumberInitiallySet = false;
            EventType eventType = null;
            if (outputChannelCount > 1) outputChannelIndex = -1;

            PayloadBuffer[] buildingBanks = new PayloadBuffer[inputChannelCount];

            int endEventCount;

            // Ring Buffer stuff - define array for convenience
            nextSequences = new long[inputChannelCount];
            availableSequences = new long[inputChannelCount];
            Arrays.fill(availableSequences, -2L);
            buildSequences = new Sequence[inputChannelCount];
            for (int i=0; i < inputChannelCount; i++) {
                buildSequences[i] = buildSequenceIn[btIndex][i];
                nextSequences[i]  = buildSequences[i].get() + 1L;
            }

            // First thing we do is look for the prestart event(s) and pass it on
            try {
                PayloadBuffer cEvent = gotAllControlEvents(buildSequences,
                                                           buildBarrierIn,
                                                           nextSequences);

//System.out.println("  EB mod: got prestart, order = " + cEvent.getByteOrder());
                // If this is the first build thread to reach this point, then
                // write prestart event on all output channels, ring 0. Other build
                // threads ignore this.
                if (firstToGetPrestart.compareAndSet(false, true)) {
                    controlToOutputAsync(cEvent, true);
                }
//System.out.println("  EB mod: final, updated prestart order = " + cEvent.getByteOrder());
                // This thread is ready to look for "go"
                waitForPrestart.countDown();
            }
            catch (Exception e) {
                e.printStackTrace();
                if (debug) System.out.println("  EB mod: interrupted while waiting for prestart event");
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null,"Interrupted waiting for prestart event");

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();
                return;
            }

            // Wait for ALL Prestart events to arrive at build threads before looking for Go
            try {
                waitForPrestart.await();
            }
            catch (InterruptedException e) {}

            // Second thing we do is look for the go event and pass it on
            try {
                PayloadBuffer gEvent = gotAllControlEvents(buildSequences,
                                                           buildBarrierIn,
                                                           nextSequences);
                if (firstToGetGo.compareAndSet(false, true)) {
                    controlToOutputAsync(gEvent, false);
                }
                // This thread is ready to build
                waitForGo.countDown();
            }
            catch (Exception e) {
                e.printStackTrace();
                if (debug) System.out.println("  EB mod: interrupted while waiting for go event");
                errorMsg.compareAndSet(null,"Interrupted waiting for go event");
                state = CODAState.ERROR;
                emu.sendStatusMessage();
                return;
            }

            // Wait for ALL Go events to arrive at build threads before building
            try {
                waitForGo.await();
            }
            catch (InterruptedException e) {}


            // Now do the event building
            while (state == CODAState.ACTIVE || paused) {

                try {
                    nonFatalError = false;

                    // The input channels' rings (1 per channel)
                    // are filled by the PreProcessor threads.

                    // Here we have what we need to build:
                    // ROC raw events from all ROCs (or partially built events from
                    // each contributing EB) each with sequential record IDs.
                    // However, there are also user and END events in the rings.

                    // Put null into buildingBanks array elements
                    Arrays.fill(buildingBanks, null);

                    // Set variables/flags
                    haveEnd = false;
                    firstToFindEnd = false;
                    firstToGetEnd.set(false);
                    gotFirstBuildEvent = false;
                    endEventCount = 0;

                    // Fill array with actual banks

                    // Grab one buildable (non-user/control) bank from each channel.
                    for (int i=0; i < ringBuffersIn.length; i++) {

                        // Loop until we get event which is NOT a user event
                        while (true) {

                            gotBank = false;

                            // Only wait for a read of volatile memory if necessary ...
                            if (availableSequences[i] < nextSequences[i]) {
                                // Will BLOCK here waiting for item if none available,
                                // but it can be interrupted.
                                // Available sequence may be larger than what we desired.
                                availableSequences[i] = buildBarrierIn[i].waitFor(nextSequences[i]);
//System.out.println("  EB mod: " + btIndex + ", available Seq = " + availableSequences[i]);
                            }

                            // While we have new data to work with ...
                            while (nextSequences[i] <= availableSequences[i]) {
                                buildingBanks[i] = (PayloadBuffer) ringBuffersIn[i].get(nextSequences[i]);
//System.out.println("  EB mod: " + btIndex + ", event order = " + buildingBanks[i].getByteOrder());
                                eventType = buildingBanks[i].getEventType();

                                // Skip over user events. These were actually already placed in
                                // first output channel's first ring by pre-processing thread.
                                if (eventType.isUser())  {
//System.out.println("  EB mod: " + btIndex + ", skip user item " + nextSequences[i]);
                                    nextSequences[i]++;
                                }
                                // Skip over events being built by other build threads
                                else if (skipCounter[i] - 1 > 0)  {
//System.out.println("  EB mod: " + btIndex + ", skip item " + nextSequences[i]);
                                    nextSequences[i]++;
                                    skipCounter[i]--;
                                }
                                // Found a bank, so do something with it (skipCounter[i] - 1 == 0)
                                else {
//System.out.println("  EB mod: " + btIndex + ", accept item " + nextSequences[i]);
                                    gotBank = true;
                                    break;
                                }
                            }

                            if (!gotBank) {
                                continue;
                            }

                            // If event needs to be built ...
                            if (!eventType.isControl()) {
                                // One-time init stuff for a group of
                                // records that will be built together.
                                if (!gotFirstBuildEvent) {
                                    // Set flag
                                    gotFirstBuildEvent = true;

                                    // Find the total # of events
                                    totalNumberEvents = buildingBanks[i].getNode().getNum();

                                    // The "block level" is the number of entangled events in one
                                    // ROC raw record which is set by the trigger supervisor.
                                    // Its value is in "totalNumberEvents" set in the line above.
                                    // This may change following each sync event.
                                    // It is important because if there are multiple BuildingThreads, then
                                    // each thread must set its expected event number according to the
                                    // blockLevel.
                                    if (!isEventNumberInitiallySet)  {
                                        firstEventNumber = 1L + (btIndex * totalNumberEvents);
                                        isEventNumberInitiallySet = true;
                                    }
                                    else {
                                        firstEventNumber += btCount*totalNumberEvents;
                                    }
                                }

                                // Go to next input channel
                                skipCounter[i] = btCount;
                                break;
                            }

                            // If we're here, we've got a CONTROL event

                            // If not END, we got problems
                            if (!buildingBanks[i].getControlType().isEnd()) {
                                throw new EmuException(buildingBanks[i].getControlType() +
                                                               " control events not allowed");
                            }

                            // At this point all controls are END events

                            // Is this build thread the first to get the END event?
                            firstToFindEnd = firstToGetEnd.compareAndSet(false, true);

                            haveEnd = true;
                            endEventCount++;

                            if (!gotFirstBuildEvent) {
                                // Don't do all the stuff for a
                                // regular build if first event is END.
                                gotFirstBuildEvent = true;
                            }

                            // Go to next input channel
                            skipCounter[i] = btCount;
                            break;
                        }
                    }

                    // Do END event stuff here
                    if (haveEnd && endEventCount != buildingBanks.length) {
                        // If we're here, not all channels got an END event.
                        // See if we can find them in the ring buffers which didn't
                        // have one. If we can, great. If not, major error.
                        // If all our channels have an END, we can end normally
                        // with a warning about the mismatch in number of events.

                        int finalEndEventCount = endEventCount;

                        // Look through ring buffers to see if we can find the rest ...
                        for (int i=0; i < ringBuffersIn.length; i++) {

                            EventType   eType = buildingBanks[i].getEventType();
                            ControlType cType = buildingBanks[i].getControlType();

                            if (cType != null)  {
                                System.out.println("  EB mod: END paired with " + cType + " event from " +
                                                           buildingBanks[i].getSourceName());
                            }
                            else {
                                System.out.println("  EB mod: END paired with " + eType + " event from " +
                                                           buildingBanks[i].getSourceName());
                            }

                            // If this channel doesn't have an END, try finding it somewhere in ring
                            if (cType != ControlType.END) {
                                int offset = 0;
                                boolean done = false;
                                PayloadBuffer pBuf;
                                long available, veryNextSequence = nextSequences[i]+1;

                                try  {
                                    while (true) {
                                        // Check to see if there is anything to read so we don't block.
                                        // If not, move on to the next ring.
                                        if (!ringBuffersIn[i].isPublished(veryNextSequence)) {
                                            break;
                                        }

                                        available = buildBarrierIn[i].waitFor(veryNextSequence);

                                        while (veryNextSequence <= available) {
                                            offset++;
                                            pBuf = (PayloadBuffer) ringBuffersIn[i].get(veryNextSequence);
                                            if (pBuf.getControlType() == ControlType.END) {
                                                // Found the END event
System.out.println("  EB mod: got END from " + buildingBanks[i].getSourceName() +
                   ", back " + offset + " places in ring");
                                                finalEndEventCount++;
                                                done = true;
                                                break;
                                            }
                                            buildSequences[i].set(veryNextSequence++);
                                        }

                                        if (done) {
                                            break;
                                        }
                                    }
                                }
                                catch (final TimeoutException e) {}
                                catch (final AlertException e)   {}
                            }
                        }

                        // If we still can't find all ENDs, throw exception - major error
                        if (finalEndEventCount!= buildingBanks.length) {
                            throw new EmuException("only " + finalEndEventCount + " ENDs for " +
                                                           buildingBanks.length + " channels");
                        }

                        // If we're here, we've found all ENDs, continue on with warning ...
                        nonFatalError = true;
System.out.println("  EB mod: have all ENDs, but differing # of physics events in channels");
                    }


                    // If we have all END events ...
                    if (haveEnd) {
                        // Only the first build thread to find all
                        // ENDs will place END on output channels.
                        if (!firstToFindEnd) return;

                        haveEndEvent = true;
System.out.println("  EB mod: found END events on all input channels");

                        // Interrupt the other BT threads which may not have processed
                        // the END event yet or experienced some error.
                        // Give them time to finish up business first. This business may
                        // never finish if there are differing numbers of events on each
                        // input channel.
                        Thread.sleep(250);

                        endBuildAndPreProcessingThreads(this, false);

                        // Throw exception if inconsistent
                        Evio.gotConsistentControlEvents(buildingBanks, runNumber, runTypeId);

System.out.println("  EB mod: have consistent END event(s)");

                        // Put 1 END event on each output channel
                        if (outputChannelCount > 0) {
                            // Take one of the control events and update
                            // it with the latest event builder data.
                            Evio.updateControlEvent(buildingBanks[0], runNumber,
                                  runTypeId, (int)eventCountTotal,
                                  (int)(firstEventNumber + totalNumberEvents - eventNumberAtLastSync),
                                  outputOrder);

                            // Send END event to first output channel
System.out.println("  EB mod: send END event to first output channel");
                            eventToOutputChannel(buildingBanks[0], 0, btIndex);
                            for (int j=1; j < outputChannelCount; j++) {
                                // Copy END event
                                PayloadBuffer bb = new PayloadBuffer(buildingBanks[0]);
                                // Write to additional output channel
System.out.println("  EB mod: copy & send END event to another output channel");
                                eventToOutputChannel(bb, j, btIndex);
                            }
                        }


                        // If this is a sync event, keep track of the next event # to be sent
                        if (Evio.isSyncEvent(buildingBanks[0].getNode())) {
                            eventNumberAtLastSync = firstEventNumber + totalNumberEvents;
                        }

                        if (endCallback != null) endCallback.endWait();
                        return;
                    }

                    // At this point there are only physics or ROC raw events, which do we have?
                    havePhysicsEvents = buildingBanks[0].getEventType().isAnyPhysics();

                    // Check for identical syncs, uniqueness of ROC ids,
                    // single-event-mode, identical (physics or ROC raw) event types,
                    // and the same # of events in each bank
                    nonFatalError |= Evio.checkConsistency(buildingBanks);

                    // Are events in single event mode?
                    boolean eventsInSEM = buildingBanks[0].isSingleEventMode();

if (debug && nonFatalError) System.out.println("\n  EB mod: non-fatal ERROR 1\n");

                    //--------------------------------------------------------------------
                    // Build trigger bank, number of ROCs given by number of buildingBanks
                    //--------------------------------------------------------------------
                    // The tag will be finally set when this trigger bank is fully created
if (debug && havePhysicsEvents)
    System.out.println("  EB mod: create combined trig w/ num (# Rocs) = " + buildingBanks.length);

                    // Get an estimate on the buffer memory needed.
                    // Start with 1K and add roughly the amount of trigger bank data
                    int memSize = 1000 + buildingBanks.length * totalNumberEvents * 10;
                    for (PayloadBuffer buildingBank : buildingBanks) {
                        memSize += buildingBank.getBuffer().capacity();
                    }

                    // Grab a stored ByteBuffer
                    ByteBufferItem bufItem = bbSupply.get();
                    ByteBuffer evBuf = bufItem.getBuffer();
                    bufItem.ensureCapacity(memSize);
                    builder.setBuffer(evBuf);

                    // Create a (top-level) physics event from payload banks
                    // and the combined trigger bank. First create the tag:
                    //   -if I'm a data concentrator or DC, the tag has 4 status bits and the ebId
                    //   -if I'm a primary event builder or PEB, the tag is 0xFF50 (or 0xFF51 if SEM)
                    //   -if I'm a secondary event builder or SEB, the tag is 0xFF70 (or 0xFF71 if SEM)
                    int tag;
                    CODAClass myClass = emu.getCodaClass();
                    switch (myClass) {
                        case SEB:
                            if (eventsInSEM) {
                                tag = CODATag.BUILT_BY_SEB_IN_SEM.getValue();
                            }
                            else {
                                tag = CODATag.BUILT_BY_SEB.getValue();
                            }
                            // output event type
                            eventType = EventType.PHYSICS;
                            break;

                        case PEB:
                            if (eventsInSEM) {
                                tag = CODATag.BUILT_BY_PEB_IN_SEM.getValue();
                            }
                            else {
                                tag = CODATag.BUILT_BY_PEB.getValue();
                            }
                            eventType = EventType.PHYSICS;
                            break;

                        //case DC:
                        default:
                            eventType = EventType.PARTIAL_PHYSICS;
                            tag = Evio.createCodaTag(buildingBanks[0].isSync(),
                                                 buildingBanks[0].hasError() || nonFatalError,
                                                 buildingBanks[0].getByteOrder() == ByteOrder.BIG_ENDIAN,
                                                 buildingBanks[0].isSingleEventMode(),
                                                 id);
//if (debug) System.out.println("  EB mod: tag = " + tag + ", is sync = " + buildingBanks[0].isSync() +
//                   ", has error = " + (buildingBanks[0].hasError() || nonFatalError) +
//                   ", is big endian = " + buildingBanks[0].getByteOrder() == ByteOrder.BIG_ENDIAN +
//                   ", is single mode = " + buildingBanks[0].isSingleEventMode());
                    }

                    // TODO: Problem, non fatal errors cannot be known in advance of building???

                    // Start top level
                    builder.openBank(tag, totalNumberEvents, DataType.BANK);


                    // If building with Physics events ...
                    if (havePhysicsEvents) {
                        //-----------------------------------------------------------------------------------
                        // The actual number of rocs will replace num in combinedTrigger definition above
                        //-----------------------------------------------------------------------------------
                        // Combine the trigger banks of input events into one (same if single event mode)
if (debug) System.out.println("  EB mod: create trig bank from built banks, sparsify = " + sparsify);
                        nonFatalError |= Evio.makeTriggerBankFromPhysics(buildingBanks, builder, id,
                                                                    runNumber, runTypeId, includeRunData,
                                                                    eventsInSEM, sparsify,
                                                                    checkTimestamps, timestampSlop);
                    }
                    // else if building with ROC raw records ...
                    else {
                        // If in single event mode, build trigger bank differently
                        if (eventsInSEM) {
                            // Create a trigger bank from data in Data Block banks
//if (debug) System.out.println("  EB mod: create trigger bank in SEM");
                            nonFatalError |= Evio.makeTriggerBankFromSemRocRaw(buildingBanks, builder,
                                                                               id, firstEventNumber,
                                                                               runNumber, runTypeId,
                                                                               includeRunData,
                                                                               checkTimestamps,
                                                                               timestampSlop);
                        }
                        else {
                            // Combine the trigger banks of input events into one
if (debug) System.out.println("  EB mod: create trigger bank from Rocs, sparsify = " + sparsify);
                            nonFatalError |= Evio.makeTriggerBankFromRocRaw(buildingBanks, builder,
                                                                            id, firstEventNumber,
                                                                            runNumber, runTypeId,
                                                                            includeRunData, sparsify,
                                                                            checkTimestamps,
                                                                            timestampSlop, btIndex);
                        }
                    }

if (debug && nonFatalError) System.out.println("\n  EB mod: non-fatal ERROR 2\n");

                    // Check input banks for non-fatal errors
                    for (PayloadBuffer pBank : buildingBanks)  {
                        nonFatalError |= pBank.hasNonFatalBuildingError();
                    }

if (debug && nonFatalError) System.out.println("\n  EB mod: non-fatal ERROR 3\n");

                    if (havePhysicsEvents) {
//if (debug) System.out.println("  EB mod: build physics event with physics banks");
                        Evio.buildPhysicsEventWithPhysics(buildingBanks, builder);
                    }
                    else {
//if (debug) System.out.println("  EB mod: build physics event with ROC raw banks");
                        Evio.buildPhysicsEventWithRocRaw(buildingBanks,
                                                         builder, eventsInSEM);
                    }

                    // Done creating event
                    builder.closeAll();

                    // stats
                    // TODO: protect since in multithreaded environs ?
                    eventCountTotal += totalNumberEvents;
                    wordCountTotal  += builder.getTotalBytes()/4 + 1;

                    // Which output channel do we use?
                    if (outputChannelCount > 1) {
                        // If we're a DC with multiple SEBs ...
                        if (chunkingForSebs) {
                            evGroupIndex = evIndex/sebChunk;
                            outputChannelIndex = (int) (evGroupIndex % outputChannelCount);
                            evIndex += btCount;
                        }
                        else {
                            // Round-robin between all output channels
                            outputChannelIndex = (outputChannelCount + 1) % outputChannelCount;
                        }
                    }

                    // Put it in the correct output channel.
                    // Important to use builder.getBuffer() method instead of evBuf directly.
                    // That's because in the method, the limit and position are set
                    // properly for reading.
                    eventToOutputRing(btIndex, outputChannelIndex, builder.getBuffer(),
                                      eventType, bufItem, bbSupply);

                    // TODO: Do we actually want to send a sync event ???
                    // But wait! One more thing.
                    // We must check for the sync bits being set. If they are set,
                    // generate a SYNC event and write both the PHYSICS & SYNC events
                    // in that order.
                    if (buildingBanks[0].isSync()) {
                        try {
                            ByteBuffer controlEvent =
                                    Evio.createControlBuffer(ControlType.SYNC,
                                         0, 0, (int) eventCountTotal,
                                         (int) (firstEventNumber + totalNumberEvents - eventNumberAtLastSync),
                                         ByteOrder.BIG_ENDIAN);
                            PayloadBuffer controlPBuf = new PayloadBuffer(controlEvent);
                            eventNumberAtLastSync = firstEventNumber + totalNumberEvents;
                            controlToOutputAsync(controlPBuf, false);
                        }
                        catch (EvioException e) {/* never happen */}
                    }

                    // Release the reusable ByteBuffers used by the input channel
                    for (int i=0; i < inputChannelCount; i++) {
                        buildingBanks[i].releaseByteBuffer();
                    }

                    // Tell input ring buffers we're done with these events
                    for (int i=0; i < ringBuffersIn.length; i++) {
//System.out.println("  EB mod: " + order + ", set seq " + nextSequences[i]);
                        buildSequences[i].set(nextSequences[i]++);
                    }

                }
                catch (InterruptedException e) {
if (debug) System.out.println("  EB mod: INTERRUPTED thread " + Thread.currentThread().getName());
                    return;
                }
                catch (final TimeoutException e) {
                    e.printStackTrace();
if (debug) System.out.println("  EB mod: timeout in ring buffer");
                    // If we haven't yet set the cause of error, do so now & inform run control
                    errorMsg.compareAndSet(null, e.getMessage());

                    // set state
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();

                    e.printStackTrace();
                    return;
                }
                catch (final AlertException e) {
                    e.printStackTrace();
if (debug) System.out.println("  EB mod: alert in ring buffer");
                    // If we haven't yet set the cause of error, do so now & inform run control
                    errorMsg.compareAndSet(null, e.getMessage());

                    // set state
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();

                    e.printStackTrace();
                    return;
                }

                catch (EmuException e) {
if (debug) System.out.println("  EB mod: MAJOR ERROR building events");
                    // If we haven't yet set the cause of error, do so now & inform run control
                    errorMsg.compareAndSet(null, e.getMessage());

                    // set state
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();

                    e.printStackTrace();
                    return;
                }
                catch (EvioException e) {
if (debug) System.out.println("  EB mod: MAJOR ERROR building events");
                    // If we haven't yet set the cause of error, do so now & inform run control
                    errorMsg.compareAndSet(null, e.getMessage());

                    // set state
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();

                    e.printStackTrace();
                    return;
                }
            }
if (debug) System.out.println("  EB mod: Building thread is ending");
        }
    }


    /**
     * End all build and pre-processing threads because an END cmd or event came through.
     * The build thread calling this method is not interrupted.
     *
     * @param thisThread the build thread calling this method; if null,
     *                   all build & pre-processing threads are interrupted
     * @param wait if <code>true</code> check if END event has arrived and
     *             if all the input ring buffers are empty, if not, wait up to 1/2 second.
     */
    private void endBuildAndPreProcessingThreads(BuildingThread thisThread, boolean wait) {

        if (wait) {
            // Look to see if anything still on the payload bank or input channel Qs
            boolean haveUnprocessedEvents = false;
            long startTime = System.currentTimeMillis();

            // Wait up to endingTimeLimit millisec for events to
            // be processed & END event to arrive, then proceed
            while ((haveUnprocessedEvents || !haveEndEvent) &&
                   (System.currentTimeMillis() - startTime < endingTimeLimit)) {
                try {Thread.sleep(200);}
                catch (InterruptedException e) {}

                haveUnprocessedEvents = false;
            }

            if (haveUnprocessedEvents || !haveEndEvent) {
                if (debug) System.out.println("  EB mod: endBuildThreads: will end building/filling threads but no END event or Qs not empty !!!");
                state = CODAState.ERROR;
            }
        }

        // NOTE: EMU has a command executing thread which calls this EB module's execute
        // method which, in turn, calls this method when an END cmd is sent. In this case
        // all build threads will be interrupted in the following code.

        // Interrupt all Building threads except the one calling this method
        for (Thread thd : buildingThreadList) {
            if (thd == thisThread) continue;
            // Try to end thread nicely but it could hang on rb.next(), if so, kill it
            thd.interrupt();
            try {
                thd.join(250);
                if (thd.isAlive()) {
                    thd.stop();
                }
            }
            catch (InterruptedException e) {}
        }

        // Interrupt all PreProcessor threads too
        if (preProcessors != null) {
            for (Thread qf : preProcessors) {
                qf.interrupt();
                try {
                    qf.join(250);
                    if (qf.isAlive()) {
                        qf.stop();
                    }
                }
                catch (InterruptedException e) {}
            }
        }
        System.out.println("  EB mod: endThreads() done");
    }


    /**
     * Start threads for stats, pre-processing incoming events, and building events.
     * It creates these threads if they don't exist yet.
     */
    private void startThreads() {
        // Rate calculating thread
        if (RateCalculator != null) {
            RateCalculator.interrupt();
        }

        RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), name+":watcher");

        if (RateCalculator.getState() == Thread.State.NEW) {
            RateCalculator.start();
        }

        int inChanCount = inputChannels.size();

        // Build threads
        buildingThreadList.clear();
        for (int i=0; i < buildingThreadCount; i++) {
            BuildingThread thd1 = new BuildingThread(i, emu.getThreadGroup(), name+":builder"+i);
            buildingThreadList.add(thd1);
            thd1.start();
        }

        // Create pre-processing threads - one for each input channel
        if (preProcessors == null) {
            preProcessors = new Thread[inChanCount];
            for (int j=0; j < inChanCount; j++) {
                preProcessors[j] = new PreProcessor(ringBuffersIn[j],
                                               preBuildBarrier[j],
                                               preBuildSequence[j],
                                               inputChannels.get(j),
                                               emu.getThreadGroup(),
                                               name+":preProcessor"+(j));
            }
        }

        // Start pre-processing threads
        for (int i=0; i < inChanCount; i++) {
            if (preProcessors[i].getState() == Thread.State.NEW) {
                preProcessors[i].start();
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

        if (RateCalculator != null) RateCalculator.interrupt();

        // Build & pre-processing threads must be immediately ended
        endBuildAndPreProcessingThreads(null, false);

        RateCalculator = null;
        preProcessors = null;
        buildingThreadList.clear();

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
    public void end() {

        state = CODAState.DOWNLOADED;

        // The order in which these threads are shutdown does(should) not matter.
        // Rocs should already have been shutdown, followed by the input transports,
        // followed by this module (followed by the output transports).
        if (RateCalculator != null) RateCalculator.interrupt();

        // Build & pre-processing threads should already be ended by END event
        endBuildAndPreProcessingThreads(null, true);

        RateCalculator = null;
        preProcessors = null;
        buildingThreadList.clear();

        paused = false;

        try {
            // Set end-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }




    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {

        // Event builder needs inputs
        if (inputChannelCount < 1) {
            errorMsg.compareAndSet(null, "no input channels to EB");
            state = CODAState.ERROR;
            emu.sendStatusMessage();
            throw new CmdExecException("no input channels to EB");
        }

        // Make sure each input channel is associated with a unique rocId
        for (int i=0; i < inputChannelCount; i++) {
            for (int j=i+1; j < inputChannelCount; j++) {
                if (inputChannels.get(i).getID() == inputChannels.get(j).getID()) {
                    errorMsg.compareAndSet(null, "input channels duplicate rocIDs");
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();
                    throw new CmdExecException("input channels duplicate rocIDs");
                }
            }
        }

        state = CODAState.PAUSED;
        paused = true;

        //------------------------------------------------
        // Disruptor (RingBuffer) stuff for input channels
        //------------------------------------------------

        // "One ring buffer to rule them all and in the darkness bind them."
        // Actually, one ring buffer for each input channel.
        ringBuffersIn = new RingBuffer[inputChannelCount];

        // One pre-build sequence and barrier for each input channel
        preBuildSequence = new Sequence[inputChannelCount];
        preBuildBarrier  = new SequenceBarrier[inputChannelCount];

        // For each input channel, 1 sequence per build thread
        buildSequenceIn = new Sequence[buildingThreadCount][inputChannelCount];

        // For each input channel, all build threads share one barrier
        buildBarrierIn = new SequenceBarrier[inputChannelCount];

        // For each channel ...
        for (int i=0; i < inputChannelCount; i++) {
            // Get channel's ring buffer
            RingBuffer<RingItem> rb = inputChannels.get(i).getRingBufferIn();
            ringBuffersIn[i] = rb;

            // First sequence & barrier is for checking of PayloadBuffer/Bank
            // data before the actual building takes place.
            preBuildBarrier[i]  = rb.newBarrier();
            preBuildSequence[i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

            // For each build thread ...
            for (int j=0; j < buildingThreadCount; j++) {
                // We have 1 sequence for each build thread & input channel combination
                buildSequenceIn[j][i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

                // This sequence may be the last consumer before producer comes along
                rb.addGatingSequences(buildSequenceIn[j][i]);
            }

            // We have 1 barrier for each channel (shared by building threads)
            // which depends on (comes after) the pre-processing sequence.
            buildBarrierIn[i] = rb.newBarrier(preBuildSequence[i]);
        }

        //------------------------------------------------
        //
        //------------------------------------------------

        // Reset some variables
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;
        runTypeId = emu.getRunTypeId();
        runNumber = emu.getRunNumber();
        eventNumberAtLastSync = 1L;

        // Do this before starting build threads
        waitForGo = new CountDownLatch(buildingThreadCount);
        waitForPrestart = new CountDownLatch(buildingThreadCount);
        firstToGetGo.set(false);
        firstToGetEnd.set(false);
        firstToGetPrestart.set(false);

        // Create & start threads
        startThreads();

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}
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


 }