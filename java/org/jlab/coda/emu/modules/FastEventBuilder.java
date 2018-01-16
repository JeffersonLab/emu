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
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODAStateIF;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * <pre><code>
 *
 *   Ring Buffer (single producer, lock free) for a single input channel
 *
 *
 *           >
 *         /            ____
 * Post build thread   /  |  \
 *              --->  /1 _|_ 2\  <---- Build Threads 1-M
 *                   |__/   \__|               |
 *                   |6 |   | 3|               V
 *             ^     |__|___|__|
 *             |      \ 5 | 4 / <---- Pre-Processing Thread
 *         Producer->  \__|__/                /
 *                                          <
 *
 *
 * Actual input channel ring buffers have thousands of events (not 6).
 * The producer is a single input channel which reads incoming data,
 * parses it and places it into the ring buffer.
 *
 * The leading consumer is the pre-processing thread - one for each input channel.
 * All build threads come after the pre-processing thread in no particular order
 * and consume slots that the pre-processing thread is finished with.
 * There are a fixed number of build threads which can be set in the config file.
 * The post-build thread - one for each input channel - releases all ring-based
 * resources in proper order and will only take slots the build threads are finished with.
 * After initially consuming and filling all slots (once around ring),
 * the producer will only take additional slots that the post-build thread
 * is finished with.
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
 *  output channel(s)        \        |       /
 *                            \       |      /
 *                             V      V     V
 * Output Channel(s):    OC1: RB1    RB2   RBM
 * (1 ring buffer for    OC2: RB1    RB2   RBM  ...
 *  each build thread
 *  in each channel)           |      |      |
 *                             |      |      |
 *  Post build threads         |      |      |
 *  release ring resources     V      V      V
 *  in proper order        PBT 1      2      N
 *
 *  M != N in general
 *  M  = 1 by default
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
    private ArrayList<BuildingThread> buildingThreadList = new ArrayList<>(6);

    /** Threads (one for each input channel) used to take Evio data from
     *  input channels and check them for proper format. */
    private Thread preProcessors[];

    /** Threads (one for each input channel) for
     *  releasing resources used to build events. */
    private ReleaseRingResourceThread releaseThreads[];

    /** Maximum time in milliseconds to wait when commanded to END but no END event received. */
    private long endingTimeLimit = 30000;

    /** The number of the experimental run. */
    private int runNumber;

    /** The number of the experimental run's configuration. */
    private int runTypeId;

    /** The eventNumber value when the last sync event arrived. */
    private volatile long eventNumberAtLastSync;

    /** If <code>true</code>, this emu has received
     *  all prestart events (1 per input channel). */
    private volatile boolean haveAllPrestartEvents;

    /** If <code>true</code>, produce debug print out. */
    private boolean debug = true;

    /**
     * If true, dump incoming data immediately after reading off input rings.
     * Use this for testing incoming data rate.
     */
    private boolean dumpData;

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

    /** If true, include run number & type in built trigger bank. */
    private boolean includeRunData;

    /** If true, do not include empty roc-specific segments in trigger bank. */
    private boolean sparsify;

    // ---------------------------------------------------
    // Control events
    // ---------------------------------------------------

    /** Have complete END event (on all input channels)
     *  detected by one of the building threads. */
    private volatile boolean haveEndEvent;

    //-------------------------------------------
    // Disruptor (RingBuffer)
    //-------------------------------------------

    /** Number of items in build thread ring buffers. */
    protected int ringItemCount;

    /** One RingBuffer per input channel (references to channels' rings). */
    private RingBuffer<RingItem>[] ringBuffersIn;

    /** Size of RingBuffer for each input channel. */
    private int[] ringBufferSize;



    /** One pre-processing sequence for each input channel. */
    private Sequence[] preBuildSequence;

    /** One pre-processing barrier for each input channel. */
    private SequenceBarrier[] preBuildBarrier;



    /** For each input channel, 1 sequence per build thread. */
    private Sequence[][] buildSequenceIn;

    /** For each input channel, all build threads share one barrier. */
    private SequenceBarrier[] buildBarrierIn;



    /** One post-build sequence for each input channel. */
    private Sequence[] postBuildSequence;

    /** The post-build (garbage-releasing) thread has one barrier per input channel. */
    private SequenceBarrier[] postBuildBarrier;


    //-------------------------------------------
    // Statistics
    //-------------------------------------------

    /** Number of events built by build-thread 0 (not all bts). */
    private long builtEventCount;

    /** Number of slots in each output channel ring buffer. */
    private int outputRingSize;


    /**
     * Constructor creates a new EventBuilding instance.
     *
     * @param name         name of module
     * @param attributeMap map containing attributes of module
     * @param emu          emu which created this module
     */
    public FastEventBuilder(String name, Map<String, String> attributeMap, Emu emu) {
        super(name, attributeMap, emu);

        // Set number of building threads (always >= 1, set in ModuleAdapter constructor)
        buildingThreadCount = eventProducingThreads;

        // If # build threads not explicitly set in config,
        // make it 2 since that produces the best performance with the least resources.
        if (!epThreadsSetInConfig) {
            buildingThreadCount = eventProducingThreads = 2;
        }
logger.info("  EB mod: " + buildingThreadCount + " number of event building threads");

        // default is NOT to include run number & type in built trigger bank
        String str = attributeMap.get("runData");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("in")   ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                includeRunData = true;
            }
        }


        // set "data dump" option on
        str = attributeMap.get("dump");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                dumpData = true;
            }
        }

        // default is NOT to sparsify (not include) roc-specific segments in trigger bank
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
            if (timestampSlop < 1) timestampSlop = 2;
        }
        catch (NumberFormatException e) {}

        // Number of items in each build thread ring. We need to limit this
        // since it costs real memory. For big events, 128 x 20MB events = 2.56GB
        // of mem used. Multiply that times the number of build threads.
        int ringCount = 128;
        // If there are multiple build threads, reduce the # of items per thread.
        if (buildingThreadCount == 3) {
            ringCount = 32;
        }
        else {
            ringCount /= buildingThreadCount;
        }

        str = attributeMap.get("ringCount");
        if (str != null) {
            try {
                ringCount = Integer.parseInt(str);
                if (ringCount < 16) {
                    ringCount = 16;
                }
           }
            catch (NumberFormatException e) {}
        }

        // Make sure it's a power of 2, round up
        ringItemCount = EmuUtilities.powerOfTwo(ringCount, true);
        outputRingSize = getInternalRingCount();
logger.info("  EB mod: internal ring buf count -> " + ringItemCount);
    }


    /** {@inheritDoc} */
    public int getInternalRingCount() {return buildingThreadCount*ringItemCount;};

    /**
     * Method to keep statistics on the size of events built by this event builder.
     * @param bufSize size in bytes of event built by this EB
     */
    private void keepStats(int bufSize) {

        if (bufSize > maxEventSize) maxEventSize = bufSize;
        if (bufSize < minEventSize) minEventSize = bufSize;

        avgEventSize = (int) ((avgEventSize*builtEventCount + bufSize) / (builtEventCount+1L));
        goodChunk_X_EtBufSize = avgEventSize * outputRingSize * 3 / 4;
//        System.out.println("avg = "+ avgEventSize + ", ev count = " + (builtEventCount+1L) +
//                           ", current buf size = " + bufSize);
        // If value rolls over, start over with avg
        if (++builtEventCount * avgEventSize < 0) {
            builtEventCount = avgEventSize = 0;
        }
    }


    /** {@inheritDoc}.
     * This queries the first EB build thread for all the input channel levels. */
    public int[] getInputLevels() {
        try {
            BuildingThread bt = buildingThreadList.get(0);
            if (bt == null) {
                return null;
            }

            for (int i=0; i < inputChannelCount; i++) {
                inputChanLevels[i] = bt.getInputLevel(i);
            }
        }
        catch (Exception e) {
            return null;
        }

        return inputChanLevels;
    }


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
            RingItem ri;

            while (moduleState == CODAState.ACTIVE || paused) {
                try {

                    while (moduleState == CODAState.ACTIVE || paused) {
//System.out.println("  EB mod: wait for Seq = " + nextSequence + " in pre-processing");
                        final long availableSequence = barrier.waitFor(nextSequence);

                        if (dumpData) {
                            while (nextSequence <= availableSequence) {
                                ri = ringBuffer.get(nextSequence);
                                ri.releaseByteBuffer();
                                nextSequence++;
                            }
                            sequence.set(availableSequence);
                            continue;
                        }


//System.out.println("  EB mod: available Seq = " + availableSequence + " in pre-processing");
                        while (nextSequence <= availableSequence) {
                            ri = ringBuffer.get(nextSequence);
                            Evio.checkPayload((PayloadBuffer)ri, channel);

                            // Take user event and place on output channel
                            if (ri.getEventType().isUser()) {
//System.out.println("  EB mod: got user event in pre-processing order = " + pBuf.getByteOrder());

                                // Swap headers, NOT DATA, if necessary
                                if (outputOrder != ri.getByteOrder()) {
                                    try {
//System.out.println("  EB mod: swap user event (not data)");
                                        ByteBuffer buffy = ri.getBuffer();
                                        EvioNode nody = ri.getNode();
                                        if (buffy != null) {
                                            // Takes care of swapping of event in its own separate buffer,
                                            // headers not data
                                            ByteDataTransformer.swapEvent(buffy, buffy, 0, 0, false, null);
                                        }
                                        else if (nody != null) {
                                            // This node may share a backing buffer with other, ROC Raw, events.
                                            // Thus we cannot change the order of the entire backing buffer.
                                            // For simplicity, let's copy it and swap it in its very
                                            // own buffer.

                                            // Copy
                                            buffy = nody.getStructureBuffer(true);
                                            // Swap headers but not data
                                            ByteDataTransformer.swapEvent(buffy, null, 0, 0, false, null);
                                            // Store in ringItem
                                            ri.setBuffer(buffy);
                                            ri.setNode(null);
                                            // Release claim on backing buffer since we are now
                                            // using a different buffer.
                                            ri.releaseByteBuffer();
                                        }
                                    }
                                    catch (EvioException e) {/* should never happen */ }
                                }

                                // If same byte order, then we may have buffer or node.
                                // Node in the usual case.

                                // Send it on.
                                // User events are thrown away if no output channels
                                // since this event builder does nothing with them.
                                // User events go into the first ring of the first channel.
                                // Since all user events are dealt with here
                                // and since they're now all in their own (non-ring) buffers,
                                // the build and
                                // post-build threads can skip over them.
                                eventToOutputChannel(ri, 0, 0);
//System.out.println("  EB mod: sent user event to output channel");
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
                    emu.setErrorState("EB: Roc raw or physics event in wrong format");
                    moduleState = CODAState.ERROR;
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
                bank.releaseByteBuffer();
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

//System.out.println("  EB mod: wait for out buf, ch" + channelNum + ", ring " + ringNum);
        long nextRingItem = rb.next();
//System.out.println("  EB mod: Got sequence " + nextRingItem + " for " + channelNum + ":" + ringNum);
        RingItem ri = (RingItem) rb.get(nextRingItem);
        ri.setBuffer(buf);
        ri.setEventType(eventType);
        ri.setControlType(null);
        ri.setSourceName(null);
        ri.setReusableByteBuffer(bbSupply, item);

//System.out.println("  EB mod: will publish to ring " + ringNum);
        rb.publish(nextRingItem);
//System.out.println("  EB mod: published to ring " + ringNum);
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
     * @param threadIndex   build thread identifying index
     * @return type of control events found
     * @throws EmuException if got non-control or non-prestart/go/end event
     * @throws InterruptedException if taking of event off of Q is interrupted
     */
    private ControlType getAllControlEvents(Sequence[] sequences,
                                     SequenceBarrier barriers[],
                                     long nextSequences[], int threadIndex)
            throws EmuException, InterruptedException {

        PayloadBuffer[] buildingBanks = new PayloadBuffer[inputChannelCount];
        ControlType controlType = null;

        // First thing we do is look for the go or prestart event and pass it on
        // Grab one control event from each ring buffer.
        for (int i=0; i < inputChannelCount; i++) {
            try  {
                ControlType cType;
                while (true) {
                    barriers[i].waitFor(nextSequences[i]);
                    buildingBanks[i] = (PayloadBuffer) ringBuffersIn[i].get(nextSequences[i]);

                    cType = buildingBanks[i].getControlType();
                    if (cType == null) {
                        // If it's not a control event, it may be a user event.
                        // If so, skip over it and look at the next one.
                        EventType eType = buildingBanks[i].getEventType();
                        if (eType != null && eType == EventType.USER) {
                            // Release ring slot
                            sequences[i].set(nextSequences[i]);
                            // Get ready to read item in next slot
                            nextSequences[i]++;
                            continue;
                        }
                        throw new EmuException("Expecting control, but got some other, non-user event");
                    }
                    break;
                }

                // Look for what the first channel sent, on the other channels
                if (controlType == null) {
                    controlType = cType;
                }
                else if (cType != controlType) {
                    throw new EmuException("Control event differs across inputs, expect " +
                                                   controlType + ", got " + cType);
                }

                if (!cType.isEnd() && !cType.isGo() && !cType.isPrestart()) {
                    if (threadIndex == 0) {
                        Utilities.printBuffer(buildingBanks[i].getBuffer(), 0, 5, "Bad control event");
                    }
                    throw new EmuException("Expecting prestart, go or end, got " + cType);
                }
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

        // control types, and in Prestart events, run #'s and run types
        // must be identical across input channels, else throw exception
        Evio.gotConsistentControlEvents(buildingBanks, runNumber, runTypeId);

        // Release the input ring slots AFTER checking for consistency.
        // If done before, the PayloadBuffer obtained from the slot can be
        // overwritten by incoming data, leading to a bad result.

        for (int i=0; i < inputChannelCount; i++) {
            // Release ring slot
            sequences[i].set(nextSequences[i]);

            // Get ready to read item in next slot
            nextSequences[i]++;

            // Release any temp buffer (from supply ring)
            // that may have been used for control event.
            // Should only be done once - by first build thread
            if (threadIndex == 0) {
                buildingBanks[i].releaseByteBuffer();
            }
        }

        return controlType;
    }


    /**
     * This method writes the specified control event into all the output channels.
     *
     * @param isPrestart {@code true} if prestart event being written, else go event
     * @throws InterruptedException if writing of event to output channels is interrupted
     */
    private void controlToOutputAsync(boolean isPrestart)
            throws InterruptedException {

        if (outputChannelCount < 1) {
            return;
        }

        // Put 1 control event on each output channel
        ControlType controlType = isPrestart ? ControlType.PRESTART : ControlType.GO;

        // Create a new control event with updated control data in it
        PayloadBuffer pBuf = Evio.createControlBuffer(controlType,
                                                   runNumber, runTypeId,
                                                   (int)eventCountTotal,
                                                   0, outputOrder, false);

        // Place event on first output channel, ring 0
        eventToOutputChannel(pBuf, 0, 0);

        // If multiple output channels ...
        if (outputChannelCount > 1) {
            for (int i = 1; i < outputChannelCount; i++) {
                // "Copy" control event
                PayloadBuffer pbCopy = new PayloadBuffer(pBuf);

                // Write event to output channel
                eventToOutputChannel(pbCopy, i, 0);
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
     * This thread is started by the PRESTART transition.
     * An empty buffer is obtained from a supply.
     * One evio bank from each input channel are together built into a new event
     * in that buffer. If this module has outputs, the built events are placed on
     * an output channel.
     * <p/>
     * If there are multiple output channels, in default operation, this thread
     * selects an output by round-robin. If however, this is a DC with multiple
     * SEBs to send events to, then things are more complicated.
     * <p/>
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



        /**
         * Constructor.
         *
         * @param btIndex place in relation to other build threads (first = 0)
         * @param group   thread group
         * @param name    thread name
         */
        BuildingThread(int btIndex, ThreadGroup group, String name) {
            super(group, name);
            this.btIndex = btIndex;
            evIndex = btIndex;
            btCount = buildingThreadCount;
System.out.println("  EB mod: create Build Thread with index " + btIndex + ", count = " + btCount);
        }


        /**
         * Get the input level (how full is the ring buffer 0-100) of a single input channel
         * @param chanIndex index of channel (starting at 0)
         * @return input level
         */
        int getInputLevel(int chanIndex) {
            // scale from 0% to 100% of ring buffer size
            //return ((int)(buildBarrierIn[chanIndex].getCursor() - nextSequences[chanIndex]) + 1)*100/ringBufferSize[chanIndex];
            //return ((int)(ringBuffersIn[chanIndex].getCursor() - nextSequences[chanIndex]) + 1)*100/ringBufferSize[chanIndex];
            return ((int)(ringBuffersIn[chanIndex].getCursor() -
                          ringBuffersIn[chanIndex].getMinimumGatingSequence()) + 1)*100/ringBufferSize[chanIndex];
        }


        /**
         * Handle the END event.
         */
        private void handleEndEvent() {

System.out.println("  EB mod: in handleEndEvent(), bt #" + btIndex + ", output chan count = " +
                           outputChannelCount);
            // Put 1 END event on each output channel
            if (outputChannelCount > 0) {
                // Create control event
                PayloadBuffer endBuf = Evio.createControlBuffer(ControlType.END,
                                                                runNumber, runTypeId, (int)eventCountTotal, 0,
                                                                outputOrder, false);

                // END event is the Nth "built" event through this EB (after go)
                endEventIndex = evIndex;

                // END event will be found on this ring in all output channels
                endEventRingIndex = btIndex;

                // Send END event to first output channel
System.out.println("  EB mod: send END event to output channel 0, ring " + endEventRingIndex +
                   ", ev# = " + evIndex);

                // Write to first output channel
                eventToOutputChannel(endBuf, 0, endEventRingIndex);

                for (int i=0; i < inputChannelCount; i++) {
                    buildSequences[i].set(nextSequences[i]++);
                }

                // Stats
                eventCountTotal ++;
                wordCountTotal  += 5;

                // Send END event to other output channels
                for (int j=1; j < outputChannelCount; j++) {
                    // Copy END event
                    PayloadBuffer pb = new PayloadBuffer(endBuf);
System.out.println("  EB mod: send END event to output channel " + j + ", ring " + endEventRingIndex +
                   ", ev# = " + evIndex);

                    // Already waited for other build threads to finish before
                    // writing END to first channel above, so now we can go ahead
                    // and write END to other channels without waiting.
                    eventToOutputChannel(pb, j, endEventRingIndex);
                }

                // Direct all output channels to the correct ring
                // from which to read & process the END event.
                // Only needed if multiple build threads AND
                // multiple output channels.
                processEndInOutput(endEventIndex, endEventRingIndex);
            }

            if (endCallback != null) endCallback.endWait();
        }

        
        /**
         * Method to search for END event on each channel when END found on one channel but
         * not at the same place on the other channels. Takes multiple build threads into
         * account.
         * @param endChannel  channel on which END event was already found
         * @param endSequence sequence at which the END event was found
         * @return total number of END events found
         */
        private int findEnd(int endChannel, long endSequence, int endEventCount) {
            // If the END event is far back on any of the communication channels, in order to be able
            // to read in those events, resources must be released after being read/used.
            // All build sequences must advance together for things to be released.

            try {
                long available;

                // For each channel ...
                for (int ch=0; ch < inputChannelCount; ch++) {

                    if (ch == endChannel) {
                        // We've already found the END event on this channel
                        continue;
                    }

                    int offset = 0;
                    boolean done = false;
                    long veryNextSequence = endSequence + 1L;

                    while (true) {
                        // Check to see if there is anything to read so we don't block.
                        // If not, move on to the next ring.
                        if (!ringBuffersIn[ch].isPublished(veryNextSequence)) {
//System.out.println("  EB mod: findEnd, for chan " + ch + ", sequence " + veryNextSequence + " not available yet");
                            // Only break (and throw a major error) if this EB has
                            // received the END command. Because only then do we know
                            // that all ROCS have ENDED and sent all their data.
                            if (moduleState == CODAState.DOWNLOADED ||
                                    moduleState != CODAState.ACTIVE) {
System.out.println("  EB mod: findEnd, stop looking for END on channel " + ch + " as module state = " + moduleState);
                                break;
                            }
                            // Wait for events to arrive
                            Thread.sleep(100);
                            // Try again
                            continue;
                        }

//System.out.println("  EB mod: findEnd, waiting for next item from chan " + ch + " at sequence " + veryNextSequence);
                        available = buildBarrierIn[ch].waitFor(veryNextSequence);
//System.out.println("  EB mod: findEnd, got items from chan " + ch + " up to sequence " + available);

                        while (veryNextSequence <= available) {
                            offset++;
                            PayloadBuffer pBuf = (PayloadBuffer) ringBuffersIn[ch].get(veryNextSequence);
                            String source = pBuf.getSourceName();
//System.out.println("  EB mod: findEnd, on chan " + ch + " found event of type " + pBuf.getEventType() + " from " + source + ", back " + offset +
//                   " places in ring with seq = " + veryNextSequence);
                            if (pBuf.getControlType() == ControlType.END) {
                                // Found the END event
System.out.println("  EB mod: findEnd, chan " + ch + " got END from " + source + ", back " + offset + " places in ring");
                                endEventCount++;
                                done = true;
                                break;
                            }

                            // Release buffer - done once. If btCount > 1, then the
                            // ReleaseRingResourceThread will release the buffer.
                            if (btCount == 1) {
                                pBuf.releaseByteBuffer();
                            }

                            // Advance sequence for all build threads
                            for (int bt = 0; bt < btCount; bt++) {
                                buildSequenceIn[bt][ch].set(veryNextSequence);
                            }
                            veryNextSequence++;
                        }

                        if (done) {
                            break;
                        }
                    }
                }
            }
            catch (InterruptedException e) {
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            return endEventCount;
        }


        public void run() {

            try {
                // Create a reusable supply of ByteBuffer objects
                // for writing built physics events into.
                //--------------------------------------------
                // Direct buffers give better performance ??
                //--------------------------------------------

                ByteBufferSupply bbSupply = new ByteBufferSupply(ringItemCount, 2000, outputOrder, false);
System.out.println("  EB mod: bbSupply -> " + ringItemCount + " # of bufs, direct = " + false);

                // Object for building physics events in a ByteBuffer
                CompactEventBuilder builder = null;
                try {
                    // Internal buffer of 8 bytes will be overwritten later
                    //builder = new CompactEventBuilder(8, outputOrder, true);
                    builder = new CompactEventBuilder(8, outputOrder, false);
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
                // Only the first build thread needs to look for the prestart and go events.
                // The others can just skip over them.
                //
                // Easiest to implement this with one counter per input channel.
                int[] skipCounter = new int[inputChannelCount];
                Arrays.fill(skipCounter, btIndex + 1);

                // Initialize
                int     endEventCount, totalNumberEvents=1;
                long    firstEventNumber=1, startTime=0L;
                boolean haveEnd, havePhysicsEvents;
                boolean isSync, nonFatalError;
                boolean gotBank, gotFirstBuildEvent;
                boolean isEventNumberInitiallySet = false;
                EventType eventType = null;

                if (outputChannelCount > 1) outputChannelIndex = -1;

                PayloadBuffer[] buildingBanks = new PayloadBuffer[inputChannelCount];

                minEventSize = Integer.MAX_VALUE;

                if (timeStatsOn) {
                    statistics = new Statistics(1000000, 30);
                }

                // Ring Buffer stuff - define array for convenience
                nextSequences = new long[inputChannelCount];
                availableSequences = new long[inputChannelCount];
                Arrays.fill(availableSequences, -2L);
                buildSequences = new Sequence[inputChannelCount];
                for (int i=0; i < inputChannelCount; i++) {
                    buildSequences[i] = buildSequenceIn[btIndex][i];
                    nextSequences[i]  = buildSequences[i].get() + 1L;
                }

                // First thing we do is look for the PRESTART event(s) and pass it on
                try {
                    // Get prestart from each input channel
                    ControlType cType = getAllControlEvents(buildSequences, buildBarrierIn,
                                                            nextSequences, btIndex);
                    if (!cType.isPrestart()) {
                        throw new EmuException("Expecting prestart event, got " + cType);
                    }

                    // 1st build thread writes prestart event on all output channels, ring 0.
                    // Other build threads ignore this.
                    if (btIndex == 0) {
                        controlToOutputAsync(true);
                    }
                }
                catch (Exception e) {
                    // If interrupted we must quit
                    if (debug) System.out.println("  EB mod: interrupted while waiting for prestart event");
                    emu.setErrorState("EB interrupted waiting for prestart event");
                    moduleState = CODAState.ERROR;
                    return;
                }

                prestartCallback.endWait();
                haveAllPrestartEvents = true;
System.out.println("  EB mod: got all PRESTART events");

                // Second thing we do is look for the GO or END event and pass it on
                try {
                    // Get go/end from each input channel
                    ControlType cType = getAllControlEvents(buildSequences, buildBarrierIn,
                                                            nextSequences, btIndex);
                    if (!cType.isGo()) {
                        if (cType.isEnd()) {
                            haveEndEvent = true;
                            if (btIndex == 0) {
                                handleEndEvent();
                            }
System.out.println("  EB mod: got all END events");
                            return;
                        }
                        else {
                            throw new EmuException("Expecting GO or END event, got " + cType);
                        }
                    }

                    if (btIndex == 0) {
                        controlToOutputAsync(false);
                    }
                }
                catch (InterruptedException e) {
                    // If interrupted, then we must quit
if (debug) System.out.println("  EB mod: interrupted while waiting for go event");
                    emu.setErrorState("EB interrupted waiting for go event");
                    moduleState = CODAState.ERROR;
                    return;
                }

System.out.println("  EB mod: got all GO events");

                long endSequence = -1;

                // Now do the event building
                while (moduleState == CODAState.ACTIVE || paused) {

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
                    gotFirstBuildEvent = false;
                    endEventCount = 0;
                    int endChannel = -1;
                    //int printCounter = 0;

                    // Start the clock on how long it takes to build the next event
                    if (timeStatsOn) startTime = System.nanoTime();

                    // Grab one buildable (non-user/control) bank from each channel.
                    for (int i=0; i < inputChannelCount; i++) {

                        // Loop until we get event which is NOT a user event
                        while (true) {

                            gotBank = false;

                            // Only wait if necessary ...
                            if (availableSequences[i] < nextSequences[i]) {
                                // Can BLOCK here waiting for item if none available, but can be interrupted
                                // Available sequence may be larger than what we asked for.
//System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", wait for event (seq [" + i + "] = " +
//                           nextSequences[i] + ")");
                                availableSequences[i] = buildBarrierIn[i].waitFor(nextSequences[i]);
//System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", available seq[" + i + "]  = " + availableSequences[i]);
                            }

                            // While we have new data to work with ...
                            while (nextSequences[i] <= availableSequences[i]) {
                                buildingBanks[i] = (PayloadBuffer) ringBuffersIn[i].get(nextSequences[i]);
//System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", event order = " + buildingBanks[i].getByteOrder());
                                eventType = buildingBanks[i].getEventType();

                                // Skip over user events. These were actually already placed in
                                // first output channel's first ring by pre-processing thread.
                                if (eventType.isUser())  {
//System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", skip user item " + nextSequences[i]);
                                    nextSequences[i]++;
                                }
                                // Skip over events being built by other build threads
                                else if (skipCounter[i] - 1 > 0)  {
//System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", skip item " + nextSequences[i]);
                                    nextSequences[i]++;
                                    skipCounter[i]--;
                                }
                                // Found a bank, so do something with it (skipCounter[i] - 1 == 0)
                                else {
//System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", accept item " + nextSequences[i]);
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
                            haveEnd = true;
                            endEventCount++;
                            endSequence = nextSequences[i];
                            endChannel = i;
System.out.println("  EB mod: bt" + btIndex + ", found END event from " + buildingBanks[i].getSourceName() + " at seq " + endSequence);

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

                    //--------------------------------------------------------
                    // At this point we have one event from each input channel
                    //--------------------------------------------------------

                    // In general, at this point there will only be 1 build thread
                    // that makes it this far and has at least one END event from
                    // an input channel. The others will be stuck trying to get
                    // an END event from that channel from a slot past where it is
                    // in the ring buffer.

                    // Do END event stuff here
                    if (haveEnd && endEventCount != inputChannelCount) {
                        // If we're here, not all channels got an END event.
                        // See if we can find them in the ring buffers which didn't
                        // have one. If we can, great. If not, major error.
                        // If all our channels have an END, we can end normally
                        // with a warning about the mismatch in number of events.

                        // Put a delay in here because "findEnd" releases ring slots
                        // which get filled with new data being read by input channels -
                        // perhaps before the other build threads have a chance to finish
                        // building their last few events and so they may get corrupted.
                        // This delay gives the other build threads the chance to finish
                        // building what they can.
                        Thread.sleep(500);

                        int finalEndEventCount = findEnd(endChannel, endSequence, endEventCount);

                        // If we still can't find all ENDs, throw exception - major error
                        if (finalEndEventCount != inputChannelCount) {
                            emu.sendRcErrorMessage("Missing " +
                                                   (inputChannelCount - finalEndEventCount) +
                                                   " END events, ending anyway");
                            throw new EmuException("only " + finalEndEventCount + " ENDs for " +
                                                           inputChannelCount + " channels");
                        }
                        else {
                            emu.sendRcErrorMessage("All END events found, but out of order");
System.out.println("  EB mod: bt" + btIndex + " have all ENDs, but differing # of physics events in channels");
                        }

                        // If we're here, we've found all ENDs, continue on with warning ...
                        nonFatalError = true;
                    }

                    // If we have all END events ...
                    if (haveEnd) {
System.out.println("  EB mod: bt#" + btIndex + " found END events on all input channels");
                        haveEndEvent = true;
                        handleEndEvent();
                        return;
                    }

                    // At this point there are only physics or ROC raw events, which do we have?
                    havePhysicsEvents = buildingBanks[0].getEventType().isAnyPhysics();

                    // Check for identical syncs, uniqueness of ROC ids,
                    // identical (physics or ROC raw) event types,
                    // and the same # of events in each bank
                    nonFatalError |= Evio.checkConsistency(buildingBanks, firstEventNumber);
                    isSync = buildingBanks[0].isSync();

                    //--------------------------------------------------------------------
                    // Build trigger bank, number of ROCs given by number of buildingBanks
                    //--------------------------------------------------------------------
                    // The tag will be finally set when this trigger bank is fully created

                    // Get an estimate on the buffer memory needed.
                    // Start with 1K and add roughly the amount of trigger bank data + data wrapper
                    int memSize = 1000 + inputChannelCount * totalNumberEvents * 40;
//System.out.println("  EB mod: estimate trigger bank bytes <= " + memSize);
                    for (PayloadBuffer buildingBank : buildingBanks) {
                        //memSize += buildingBank.getBuffer().capacity();
                        memSize += buildingBank.getNode().getTotalBytes();
//System.out.println("  EB mod: add data bytes from ev, " + buildingBank.getNode().getTotalBytes());
                    }

                    // Grab a stored ByteBuffer
                    ByteBufferItem bufItem = bbSupply.get();
                    bufItem.ensureCapacity(memSize);
//System.out.println("  EB mod: ensure buf has size " + memSize + "\n");
                    ByteBuffer evBuf = bufItem.getBuffer();
                    builder.setBuffer(evBuf);

                    // Create a (top-level) physics event from payload banks
                    // and the combined trigger bank. First create the tag:
                    //   -if I'm a data concentrator or DC, the tag has 4 status bits and the ebId
                    //   -if I'm a primary event builder or PEB, the tag is 0xFF50
                    //   -if I'm a secondary event builder or SEB, the tag is 0xFF70
                    int tag;
                    CODAClass myClass = emu.getCodaClass();
                    switch (myClass) {
                        case SEB:
                            if (isSync) {
                                tag = CODATag.BUILT_BY_SEB_SYNC.getValue();
                            }
                            else {
                                tag = CODATag.BUILT_BY_SEB.getValue();
                            }
                            // output event type
                            eventType = EventType.PHYSICS;
                            break;

                        case PEB:
                            if (isSync) {
                                tag = CODATag.BUILT_BY_PEB_SYNC.getValue();
                            }
                            else {
                                tag = CODATag.BUILT_BY_PEB.getValue();
                            }
                            eventType = EventType.PHYSICS;
                            break;

                        //case DC:
                        default:
                            eventType = EventType.PARTIAL_PHYSICS;
                            // Check input banks for non-fatal errors
                            for (PayloadBuffer pBank : buildingBanks)  {
                                nonFatalError |= pBank.hasNonFatalBuildingError();
                            }

                            tag = Evio.createCodaTag(isSync,
                                                     buildingBanks[0].hasError() || nonFatalError,
                                                     buildingBanks[0].getByteOrder() == ByteOrder.BIG_ENDIAN,
                                                     false, /* don't use single event mode */
                                                     id);
//if (debug) System.out.println("  EB mod: tag = " + tag + ", is sync = " + isSync +
//                   ", has error = " + (buildingBanks[0].hasError() || nonFatalError) +
//                   ", is big endian = " + buildingBanks[0].getByteOrder() == ByteOrder.BIG_ENDIAN +
//                   ", is single mode = " + buildingBanks[0].isSingleEventMode());
                    }

                    // Start top level
                    builder.openBank(tag, totalNumberEvents, DataType.BANK);

//Utilities.printBuffer(builder.getBuffer(), 0, 20, "TOP LEVEL OPEN event");
                    // Reset this to see if creating trigger bank causes an error
                    nonFatalError = false;

                    // If building with Physics events ...
                    if (havePhysicsEvents) {
                        //-----------------------------------------------------------------------------------
                        // The actual number of rocs will replace num in combinedTrigger definition above
                        //-----------------------------------------------------------------------------------
                        // Combine the trigger banks of input events into one (same if single event mode)
//if (debug) System.out.println("  EB mod: create trig bank from built banks, sparsify = " + sparsify);
                        nonFatalError |= Evio.makeTriggerBankFromPhysics(buildingBanks, builder, id,
                                                           runNumber, runTypeId, includeRunData,
                                                           sparsify, checkTimestamps, timestampSlop);
                    }
                    // else if building with ROC raw records ...
                    else {
                        // Combine the trigger banks of input events into one
//if (debug) System.out.println("  EB mod: create trigger bank from Rocs, sparsify = " + sparsify);
                        nonFatalError |= Evio.makeTriggerBankFromRocRaw(buildingBanks, builder,
                                                                        id, firstEventNumber,
                                                                        runNumber, runTypeId,
                                                                        includeRunData, sparsify,
                                                                        checkTimestamps,
                                                                        timestampSlop, btIndex);
                    }

                    // If the trigger bank has an error, go back and reset its tag
                    if (nonFatalError && emu.getCodaClass() == CODAClass.DC) {
                        tag = Evio.createCodaTag(isSync,true,
                                                 buildingBanks[0].getByteOrder() == ByteOrder.BIG_ENDIAN,
                                                 false, /* don't use single event mode */
                                                 id);
                        builder.setTopLevelTag((short)tag);
                    }

                    if (havePhysicsEvents) {
//if (debug) System.out.println("  EB mod: build physics event with physics banks");
                        Evio.buildPhysicsEventWithPhysics(buildingBanks, builder);
                    }
                    else {
//if (debug) System.out.println("  EB mod: build physics event with ROC raw banks");
                        Evio.buildPhysicsEventWithRocRaw(buildingBanks, builder);
                    }

                    // Done creating event
                    builder.closeAll();

                    //-------------------------
                    // Stats
                    //-------------------------

                    // Only have the first build thread keep time-to-build stats
                    // so we don't have to worry about multithreading issues.
                    if (timeStatsOn && btIndex == 0) {
                        // Total time in nanoseconds spent building this event.
                        // NOTE: nanoTime() is very expensive and will slow EB (by 50%)
                        // Work on creating a time histogram.
                        statistics.addValue((int) (System.nanoTime() - startTime));
                    }

                    //-------------------------

                    // Which output channel do we use?
                    if (outputChannelCount > 1) {
                        // If we're a DC with multiple SEBs ...
                        if (chunkingForSebs) {
                            evGroupIndex = evIndex/sebChunk;
                            outputChannelIndex = (int) (evGroupIndex % outputChannelCount);
                        }
                        // Otherwise round-robin between all output channels
                        else {
                            outputChannelIndex = (outputChannelIndex + 1) % outputChannelCount;
                        }
                    }
                    evIndex += btCount;

                    // Put event in the correct output channel.
                    // Important to use builder.getBuffer() method instead of evBuf directly.
                    // That's because in the method, the limit and position are set
                    // properly for reading.
                    eventToOutputRing(btIndex, outputChannelIndex, builder.getBuffer(),
                                      eventType, bufItem, bbSupply);

                    // If sync bit being set ...
                    if (isSync) {
                        eventNumberAtLastSync = firstEventNumber + totalNumberEvents;
                    }

                    // Each build thread must release the "slots" in the input channel
                    // ring buffers of the components it uses to build the physics event.
                    // This releases them up for the postBuild thread to free
                    // up all the resources used in building in proper order.
                    // PostBuild thread is only run if more than 1 build thread.
                    // If only one, do release of resources here.
                    for (int i=0; i < inputChannelCount; i++) {
                        if (btCount == 1) {
                            buildingBanks[i].releaseByteBuffer();
                        }
                        buildSequences[i].set(nextSequences[i]++);
                    }

                    // Stats (need to be thread-safe)
                    eventCountTotal += totalNumberEvents;
                    wordCountTotal  += builder.getTotalBytes() / 4 + 1;
                    keepStats(builder.getTotalBytes());
                }
            }
            catch (InterruptedException e) {
System.out.println("  EB mod: INTERRUPTED build thread " + Thread.currentThread().getName());
                return;
            }
            catch (final TimeoutException e) {
System.out.println("  EB mod: timeout in ring buffer");
                emu.setErrorState("EB timeout in ring buffer");
                moduleState = CODAState.ERROR;
                return;
            }
            catch (final AlertException e) {
System.out.println("  EB mod: alert in ring buffer");
                emu.setErrorState("EB alert in ring buffer");
                moduleState = CODAState.ERROR;
                return;
            }
            catch (Exception e) {
System.out.println("  EB mod: MAJOR ERROR building event: " + e.getMessage());
                emu.setErrorState("EB MAJOR ERROR building event: " + e.getMessage());
                moduleState = CODAState.ERROR;
                return;
            }
            finally {
                PayloadBuffer buildingBank;
                long cursor, lastCursor;

                // If we're exiting due to an error, make sure all the input channels
                // are drained. This makes ROC recovery much easier.

                // Grab banks from each channel
                for (int i=0; i < inputChannelCount; i++) {

                    cursor = buildBarrierIn[i].getCursor();

                    while (true) {
                        try {
                            availableSequences[i] = buildBarrierIn[i].waitFor(cursor);
                        }
                        catch (Exception e) {}

                        // While we have data to read ...
                        while (nextSequences[i] <= availableSequences[i]) {
                            buildingBank = (PayloadBuffer) ringBuffersIn[i].get(nextSequences[i]);
//System.out.println("  EB mod: clean inputs, releasing seq " + nextSequences[i] + " from channel #" + i);
                            if (btCount == 1) {
                                buildingBank.releaseByteBuffer();
                            }
                            nextSequences[i]++;
                        }
                        buildSequences[i].set(availableSequences[i]);

                        lastCursor = cursor;
                        cursor = buildBarrierIn[i].getCursor();

                        if (cursor == lastCursor) {
                            break;
                        }
                    }
                }
            }

if (debug) System.out.println("  EB mod: Building thread is ending");
        }

    }


    /**
     * This class is a garbage-freeing thread which takes the ByteBuffers and
     * banks used to build an event and frees up their ring-based resources.
     * It takes the burden of doing this off of the build threads and allows
     * them to build without bothering to synchronize between themselves.
     */
    final class ReleaseRingResourceThread extends Thread {

        /** The total number of build threads. */
        private final int btCount = buildingThreadCount;

        /** Time to quit thread. */
        private volatile boolean quit;

        /** Which input channel are we associated with? */
        private final int order;


        /**
         * Constructor.
         *
         * @param group   thread group.
         * @param name    thread name.
         * @param order   input channel index (starting at 0).
         */
        ReleaseRingResourceThread(ThreadGroup group, String name, int order) {
            super(group, name);
            this.order = order;
        }


        /**
         * Stop this freeing-resource thread.
         * @param force if true, call Thread.stop().
         */
        void killThread(boolean force) {
            quit = true;
            if (force) {
                this.stop();
            }
        }


        public void run() {

            // Ring Buffer stuff
            long nextSequence = 0L;
            long availableSequence;
            RingItem ri;
            Sequence sequence =  postBuildSequence[order];
            SequenceBarrier barrier = postBuildBarrier[order];
            RingBuffer<RingItem> ringBufferIn = ringBuffersIn[order];


            try {
                while (true) {
                    // Available sequence may be larger than what we desired
                    availableSequence = barrier.waitFor(nextSequence);

                    // While we have new data to work with ...
                    while (nextSequence <= availableSequence) {
                        ri = ringBufferIn.get(nextSequence);
                        nextSequence++;

                        // Skip over non-built events since control
                        // events do not use a supply buffer for their data.
                        // User events may use the input channel supply
                        // buffers, but they're released by the output channel.
                        if (!ri.getEventType().isBuildable()) {
                            continue;
                        }

                        // Free claim on ByteBuffer
                        ri.releaseByteBuffer();
                    }

                    // Free RingItem(s) in input channel's ring for more input data
                    sequence.set(availableSequence);

                    if (quit) return;
                }
            }
            catch (AlertException e)       { /* won't happen */ }
            catch (InterruptedException e) { /* won't happen */ }
            catch (TimeoutException e)     { /* won't happen */ }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }



    /**
     * If some output channels are blocked on reading from this module
     * because the END event arrived on an unexpected ring
     * (possible if module has more than one event-producing thread
     * AND there is more than one output channel),
     * this method interrupts and allows the channels to read the
     * END event from the proper ring.
     */
    private void processEndInOutput(long evIndex, int ringIndex) {
        if (buildingThreadCount < 2 || outputChannelCount < 2) {
            return;
        }

        for (int i=0; i < outputChannelCount; i++) {
System.out.println("\n  EB mod: calling processEnd() for chan " + i + '\n');
            outputChannels.get(i).processEnd(evIndex, ringIndex);
        }
    }


    /**
     * End all EB threads because an END cmd/event or RESET cmd came through.
     *
     * @param end if <code>true</code> called from end(), else called from reset()
     */
    private void endThreads(boolean end) {
        // Check if END event has arrived and if all the input ring buffers
        // are empty, if not, wait up to endingTimeLimit (30) seconds.
        if (end) {
            long startTime = System.currentTimeMillis();

            // Wait up to endingTimeLimit millisec for events to
            // be processed & END event to arrive, then proceed
            while (!haveEndEvent &&
                   (System.currentTimeMillis() - startTime < endingTimeLimit)) {
                try {Thread.sleep(200);}
                catch (InterruptedException e) {}
            }

            if (!haveEndEvent) {
System.out.println("  EB mod: endBuildThreads: will end building/filling threads but no END event or rings not empty");
                moduleState = CODAState.ERROR;
                emu.setErrorState("EB will end building/filling threads but no END event or rings not empty");
            }
        }

        // Kill the rate calculating thread
        if (RateCalculator != null) {
            RateCalculator.interrupt();
            try {
                RateCalculator.join(250);
//                if (RateCalculator.isAlive()) {
//                    RateCalculator.stop();
//                }
            }
            catch (InterruptedException e) {}
            RateCalculator = null;
        }

        // NOTE: EMU has a command executing thread which calls this EB module's execute
        // method which, in turn, calls this method when an END cmd is sent. In this case
        // all build threads will be interrupted in the following code.

        // Interrupt all Building threads except the one calling this method
        // (is only ever called by cmsg callback in emu, never by build thread)
        for (Thread thd : buildingThreadList) {
            // Try to end thread nicely but it could hang on rb.next(), if so, kill it
            thd.interrupt();
            try {
                thd.join(250);
//                if (thd.isAlive()) {
//                    thd.stop();
//                }
            }
            catch (InterruptedException e) {}
        }
        buildingThreadList.clear();

        // Interrupt all PreProcessor threads
        if (preProcessors != null) {
            for (Thread qf : preProcessors) {
                qf.interrupt();
                try {
                    qf.join(250);
//                    if (qf.isAlive()) {
//                        qf.stop();
//                    }
                }
                catch (InterruptedException e) {}
            }
        }
        preProcessors = null;

        // Interrupt release threads too
        if (buildingThreadCount > 1 && releaseThreads != null) {
            for (ReleaseRingResourceThread rt : releaseThreads) {
                // If ending, try gradual approach
                if (end) {
                    rt.interrupt();
                    try {
                        rt.join(250);
//                        if (rt.isAlive()) {
//                            rt.stop();
//                        }
                    }
                    catch (InterruptedException e) {
                    }
                }
                else {
                    // If resetting, immediately force thread to stop
                    rt.killThread(true);
                }
            }
        }
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

        if (!dumpData) {
            // Build threads
            buildingThreadList.clear();
            for (int i = 0; i < buildingThreadCount; i++) {
                BuildingThread thd1 = new BuildingThread(i, emu.getThreadGroup(), name + ":builder" + i);
                buildingThreadList.add(thd1);
                thd1.start();
            }

            if (buildingThreadCount > 1) {
                releaseThreads = new ReleaseRingResourceThread[inChanCount];
                for (int j=0; j < inChanCount; j++) {
                    releaseThreads[j] = new ReleaseRingResourceThread(emu.getThreadGroup(),
                                                                      name + ":release"+j, j);
                    releaseThreads[j].start();
                }
            }
        }
    }


    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        CODAStateIF previousState = moduleState;
        moduleState = CODAState.CONFIGURED;

        // EB threads must be immediately ended
        endThreads(false);

        paused = false;

        if (previousState.equals(CODAState.ACTIVE)) {
            try {
                // Set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
            }
            catch (DataNotFoundException e) {}
        }
    }


    /** {@inheritDoc} */
    public void end() {
System.out.println("  EB mod: end(), set state tp DOWNLOADED");
        moduleState = CODAState.DOWNLOADED;

        // Print out time-to-build-event histogram
        if (timeStatsOn) {
System.out.println("  EB mod: end(), print histogram");
            statistics.printBuildTimeHistogram("Time to build one event:", "nsec");
        }

        // Build & pre-processing threads should already be ended by END event
System.out.println("  EB mod: end(), call endThreads(true)");
        endThreads(true);

        paused = false;

        try {
            // Set end-of-run time in local XML config / debug GUI
System.out.println("  EB mod: end(), set internal ending time parameter");
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
System.out.println("  EB mod: end(), done");
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {

        // Event builder needs input
        if (inputChannelCount < 1) {
            moduleState = CODAState.ERROR;
System.out.println("  EB mod: prestart, no input channels to EB");
            emu.setErrorState("no input channels to EB");
            throw new CmdExecException("no input channels to EB");
        }

        // Make sure each input channel is associated with a unique rocId
        for (int i=0; i < inputChannelCount; i++) {
            for (int j=i+1; j < inputChannelCount; j++) {
                if (inputChannels.get(i).getID() == inputChannels.get(j).getID()) {
                    moduleState = CODAState.ERROR;
System.out.println("  EB mod: prestart, input channels have duplicate rocIDs");
                    emu.setErrorState("input channels have duplicate rocIDs");
                    throw new CmdExecException("input channels have duplicate rocIDs");
                }
            }
        }

        moduleState = CODAState.PAUSED;
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

        // One post-build sequence and barrier for each input channel
        postBuildSequence = new Sequence[inputChannelCount];
        postBuildBarrier  = new SequenceBarrier[inputChannelCount];

        // Place to put ring level stats
        inputChanLevels  = new int[inputChannelCount];
        outputChanLevels = new int[outputChannelCount];

        // Collect channel names for easy gathering of stats
        int indx=0;
        inputChanNames = new String[inputChannelCount];
        for (DataChannel ch : inputChannels) {
            inputChanNames[indx++] = ch.name();
        }
        indx = 0;
        outputChanNames  = new String[outputChannelCount];
        for (DataChannel ch : outputChannels) {
            outputChanNames[indx++] = ch.name();
        }

        // Have ring sizes handy for calculations
        ringBufferSize = new int[inputChannelCount];

        // For each channel ...
        for (int i=0; i < inputChannelCount; i++) {
            // Get channel's ring buffer
            RingBuffer<RingItem> rb = inputChannels.get(i).getRingBufferIn();
            ringBuffersIn[i]  = rb;
            ringBufferSize[i] = rb.getBufferSize();

            // First sequence & barrier is for checking of PayloadBuffer/Bank
            // data before the actual building takes place.
            preBuildBarrier[i]  = rb.newBarrier();
            preBuildSequence[i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

            if (dumpData) {
                rb.addGatingSequences(preBuildSequence[i]);
            }
            else {
                // Repackage the build thread sequences to use as barrier for post build thread.
                // Each input channel ring buffer needs the sequences from each build thread for
                // that channel.
                Sequence[] buildThdSequencesForChannel = new Sequence[buildingThreadCount];

                // For each build thread ...
                for (int j = 0; j < buildingThreadCount; j++) {
                    // We have 1 sequence for each build thread & input channel combination
                    buildSequenceIn[j][i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

                    if (buildingThreadCount > 1) {
                        // Store for later use in creating post-processing barrier
                        buildThdSequencesForChannel[j] = buildSequenceIn[j][i];
                    }
                    else {
                        // This sequence may be the last consumer before producer comes along
                        rb.addGatingSequences(buildSequenceIn[j][i]);
                    }
                }

                // We have 1 barrier for each channel (shared by building threads)
                // which depends on (comes after) the pre-processing sequence.
                buildBarrierIn[i] = rb.newBarrier(preBuildSequence[i]);

                if (buildingThreadCount > 1) {
                    // Last barrier is for releasing resources used in the building
                    // and it depends on each build thread sequence associated with
                    // a single channel.
                    postBuildBarrier[i] = rb.newBarrier(buildThdSequencesForChannel);

                    // Last sequence is for thread releasing resources used in the building
                    postBuildSequence[i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

                    // This post-processing sequence is the last consumer before producer comes along
                    rb.addGatingSequences(postBuildSequence[i]);
                }
            }
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
        haveEndEvent = false;
        haveAllPrestartEvents = false;

        // Print thread names
//        int thdCount = Thread.activeCount();
//        Thread[] thds = new Thread[thdCount];
//        Thread.enumerate(thds);
//        System.out.println("Running thd count = " + thdCount + " :");
//        for (Thread t : thds) {
//            System.out.println(t.getName());
//        }
        startThreads();

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}
    }


    /**
     * {@inheritDoc}
     * @throws CmdExecException if not all prestart events were received
     */
    public void go() throws CmdExecException {
        if (!haveAllPrestartEvents && !dumpData) {
System.out.println("  EB mod: go, have not received all prestart events");
            throw new CmdExecException("have not received all prestart events");
        }

        moduleState = CODAState.ACTIVE;
        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }


 }