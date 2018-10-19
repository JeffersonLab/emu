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
 *                        | Gate (producer cannot go beyond this point)
 *                        |
 *                      __|__
 *                     /  |  \
 *                    /1 _|_ 2\  <---- Build Threads 1-M
 *                   |__/   \__|        |
 *                   |6 |   | 3|        |
 *             ^     |__|___|__|        |
 *             |      \ 5 | 4 /         |
 *         Producer->  \__|__/          V
 *                        |
 *                        |
 *                        | Barrier (at last sequence produced)
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

    /** Threads (one for each input channel) for
     *  releasing resources used to build events. */
    private ReleaseRingResourceThread releaseThreads[];

    private boolean useReleaseThread = false;
    private boolean useGarbageFree;

    /** The number of the experimental run. */
    private int runNumber;

    /** The number of the experimental run's configuration. */
    private int runTypeId;

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



//    /** One pre-processing sequence for each input channel. */
//    private Sequence[] preBuildSequence;
//
//    /** One pre-processing barrier for each input channel. */
//    private SequenceBarrier[] preBuildBarrier;



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

        outputOrder = ByteOrder.LITTLE_ENDIAN;
logger.info("  EB mod: output byte order = little endian");

logger.info("  EB mod: # of event building threads = " + buildingThreadCount);

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


        // set "garbage free" option on
        useGarbageFree = false;
        str = attributeMap.get("garbageFree");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                useGarbageFree = true;
            }
        }


        // set "garbage free" option on
        useReleaseThread = false;
        str = attributeMap.get("releaseThd");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                useReleaseThread = true;
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
//dumpData = true;

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
        int ringCount = 256;
        // If there are multiple build threads, reduce the # of items per thread.
        if (buildingThreadCount == 3) {
            ringCount = 64;
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
        long nextRingItem = rb.nextIntr(1);
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
            // Release ring slot. Each build thread has its own sequences array
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

        // We have GO or PRESTART?
        ControlType controlType = isPrestart ? ControlType.PRESTART : ControlType.GO;

        // Space for 1 control event per channel
        PayloadBuffer[] controlBufs = new PayloadBuffer[outputChannelCount];

        // Create a new control event with updated control data in it
        controlBufs[0] = Evio.createControlBuffer(controlType,
                                                  runNumber, runTypeId,
                                                  (int)eventCountTotal,
                                                  0, outputOrder, false);

        // For the other output channels, duplicate first with separate position & limit.
        // Important to do this duplication BEFORE sending to output channels or position
        // and limit can be copied while changing.
        for (int i=1; i < outputChannelCount; i++) {
            controlBufs[i] =  new PayloadBuffer(controlBufs[0]);
        }

        // Write event to output channels
        for (int i=0; i < outputChannelCount; i++) {
            eventToOutputChannel(controlBufs[i], i, 0);
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

        /**
         * This is true if the roc data buffers have a backing byte array,
         * this object's ByteBufferSupply has buffers with a backing byte array,
         * AND they both have the same endian value.
         * This allows for a quick copy of data from one buffer to the other.
         * Useful for efficiency in creating trigger bank.
         */
        private boolean fastCopyReady;

        // Stuff needed to direct built events to proper output channel(s)

        /** Number (index) of the current, sequential-between-all-built-thds,
         * built event produced from this EMU.
         * 1st build thread starts at 0, 2nd starts at 1, etc.
         * 1st build thread's 2nd event is btCount, 2nd thread's 2nd event is btCount + 1, etc.*/
        private long evIndex = 0;

        /** Which channel does this thread currently output to, starting at btIndex?
         * channel = (prev_channel + btCount) % outputChannelCount.*/
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
         * @param buildingBanks all banks holding END events, one for each input channel
         */
        private void handleEndEvent(PayloadBuffer[] buildingBanks) {

System.out.println("  EB mod: in handleEndEvent(), bt #" + btIndex + ", output chan count = " +
                           outputChannelCount);

            PayloadBuffer[] endBufs;

            if (outputChannelCount > 0) {
                // Tricky stuff:
                // Handing a buffer with the END event off to an output channel,
                // one needs to be aware that its limit will most likely change when
                // being written by that channel. Thus, when copying the END event,
                // do that FIRST, BEFORE these things are being written by their output channels.
                // Been burned by this.

                // Create END event(s)
                endBufs = new PayloadBuffer[outputChannelCount];

                // For the first output channel
                endBufs[0] = Evio.createControlBuffer(ControlType.END, runNumber, runTypeId,
                                                      (int) eventCountTotal, 0, outputOrder, false);

                // For the other output channel(s), duplicate first with separate position & limit
                for (int i=1; i < outputChannelCount; i++) {
                    endBufs[i] =  new PayloadBuffer(endBufs[0]);
                }

                // END needs to be sent over each channel.
                // The question is, which ring will each channel be waiting to read from?
                // The PRESTART and GO events are always sent to ring 0; however,
                // now that physics events are being read by each channel, each
                // channel calculates which ring to read from. This depends
                // on the number of buildthreads (BTs). It's much easier to send
                // the END to the ring each channel is expecting to read a physics
                // event from than it is to send it to ring 0 and try interrupt
                // the reader, etc.
                // Each BT writes to the same numbered ring in each
                // channel since we carefully created one ring/BT in each channel.

                // The channel due to receive the next physics event is ...
                outputChannelIndex = (int) (evIndex % outputChannelCount);

System.out.println("  EB mod: try sending END event to output channel " + outputChannelIndex +
               ", ring " + btIndex + ", ev# = " + evIndex);
                // Send END event to first output channel
                try {
                    eventToOutputChannel(endBufs[0], outputChannelIndex, btIndex);
                }
                catch (InterruptedException e) {
                    return;
                }
                System.out.println("  EB mod: sent END event to output channel  " + outputChannelIndex);

                // Give the other build threads (BTs) time to finish writing their last event
                // since we'll be writing END to rings this BT does not normally write to.
                // Wait up to 2 seconds for each BT before printing a warning.
                // That should be plenty of time.
                if (outputChannelCount > 1) {
                    // One clean up thread / input channel ...
                    for (int i=0; i < inputChannelCount; i++) {
                        // 2 sec
                        int timeLeft = 2000;
                        long ev;

                        // The last event to be cleaned up for this input chan
                        if (buildingThreadCount > 1) {
                            if (useReleaseThread) {
                                // For multiple build threads, there's an additional
                                // cleanup or release thread which must process the last event.
                                ev = releaseThreads[i].getLastSequence();
                            }
                            else {
                                // For multiple build threads, each thread may be at a different sequence.
                                // Pick the minimum.
                                long seq;
                                ev = Long.MAX_VALUE;
                                for (int j = 0; j < buildingThreadCount; j++) {
                                    seq = buildSequenceIn[j][i].get();
                                    ev = Math.min(seq, ev);
                                }
                            }
                        }
                        else {
                            ev = buildSequenceIn[0][i].get();
                        }

                        // If it's not to the one before END, it must still be writing
                        while (ev < evIndex - 1 && timeLeft > 0) {
                            try {Thread.sleep(200);}
                            catch (InterruptedException e) {}

                            if (buildingThreadCount > 1) {
                                if (useReleaseThread) {
                                    ev = releaseThreads[i].getLastSequence();
                                }
                                else {
                                    long seq;
                                    ev = Long.MAX_VALUE;
                                    for (int j = 0; j < buildingThreadCount; j++) {
                                        seq = buildSequenceIn[j][i].get();
                                        ev = Math.min(seq, ev);
                                    }
                                }
                            }
                            else {
                                ev = buildSequenceIn[0][i].get();
                            }

                            timeLeft -= 200;
                        }

                        // If it still isn't done, hope for the best ...
                        if (ev < evIndex - 1) {
                            System.out.println("  EB mod: WARNING, might have a problem writing END event");
                        }
                    }
                }

                // Now send END to the other channels. Do this by going forward.
                // Physics events are sent to output channels round-robin and are
                // processed by build threads round-robin. So ...
                // go to the next channel we would normally send a physics event on,
                // calculate which ring & channel, and write END to it.
                // Then continue by going to the next channel until all channels are done.
                for (int i=1; i < outputChannelCount; i++) {

                    // Next channel to be sent a physics event
                    int nextChannel = (int) ((evIndex + i) % outputChannelCount);

                    // Next build thread to write (and therefore ring to receive) a physics event
                    int nextBtIndex = (int) ((evIndex + i) % btCount);

                    // One issue here is that each build thread only writes to a single
                    // ring in an output channel. This allows us not to use locks when writing.
                    // NOW, however, we're using this one build thread
                    // to write the END event to all channels and other rings.
                    // We must make sure that the last physics event has been written already
                    // or there may be conflict when writing the END.

                    // Already waited for other build threads to finish before
                    // writing END to first channel above, so now we can go ahead
                    // and write END to other channels without waiting.
System.out.println("  EB mod: try sending END event to output channel " + nextChannel +
                   ", ring " + nextBtIndex + ", ev# = " + evIndex);
                    // Send END event to first output channel
                    try {
                        eventToOutputChannel(endBufs[i] , nextChannel, nextBtIndex);
                    }
                    catch (InterruptedException e) {
                        return;
                    }
                    System.out.println("  EB mod: sent END event to output channel  " + nextChannel);
                }

                // Stats
                eventCountTotal++;
                wordCountTotal += 5;
            }

            for (int i=0; i < inputChannelCount; i++) {
                // Release input channel's ring's slot
                buildSequences[i].set(nextSequences[i]++);
                // Release byte buffer from a supply holding END event.
                // If more than 1 BT, release is done by ReleaseRingResourceThread
                //if (btCount == 1 && buildingBanks != null) {
                if (buildingBanks != null) {
                    buildingBanks[i].releaseByteBuffer();
                }
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
                        if (ringBuffersIn[ch].getCursor() < veryNextSequence) {
                        //if (!ringBuffersIn[ch].isPublished(veryNextSequence)) {
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

                            // Release buffer - done once
                            pBuf.releaseByteBuffer();

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
            if (useGarbageFree) {
                runNoBuilder();
            }
            else {
                runWithBuilder();
            }
        }


        private void runWithBuilder() {

            try {
                // Create a reusable supply of ByteBuffer objects
                // for writing built physics events into.
                //--------------------------------------------
                // Direct buffers give better performance ??
                //--------------------------------------------
                // If there's only one output channel, release should be sequential
                boolean releaseSequentially = true;
                if (outputChannelCount > 1)  {
                    releaseSequentially = false;
                }
                boolean useDirectBB = false;
                ByteBufferSupply bbSupply = new ByteBufferSupply(ringItemCount, 2000, outputOrder,
                                                                 useDirectBB, releaseSequentially);
System.out.println("  EB mod: bbSupply -> " + ringItemCount + " # of bufs, direct = " + false +
                   ", seq = " + releaseSequentially);

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
                int     tag, endEventCount, entangledEventCount=0, entangledEventCountNew;
                long    firstEventNumber=1, startTime=0L;
                boolean haveEnd, havePhysicsEvents;
                boolean isSync, nonFatalError;
                boolean gotBank, gotFirstBuildEvent, recordIdError;
                boolean isEventNumberInitiallySet = false, generalInitDone = false;
                EventType eventType = null;
                EvioNode  inputNode = null;

                // Allocate arrays once here so building method does not have to
                // allocate once per built event. Size is set later when the # of
                // entangled events (block level) is known.
                long[]  longData    = null; // allocated later
                short[] evData      = null;
                int[]   segmentData = new int[20];  // currently only use 3 ints
                int[]   returnLen   = new int[1];

                // Some of the above arrays need to be cleared for each event being built.
                // Copy the contents (all 0's) of the following arrays for efficient clearing.
                long[]  longDataZero = null;  // allocated later

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
//TODO: can pass in buildingBanks and save some mem allocation/garbage
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
                                handleEndEvent(null);
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
                    top:
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
                                inputNode = buildingBanks[i].getNode();
                                eventType = buildingBanks[i].getEventType();
                                recordIdError = false;
//System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", event order = " + buildingBanks[i].getByteOrder());

                                // Check payload buffer for event & evio structure types,
                                // and also record id mismatches (which is best done in a single
                                // thread as each buffer is compared to the last one).
                                //if (!dumpData && btIndex == 0) {
                                if (btIndex == 0) {
                                    recordIdError = Evio.checkInputType(buildingBanks[i].getRecordId(),
                                                                        inputChannels.get(i),
                                                                        eventType, inputNode);
                                }

                                // Deal with user event
                                if (eventType.isUser()) {
System.out.println("  EB mod: got user event");

                                    // User events are placed in first output channel's first ring.
                                     // Only the first build thread will deal with them.
                                     if (btIndex == 0) {
//System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", skip user item " + nextSequences[i]);
                                         //System.out.println("  EB mod: got user event in pre-processing order = " + pBuf.getByteOrder());

                                         // Check payload buffer for source id.
                                         // Store sync and error info in payload buffer.
                                         //if (!dumpData)
                                         Evio.checkInput(buildingBanks[i], inputChannels.get(i),
                                                         eventType, inputNode, recordIdError);

                                         // Swap headers, NOT DATA, if necessary
                                         if (outputOrder != buildingBanks[i].getByteOrder()) {
                                             try {
                                                 //System.out.println("  EB mod: swap user event (not data)");
                                                 ByteBuffer buffy = buildingBanks[i].getBuffer();
                                                 if (buffy != null) {
                                                     // Takes care of swapping of event in its own separate buffer,
                                                     // headers not data
                                                     ByteDataTransformer.swapEvent(buffy, buffy, 0, 0, false, null);
                                                 }
                                                 else if (inputNode != null) {
                                                     // This node may share a backing buffer with other, ROC Raw, events.
                                                     // Thus we cannot change the order of the entire backing buffer.
                                                     // For simplicity, let's copy it and swap it in its very
                                                     // own buffer.

                                                     // Copy
                                                     buffy = inputNode.getStructureBuffer(true);
                                                     // Swap headers but not data
                                                     ByteDataTransformer.swapEvent(buffy, null, 0, 0, false, null);
                                                     // Store in ringItem
                                                     buildingBanks[i].setBuffer(buffy);
                                                     buildingBanks[i].setNode(null);
                                                     // Release claim on backing buffer since we are now
                                                     // using a different buffer.
                                                     buildingBanks[i].releaseByteBuffer();
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
                                         // the post-build threads can skip over them.
                                         eventToOutputChannel(buildingBanks[i], 0, 0);
                                         //System.out.println("  EB mod: sent user event to output channel");
                                     }

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
                                    // Check payload buffer for source id.
                                    // Store sync and error info in payload buffer.
                                    //if (!dumpData)
                                    Evio.checkInput(buildingBanks[i], inputChannels.get(i),
                                                    eventType, inputNode, recordIdError);
                                    gotBank = true;
                                    break;
                                }
                            }

                            if (!gotBank) {
                                continue;
                            }

//                            if (dumpData) {
//                                // The ByteBufferSupply takes care of releasing buffers in proper order.
//                                buildingBanks[i].releaseByteBuffer();
//                                // Each build thread must release the "slots" in the input channel
//                                // ring buffers of the components it uses to build the physics event.
//                                buildSequences[i].set(nextSequences[i]++);
//
//                                skipCounter[i] = btCount;
//                                break;
//                            }

                            // If event needs to be built ...
                            if (!eventType.isControl()) {
                                // Find the # of entangled events or "block level".
                                // This is the number of entangled events in one
                                // ROC raw record which is set by the trigger supervisor.
                                // This may change immediately after each sync event.
                                // It is important because if there are multiple BuildingThreads, then
                                // each thread must set its own blockLevel.
                                entangledEventCountNew = inputNode.getNum();

                                // If there are changes, adjust.
                                if (entangledEventCountNew != entangledEventCount) {

                                    // If the block level really changed (instead of from its
                                    // initial value of 0) and we have multiple build threads,
                                    // throw an exception cause this will mess things up.
                                    // We don't have any synchronization between build threads
                                    // for this situation (yet).
                                    if (entangledEventCount != 0 && btCount > 1) {
                                        throw new EmuException("changing block level (" + entangledEventCount +
                                                               " to " + entangledEventCountNew +
                                                               ") not permitted with multiple build threads");
                                    }

                                    // Get convenience arrays ready for efficient trigger bank building
                                    longData     = new long[entangledEventCountNew + 2];
                                    longDataZero = new long[entangledEventCountNew + 2];
                                    evData       = new short[entangledEventCountNew];
                                    // Store the # of entangled events
//                                    System.out.println("SWITCHING BLOCK LEVEL from " + entangledEventCount + " to " + entangledEventCountNew +
//                                                               ", gotFirstBuildEvent = " + gotFirstBuildEvent + ", firstEv# = " + firstEventNumber +
//                                                               ", bt# = " + btIndex);
                                    entangledEventCount = entangledEventCountNew;
                                }

                                // One-time init stuff for a group of
                                // records that will be built together.
                                if (!gotFirstBuildEvent) {
                                    // Set flag
                                    gotFirstBuildEvent = true;
//long oldFirstEventNumber = firstEventNumber;
                                    // This sets the first event number for DCs and PEBs only!
                                    // For SEBs it depends on which data stream it's a part of
                                    // so more work needs to be done to calculate it correctly.
                                    if (!isEventNumberInitiallySet)  {
                                        firstEventNumber = 1L + (btIndex * entangledEventCount);
                                        isEventNumberInitiallySet = true;
                                    }
                                    else {
                                        firstEventNumber += btCount*entangledEventCount;
                                    }

                                    // Do this once per build thread on first buildable event
                                    if (!generalInitDone) {
                                        // Find out if the event's buffer has a backing byte array,
                                        // if this object's ByteBufferSupply has buffers with backing byte arrays,
                                        // and if the 2 buffers have same endianness.
                                        // We do this so that when constructing the trigger bank, we can do
                                        // efficient copying of data from ROC to trigger bank if possible.
                                        try {
                                            ByteBuffer backingBuf = buildingBanks[i].getNode().getBufferNode().getBuffer();
                                            if (backingBuf.hasArray() && !useDirectBB &&
                                                backingBuf.order() == outputOrder) {
                                                fastCopyReady = true;
System.out.println("\nEFFICIENT copying is possible!!!\n");
                                            }
                                            else {
                                                System.out.println("\nEFFICIENT copying is NOT possible:\n" +
                                                "     backingBuf.hasArray = " + backingBuf.hasArray() +
                                                "\n     supplyBuf is direct = " + useDirectBB +
                                                "\n     backingBuf end = " + backingBuf.order() +
                                                "\n     outputORder = " + outputOrder);
                                            }
                                        }
                                        catch (Exception e) {
                                            fastCopyReady = false;
System.out.println("\nEFFICIENT copying is NOT possible\n");
                                        }
                                        generalInitDone = true;
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

                        // repeat for loop endlessly
//                        if (dumpData && (i == (inputChannelCount - 1))) i = -1;
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
                        handleEndEvent(buildingBanks);
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
                    int memSize = 1000 + inputChannelCount * entangledEventCount * 40;
//System.out.println("  EB mod: estimate trigger bank bytes <= " + memSize);
                    for (int i=0; i < inputChannelCount; i++) {
                        memSize += buildingBanks[i].getNode().getTotalBytes();
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
                    builder.openBank(tag, entangledEventCount, DataType.BANK);

                    // Reset this to see if creating trigger bank causes an error
                    nonFatalError = false;

                    // If building with Physics events ...
                    if (havePhysicsEvents) {
                        //-----------------------------------------------------------------------------------
                        // The actual number of rocs will replace num in combinedTrigger definition above
                        //-----------------------------------------------------------------------------------
                        // Combine the trigger banks of input events into one (same if single event mode)
//if (debug) System.out.println("  EB mod: create trig bank from built banks, sparsify = " + sparsify);
                        nonFatalError |= Evio.makeTriggerBankFromPhysics(buildingBanks,
                                                                         builder, id,
                                                                         runNumber, runTypeId,
                                                                         includeRunData,
                                                                         sparsify,
                                                                         checkTimestamps,
                                                                         timestampSlop);

                    }
                    // else if building with ROC raw records ...
                    else {
                        // Combine the trigger banks of input events into one
//if (debug) System.out.println("  EB mod: create trigger bank from Rocs, sparsify = " + sparsify);
                        // Clear arrays for reuse
                        System.arraycopy(   longDataZero, 0,    longData, 0, entangledEventCount + 2);
                        returnLen[0] = 0;

                        nonFatalError |= Evio.makeTriggerBankFromRocRaw(buildingBanks, builder,
                                                                        id, firstEventNumber,
                                                                        runNumber, runTypeId,
                                                                        includeRunData, sparsify,
                                                                        checkTimestamps,
                                                                        timestampSlop, btIndex,
                                                                        longData, evData,
                                                                        segmentData, returnLen);
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

//                    // Done creating event
                    builder.closeAll();

//                    if (isSync) {
//                        ByteBuffer buf = builder.getBuffer();
//                        int i1 = buf.getInt(4*5);
//                        int i2 = buf.getInt(4*6);
//                        System.out.println("first ev, word 1 = 0x" + Integer.toHexString(i1) +
//                                           ", word 2 = 0x" + Integer.toHexString(i2));
//
//                        Utilities.printBufferBytes(buf, 0, 32, "First 8 built words" );
//                        System.out.println();
//                    }

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

                    // Which output channel do we use?  Round-robin.
                    if (outputChannelCount > 1) {
                        outputChannelIndex = (int) (evIndex % outputChannelCount);
                    }

                    // Put event in the correct output channel.
                    // Important to use builder.getBuffer() method instead of evBuf directly.
                    // That's because in the method, the limit and position are set
                    // properly for reading.
//                    for (int k=0; k < btIndex; k++) {
//                        System.out.print("  ");
//                    }
//System.out.println("  EB mod: bt#" + btIndex + " write event " + evIndex + " on ch" + outputChannelIndex + ", ring " + btIndex);
                    eventToOutputRing(btIndex, outputChannelIndex, builder.getBuffer(),
                                      eventType, bufItem, bbSupply);

                    evIndex += btCount;

                    for (int i=0; i < inputChannelCount; i++) {
                        if (useReleaseThread) {
                            if (btCount == 1) {
                                buildingBanks[i].releaseByteBuffer();
                            }
                        }
                        else {
                            // The ByteBufferSupply takes care of releasing buffers in proper order.
                            buildingBanks[i].releaseByteBuffer();
                        }
                        // Each build thread must release the "slots" in the input channel
                        // ring buffers of the components it uses to build the physics event.
                        buildSequences[i].set(nextSequences[i]++);
                    }

                    // Stats (need to be thread-safe)
                    eventCountTotal += entangledEventCount;
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
            catch (EmuException e) {
                // EmuException from Evio.checkPayload() if
                // Roc raw or physics banks are in the wrong format
                e.printStackTrace();
                System.out.println("  EB mod: Roc raw or physics event in wrong format");
                emu.setErrorState("EB: Roc raw or physics event in wrong format");
                moduleState = CODAState.ERROR;
                return;
            }
            catch (Exception e) {
                e.printStackTrace();
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
                            //if (btCount == 1) {
                                buildingBank.releaseByteBuffer();
                            //}
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


        private void runNoBuilder() {

             try {
                 // Create a reusable supply of ByteBuffer objects
                 // for writing built physics events into.
                 //--------------------------------------------
                 // Direct buffers give better performance ??
                 //--------------------------------------------
                 // If there's only one output channel, release should be sequential
                 boolean releaseSequentially = true;
                 if (outputChannelCount > 1)  {
                     releaseSequentially = false;
                 }
                 boolean useDirectBB = false;
                 ByteBufferSupply bbSupply = new ByteBufferSupply(ringItemCount, 2000, outputOrder,
                                                                  useDirectBB, releaseSequentially);
                 System.out.println("  EB mod: bbSupply -> " + ringItemCount + " # of bufs, direct = " + false +
                                            ", seq = " + releaseSequentially);


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
                 int     tag, endEventCount, entangledEventCount=0, entangledEventCountNew;
                 long    firstEventNumber=1, startTime=0L;
                 boolean haveEnd, havePhysicsEvents;
                 boolean isSync, nonFatalError;
                 boolean gotBank, gotFirstBuildEvent, recordIdError;
                 boolean isEventNumberInitiallySet = false, generalInitDone = false;
                 EventType eventType = null;
                 EvioNode  inputNode = null;

                 // Allocate arrays once here so building method does not have to
                 // allocate once per built event. Size is set later when the # of
                 // entangled events (block level) is known.
                 long[]  longData   = null; // allocated later
                 long[]  commonLong = null;
                 long[]  firstInputCommonLong = null;
                 long[]  timeStampMin = null;
                 long[]  timeStampMax = null;
                 short[] evData       = null;
                 short[] eventTypesRoc1 = null;

                 int[]   segmentData = new int[20];  // currently only use 3 ints
                 int[]   returnLen   = new int[1];
                 int[] backBufOffsets     = new int[inputChannelCount];
                 ByteBuffer[] backingBufs = new ByteBuffer[inputChannelCount];
                 EvioNode[] rocNodes      = new EvioNode[inputChannelCount];

                 //TODO: only if SEB
                 EvioNode[] trigNodes     = new EvioNode[inputChannelCount];

                 // Some of the above arrays need to be cleared for each event being built.
                 // Copy the contents (all 0's) of the following arrays for efficient clearing.
                 long[]  longDataZero    = null;  // allocated later
                 long[]  longDataMin     = null;  // allocated later
                 short[] evDataZero      = null;  // allocated later
                 int[]   segmentDataZero = new int[20];

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
 //TODO: can pass in buildingBanks and save some mem allocation/garbage
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
                                 handleEndEvent(null);
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
                     top:
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
                                 inputNode = buildingBanks[i].getNode();
                                 eventType = buildingBanks[i].getEventType();
                                 recordIdError = false;
 //System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", event order = " + buildingBanks[i].getByteOrder());

                                 // Check payload buffer for event & evio structure types,
                                 // and also record id mismatches (which is best done in a single
                                 // thread as each buffer is compared to the last one).
                                 //if (!dumpData && btIndex == 0) {
                                 if (btIndex == 0) {
                                     recordIdError = Evio.checkInputType(buildingBanks[i].getRecordId(),
                                                                         inputChannels.get(i),
                                                                         eventType, inputNode);
                                 }

                                 // Deal with user event
                                 if (eventType.isUser()) {
 //System.out.println("  EB mod: got user event");

                                     // User events are placed in first output channel's first ring.
                                     // Only the first build thread will deal with them.
                                     if (btIndex == 0) {
 //System.out.println("  EB mod: bt" + btIndex + " ch" + i + ", skip user item " + nextSequences[i]);
                                         //System.out.println("  EB mod: got user event in pre-processing order = " + pBuf.getByteOrder());

                                         // Check payload buffer for source id.
                                         // Store sync and error info in payload buffer.
                                         //if (!dumpData)
                                         Evio.checkInput(buildingBanks[i], inputChannels.get(i),
                                                         eventType, inputNode, recordIdError);

                                         // Swap headers, NOT DATA, if necessary
                                         if (outputOrder != buildingBanks[i].getByteOrder()) {
                                             try {
                                                 //System.out.println("  EB mod: swap user event (not data)");
                                                 ByteBuffer buffy = buildingBanks[i].getBuffer();
                                                 if (buffy != null) {
                                                     // Takes care of swapping of event in its own separate buffer,
                                                     // headers not data
                                                     ByteDataTransformer.swapEvent(buffy, buffy, 0, 0, false, null);
                                                 }
                                                 else if (inputNode != null) {
                                                     // This node may share a backing buffer with other, ROC Raw, events.
                                                     // Thus we cannot change the order of the entire backing buffer.
                                                     // For simplicity, let's copy it and swap it in its very
                                                     // own buffer.

                                                     // Copy
                                                     buffy = inputNode.getStructureBuffer(true);
                                                     // Swap headers but not data
                                                     ByteDataTransformer.swapEvent(buffy, null, 0, 0, false, null);
                                                     // Store in ringItem
                                                     buildingBanks[i].setBuffer(buffy);
                                                     buildingBanks[i].setNode(null);
                                                     // Release claim on backing buffer since we are now
                                                     // using a different buffer.
                                                     buildingBanks[i].releaseByteBuffer();
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
                                         // the post-build threads can skip over them.
                                         eventToOutputChannel(buildingBanks[i], 0, 0);
                                         //System.out.println("  EB mod: sent user event to output channel");
                                     }

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
                                     // Check payload buffer for source id.
                                     // Store sync and error info in payload buffer.
                                     //if (!dumpData)
                                     Evio.checkInput(buildingBanks[i], inputChannels.get(i),
                                                     eventType, inputNode, recordIdError);
                                     gotBank = true;
                                     break;
                                 }
                             }

                             if (!gotBank) {
                                 continue;
                             }

 //                            if (dumpData) {
 //                                // The ByteBufferSupply takes care of releasing buffers in proper order.
 //                                buildingBanks[i].releaseByteBuffer();
 //                                // Each build thread must release the "slots" in the input channel
 //                                // ring buffers of the components it uses to build the physics event.
 //                                buildSequences[i].set(nextSequences[i]++);
 //
 //                                skipCounter[i] = btCount;
 //                                break;
 //                            }

                             // If event needs to be built ...
                             if (!eventType.isControl()) {
                                 // Find the # of entangled events or "block level".
                                 // This is the number of entangled events in one
                                 // ROC raw record which is set by the trigger supervisor.
                                 // This may change immediately after each sync event.
                                 // It is important because if there are multiple BuildingThreads, then
                                 // each thread must set its own blockLevel.
                                 entangledEventCountNew = inputNode.getNum();

                                 // If there are changes, adjust.
                                 if (entangledEventCountNew != entangledEventCount) {

                                     // If the block level really changed (instead of from its
                                     // initial value of 0) and we have multiple build threads,
                                     // throw an exception cause this will mess things up.
                                     // We don't have any synchronization between build threads
                                     // for this situation (yet).
                                     if (entangledEventCount != 0 && btCount > 1) {
                                         throw new EmuException("changing block level (" + entangledEventCount +
                                                                " to " + entangledEventCountNew +
                                                                ") not permitted with multiple build threads");
                                     }

                                     // Get convenience arrays ready for efficient trigger bank building
                                     longData = new long[entangledEventCountNew + 2];
                                     evData   = new short[entangledEventCountNew];

                                     if (emu.getCodaClass() == CODAClass.SEB) {
                                         eventTypesRoc1       = new short[entangledEventCountNew];
                                         timeStampMin         = new long[entangledEventCountNew];
                                         timeStampMax         = new long[entangledEventCountNew];
                                         commonLong           = new long[entangledEventCountNew + 2];
                                         firstInputCommonLong = new long[entangledEventCountNew + 2];
                                     }

                                     longDataZero = new long[entangledEventCountNew + 2];
                                     longDataMin  = new long[entangledEventCountNew];
                                     Arrays.fill(longDataMin, Long.MAX_VALUE);
                                     evDataZero   = new short[entangledEventCountNew];
                                     // Store the # of entangled events
 //                                    System.out.println("SWITCHING BLOCK LEVEL from " + entangledEventCount + " to " + entangledEventCountNew +
 //                                                               ", gotFirstBuildEvent = " + gotFirstBuildEvent + ", firstEv# = " + firstEventNumber +
 //                                                               ", bt# = " + btIndex);
                                     entangledEventCount = entangledEventCountNew;
                                 }

                                 // One-time init stuff for a group of
                                 // records that will be built together.
                                 if (!gotFirstBuildEvent) {
                                     // Set flag
                                     gotFirstBuildEvent = true;
 //long oldFirstEventNumber = firstEventNumber;
                                     if (!isEventNumberInitiallySet)  {
                                         firstEventNumber = 1L + (btIndex * entangledEventCount);
                                         isEventNumberInitiallySet = true;
                                     }
                                     else {
                                         firstEventNumber += btCount*entangledEventCount;
                                     }

                                     // Do this once per build thread on first buildable event
                                     if (!generalInitDone) {
                                         // Find out if the event's buffer has a backing byte array,
                                         // if this object's ByteBufferSupply has buffers with backing byte arrays,
                                         // and if the 2 buffers have same endianness.
                                         // We do this so that when constructing the trigger bank, we can do
                                         // efficient copying of data from ROC to trigger bank if possible.
                                         try {
                                             ByteBuffer backingBuf = buildingBanks[i].getNode().getBufferNode().getBuffer();
                                             if (backingBuf.hasArray() && !useDirectBB &&
                                                     backingBuf.order() == outputOrder) {
                                                 fastCopyReady = true;
                                                 System.out.println("\nEFFICIENT copying is possible!!!\n");
                                             }
                                             else {
                                                 System.out.println("\nEFFICIENT copying is NOT possible:\n" +
                                                                            "     backingBuf.hasArray = " + backingBuf.hasArray() +
                                                                            "\n     supplyBuf is direct = " + useDirectBB +
                                                                            "\n     backingBuf end = " + backingBuf.order() +
                                                                            "\n     outputORder = " + outputOrder);
                                             }
                                         }
                                         catch (Exception e) {
                                             fastCopyReady = false;
                                             System.out.println("\nEFFICIENT copying is NOT possible\n");
                                         }
                                         generalInitDone = true;
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

                         // repeat for loop endlessly
 //                        if (dumpData && (i == (inputChannelCount - 1))) i = -1;
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
                         handleEndEvent(buildingBanks);
                         return;
                     }

                     // At this point there are only physics or ROC raw events, which do we have?
                     havePhysicsEvents = buildingBanks[0].getEventType().isAnyPhysics();

                     // Check for identical syncs, uniqueness of ROC ids,
                     // identical (physics or ROC raw) event types,
                     // and the same # of events in each bank
                     nonFatalError |= Evio.checkConsistency(buildingBanks, firstEventNumber);
                     isSync = buildingBanks[0].isSync();
                     if (isSync) {
 System.out.println("  EB mod: sync bit set for ev #" + (firstEventNumber + entangledEventCount - 1));
                     }

                     //--------------------------------------------------------------------
                     // Build trigger bank, number of ROCs given by number of buildingBanks
                     //--------------------------------------------------------------------
                     // The tag will be finally set when this trigger bank is fully created

                     // Get an estimate on the buffer memory needed.
                     // Start with 1K and add roughly the amount of trigger bank data + data wrapper
                     int memSize = 1000 + inputChannelCount * entangledEventCount * 40;
 //System.out.println("  EB mod: estimate trigger bank bytes <= " + memSize);
                     for (int i=0; i < inputChannelCount; i++) {
                         rocNodes[i] = buildingBanks[i].getNode();
                         memSize += rocNodes[i].getTotalBytes();
                         // Get the backing buffer
                         backingBufs[i] = rocNodes[i].getBufferNode().getBuffer();
                         // Offset into backing buffer to start of given input's event
                         backBufOffsets[i] = rocNodes[i].getPosition();
                     }

                     // Grab a stored ByteBuffer
                     ByteBufferItem bufItem = bbSupply.get();
                     bufItem.ensureCapacity(memSize);
 //System.out.println("  EB mod: ensure buf has size " + memSize + "\n");
                     ByteBuffer evBuf = bufItem.getBuffer();
                     int builtEventHeaderWord2;

                     // Create a (top-level) physics event from payload banks
                     // and the combined trigger bank. First create the tag:
                     //   -if I'm a data concentrator or DC, the tag has 4 status bits and the ebId
                     //   -if I'm a primary event builder or PEB, the tag is 0xFF50
                     //   -if I'm a secondary event builder or SEB, the tag is 0xFF70
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

                     // 2nd word of top level header
                     builtEventHeaderWord2 = (tag << 16) |
                             ((DataType.BANK.getValue() & 0x3f) << 8) |
                             (entangledEventCount & 0xff);

                     // Start top level. Write the length later, now, just the 2nd header word of top bank
                     evBuf.putInt(4, builtEventHeaderWord2);

                     // Reset this to see if creating trigger bank causes an error
                     nonFatalError = false;
                     int writeIndex=0;

                     // If building with Physics events ...
                     if (havePhysicsEvents) {
                         //-----------------------------------------------------------------------------------
                         // The actual number of rocs will replace num in combinedTrigger definition above
                         //-----------------------------------------------------------------------------------
                         // Combine the trigger banks of input events into one

                         System.arraycopy(longDataZero, 0, timeStampMax, 0, entangledEventCount);
                         System.arraycopy(longDataMin,  0, timeStampMin, 0, entangledEventCount);
                         for (int i=0; i < inputChannelCount; i++) {
                             trigNodes[i] = buildingBanks[i].getNode().getChildAt(0);
                         }

                         nonFatalError |= Evio.makeTriggerBankFromPhysics(buildingBanks,
                                                                          rocNodes, trigNodes,
                                                                          backingBufs,
                                                                          evBuf, id,
                                                                          runNumber, runTypeId,
                                                                          includeRunData,
                                                                          sparsify,
                                                                          checkTimestamps,
                                                                          fastCopyReady,
                                                                          timestampSlop,
                                                                          returnLen,
                                                                          longData,
                                                                          commonLong,
                                                                          firstInputCommonLong,
                                                                          timeStampMin,
                                                                          timeStampMax,
                                                                          evData,
                                                                          eventTypesRoc1);
                         writeIndex = returnLen[0];

 //                        if (emu.getCodaClass() != CODAClass.DC) {
 //                            Utilities.printBufferBytes(evBuf, 0, writeIndex, "NEW Built TRIGGER BANK");
 //                            System.out.println("PAUSE ..................................................");
 //                            Thread.sleep(1000);
 //                        }
                     }
                     // else if building with ROC raw records ...
                     else {
                         // Combine the trigger banks of input events into one
                         System.arraycopy(longDataZero, 0, longData, 0, entangledEventCount + 2);

                         nonFatalError |= Evio.makeTriggerBankFromRocRaw(buildingBanks, evBuf,
                                                                         id, firstEventNumber,
                                                                         runNumber, runTypeId,
                                                                         includeRunData, sparsify,
                                                                         checkTimestamps,
                                                                         timestampSlop, btIndex,
                                                                         longData, evData,
                                                                         segmentData, returnLen,
                                                                         backBufOffsets, backingBufs,
                                                                         rocNodes, fastCopyReady);
                         writeIndex = returnLen[0];
                     }

                     // If the trigger bank has an error, go back and reset built event's tag
                     if (nonFatalError && emu.getCodaClass() == CODAClass.DC) {
                         tag = Evio.createCodaTag(isSync,true,
                                                  buildingBanks[0].getByteOrder() == ByteOrder.BIG_ENDIAN,
                                                  false, /* don't use single event mode */
                                                  id);

                         // 2nd word of top level header
                         builtEventHeaderWord2 = (tag << 16) |
                                                 ((DataType.BANK.getValue() & 0x3f) << 8) |
                                                 (entangledEventCount & 0xff);
                         evBuf.putInt(4, builtEventHeaderWord2);
                     }

                     if (havePhysicsEvents) {
 //if (debug) System.out.println("  EB mod: build physics event with physics banks");
                         Evio.buildPhysicsEventWithPhysics(rocNodes,
                                                           evBuf,
                                                           inputChannelCount,
                                                           writeIndex,
                                                           fastCopyReady,
                                                           returnLen,
                                                           backingBufs);
                         writeIndex = returnLen[0];
                     }
                     else {
 //if (debug) System.out.println("  EB mod: build physics event with ROC raw banks");
                         Evio.buildPhysicsEventWithRocRaw(rocNodes,
                                                          fastCopyReady,
                                                          inputChannelCount,
                                                          writeIndex, evBuf, returnLen,
                                                          backingBufs);
                         writeIndex = returnLen[0];
                     }

                     // Write the length of top bank
 //                    System.out.println("writeIndex = " + writeIndex + ", %4 = " + (writeIndex % 4));
                     evBuf.putInt(0, writeIndex/4 - 1);
                     evBuf.limit(writeIndex).position(0);

 //                    if (emu.getCodaClass() != CODAClass.DC) {
 //                        Utilities.printBufferBytes(evBuf, 0, writeIndex, "NEW Built Event Buf");
 //                        System.out.println("PAUSE ..................................................");
 //                        Thread.sleep(5000);
 //                    }


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

                     // Which output channel do we use?  Round-robin.
                     if (outputChannelCount > 1) {
                         outputChannelIndex = (int) (evIndex % outputChannelCount);
                     }

                     // Put event in the correct output channel.
                     // Important to use builder.getBuffer() method instead of evBuf directly.
                     // That's because in the method, the limit and position are set
                     // properly for reading.
 //                    for (int k=0; k < btIndex; k++) {
 //                        System.out.print("  ");
 //                    }
 //System.out.println("  EB mod: bt#" + btIndex + " write event " + evIndex + " on ch" + outputChannelIndex + ", ring " + btIndex);
                     eventToOutputRing(btIndex, outputChannelIndex, evBuf,
                                       eventType, bufItem, bbSupply);

                     evIndex += btCount;

                     for (int i=0; i < inputChannelCount; i++) {
                         if (useReleaseThread) {
                             if (btCount == 1) {
                                 buildingBanks[i].releaseByteBuffer();
                             }
                         }
                         else {
                             // The ByteBufferSupply takes care of releasing buffers in proper order.
                             buildingBanks[i].releaseByteBuffer();
                         }
                         // Each build thread must release the "slots" in the input channel
                         // ring buffers of the components it uses to build the physics event.
                         buildSequences[i].set(nextSequences[i]++);
                     }

                     // Stats (need to be thread-safe)
                     eventCountTotal += entangledEventCount;
                     wordCountTotal  += writeIndex / 4;
                     keepStats(writeIndex);
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
             catch (EmuException e) {
                 // EmuException from Evio.checkPayload() if
                 // Roc raw or physics banks are in the wrong format
                 e.printStackTrace();
                 System.out.println("  EB mod: Roc raw or physics event in wrong format");
                 emu.setErrorState("EB: Roc raw or physics event in wrong format");
                 moduleState = CODAState.ERROR;
                 return;
             }
             catch (Exception e) {
                 e.printStackTrace();
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
                             //if (btCount == 1) {
                             buildingBank.releaseByteBuffer();
                             //}
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



    } // BuildingThread



    /**
     * This class is a garbage-freeing thread which takes the ByteBuffers and
     * banks used to build an event and frees up their ring-based resources.
     * It takes the burden of doing this off of the build threads and allows
     * them to build without bothering to synchronize between themselves.
     */
    final class ReleaseRingResourceThread extends Thread {

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
         */
        void endThread() {
            quit = true;
            this.interrupt();
        }


        /**
         * Get the last sequence to be freed.
         * @return last sequence to be freed.
         */
        long getLastSequence() {
            return postBuildSequence[order].get();
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
            catch (InterruptedException e) {}
            catch (TimeoutException e)     { /* won't happen */ }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Interrupt all EB threads because an END cmd/event or RESET cmd came through.
     * @param end if <code>true</code> called from end(), else called from reset()
     */
    private void interruptThreads(boolean end) {
        // Although the emu's end() method checks to see if the END event has made it
        // all the way through, it gives up if it takes longer than about 30 seconds
        // at each channel or module.
        // Check again if END event has arrived.
        if (end && !haveEndEvent) {
System.out.println("  EB mod: endBuildThreads: will end building/filling threads but no END event");
            moduleState = CODAState.ERROR;
            emu.setErrorState("EB will end building/filling threads but no END event");
        }

        // Interrupt the rate calculating thread
        if (RateCalculator != null) {
            RateCalculator.interrupt();
        }

        // Interrupt all Building threads
        for (Thread thd : buildingThreadList) {
            // Try to end thread nicely but it could block on rb.next()
            // when writing to output channel ring if no available space
            thd.interrupt();
        }

        if (useReleaseThread) {
            // Interrupt release threads too
            if (buildingThreadCount > 1 && releaseThreads != null) {
                for (ReleaseRingResourceThread rt : releaseThreads) {
                    rt.endThread();
                }
            }
        }
    }

    /**
     * Try joining all EB threads, up to 1 sec each.
     */
    private void joinThreads() {
        // Join rate calculating thread
        if (RateCalculator != null) {
            try {
                RateCalculator.join(1000);
            }
            catch (InterruptedException e) {}
        }

        // Join all Building threads
        for (Thread thd : buildingThreadList) {
            try {
                thd.join(1000);
            }
            catch (InterruptedException e) {}
        }

        if (useReleaseThread) {
            // Join release threads
            if (buildingThreadCount > 1 && releaseThreads != null) {
                for (ReleaseRingResourceThread rt : releaseThreads) {
                    try {
                        rt.join(1000);
                    }
                    catch (InterruptedException e) {
                    }
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

//        if (!dumpData) {
            // Build threads
            buildingThreadList.clear();
            for (int i = 0; i < buildingThreadCount; i++) {
                BuildingThread thd1 = new BuildingThread(i, emu.getThreadGroup(), name + ":builder" + i);
                buildingThreadList.add(thd1);
                thd1.start();
            }

            if (useReleaseThread) {
                if (buildingThreadCount > 1) {
                    releaseThreads = new ReleaseRingResourceThread[inChanCount];
                    for (int j = 0; j < inChanCount; j++) {
                        releaseThreads[j] = new ReleaseRingResourceThread(emu.getThreadGroup(),
                                                                          name + ":release" + j, j);
                        releaseThreads[j].start();
                    }
                }
            }

//        }
    }


    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        CODAStateIF previousState = moduleState;
        moduleState = CODAState.CONFIGURED;

        // EB threads must be immediately ended
        interruptThreads(false);
        joinThreads();
        //stopBlockingThreads();
        RateCalculator = null;
        buildingThreadList.clear();

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
System.out.println("  EB mod: end(), set state to DOWNLOADED");
        moduleState = CODAState.DOWNLOADED;

        // Print out time-to-build-event histogram
        if (timeStatsOn) {
System.out.println("  EB mod: end(), print histogram");
            statistics.printBuildTimeHistogram("Time to build one event:", "nsec");
        }

        // Build & pre-processing threads should already be ended by END event
System.out.println("  EB mod: end(), interrupt threads");
        interruptThreads(true);
        joinThreads();
        RateCalculator = null;
        buildingThreadList.clear();

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

        // For each input channel, 1 sequence per build thread
        buildSequenceIn = new Sequence[buildingThreadCount][inputChannelCount];

        // For each input channel, all build threads share one barrier
        buildBarrierIn = new SequenceBarrier[inputChannelCount];

        // One post-build sequence and barrier for each input channel
        if (useReleaseThread) {
            postBuildSequence = new Sequence[inputChannelCount];
            postBuildBarrier = new SequenceBarrier[inputChannelCount];
        }

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

            if (useReleaseThread) {
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
            else {
                // For each build thread ...
                for (int j = 0; j < buildingThreadCount; j++) {
                    // We have 1 sequence for each build thread & input channel combination
                    buildSequenceIn[j][i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
                    // This sequence may be the last consumer before producer comes along
                    rb.addGatingSequences(buildSequenceIn[j][i]);
                }

            }

            // We have 1 barrier for each channel (shared by building threads)
            buildBarrierIn[i] = rb.newBarrier();
        }

        //------------------------------------------------
        //
        //------------------------------------------------

        // Reset some variables
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;
        runTypeId = emu.getRunTypeId();
        runNumber = emu.getRunNumber();
//        eventNumberAtLastSync = 1L;
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