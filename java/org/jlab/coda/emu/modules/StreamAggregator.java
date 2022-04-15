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
import java.util.*;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * This class implements an event builder.
 *
 * <pre><code>
 *
 *   Ring Buffer (single producer, lock free) for a single input channel
 *
 *                        | Gate (producer cannot go beyond this point)
 *                        |
 *                      __|__
 *                     /  |  \
 *                    /1 _|_ 2\  &lt;---- Sorter Thread
 *                   |__/   \__|        |
 *                   |6 |   | 3|        |
 *             ^     |__|___|__|        |
 *             |      \ 5 | 4 /         |
 *         Producer-&gt;  \__|__/          V
 *                        |
 *                        |
 *                        | Barrier (at last sequence produced)
 *
 * Actual input channel ring buffers have thousands of events (not 6).
 * The producer is a single input channel which reads incoming data,
 * parses it and places it into the ring buffer.
 *
 * The leading consumer of each input channel ring is the sorter thread. This thread
 * sends all events of the same time frame to the ring of the same build thread.
 * Each build thread consumes ring items that the sorter thread fills.
 * There are a fixed number of build threads which can be set in the config file.
 * After initially consuming and filling all input channel ring slots (once around ring),
 * the producer (input channel) will only take additional slots that the build thread
 * is finished with.
 *
 * N Input Channels
 * (evio bank ring bufs)    RB1       RB2  ...  RBN
 *                           3         4         4
 *  time frame of event      3         4         3
 *                           3         3         2
 *                           2         3         1
 *                           2         2         |
 *                           2         2         |
 *                           1         1         |
 *                           1         1         |
 *                           1         |         |
 *                           V         V         V
 *  Sorter sends all     ____________________________
 *  banks of the same    -----   Sorter Thread  -----
 *  frame to the same    ____________________________
 *  build thread                3          4
 *                              3          4
 *                              3          4
 *                              3          4
 *                              3          4
 *                              3          4
 *                              1          2
 *                              1          2
 *                              1          2
 *                              1          2
 *                              1          2
 *                              1          2
 *                              |          |
 *                              V          V
 *  BuildingThreads:          BT1         BT2  ...  BTM
 *  Grab all identical frame   |           |
 *  banks from 1 ring,         |           |
 *  build event, and           |           |
 *  place in                   |           |
 *  output channel(s)          |           |
 *                             |           |
 *                             V           V
 * Output Channel(s):    OC1: RB1         RB2
 * (1 ring buffer for    OCZ: RB1         RB2
 *  each build thread
 *  in each channel)
 *
 *  M != N != Z in general
 *  M  = 1 by default
 *
 *
 * </code></pre>
 *
 *     <p>Before an input channel can reuse a place on the ring (say 4, although at that
 *     point its number would be 6+4=10), the gating sequence for that ring must reach that same value
 *     (10) or higher. This signals that the user (Sorter which depends on BT1 and BT2) are done using
 *     that ring item.</p>
 *
 *  --------------------------------------------------------------------------------------------------
 *
 * <p>Each BuildingThread - of which there may be any number - takes
 * all banks with identical frame number from its own ring buffer,
 * and builds them into a single event. The built event is placed in a ring buffer of
 * an output channel. This is by round robin if more than one channel or on all output channels
 * if a control event, or the first output channel's first ring if user event.
 * Each output channel has the same number of ring buffers
 * as build threads. This avoids any contention and locking while writing. Each build thread
 * only writes to a fixed, single ring buffer of each output channel. It is the job of each
 * output channel to merge the contents of their rings into a single, ordered output stream.</p>
 *
 * NOTE: When building, any Control events must appear on each channel in the same order.
 * If not, an exception may be thrown. If so, the Control event is passed along to all output channels.
 * If no output channels are defined in the config file, this module builds, but discards all events.
 */
public class StreamAggregator extends ModuleAdapter {


    /** The number of BuildingThread objects. */
    private int buildingThreadCount;

    /** Container for threads used to build events. */
    private final ArrayList<BuildingThread> buildingThreadList = new ArrayList<>(6);

    /** The number of the experimental run. */
    private int runNumber;

    /** The number of the experimental run's configuration. */
    private int runTypeId;

    /** If <code>true</code>, this emu has received
     *  all prestart events (1 per input channel). */
    private volatile boolean haveAllPrestartEvents;

    /** If <code>true</code>, produce debug print out. */
    private final boolean debug = true;

    /**
     * If true, dump incoming data immediately after reading off input rings.
     * Use this for testing incoming data rate.
     */
    private boolean dumpData;

    // ---------------------------------------------------
    // Configuration parameters
    // ---------------------------------------------------

    /**
     * The maximum difference in ticks for timestamps for a single event before
     * an error condition is flagged.
     */
    private int timestampSlop;

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

    // Sorter thread is receiving data from all input channels

    /** For each input channel, 1 sequence for sorter thread. */
    private Sequence[] sorterSequenceIn;

    /** For each input channel, one barrier. */
    private SequenceBarrier[] sorterBarrierIn;

    /** Array of available sequences (largest index of items desired), one per input channel. */
    private long[] availableSequences;

    /** Array of next sequences (index of next item desired), one per input channel. */
    private long[] nextSequences;

    /** One ring buffer for each build thread to hold time slice banks. */
    private final RingBuffer<TimeSliceBankItem>[] sorterRingBuffers;

    /** Thread to sort incoming time slice banks to proper build thread. */
    private TimeSliceSorter timeSliceSorterThread;

    /** Number of elements in each sorter ring. */
    private int sorterRingSize;

    // Each build thread is receiving data from 1 sorterRingBuffer

    /** For each sorter ring, 1 sequence since only 1 build thread uses it. */
    private Sequence[] buildSequenceIn;

    /** For each sorter ring, 1 barrier since only 1 build thread uses it. */
    private SequenceBarrier[] buildBarrierIn;

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
    public StreamAggregator(String name, Map<String, String> attributeMap, Emu emu) {
        super(name, attributeMap, emu);

        // Set number of building threads (always >= 1, set in ModuleAdapter constructor)
        buildingThreadCount = eventProducingThreads;

        // If # build threads not explicitly set in config,
        // make it 2 since that produces the best performance with the least resources.
        if (!epThreadsSetInConfig) {
            buildingThreadCount = eventProducingThreads = 2;
        }
        //buildingThreadCount = eventProducingThreads = 2;

        //outputOrder = ByteOrder.LITTLE_ENDIAN;
logger.info("  EB mod: output byte order = little endian");

logger.info("  EB mod: # of event building threads = " + buildingThreadCount);

        // set "data dump" option on
        String str = attributeMap.get("dump");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                dumpData = true;
            }
        }
//dumpData = true;wq

        // The only way a StreamAggregator gets created in the first place
        // is for streaming="on" to be set in jcedit.
        // So just ignore the "streaming" attribute.
        streamingData = true;

        // default to 2 clock ticks
        timestampSlop = 2;
        try {
            timestampSlop = Integer.parseInt(attributeMap.get("tsSlop"));
            if (timestampSlop < 1) timestampSlop = 2;
        }
        catch (NumberFormatException e) {}

        //--------------------------------------------------------------------
        // Set parameters for the ByteBufferSupply which provides ByteBuffers
        // to hold built events.
        //--------------------------------------------------------------------

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

        //--------------------------------------------------------------------
        // Create rings to hold TimeSliceBanks, 1 ring for each build thread.
        // These rings only hold references to objects so no real memory
        // being used. Do this in prestart() but define array here.
        //--------------------------------------------------------------------
        sorterRingSize = 4096;
        sorterRingBuffers = new RingBuffer[buildingThreadCount];
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


    /** {@inheritDoc} */
    public boolean isStreamingData() {return true;}


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

        RingBuffer<RingItem> rb = outputChannels.get(channelNum).getRingBuffersOut()[ringNum];

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
     * @param ringNum    the id number of the output channel's ring buffer.
     * @param channelNum the number of the output channel.
     * @param eventCount number of evio events contained in this bank.
     * @param buf        the event to place on output channel ring buffer.
     * @param eventType  what type of data are we sending.
     * @param item       item corresponding to the buffer allowing buffer to be reused.
     * @param bbSupply   supply from which the item (and buf) came.
     * @throws InterruptedException
     */
    private void eventToOutputRing(int ringNum, int channelNum, int eventCount,
                                   ByteBuffer buf, EventType eventType,
                                   ByteBufferItem item, ByteBufferSupply bbSupply)
            throws InterruptedException {

        // Have output channels?
        if (outputChannelCount < 1) {
            bbSupply.release(item);
            return;
        }

        RingBuffer<RingItem> rb = outputChannels.get(channelNum).getRingBuffersOut()[ringNum];

//System.out.println("  EB mod: wait for out buf, ch" + channelNum + ", ring " + ringNum);
        long nextRingItem = rb.nextIntr(1);
//System.out.println("  EB mod: Got sequence " + nextRingItem + " for " + channelNum + ":" + ringNum);
        RingItem ri = (RingItem) rb.get(nextRingItem);
        ri.setBuffer(buf);
        ri.setEventType(eventType);
        ri.setControlType(null);
        ri.setSourceName(null);
        ri.setReusableByteBuffer(bbSupply, item);
        ri.setEventCount(eventCount);

//System.out.println("  EB mod: will publish to ring " + ringNum);
        rb.publish(nextRingItem);
//System.out.println("  EB mod: published to ring " + ringNum);
    }


    /**
     * This method looks for either a prestart or go event in all the
     * input channels' ring buffers.
     *
     * @param sequences     one sequence per ring buffer
     * @param barriers      one barrier per ring buffer
     * @param buildingBanks empty array of payload buffers (reduce garbage)
     * @param nextSequences one "index" per ring buffer to keep track of which event
     *                      sorter is at in each ring buffer.
     * @return type of control events found
     * @throws EmuException if got non-control or non-prestart/go/end event
     * @throws InterruptedException if taking of event off of Q is interrupted
     */
    private ControlType getAllControlEvents(Sequence[] sequences,
                                     SequenceBarrier[] barriers,
                                     PayloadBuffer[] buildingBanks,
                                     long[] nextSequences)
            throws EmuException, InterruptedException {

        ControlType controlType = null;
System.out.println("  EB mod: getAllControlEvents IN");

        // First thing we do is look for the go or prestart event and pass it on
        // Grab one control event from each ring buffer.
        for (int i=0; i < inputChannelCount; i++) {
System.out.println("  EB mod: getAllControlEvents input chan " + i);
            try  {
                ControlType cType;
                while (true) {
System.out.println("  EB mod: getAllControlEvents wait for seq " + nextSequences[i]);
                    barriers[i].waitFor(nextSequences[i]);
                    buildingBanks[i] = (PayloadBuffer) ringBuffersIn[i].get(nextSequences[i]);
System.out.println("  EB mod: getAllControlEvents got seq " + nextSequences[i]);

                    cType = buildingBanks[i].getControlType();
                    if (cType == null) {
                        // If it's not a control event, it may be a user event.
                        // If so, skip over it and look at the next one.
                        EventType eType = buildingBanks[i].getEventType();
                        if (eType == EventType.USER) {
                            // Send it to the output channel
                            handleUserEvent(buildingBanks[i], inputChannels.get(i), false);
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
                    Utilities.printBuffer(buildingBanks[i].getBuffer(), 0, 5, "Bad control event");
                    throw new EmuException("Expecting prestart, go or end, got " + cType);
                }
            }
            catch (final TimeoutException | AlertException e) {
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
            // Should only be done once - by single sorter thread
            buildingBanks[i].releaseByteBuffer();
        }

        return controlType;
    }



   /**
    * This method writes the specified control event into all the output channels, ring 0.
    *
    * @param isPrestart {@code true} if prestart event being written, else go event
    * @param isEnd {@code true} if END event being written. END take precedence.
    * @param sourceName name of event source.
    * @throws InterruptedException if writing of event to output channels is interrupted
    */
    private void controlToOutputAsync(boolean isPrestart, boolean isEnd, String sourceName)
            throws InterruptedException {

        if (outputChannelCount < 1) {
            return;
        }

        // We have GO or PRESTART?
        ControlType controlType = isPrestart ? ControlType.PRESTART : ControlType.GO;
        controlType = isEnd ? ControlType.END : controlType;

        // Space for 1 control event per channel
        PayloadBuffer[] controlBufs = new PayloadBuffer[outputChannelCount];

        // Create a new control event with updated control data in it
 // TODO: for streaming, event count does NOT make sense!!
        controlBufs[0] = Evio.createControlBuffer(controlType,
                                                  runNumber, runTypeId,
                                                  (int)frameCountTotal, (int)frameCountTotal,
                                                  0, outputOrder,
                                                    sourceName, false, streamingData);

        // For the other output channels, duplicate first with separate position & limit.
        // Important to do this duplication BEFORE sending to output channels or position
        // and limit can be copied while changing.
        for (int i=1; i < outputChannelCount; i++) {
            controlBufs[i] =  new PayloadBuffer(controlBufs[0]);
        }

        // Write event to output channels
        for (int i=0; i < outputChannelCount; i++) {
System.out.println("WRITE CONTROL EVENT to chan #" + i + ", ring 0");
            eventToOutputChannel(controlBufs[i], i, 0);
        }

        if (isEnd) {
            System.out.println("  EB mod: wrote immediate END from sorter thread");
        }
        else if (isPrestart) {
            System.out.println("  EB mod: wrote PRESTART from sorter thread");
        }
        else {
            System.out.println("  EB mod: wrote GO from sorter thread");
        }
    }


    /**
     * Handle a USER event by sending it to the first output channel.
     * If same endian, it may be in EvioNode form, else it'll be
     * written, swapped, into a buffer.
     * 
     * @param buildingBank  bank holding USER event.
     * @param inputChannel
     * @param recordIdError
     * @throws InterruptedException
     */
    private void handleUserEvent(PayloadBuffer buildingBank, DataChannel inputChannel,
                                 boolean recordIdError)
            throws InterruptedException {


        ByteBuffer buffy    = buildingBank.getBuffer();
        EvioNode inputNode  = buildingBank.getNode();
        EventType eventType = buildingBank.getEventType();

        // Check payload buffer for source id.
        // Store sync and error info in payload buffer.
        //if (!dumpData)
        Evio.checkInput(buildingBank, inputChannel,
                        eventType, inputNode, recordIdError);

        // Swap headers, NOT DATA, if necessary
        if (outputOrder != buildingBank.getByteOrder()) {
            try {
                // Check to see if user event is already in its own buffer
                if (buffy != null) {
                    // Takes care of swapping of event in its own separate buffer,
                    // headers not data. This doesn't ever happen.
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
                    buildingBank.setBuffer(buffy);
                    buildingBank.setNode(null);
                    // Release claim on backing buffer since we are now
                    // using a different buffer.
                    buildingBank.releaseByteBuffer();
                }
            }
            catch (EvioException e) {/* should never happen */ }
        }
        else if (buffy == null) {
            // We could let things "slide" and pass on an EvioNode to the output channel.
            // HOWEVER, since we are now reusing EvioNode objects, this is a bad strategy.
            // We must copy it into a new buffer and pass that along, allowing us to free
            // up the EvioNode.

            // Copy
            buffy = inputNode.getStructureBuffer(true);
            // Store in ringItem
            buildingBank.setBuffer(buffy);
            buildingBank.setNode(null);
            // Release claim on backing buffer since we are now
            // using a different buffer.
            buildingBank.releaseByteBuffer();
        }

        // Do this so we can use fifo as output & get accurate stats
        buildingBank.setEventCount(1);

        // User event is stored in a buffer from here on

        // Send it on.
        // User events are thrown away if no output channels
        // since this event builder does nothing with them.
        // User events go into the first ring of the first channel.
        // Since all user events are dealt with here
        // and since they're now all in their own (non-ring) buffers,
        // the post-build threads can skip over them.
        eventToOutputChannel(buildingBank, 0, 0);
    }


    /** Enum to describe a difference in timestamps in relation to time slices. */
    enum TimestampDiff {
        /** Same time slice. */
        SAME(),
        /** Next time slice. */
        NEXT(),
        /** Multiple slices removed. */
        MULTIPLE();
    }




    /**
     * Compare two time frames to see if they're in the same time slice,
     * in sequential time slices, or differ by multiple time slices.
     * Frame is just the sequential slice number.
     *
     * @param tf1   first time frame to examine.
     * @param tf2   second time frame to examine.
     * @return {@link TimestampDiff#SAME} if in same time slice,
     *         {@link TimestampDiff#NEXT} if in sequential time slices, or
     *         {@link TimestampDiff#MULTIPLE} if in different and non-sequential time slices.
     */
    private TimestampDiff compareTimesFrames(long tf1, long tf2) {
        // Same frame?
        boolean same = (tf1 == tf2);

        if (same) {
            // Same as last time slice
            return TimestampDiff.SAME;
        }
        else if (Math.abs(tf1 - tf2) == 1) {
            // Do times differ by 1 slice only?
            return TimestampDiff.NEXT;
        }

        // Times differ by multiple slices
        return TimestampDiff.MULTIPLE;
    }


    /**
     * <p>
     * Find the difference in time frames between the given value and the value
     * being looked for. This is difference is expressed in relation to
     * time slices. In other words, are they in the same, in the next, or in a
     * multiply removed time slice?.</p>
     *
     * @param tf          time frame to examine (>= lookedForTF)
     * @param lookedForTF time frame we're looking for on a channel.
     *
     * @return {@link TimestampDiff#SAME} if in same time slice,
     *         {@link TimestampDiff#NEXT} if in sequential time slices, or
     *         {@link TimestampDiff#MULTIPLE} if in different and non-sequential time slices.
     * @throws EmuException if given timestamp is moving backward in time.
     */
    private TimestampDiff compareWithLookedForTF(long tf, long lookedForTF) throws EmuException {
        TimestampDiff diff = compareTimesFrames(tf, lookedForTF);

        if (diff != TimestampDiff.SAME) {
            if ((tf - lookedForTF) < 0) {
                throw new EmuException("timestamps moving backward in time");
            }
        }

        return diff;
    }


    /**
     * The leading consumer of each input channel ring is the sorter thread. This thread
     * sends all events of the same time frame to the ring of the same build thread.
     * Each build thread consumes ring items that the sorter thread fills.
     */
    class TimeSliceSorter extends Thread {

        /** Index of build thread currently receiving TimeSliceBanks. */
        private int currentBT = 0;

        /** Place to store events read off of channels but not immediately needed. */
        private final PayloadBuffer[] storedBank = new PayloadBuffer[inputChannelCount];
        PayloadBuffer bank = null;

        /** First time through the sorting? */
        boolean firstTimeThru = true;
        /** Time frame currently being written to a build thread ring. */
        long lookingForFrame = 0L;

        // RingBuffer Stuff

        /** Get empty items from each build thread's sorted TSB ring. */
        private final long[] getSequences = new long[buildingThreadCount];


        TimeSliceSorter() {
            Arrays.fill(storedBank, null);
        }


        /**
         * Get the input level (how full is the ring buffer 0-100) of a single input channel
         * @param chanIndex index of channel (starting at 0)
         * @return input level
         */
        int getInputLevel(int chanIndex) {
            // scale from 0% to 100% of ring buffer size
            return ((int)(ringBuffersIn[chanIndex].getCursor() -
                    ringBuffersIn[chanIndex].getMinimumGatingSequence()) + 1)*100/ringBufferSize[chanIndex];
        }



        /**
         * Send a bank, from one of the input channels, to a TimeSliceBank ring buffer
         * that feeds the build thread corresponding to its timestamp.
         *
         * @param bank PayloadBuffer from one of the input channels
         * @param btIndex index indicating which build thread's ring to send bank to.
         * @throws InterruptedException if thread interrupted waiting on ring get().
         */
        private void sendToTimeSliceBankRing(PayloadBuffer bank, int btIndex)
                                    throws InterruptedException {

            getSequences[btIndex] = sorterRingBuffers[btIndex].nextIntr(1);
// System.out.println("  EB mod: sendToTimeSliceBankRing: got sorter ring seq = " + getSequences[btIndex] + ". type = " + bank.getEventType());
            TimeSliceBankItem item = sorterRingBuffers[btIndex].get(getSequences[btIndex]);
            item.setBuf(bank);
            sorterRingBuffers[btIndex].publish(getSequences[btIndex]);
        }


        /**
         * Method to search for END event on each channel when END found on one channel.
         * @param endChannel    channel in which END event first appeared.
         * @param lookedForTF   time frame of slice currently being written to the ring
         *                      of a build thread when END found.
         * @return the total number of END events found in all channels.
         */
        private int findEnds(int endChannel, long lookedForTF) {
            // If the END event is far back on any of the communication channels, in order to be able
            // to read in those events, resources must be released after being read/used.

            long available;
            // One channel already found END event
            int endEventCount = 1;

            try {
                // For each channel ...
                for (int ch=0; ch < inputChannelCount; ch++) {

                    if (ch == endChannel) {
                        // We've already found the END event on this channel
                        continue;
                    }

                    int offset = 0;
                    boolean done = false, written;
                    long veryNextSequence = nextSequences[ch];

                    while (true) {
                        // Check to see if there is anything to read so we don't block.
                        // If not, move on to the next ring.
                        if (ringBuffersIn[ch].getCursor() < veryNextSequence) {
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
                        available = sorterBarrierIn[ch].waitFor(veryNextSequence);
//System.out.println("  EB mod: findEnd, got items from chan " + ch + " up to sequence " + available);

                        while (veryNextSequence <= available) {
                            PayloadBuffer bank = (PayloadBuffer) ringBuffersIn[ch].get(veryNextSequence);
                            String source = bank.getSourceName();
//System.out.println("  EB mod: findEnd, on chan " + ch + " found event of type " + bank.getEventType() + " from " + source + ", back " + offset +
//                   " places in ring with seq = " + veryNextSequence);
                            EventType eventType = bank.getEventType();
                            written = false;

                            if (eventType == EventType.CONTROL) {
                                if (bank.getControlType() == ControlType.END) {
                                    // Found the END event
System.out.println("  EB mod: findEnd, chan " + ch + " got END from " + source + ", back " + offset + " places in ring");
                                    // Release buffer back to ByteBufferSupply
                                    bank.releaseByteBuffer();
                                    endEventCount++;
                                    done = true;
                                    break;
                                }
                            }
                            else if (eventType != EventType.USER) {
                                // Check bank for source id & print out if ids don't match with channel's
                                Evio.checkStreamInput(bank, inputChannels.get(ch), eventType,
                                                      bank.getNode(), false);

                                // This sequence needs to be released later, by build thread, so store here
                                bank.setChannelSequence(nextSequences[ch]);
                                bank.setChannelSequenceObj(sorterSequenceIn[ch]);

                                // Get TF from bank just read from chan
                                // Compare to what we're looking for
                                TimestampDiff diff = compareWithLookedForTF(bank.getTimeFrame(), lookedForTF);
                                if (diff == TimestampDiff.SAME) {
                                    // If it's what we're looking for, write it out
                                    sendToTimeSliceBankRing(bank, currentBT);
                                    written = true;
                                }
                            }

                            // Release buffer back to ByteBufferSupply if not being passed on to build thread
                            if (!written) {
                                bank.releaseByteBuffer();
                            }

                            // Advance sequence
                            sorterSequenceIn[ch].set(veryNextSequence);
                            veryNextSequence++;
                            offset++;
                        }

                        if (done) {
                            break;
                        }
                    }
                } // for each channel
            }
            catch (InterruptedException e) {
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            return endEventCount;
        }


        /**
         * Since we've received END events on the input channels, create an END event now
         * and send it to the build thread next in line.
         */
        private void endEventToBuildThread(String sourceName) throws InterruptedException {
            // Create END event
            PayloadBuffer endEvent = Evio.createControlBuffer(ControlType.END, runNumber, runTypeId,
                    (int) frameCountTotal, (int)frameCountTotal, 0,
                    outputOrder, sourceName, false, streamingData);

            int nextBt = (currentBT + 1) % buildingThreadCount;
            sendToTimeSliceBankRing(endEvent, nextBt);
        }


        /** Run this thread. */
        public void run() {

            // Initialize
            boolean streaming, gotBank, recordIdError;
            EventType eventType = null;
            EvioNode inputNode;
            PayloadBuffer[] buildingBanks = new PayloadBuffer[inputChannelCount];

            // Channel currently being examined
            int chan = 0;
            int lastWrittenChan = 0;


            // Ring Buffer stuff - define array for convenience
            nextSequences = new long[inputChannelCount];
            availableSequences = new long[inputChannelCount];
            Arrays.fill(availableSequences, -2L);
            for (int i = 0; i < inputChannelCount; i++) {
                // Gating sequence for each of the input channel rings
                nextSequences[i] = sorterSequenceIn[i].get() + 1L;
            }


            // First thing we do is look for the PRESTART event(s) and pass it on
            try {
                // Sorter thread writes prestart event on all output channels, ring 0.
                // Get prestart from each input channel.
                ControlType cType = getAllControlEvents(sorterSequenceIn, sorterBarrierIn,
                                                        buildingBanks, nextSequences);

                // In previous call, buildingBanks filled with control events from each channel
                streaming = buildingBanks[0].isStreaming();
                if (streaming != streamingData) {
                    if (streamingData) {
                        throw new EmuException("Expecting streamed data, got triggered");
                    }
                    throw new EmuException("Expecting triggered data, got streamed");
                }

                if (!cType.isPrestart()) {
                    throw new EmuException("Expecting prestart event, got " + cType);
                }

                controlToOutputAsync(true, false, name);
            }
            catch (EmuException e) {
                // To help with FPGA-based VTP - with no PRESTART event - ignore its lack
                if (!streamingData) {
                    e.printStackTrace();
                    if (debug) System.out.println("  EB mod: error getting prestart event");
                    emu.setErrorState("EB error getting prestart event");
                    moduleState = CODAState.ERROR;
                    return;
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
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
                // Sorter thread writes GO event on all output channels, ring 0.
                // Other build threads ignore this.
                // Get GO from each input channel
                ControlType cType = getAllControlEvents(sorterSequenceIn, sorterBarrierIn,
                                                        buildingBanks, nextSequences);
                if (!cType.isGo()) {
                    if (cType.isEnd()) {
                        haveEndEvent = true;
                        controlToOutputAsync(false, true, name);
                        System.out.println("  EB mod: got all END events");
                        return;
                    } else {
                        throw new EmuException("Expecting GO or END event, got " + cType);
                    }
                }

                controlToOutputAsync(false, false, name);
            }
            catch (EmuException e) {
                // To help with FPGA-based VTP - with no GO event - ignore its lack
                if (!streamingData) {
                    e.printStackTrace();
                    if (debug) System.out.println("  EB mod: error getting go event");
                    emu.setErrorState("EB error getting go event");
                    moduleState = CODAState.ERROR;
                    return;
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                // If interrupted, then we must quit
                if (debug) System.out.println("  EB mod: interrupted while waiting for go event");
                emu.setErrorState("EB interrupted waiting for go event");
                moduleState = CODAState.ERROR;
                return;
            }

            System.out.println("  EB mod: got all GO events");


            try {
                // Now do the sorting
                while (moduleState == CODAState.ACTIVE || paused) {

                    // Here we have what we need to build:
                    // ROC raw events from all ROCs (or partially built events from
                    // each contributing EB) each with sequential time slices.
                    // However, there may also be user and END events in the rings.

                    // Put null into buildingBanks array elements
                    Arrays.fill(buildingBanks, null);

                    // Cycle through the channels over and over.
                    // Grab all identical TSs from the first channel. Go to the next and
                    // and so on until all identical slices are read and placed into the
                    // ring of the same build thread.
                    // Next go to the next slice, copy them to the ring of the next
                    // build thread - round and round.
                    while (true) {

//System.out.println("  EB mod: ch" + chan + ",sorter set gotBank = FALSE");
                        gotBank = false;

                        //----------------------------------------------------
                        // Loop until we get event which is NOT a user event
                        //----------------------------------------------------
                        while (!gotBank) {

                            // Make sure there are available data on this channel.
                            // Only wait if necessary ...
                            if (availableSequences[chan] < nextSequences[chan]) {
//System.out.println("  EB mod: ch" + chan + ", sorter wait for event (seq [" + chan + "] = " + nextSequences[chan] + ")");
                                availableSequences[chan] = sorterBarrierIn[chan].waitFor(nextSequences[chan]);
//System.out.println("  EB mod: ch" + chan + ", sorter available seq[" + chan + "]  = " + availableSequences[chan]);
                            }

                            // While we have new data to work with ...
                            while ((nextSequences[chan] <= availableSequences[chan]) || (storedBank[chan] != null)) {
                                // The stored bank is never a user or control event, always a time slice
                                if (storedBank[chan] != null) {
//System.out.println("  EB mod: sorter picking up stored bank at seq = " + nextSequences[chan]);
                                    bank = storedBank[chan];
                                    storedBank[chan] = null;
                                    eventType = bank.getEventType();
                                }
                                else {
                                    bank = (PayloadBuffer) ringBuffersIn[chan].get(nextSequences[chan]);
                                    inputNode = bank.getNode();
                                    eventType = bank.getEventType();
                                    recordIdError = false;
//System.out.println("  EB mod: ch" + chan + ",sorter event type = " + eventType);

                                    // Deal with user event
                                    if (eventType.isUser()) {
                                        // User events are placed in first output channel's first ring.
                                        // Only the first build thread will deal with them.
System.out.println("  EB mod: sorter got user event from channel " + inputChannels.get(chan).name());
                                        //System.out.println("  EB mod: ch" + chan + ", skip user item " + nextSequences[chan]);
                                        //System.out.println("  EB mod: user event order = " + bank.getByteOrder());
                                        handleUserEvent(bank, inputChannels.get(chan), recordIdError);
                                        sorterSequenceIn[chan].set(nextSequences[chan]);
                                        nextSequences[chan]++;
                                    }
                                    // Found a bank, so do something with it
                                    else {
//System.out.println("  EB mod: ch" + chan + ", sorter accept item " + nextSequences[chan] + ", set gotBank = true");
                                        // Check payload buffer for source id.
                                        // Store error info in payload buffer.
                                        Evio.checkStreamInput(bank, inputChannels.get(chan),
                                                              eventType, inputNode, recordIdError);
                                        gotBank = true;
                                        // This sequence needs to be released later, by build thread, so store here
                                        bank.setChannelSequence(nextSequences[chan]);
                                        bank.setChannelSequenceObj(sorterSequenceIn[chan]);

                                        break;
                                    }
                                }
                            }
                        }

//System.out.println("  EB mod: ch" + chan + ", sorter out of TOP LOOP");
                        //----------------------------------------------------

                        // If event needs to be built - a real time slice ...
                        if (!eventType.isControl()) {

                            // Get time frame from bank just read from chan
                            long frame = bank.getTimeFrame();

                            // If this is the first read from the first channel
                            if (firstTimeThru) {
                                // This is the time frame will be looking for in each channel
                                lookingForFrame = frame;
                                firstTimeThru = false;
                            }

                            // Compare bank's TS to the one we're looking for -
                            // those to be placed into the current build thread's ring.
                            // First time thru this comes back as "SAME".
                            TimestampDiff diff = compareWithLookedForTF(frame, lookingForFrame);

//System.out.println("  EB mod: ch" + chan + ", sorter NOT CONTROL EVENT, frame = " + frame + ", looking for " + lookingForFrame + ", diff = " + diff);
                            // Bank was has same Time Slice as the one we're looking for.
                            // This means that this bank must be written out to the current
                            // receiving ring buffer. That's because all identical time slices
                            // go to the same ring buffer no matter the input channel.
                            if (diff == TimestampDiff.SAME) {
//System.out.println("  EB mod: ch" + chan + ", sorter send time slice to BT# = " + currentBT +", event type = " + bank.getEventType() + ", frame = " + frame);
                                sendToTimeSliceBankRing(bank, currentBT);
                                // This bank must be released AFTER build thread finishes with it
                                nextSequences[chan]++;
                                lastWrittenChan = chan;

                                // Next read will be on the same channel to see if there are
                                // more identical time slice banks there. Keep at it until all
                                // identical time slices from this channel are in one ring.
                                continue;
                            }
                            // Bank was has DIFFERENT Time Slice than previous bank from same channel that was
                            // written out. This means that this bank must be put on hold for a while - store
                            // it for later use.
                            // Check the other channels to see if they have banks with the same time slices
                            // as the one last written.
                            else if (diff == TimestampDiff.NEXT) {
//System.out.println("  EB mod: ch" + chan + ", sorter DIFF timestamp, frame = " + frame);
                                // If the last write was on this channel, then the bank we just
                                // read from that channel is part of the next time slice.
                                // This is our clue to move to the next channel to see if it has
                                // banks with the previous time slice.
                                if (chan == lastWrittenChan) {
                                    // Store what we just read for the next time
                                    // we're getting data from this channel
                                    storedBank[chan] = bank;

                                    // if this is the last input channel ...
                                    if (chan >= inputChannelCount - 1) {
                                        // Go back to the first channel
                                        chan = 0;
                                        // Start looking for the next slice
                                        lookingForFrame = frame;
                                        // Which will go to the next build thread
                                        currentBT = (currentBT + 1) % buildingThreadCount;
                                    }
                                    else {
                                        // Go to the next channel & keep looking for the SAME slice
                                        chan++;
                                    }
                                }
                                // If the last write was on a previous channel ...
                                else {
                                    // This channel should have had an event with an identical timestamp.
                                    // Since it didn't, there's a slice missing!
                                    throw new EmuException("Too big of a jump in timestamp");
                                }
                            }
                            else {
System.out.println("  EB mod: ch" + chan + ", sorter DIFF timestamp, time frame = " + frame + ", looking for " + lookingForFrame);
                                throw new EmuException("Too big of a jump in timestamp");
                            }

                            continue;
                        }

                        //-------------------------------------------
                        // If we're here, we've got a CONTROL event
                        //-------------------------------------------

                        // If not END, we got problems
                        if (!bank.getControlType().isEnd()) {
                            EmuUtilities.printStackTrace();
                            if (debug) System.out.println("  EB mod: " + bank.getControlType() +
                                                          " control events not allowed");
                            emu.setErrorState("EB error: " + bank.getControlType() +
                                              " control events not allowed");
                            moduleState = CODAState.ERROR;
                            return;
                        }

                        //-------------------------------------------
                        // If we're here, we've got 1 END event
                        //-------------------------------------------

                        // We need one from each channel so find them now.
                        int endEventCount = findEnds(chan, lookingForFrame);

System.out.println("  EB mod: sorter found END event from " + bank.getSourceName() + " at seq " + nextSequences[chan]);

                        if (endEventCount != inputChannelCount) {
                            // We do NOT have all END events
                            emu.sendRcErrorMessage("Missing " + (inputChannelCount - endEventCount) +
                                                   " END events, ending anyway");
                            throw new EmuException("only " + endEventCount + " ENDs for " +
                                                   inputChannelCount + " channels");
                        }
                        else {
System.out.println("  EB mod: sorter found END events on all input channels");
                            endEventToBuildThread(name);
                            return;
                        }
                    }
                    // repeat loop endlessly
                }
            }
            catch (EmuException e) {
                e.printStackTrace();
                emu.sendRcErrorMessage("EB: Error sorting time slices");
                emu.setErrorState("EB: Error sorting time slices: " + e.getMessage());
                moduleState = CODAState.ERROR;
            }
            catch (InterruptedException e) {
                //e.printStackTrace();
            }
            catch (AlertException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }



    /**
     * <p>
     * This thread is started by the PRESTART transition.
     * An empty buffer is obtained from a supply.
     * All time slice banks with the same frame # (from a single input ring buffer
     * containing all slices with the same time stamp) are built into a new aggregated
     * bank in that buffer.
     * If this module has outputs, the built banks are placed on an output channel.
     * If there are multiple output channels, output is selected by round-robin.
     * </p>
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
         * built bank produced from this thread.
         * 1st build thread starts at 0, 2nd starts at 1, etc.
         * 1st build thread's 2nd event is btCount, 2nd thread's 2nd event is btCount + 1, etc.*/
        private long evIndex = 0;

        /** Which channel does this thread currently output to, using round-robin?
         * outputChannelIndex = (int) (evIndex % outputChannelCount). */
        private int outputChannelIndex = 0;

        // RingBuffer Stuff

        /** Array of available sequences (largest index of items desired), one per input channel. */
        private long availableSequence = -2;
        /** Array of next sequences (index of next item desired), one per input channel. */
        private long nextSequence;


        private String thdName;



        /**
         * Constructor.
         *
         * @param btIndex place in relation to other build threads (first = 0)
         * @param group   thread group
         * @param name    thread name
         */
        BuildingThread(int btIndex, ThreadGroup group, String name) {
            super(group, name);
            thdName = name;
            this.btIndex = btIndex;
            evIndex = btIndex;
            btCount = buildingThreadCount;
System.out.println("  EB mod: create Build Thread (" + name + ") with index " + btIndex + ", count = " + btCount);
        }



        /**
         * Handle the END event.
         * @param bank the banks holding the locally created END event
         */
        private void handleEndEvent(PayloadBuffer bank) {

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
                endBufs[0] = bank;

                // For the other output channel(s), duplicate first with separate position & limit
                for (int i=1; i < outputChannelCount; i++) {
                    endBufs[i] = new PayloadBuffer(bank);
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
                //
                // One output channel is easy, have this build thread
                // write the END as it would the very next physics.
                //
                // Here's the strategy for multiple output channels:
                //
                // - This build thread (btIndex) has the END event.
                //   It writes that as it would the next physics -
                //   to the channel found by:
                //        outputChannelIndex = evIndex % outputChannelCount
                //   The ring is that same one this thread always writes to.
                //
                // - Writing an END to the next channel is the exact equivalent of having
                //   the next build thread writing its next physics. This tells us
                //   the ring it's written to:
                //       btIndexNext = (btIndex + 1) % buildThreadCount
                //   The channel is found by the normal round robin method:
                //       outputChannelIndex = (evIndex + 1) % outputChannelCount.


                // The channel due to receive the next physics event is ...
                outputChannelIndex = (int) (evIndex % outputChannelCount);

System.out.println("  EB mod: try sending END event to output channel " + outputChannelIndex +
               ", ring " + btIndex + ", ev# = " + evIndex);
                // Send END event to that channel
                try {
                    eventToOutputChannel(endBufs[0], outputChannelIndex, btIndex);
                }
                catch (InterruptedException e) {
                    return;
                }
System.out.println("  EB mod: sent END event to output channel  " + outputChannelIndex);

                // If there are multiple channels,
                // give any other build threads time to finish writing their last event
                // since we'll be writing END to rings this thread does not normally write to.
                // Wait 2 seconds, print warning
                if ((outputChannelCount > 1) && (buildingThreadCount > 1)) {
                        try {
                            Thread.sleep(2000);
                        }
                        catch (InterruptedException e) {}
System.out.println("  EB mod: WARNING, might have a problem writing END event");
                }

                // Now send END to the other channels. Do this by going forward.
                // Physics events are sent to output channels round-robin and are
                // processed by build threads round-robin. So ...
                // go to the next channel we would normally send a physics event on,
                // calculate which ring & channel, and write END to it.
                // Then continue by going to the next channel until all additional
                // channels are done.
                for (int i=1; i < outputChannelCount; i++) {

                    // Next channel to be sent a physics event
                    int nextChannel = (int) ((evIndex + i) % outputChannelCount);

                    // Next build thread to write (and therefore ring to receive) a physics event.
                    // int nextBtIndex = (btIndex + i) % btCount; OR
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

            // No END events need to be released from a ByteBuffer supply since they
            // were locally created in the Sorter thread.

            if (endCallback != null) endCallback.endWait();
        }



        /** Run this thread. */
        public void run() {

            try {
                // Create a reusable supply of ByteBuffer objects
                // for writing built physics events into.
                //--------------------------------------------
                // Direct buffers give better performance ??
                //--------------------------------------------
                // If there's only one output channel, release should be sequential
// TODO: Double check to make sure output channels don't do something weird
                boolean releaseSequentially = true;
                if (outputChannelCount > 1)  {
                    releaseSequentially = false;
                }
                boolean useDirectBB = false;
                ByteBufferSupply bbSupply = new ByteBufferSupply(ringItemCount, 2000, outputOrder,
                                                                 useDirectBB, releaseSequentially);
System.out.println("  EB mod: bbSupply -> " + ringItemCount + " # of bufs, direct = " + false +
                   ", seq = " + releaseSequentially);


                // Initialize
                int     tag;
                long    startTime=0L;
                long    storedSequence = 0L;
                long    endSequence = -1;
                boolean havePhysicsEvents;
                boolean nonFatalError;
                boolean generalInitDone = false;
                EventType eventType;

                int[] bankData  = new int[3];
                int[] returnLen = new int[1];

                if (outputChannelCount > 1) {
                    outputChannelIndex = -1;
                }

                long frame, prevFrame=0;
                // The time frame we're currently looking for
                long lookingForTF = 0;
                // Current number of banks that have the same stamp
                int sliceCount;
                // Places to store banks, input nodes, and backing buffers with the same stamp.
                // The array should be sliceCount size, but we don't know
                // what that is yet. If it needs to be increased, do it later.
                PayloadBuffer[] sameStampBanks = new PayloadBuffer[200];
                EvioNode[] inputNodes = new EvioNode[200];
                ByteBuffer[] backingBufs = new ByteBuffer[200];

                PayloadBuffer bank, storedBank = null;
                TimeSliceBankItem timeSliceItem;

                minEventSize = Integer.MAX_VALUE;
                if (timeStatsOn) {
                    statistics = new Statistics(1000000, 30);
                }

                // Ring Buffer stuff
                nextSequence = buildSequenceIn[btIndex].get() + 1L;

                // All banks on the ring are ROC raw / physics slices with the single possibility
                // that there is an END event. User and other controls have already
                // been dealt with in the sorter thread and been sent to output channels.

                // Now do the event building
                while (moduleState == CODAState.ACTIVE || paused) {

                    nonFatalError = false;

                    // Here we have what we need to build:
                    // ROC raw events from all ROCs (or partially built events from
                    // each contributing EB) each with sequential time stamps.
                    // There may be several contiguous banks with the same time stamp.
                    // However, there are also END events in the rings.

                    // Set variables/flags
                    sliceCount = 0;

                    // Start the clock on how long it takes to build the next event
                    if (timeStatsOn) startTime = System.nanoTime();


                    // Loop through all events placed into ring by sorter thread
                    while (true) {

                        // Only wait if necessary ...
                        if (availableSequence < nextSequence) {
                            // Can BLOCK here waiting for item if none available, but can be interrupted
                            // Available sequence may be larger than what we asked for.
//System.out.println("  EB mod: bt" + btIndex + " ***** wait for event seq = " + nextSequence + ")");
                            availableSequence = buildBarrierIn[btIndex].waitFor(nextSequence);
//System.out.println("  EB mod: bt" + btIndex + " ***** available seq  = " + availableSequence);
                        }

                        if (storedBank != null) {
                            bank = storedBank;
                            nextSequence = storedSequence;
                            storedBank = null;
//System.out.println("  EB mod: bt" + btIndex + " ***** picking up stored bank at seq = " + nextSequence);
                        }
                        else {
                            // Next bank to work with ...
                            timeSliceItem = sorterRingBuffers[btIndex].get(nextSequence);
                            bank = timeSliceItem.getBuf();
                        }

                        frame = bank.getTimeFrame();
                        eventType = bank.getEventType();
//                        ControlType controlType = bank.getControlType();
//System.out.println("  EB mod: bt" + btIndex + " ***** event order = " + bank.getByteOrder());
//System.out.println("  EB mod: bt" + btIndex + " ***** frame = " + frame + ", event type = " + eventType + ", control type = " + controlType);

                        // Found a bank, so do something with it
//System.out.println("  EB mod: bt" + btIndex + " ***** accept item " + thisSequence);

                        // If event needs to be built ...
                        if (!eventType.isControl()) {

                            // Do this once per build thread on first buildable event
                            if (!generalInitDone) {

                                lookingForTF = frame;

                                // Find out if the event's buffer has a backing byte array,
                                // if this object's ByteBufferSupply has buffers with backing byte arrays,
                                // and if the 2 buffers have same endianness.
                                // We do this so that when constructing the trigger bank, we can do
                                // efficient copying of data from ROC to trigger bank if possible.
                                try {
                                    ByteBuffer backingBuf = bank.getNode().getBuffer();
                                    if (backingBuf.hasArray() && !useDirectBB &&
                                            backingBuf.order() == outputOrder) {
                                        fastCopyReady = true;
                                        System.out.println("\nEFFICIENT copying is possible!!!\n");
                                    }
                                    else {
                                        fastCopyReady = false;
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

                            TimestampDiff diff = compareWithLookedForTF(frame, lookingForTF);

                            if (diff == TimestampDiff.SAME) {

                                // First check to make sure we have room to store this bank
                                if (sameStampBanks.length < (sliceCount + 1)) {
                                    // If not enough room, double the relevant arrays
                                    int newLength = 2*sameStampBanks.length;
//System.out.println("\n  EB mod: bt" + btIndex + " ***** EXPAND arrays from " + sameStampBanks.length + " to " + newLength);
                                    PayloadBuffer[] sameStampBanksNew = new PayloadBuffer[newLength];
                                    inputNodes  = new EvioNode[newLength];
                                    backingBufs = new ByteBuffer[newLength];

                                    // Copy over array elements
                                    System.arraycopy(sameStampBanks, 0, sameStampBanksNew, 0, sameStampBanks.length);
                                    sameStampBanks = sameStampBanksNew;
                                }

                                sameStampBanks[sliceCount] = bank;
                                sliceCount++;
                                nextSequence++;
                                prevFrame = frame;
//System.out.println("\n  EB mod: bt" + btIndex + " ***** found bank, look for another ---> continue, next seq = " + nextSequence +
//        ", frame (prevFrame) = " + frame + ", lookingForTF = " +lookingForTF);
                                continue;
                            }
                            else {
                                // If here, we're at the next time stamp,
                                // so go ahead and build what we've collected
                                // and use this bank in the next round
                                storedBank = bank;
                                storedSequence = nextSequence;
                                // Start looking for the next frame
                                lookingForTF = frame;
//System.out.println("\n  EB mod: bt" + btIndex + " ***** at next timestamp, got to next seq = " + nextSequence + ", frame = " + frame +
//        ", prevFrame = " + prevFrame + ", lookingForTF = " + lookingForTF);
                            }

                            break;
                        }

                        // If we're here, we've got a CONTROL event and it's guaranteed to be an END
                        // since the only control events placed into the ring buffer is such.
                        // This will be the only build thread with the END event.

                        endSequence = nextSequence;
                        haveEndEvent = true;
                        handleEndEvent(bank);
System.out.println("  EB mod: bt" + btIndex + " ***** found END event at seq " + endSequence);
                        return;
                    }


                    // At this point there are only physics or ROC raw events, which do we have?
                    havePhysicsEvents = sameStampBanks[0].getEventType().isAnyPhysics();


                    //--------------------------------------------------------------------
                    // Build Stream Info Bank (SIB)
                    //--------------------------------------------------------------------
                    // The tag will be finally set when this bank is fully created

                    // Get an estimate on the buffer memory needed.
                    // Start with 10K and add roughly the amount of trigger bank data + data wrapper
                    int memSize = 10000; // Start with a little extra room
                    for (int i=0; i < sliceCount; i++) {
                        inputNodes[i] = sameStampBanks[i].getNode();
                        memSize += inputNodes[i].getTotalBytes();
                        // Get the backing buffer
                        backingBufs[i] = inputNodes[i].getBuffer();
                    }

                    // Grab a stored ByteBuffer
                    ByteBufferItem bufItem = bbSupply.get();
                    bufItem.ensureCapacity(memSize);
//System.out.println("  EB mod: bt" + btIndex + " ***** ensure buf has size " + memSize + ", frame = " + frame + ", prevFrame = " + prevFrame);
                    ByteBuffer evBuf = bufItem.getBuffer();
                    int builtEventHeaderWord2;

                    // Create a (top-level) physics event from payload banks
                    // and the combined SIB bank. First create the tag:
                    //   -if I'm a data concentrator or DCAG
                    //   -if I'm a primary aggregatpr or PAG
                    //   -if I'm a secondary aggregator or SAG
                    CODAClass myClass = emu.getCodaClass();
                    tag = CODATag.STREAMING_TIB.getValue();
                    switch (myClass) {
                        case SAG:
                        case PAG:
                            eventType = EventType.PHYSICS;
                            break;

                        case DCAG:
                        default:
                            eventType = EventType.PARTIAL_PHYSICS;
                            // Check input banks for non-fatal errors
                            for (int i=0; i < sliceCount; i++) {
                                nonFatalError |= sameStampBanks[i].hasNonFatalBuildingError();
                            }
                    }

                    // 2nd word of top level header
                    builtEventHeaderWord2 = (tag << 16) |
                            ((DataType.BANK.getValue() & 0x3f) << 8) |
                            (sliceCount & 0xff);

                    // Start top level. Write the length later, now, just the 2nd header word of top bank
                    evBuf.putInt(4, builtEventHeaderWord2);

                    // Reset this to see if creating trigger bank causes an error
                    nonFatalError = false;
                    int writeIndex=0;

                    // If building with Physics events ...
                    if (havePhysicsEvents) {
                        //-----------------------------------------------------------------------------------
                        // Combine the SIB banks of input events into one
                        //-----------------------------------------------------------------------------------
//System.out.println("  EB mod: bt" + btIndex + " ***** Building frame " + prevFrame + " with " + sliceCount + " BUILT slices");
                        nonFatalError |= Evio.combineAggregatedStreams(
                                sliceCount,
                                sameStampBanks,
                                evBuf,
                                tag,
                                timestampSlop,
                                prevFrame,
                                bankData,
                                returnLen,
                                backingBufs,
                                inputNodes,
                                fastCopyReady);

                            writeIndex = returnLen[0];

//                        if (emu.getCodaClass() != CODAClass.DC) {
//                            Utilities.printBufferBytes(evBuf, 0, writeIndex, "NEW Built TRIGGER BANK");
//                            System.out.println("PAUSE ..................................................");
//                            Thread.sleep(1000);
//                        }
                    }
                    // else if building with ROC raw records ...
                    else {
//System.out.println("  EB mod: bt" + btIndex + " ***** Building frame " + prevFrame + " with " + sliceCount + " ROC RAW time slices");
                        nonFatalError |= Evio.combineRocStreams(
                                sliceCount,
                                sameStampBanks,
                                evBuf,
                                CODATag.STREAMING_TIB.getValue(),
                                timestampSlop,
                                prevFrame,
                                bankData,
                                returnLen,
                                backingBufs,
                                inputNodes,
                                fastCopyReady);

                        writeIndex = returnLen[0];
                    }

                    // If there is a non-fatal error, go back and reset built event's tag
                    if (nonFatalError && emu.getCodaClass() == CODAClass.DC) {
                        tag = CODATag.STREAMING_TIB_ERROR.getValue();

                        // 2nd word of top level header
                        builtEventHeaderWord2 = (tag << 16) |
                                ((DataType.BANK.getValue() & 0x3f) << 8) |
                                (sliceCount & 0xff);
                        evBuf.putInt(4, builtEventHeaderWord2);
                    }

                    // Write the length of top bank
//                    System.out.println("writeIndex = " + writeIndex + ", %4 = " + (writeIndex % 4));
                    evBuf.putInt(0, writeIndex/4 - 1);
                    evBuf.limit(writeIndex).position(0);

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
//System.out.println("  EB mod: bt#" + btIndex + " write event " + evIndex + " on ch" + outputChannelIndex + ", ring " + btIndex);
                    eventToOutputRing(btIndex, outputChannelIndex, sliceCount,
                                      evBuf, eventType, bufItem, bbSupply);

                    evIndex += btCount;

                    for (int i=0; i < sliceCount; i++) {
                        // The ByteBufferSupply takes care of releasing buffers in proper order.
                        sameStampBanks[i].releaseByteBuffer();

                        // Since we're done building with sameStampBanks,
                        // we can release them back to input channel rings.
                        long seq = sameStampBanks[i].getChannelSequence();
                        Sequence seqObj = sameStampBanks[i].getChannelSequenceObj();
                        // The ring will now have access to this sequence
                        seqObj.set(seq);
                    }

                    // Each build thread must release the "slots" in the build thread ring
                    // buffer of the components it uses to build the physics event.
//System.out.println("  EB mod: bt#" + btIndex + " release build seq " + (nextSequence - 1));
                    buildSequenceIn[btIndex].set(nextSequence - 1);

                    // Stats (need to be thread-safe)
                    frameCountTotal++;
                    // TODO: Keep this here temporarily
                    eventCountTotal++;
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

                    cursor = sorterBarrierIn[i].getCursor();

                    while (true) {
                        try {
                            availableSequences[i] = sorterBarrierIn[i].waitFor(cursor);
                        }
                        catch (Exception e) {}

                        // While we have data to read ...
                        while (nextSequences[i] <= availableSequences[i]) {
                            buildingBank = (PayloadBuffer) ringBuffersIn[i].get(nextSequences[i]);
System.out.println("  EB mod: bt" + btIndex + " %%%%% clean inputs, releasing input seq " + nextSequences[i] + " from channel #" + i);
                            //if (btCount == 1) {
                            buildingBank.releaseByteBuffer();
                            //}
                            nextSequences[i]++;
                        }
System.out.println("  EB mod: bt" + btIndex + " %%%%% clean inputs, releasing sorter seq " + availableSequences[i] + " from channel #" + i);
                        sorterSequenceIn[i].set(availableSequences[i]);

                        lastCursor = cursor;
                        cursor = sorterBarrierIn[i].getCursor();

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
     * Interrupt all EB threads because an END cmd/event or RESET cmd came through.
     * @param end if <code>true</code> called from end(), else called from reset()
     */
    private void interruptThreads(boolean end) {
        // Although the emu's end() method checks to see if the END event has made it
        // all the way through, it gives up if it takes longer than about 30 seconds
        // at each channel or module.
        // Check again if END event has arrived.
        if (end && !haveEndEvent) {
System.out.println("  EB mod: interruptThreads: will end building/filling threads but no END event");
            moduleState = CODAState.ERROR;
            emu.setErrorState("EB will end building/filling threads but no END event");
        }

        // Interrupt the rate calculating thread
        if (RateCalculator != null) {
            RateCalculator.interrupt();
        }

        // Interrupt the sorter thread
        if (timeSliceSorterThread != null) {
            timeSliceSorterThread.interrupt();
        }

        // Interrupt all Building threads
        for (Thread thd : buildingThreadList) {
            // Try to end thread nicely but it could block on rb.next()
            // when writing to output channel ring if no available space
            thd.interrupt();
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

        // Join sorter thread
        if (timeSliceSorterThread != null) {
            try {
                timeSliceSorterThread.join(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Join all Building threads
        for (Thread thd : buildingThreadList) {
            try {
                thd.join(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Start threads for stats, processing incoming events, and building events.
     * It creates these threads if they don't exist yet.
     */
    private void startThreads() {
        // Rate calculating thread
        if (RateCalculator != null) {
            RateCalculator.interrupt();
            try {
                RateCalculator.join(1000);
            }
            catch (Exception e) {}
        }

        RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), name+":watcher");
        if (RateCalculator.getState() == Thread.State.NEW) {
            RateCalculator.start();
        }

        // Time slice sorting thread
        if (timeSliceSorterThread != null) {
            timeSliceSorterThread.interrupt();
            try {
                timeSliceSorterThread.join(1000);
            }
            catch (Exception e) {}
        }
        timeSliceSorterThread = new TimeSliceSorter();
        if (timeSliceSorterThread.getState() == Thread.State.NEW) {
            timeSliceSorterThread.start();
        }

//        if (!dumpData) {
            // Build threads
            buildingThreadList.clear();
            for (int i = 0; i < buildingThreadCount; i++) {
                BuildingThread thd1 = new BuildingThread(i, emu.getThreadGroup(), name + ":builder" + i);
                buildingThreadList.add(thd1);
//System.out.println("  EB mod: startThreads(), start build thread " + i);
                thd1.start();
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

        RateCalculator = null;
        timeSliceSorterThread = null;
        buildingThreadList.clear();
        Arrays.fill(sorterRingBuffers, null);

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
//System.out.println("  EB mod: end(), set state to DOWNLOADED");
        moduleState = CODAState.DOWNLOADED;

        // Print out time-to-build-event histogram
        if (timeStatsOn) {
//System.out.println("  EB mod: end(), print histogram");
            statistics.printBuildTimeHistogram("Time to build one event:", "nsec");
        }

        // Build & pre-processing threads should already be ended by END event
        interruptThreads(true);
        joinThreads();

        RateCalculator = null;
        timeSliceSorterThread = null;
        buildingThreadList.clear();
        Arrays.fill(sorterRingBuffers, null);

        paused = false;

        try {
            // Set end-of-run time in local XML config / debug GUI
//System.out.println("  EB mod: end(), set internal ending time parameter");
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
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

        // For each input channel, 1 ring buffer
        ringBuffersIn = new RingBuffer[inputChannelCount];
        // For each input channel, 1 sequence (used by sorter thread)
        sorterSequenceIn = new Sequence[inputChannelCount];
        // For each input channel, 1 barrier
        sorterBarrierIn = new SequenceBarrier[inputChannelCount];

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

            // For sorter thread

            // We have 1 sequence for each input channel
            sorterSequenceIn[i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
            // This sequence will be the last consumer before producer comes along
            rb.addGatingSequences(sorterSequenceIn[i]);

            // We have 1 barrier for each channel
            sorterBarrierIn[i] = rb.newBarrier();
        }

        //------------------------------------------------
        // Disruptor (RingBuffer) stuff for build threads
        //------------------------------------------------

        // 1 sequence for each build thread
        buildSequenceIn = new Sequence[buildingThreadCount];
        // 1 barrier for each build thread
        buildBarrierIn = new SequenceBarrier[buildingThreadCount];

        // For each build thread ...
        for (int j = 0; j < buildingThreadCount; j++) {
            // Create 1 ring to hold TimeSliceBanks.
            // This ring only holds references to objects so no real memory being used.
            sorterRingBuffers[j] = createSingleProducer(new TimeSliceBankItemFactory(), sorterRingSize,
                    new SpinCountBackoffWaitStrategy(10000, new LiteBlockingWaitStrategy()));
            // We have 1 sequence
            buildSequenceIn[j] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
            // This sequence will be the last consumer before sorter produces more
            sorterRingBuffers[j].addGatingSequences(buildSequenceIn[j]);
            // We have 1 barrier
            buildBarrierIn[j] = sorterRingBuffers[j].newBarrier();
        }

        //------------------------------------------------
        // Reset some variables
        //------------------------------------------------
        eventRate = wordRate = 0F;
        frameCountTotal = eventCountTotal = wordCountTotal = 0L;
        runTypeId = emu.getRunTypeId();
        runNumber = emu.getRunNumber();
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