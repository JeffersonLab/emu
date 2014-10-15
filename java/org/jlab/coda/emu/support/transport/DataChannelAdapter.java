/*
 * Copyright (c) 2013, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.transport;


import com.lmax.disruptor.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuEventNotify;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAStateMachineAdapter;
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.ModuleIoType;
import org.jlab.coda.emu.support.data.RingItem;
import org.jlab.coda.emu.support.data.RingItemFactory;
import org.jlab.coda.emu.support.logger.Logger;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * This class provides boilerplate code for the DataChannel
 * interface (which includes the CODAStateMachine interface).
 * Extending this class implements the DataChannel interface and frees
 * any subclass from having to implement common methods or those that aren't used.<p>
 * This class defines an object that can send and
 * receive banks of data in the CODA evio format. It
 * refers to a particular connection (eg. an et open
 * or cMsg connection id).
 *
 * @author timmer
 *         (Apr 25, 2013)
 */
public class DataChannelAdapter extends CODAStateMachineAdapter implements DataChannel {

    /** Channel id (corresponds to sourceId of ROCs for CODA event building). */
    protected int id;

    /** Record id (corresponds to evio events flowing through data channel). */
    protected int recordId;

    /**
     * If we're a PEB or SEB and want to send 1 evio event per et-buffer/cmsg-message,
     * then this is true. If we want to cram as many evio events as possible in each
     * et-buffer/cmsg-message, then this is false.
     * In the EMU domain, do we send out individual events (true) or do we
     * marshall them into one evio-file-format buffer (false)?
     */
    protected boolean singleEventOut;

    /** Channel state. */
    protected State state;

    /**
     * Channel error message. reset() sets it back to null.
     * Making this an atomically settable String ensures that only 1 thread
     * at a time can change its value. That way it's only set once per error.
     */
    protected AtomicReference<String> errorMsg = new AtomicReference<String>();

    /** Channel name */
    protected final String name;

    /** Is this channel an input (true) or output (false) channel? */
    protected final boolean input;

    /** EMU object that created this channel. */
    protected final Emu emu;

    /** Logger associated with this EMU (convenience member). */
    protected final Logger logger;

    /** Byte order of output data. */
    protected ByteOrder byteOrder;

    /** Object used by Emu to be notified of END event arrival. */
    protected EmuEventNotify endCallback;

    /** Module to which this channel belongs. */
    protected final EmuModule module;

    /** Object used by Emu to create this channel. */
    protected final DataTransport dataTransport;

    /** Type of object to expect in each ring item. */
    protected ModuleIoType ringItemType;

    /** Do we pause the dataThread? */
    protected volatile boolean pause;

    /** Got END command from Run Control. */
    protected volatile boolean gotEndCmd;

    /** Got RESET command from Run Control. */
    protected volatile boolean gotResetCmd;

    //---------------------------------------------------------------------------
    // Used to determine which ring to get event from if multiple output channels
    //---------------------------------------------------------------------------

    /** Total number of module's output channels. */
    protected int outputChannelCount;

    /** Total number of module's event-building threads and therefore output ring buffers. */
    protected int outputRingCount;

    /** This output channel's order in relation to the other output channels
     * for module, starting at 0. First event goes to channel 0, etc. */
    protected int outputIndex;

    /**
     * If multiple SEBs exist, since all DCs are connected to all SEBs, each DC must send
     * the same number of buildable events to each SEB in the proper sequence for building
     * to take place. This value should be set in the config file by jcedit for module.
     * If module is not a DC or there is only 1 SEB, this is 1.
     */
    protected int sebChunk;

    /** Counter to help read events from correct ring. */
    private int sebChunkCounter;

    private boolean chunkingForSebs;

    /** Ring that the next event will show up on. */
    protected int ringIndex;

    /**
     * Number of the module's buildable event produced
     * which this channel will output next (starting at 0).
     * Depends on the # of output channels as well as the order of this
     * channel (outputIndex).
     */
    protected long nextEvent;

    /** Ring that the END event will show up on. */
    protected volatile int ringIndexEnd;

    /** END event's number or index (currently not used). */
    protected volatile long eventIndexEnd;

    /** Keep track of output channel thread's state. */
    protected enum ThreadState {RUNNING, DONE, INTERRUPTED};

    //-------------------------------------------
    // Disruptor (RingBuffer)  Stuff
    //-------------------------------------------

    // Input
    /** Ring buffer - one per input channel. */
    protected RingBuffer<RingItem> ringBufferIn;

    /** Number of items in input ring buffer. */
    protected int inputRingItemCount  = 2048;

    // Output
    /** Array holding all ring buffers for output. */
    protected RingBuffer<RingItem>[] ringBuffersOut;

    /** Number of items in output ring buffers. */
    protected int outputRingItemCount = 4096;

    /** One barrier for each output ring. */
    protected SequenceBarrier[] sequenceBarriers;

    /** One sequence for each output ring. */
    protected Sequence[] sequences;

    /** Index of next ring item. */
    protected long[] nextSequences;

    /** Maximum index of available ring items. */
    protected long[] availableSequences;



    /**
     * Constructor to create a new DataChannel instance.
     * Used only by a transport's createChannel() method
     * which is only called during PRESTART in the Emu.
     *
     * @param name          the name of this channel
     * @param transport     the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     * @param emu           emu this channel belongs to
     * @param module        module this channel belongs to
     * @param outputIndex   order in which module's events will be sent to this
     *                      output channel (0 for first output channel, 1 for next, etc.).
    */
    public DataChannelAdapter(String name, DataTransport transport,
                              Map<String, String> attributeMap,
                              boolean input, Emu emu, EmuModule module,
                              int outputIndex) {
        this.emu = emu;
        this.name = name;
        this.input = input;
        this.module = module;
        this.outputIndex = outputIndex;
        this.dataTransport = transport;
        logger = emu.getLogger();

        if (input) {
            ringItemType = module.getInputRingItemType();
//logger.info("      DataChannel Adapter: input type = " + ringItemType);
        }
        else {
            ringItemType = module.getOutputRingItemType();
//logger.info("      DataChannel Adapter: output type = " + ringItemType);
        }


        // Set id number. Use any defined in config file, else use default = 0
        id = 0;
        String attribString = attributeMap.get("id");
        if (attribString != null) {
            try {
                id = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {}
        }

        if (!input) {
            // Do we send out single events or do we
            // marshall them into one output buffer?
            singleEventOut = false;
            attribString = attributeMap.get("single");
            if (attribString != null) {
                if (attribString.equalsIgnoreCase("true") ||
                        attribString.equalsIgnoreCase("on")   ||
                        attribString.equalsIgnoreCase("yes"))   {
                    singleEventOut = true;
                }
            }

            // Set endianness of output data, must be same as its module
            byteOrder = module.getOutputOrder();
            logger.info("      DataChannel Adapter: byte order = " + byteOrder);

            // Set number of data output ring buffers (1 for each build thread)
            outputRingCount = module.getEventProducingThreadCount();
logger.info("      DataChannel Adapter: # of ring buffers = " + outputRingCount);

            // Create RingBuffers
            ringBuffersOut = new RingBuffer[outputRingCount];
            setupOutputRingBuffers();
        }
        else {
           // Create RingBuffers
            setupInputRingBuffers();
        }
     }


    /** Setup the output channel ring buffers. */
    void setupOutputRingBuffers() {
        sequenceBarriers = new SequenceBarrier[outputRingCount];
        sequences = new Sequence[outputRingCount];

        nextSequences = new long[outputRingCount];
        availableSequences = new long[outputRingCount];
        Arrays.fill(availableSequences, -1L);

        for (int i=0; i < outputRingCount; i++) {
            if (ringItemType == ModuleIoType.PayloadBuffer) {
                ringBuffersOut[i] =
                        createSingleProducer(new RingItemFactory(ModuleIoType.PayloadBuffer),
                                             outputRingItemCount, new YieldingWaitStrategy());
            }
            else {
                ringBuffersOut[i] =
                        createSingleProducer(new RingItemFactory(ModuleIoType.PayloadBank),
                                             outputRingItemCount, new YieldingWaitStrategy());
            }

            // One barrier for each ring
            sequenceBarriers[i] = ringBuffersOut[i].newBarrier();
            sequenceBarriers[i].clearAlert();

            // One sequence for each ring for reading in output channel
            sequences[i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
            ringBuffersOut[i].addGatingSequences(sequences[i]);
            nextSequences[i] = sequences[i].get() + 1L;
        }
    }


    /** Setup the input channel ring buffers. */
    void setupInputRingBuffers() {
        if (ringItemType == ModuleIoType.PayloadBuffer) {
            ringBufferIn =
                    createSingleProducer(new RingItemFactory(ModuleIoType.PayloadBuffer),
                                         inputRingItemCount, new YieldingWaitStrategy());
        }
        else {
            ringBufferIn =
                    createSingleProducer(new RingItemFactory(ModuleIoType.PayloadBank),
                                         inputRingItemCount, new YieldingWaitStrategy());
        }
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        // Need to set up a few things for all output channels
        if (input) return;

        // Get more info from module
        sebChunk = module.getSebChunk();
        outputChannelCount = module.getOutputChannels().size();
        // This method is run after all channels are created
        // and therefore module knows if it is seb chunking.
        chunkingForSebs = module.getSebChunking();
System.out.println("      DataChannel Adapter: chunking for SEBs = " + chunkingForSebs);

        // Initialize counter
        sebChunkCounter = sebChunk;
        // Initialize the event number
        nextEvent = outputIndex * sebChunk;
        // Initialize the ring number
        ringIndex = (int) (nextEvent % outputRingCount);
//System.out.println("      DataChannel Adapter: prestart, nextEv (" + nextEvent + "), ringIndex (" + ringIndex + ")");
    }


//    /**
//     * Set the index of the next event to get from the module
//     * and the ring it will appear on.
//     */
//    protected void setNextEventAndRing() {
//        if (chunkingForSebs && (--sebChunkCounter > 0)) {
//            nextEvent++;
//            ringIndex = (int) (nextEvent % outputRingCount);
////System.out.println("      DataChannel Adapter: set next ev (" + nextEvent + "), ring (" + ringIndex +
////                           "), sebChunkCounter = " + sebChunkCounter);
//            return;
//        }
//
//        sebChunkCounter = sebChunk;
//        nextEvent += sebChunk*(outputChannelCount - 1) + 1;
//        ringIndex = (int) (nextEvent % outputRingCount);
////System.out.println("      DataChannel Adapter: set next ev (" + nextEvent + "), ring (" + ringIndex + ")");
//    }
//

    /**
     * Set the index of the next event to get from the module
     * and the ring it will appear on.<p>
     * NOTE: only called IFF outputRingCount > 1.
     */
    protected void setNextEventAndRing() {
        if (chunkingForSebs) {
            if (--sebChunkCounter > 0) {
                nextEvent++;
                ringIndex = (int) (nextEvent % outputRingCount);
//System.out.println("      DataChannel Adapter: set next ev (" + nextEvent + "), ring (" + ringIndex +
//                           "), sebChunkCounter = " + sebChunkCounter);
                return;
            }

            sebChunkCounter = sebChunk;
            nextEvent += sebChunk*(outputChannelCount - 1) + 1;
            ringIndex = (int) (nextEvent % outputRingCount);
//System.out.println("      DataChannel Adapter: set next ev (" + nextEvent + "), ring (" + ringIndex + ")");
        }
        // We may have multiple build threads (but are NOT chunking).
        // In this case, just switch between output channel rings,
        // (nextEvent is not used). NOTE: only here if outputRingCount > 1.
        else {

            nextEvent += sebChunk*(outputChannelCount - 1) + 1;
            ringIndex = (int) (nextEvent % outputRingCount);
//System.out.println("      DataChannel Adapter: set next ev (" + nextEvent + "), ring (" + ringIndex + ")");



//            ringIndex = ++ringIndex % outputRingCount;
//System.out.println("      DataChannel Adapter: set next ring (" + ringIndex + ")");
        }
    }


    /** {@inheritDoc} */
    public EmuModule getModule() {return module;}

    /** {@inheritDoc} */
    public int getID() {return id;}

    /** {@inheritDoc} */
    public void setID(int id) {this.id = id;}

    /** {@inheritDoc} */
    public int getRecordId() {return recordId;}

    /** {@inheritDoc} */
    public void setRecordId(int recordId) {this.recordId = recordId;}

    /** {@inheritDoc} */
    public State state() {return state;}

    /** {@inheritDoc} */
    public String getError() {return errorMsg.get();}

    /** {@inheritDoc} */
    public String name() {return name;}

    /** {@inheritDoc} */
    public boolean isInput() {return input;}

    /** {@inheritDoc} */
    public DataTransport getDataTransport() {return dataTransport;}

    /** {@inheritDoc} */
    public int getOutputRingCount() {return outputRingCount;}

    /** {@inheritDoc} */
    public RingBuffer<RingItem> getRingBufferIn() {return ringBufferIn;}

    /** {@inheritDoc} */
    public RingBuffer<RingItem>[] getRingBuffersOut() {return ringBuffersOut;}


    /** {@inheritDoc} */
    public void registerEndCallback(EmuEventNotify callback) {endCallback = callback;}

    /** {@inheritDoc} */
    public EmuEventNotify getEndCallback() {return endCallback;}

    /** {@inheritDoc} */
    public void processEnd(long eventIndex, int ringIndex) {
        eventIndexEnd = eventIndex;
        ringIndexEnd  = ringIndex;
    }

    /**
     * Gets the next ring buffer item placed there by the last module.
     * Only call from one thread. MUST be followed by call to
     * {@link #releaseOutputRingItem(int)} AFTER the returned item
     * is used or nothing will work right.
     *
     * @param ringIndex ring buffer to take item from
     * @return next ring buffer item
     */
    protected RingItem getNextOutputRingItem(int ringIndex)
            throws InterruptedException, EmuException {

        RingItem item = null;
//            System.out.println("getNextOutputRingITem: index = " + ringIndex);
//            System.out.println("                     : availableSequences = " + availableSequences[ringIndex]);
//            System.out.println("                     : nextSequences = " + nextSequences[ringIndex]);

        try  {
            // Only wait for read-volatile-memory if necessary ...
            if (availableSequences[ringIndex] < nextSequences[ringIndex]) {
//System.out.println("getNextOutputRingITem: WAITING");
                availableSequences[ringIndex] = sequenceBarriers[ringIndex].waitFor(nextSequences[ringIndex]);
            }
//System.out.println("getNextOutputRingITem: available seq = " + availableSequences[ringIndex]);
//System.out.println("getNextOutputRingITem: try seq = " + nextSequences[ringIndex]);

            item = ringBuffersOut[ringIndex].get(nextSequences[ringIndex]);
//System.out.println("getNextOutputRingITem: got seq = " + nextSequences[ringIndex]);
//System.out.println("Got ring item " + item.getRecordId());
        }
        catch (final com.lmax.disruptor.TimeoutException ex) {
            // never happen since we don't use timeout wait strategy
            ex.printStackTrace();
        }
        catch (final AlertException ex) {
            ex.printStackTrace();
            throw new EmuException("Ring buffer error",ex);
        }

        return item;
    }


    /**
     * Releases the item obtained by calling {@link #getNextOutputRingItem(int)},
     * so that it may be reused for writing into by the last module.
     * And it prepares to get the next ring item when that method is called.
     *
     * Must NOT be used in conjunction with {@link #releaseOutputRingItem(int)}
     * and {@link #gotoNextRingItem(int)}.
     *
     * @param ringIndex ring buffer to release item to
     */
    protected void releaseCurrentAndGoToNextOutputRingItem(int ringIndex) {
        sequences[ringIndex].set(nextSequences[ringIndex]);
        nextSequences[ringIndex]++;
    }

    //
    // The next 2 methods are to be used together in place of the above method.
    //

    /**
     * Releases the item obtained by calling {@link #getNextOutputRingItem(int)},
     * so that it may be reused for writing into by the last module.
     * Must NOT be used in conjunction with {@link #releaseCurrentAndGoToNextOutputRingItem(int)}
     * and must be called after {@link #gotoNextRingItem(int)}.
     * @param ringIndex ring buffer to release item to
     */
    protected void releaseOutputRingItem(int ringIndex) {
//System.out.println("releaseOutputRingItem: got seq = " + (nextSequences[ringIndex]-1));
        sequences[ringIndex].set(nextSequences[ringIndex] - 1);
    }

    /**
     * It prepares to get the next ring item after {@link #getNextOutputRingItem(int)}
     * is called.
     * Must NOT be used in conjunction with {@link #releaseCurrentAndGoToNextOutputRingItem(int)}
     * and must be called before {@link #releaseOutputRingItem(int)}.
     * @param ringIndex ring buffer to release item to
     */
    protected void gotoNextRingItem(int ringIndex) {
//System.out.println("gotoNextRingItem: got seq = " + (nextSequences[ringIndex]+1));
        nextSequences[ringIndex]++;
    }

}
