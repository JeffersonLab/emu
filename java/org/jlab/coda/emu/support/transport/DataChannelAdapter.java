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
import org.jlab.coda.emu.*;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODAStateMachineAdapter;
import org.jlab.coda.emu.support.codaComponent.CODAStateIF;
import org.jlab.coda.emu.support.control.CmdExecException;
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
public abstract class DataChannelAdapter extends CODAStateMachineAdapter implements DataChannel {

    /** Channel id (corresponds to sourceId of ROCs for CODA event building). */
    protected int id;

    /** Record id (corresponds to evio events flowing through data channel). */
    protected int recordId;

    /** For ET output channel of ER, select a prescaled number of events to be sent. */
    protected int prescale;

    /**
     * If we're a PEB or SEB and want to send 1 evio event per et-buffer/cmsg-message,
     * then this is true. If we want to cram as many evio events as possible in each
     * et-buffer/cmsg-message, then this is false.
     * In the EMU domain, do we send out individual events (true) or do we
     * marshall them into one evio-file-format buffer (false)?
     */
    protected boolean singleEventOut;

    /** Channel state. */
    protected CODAStateIF channelState;

    /**
     * Channel error message. reset() sets it back to null.
     * Making this an atomically settable String ensures that only 1 thread
     * at a time can change its value. That way it's only set once per error.
     */
    protected AtomicReference<String> errorMsg = new AtomicReference<>();

    /** Channel name */
    protected final String name;

    /** Any error reading/parsing data can be ignored if true.
     * Currently this is only implemented for ER's ET input channel if the main data
     * is coming over another input (presumably emu socket) channel. */
    protected boolean ignoreDataErrors;

    /** Is this channel an input or output fifo channel? Convenience variable. */
    protected final boolean isFifo;

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

    /** Object used by Emu to be notified of PRESTART event arrival. */
    protected EmuEventNotify prestartCallback;

    /** Module to which this channel belongs. */
    protected final EmuModule module;

    /** Object used by Emu to create this channel. */
    protected final DataTransport dataTransport;

    /** Do we pause the dataThread? */
    protected volatile boolean pause;

    /** Got END command from Run Control. */
    protected volatile boolean gotEndCmd;

    /** Got RESET command from Run Control. */
    protected volatile boolean gotResetCmd;

    /** List of destination IP addresses. */
    protected String[] ipAddrList;

    /** List of broadcast addresses corresponding to ipAddrList. */
    protected String[] bAddrList;

    //---------------------------------------------------------------------------
    // OUTPUT DATA FLOW
    //---------------------------------------------------------------------------

    /** Use both bufsPerSec and eventsPerBuffer to regulate the data flow out of this channel. */
    protected boolean regulateBufferRate;

    /** Number of buffers per second of data to write over the output channel. */
    protected double buffersPerSec;

    /** Number of milliseconds between each buffer being sent. */
    protected long nanoSecPerBuf;

    /** Number of events to place into one data buffer for the output channel. */
    protected int eventsPerBuffer;

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

    /** Ring that the next event will show up on. */
    protected int ringIndex;

    /**
     * Number of the module's buildable event produced
     * which this channel will output next (starting at 0).
     * Depends on the # of output channels as well as the order of this
     * channel (outputIndex).
     */
    protected long nextEvent;

    /** Keep track of output channel thread's state. */
    protected enum ThreadState {RUNNING, DONE, INTERRUPTED};

    //-------------------------------------------
    // Disruptor (RingBuffer)  Stuff
    //-------------------------------------------

    // Input
    /** Ring buffer - one per input channel. */
    protected RingBuffer<RingItem> ringBufferIn;

    /** Number of items in input ring buffer. */
    protected int inputRingItemCount;

    // Output
    /** Array holding all ring buffers for output. */
    protected RingBuffer<RingItem>[] ringBuffersOut;

    /** Number of items in output ring buffers. */
    protected int outputRingItemCount;

    /** Number of items in output channel's internal ByteBuffer supplies. */
    protected int outputModuleInternalRingCount;

    /** One barrier for each output ring. */
    protected SequenceBarrier[] sequenceBarriers;

    /** One sequence for each output ring. */
    protected Sequence[] sequences;

    /** Index of next ring item. */
    protected long[] nextSequences;

    /** Maximum index of available ring items. */
    protected long[] availableSequences;

    /** When releasing in sequence, the last sequence to have been released. */
    private long[] lastSequencesReleased;

    /** When releasing in sequence, the highest sequence to have asked for release. */
    private long[] maxSequences;

    /** When releasing in sequence, the number of sequences between maxSequence &
     * lastSequenceReleased which have called release(), but not been released yet. */
    private int[] betweens;

    /** Number of entries (filled slots) in each ring buffer of this output channel. */
    protected int[] outputChannelFill;

    /** Total number of slots in all output channel ring buffers. */
    protected int totalRingCapacity;




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


        // Set id number. Use any defined in config file, else use default = 0
        id = 0;
        String attribString = attributeMap.get("id");
        if (attribString != null) {
            try {
                id = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {}
        }

        // can we ignore data reading/parsing errors?
        ignoreDataErrors = false;
        attribString = attributeMap.get("ignoreErrors");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("true") ||
                attribString.equalsIgnoreCase("on")   ||
                attribString.equalsIgnoreCase("yes"))   {
                ignoreDataErrors = true;
            }
        }

logger.info("      DataChannel Adapter: ignore data errors = " + ignoreDataErrors);

        // Fifos are treated a little differently than other channels since 1 fifo is both
        // an in & out channel. THE OUTPUT FIFO IS CREATED FIRST (emu config forces this).
        // It then creates and defines the channel. The input channel uses what is created
        // by its output counterpart.
        //
        // As part of the fifo, there is one input ring buffer for each of the module's
        // event-producing threads, which are used to get events into the fifo.
        // And there is an additional output ring buffer for the input fifo channel to read from.
        // The procedure is to get events from the input rings and transfer them to the
        // output ring.
        isFifo = this instanceof DataChannelImplFifo;
logger.info("      DataChannel Adapter: channel " + name + " is a fifo = " + isFifo);

        if (input) {
            // FIFO code never makes it here.
            // Each FIFO must be listed in config file with output fifo coming first
            // and its input counterpart coming second. Thus, when initially constructed,
            // it is always as an output channel. To get the already-existing fifo as an
            // input channel, a lookup is done.
            // Input Fifo calls setupInputRingBuffers(), but does not construct this channel.

            // Set the number of items for the input ring buffers.
            // These contain evio events parsed from ET, cMsg,
            // or Emu domain buffers. They should not take up much mem.
            // Or it can contain fully built events from fifo.
            inputRingItemCount = 4096;

            attribString = attributeMap.get("ringSize");
            if (attribString != null) {
                try {
                    // Make sure it's a power of 2, rounded up
                    int ringSize = EmuUtilities.powerOfTwo(Integer.parseInt(attribString), true);
                    if (ringSize < 128) {
                        ringSize = 128;
                    }
                    inputRingItemCount = ringSize;
                }
                catch (NumberFormatException e) {}
            }

            // Create RingBuffers
            setupInputRingBuffers();
            logger.info("      DataChannel Adapter: input ring item count -> " + inputRingItemCount);
        }
        else {
            // Set the number of items for the output chan ring buffers.
            // We don't need any more slots than we have internal buffers.
            // The number returned by getInternalRingCount is for a single build thread
            // times the number of build threads. Since a channel has one ring for each
            // build thread, the # of items in any one ring is
            // getInternalRingCount / buildThreadCount. Should be a power of 2 already
            // but will enforce that.

            outputModuleInternalRingCount = module.getInternalRingCount();
            outputRingItemCount = outputModuleInternalRingCount/module.getEventProducingThreadCount();
            // If the module is not setting this, then it has no internal
            // ring of buffers, so set this to some reasonable value.
            if (outputRingItemCount < 1) {
                outputRingItemCount = 128;
            }

            // Event recorder has no internal byte buffer supply, so we can make this big
            if (emu.getCodaClass().isEventRecorder()) {
                outputRingItemCount = 4096;
            }

            outputRingItemCount = EmuUtilities.powerOfTwo(outputRingItemCount, false);
logger.info("      DataChannel Adapter: output ring item count -> " + outputRingItemCount);

            prescale = 1;
            attribString = attributeMap.get("prescale");
            if (attribString != null) {
                try {
                    int val = Integer.parseInt(attribString);
                    if (val < 1) {
                        val = 1;
                    }
                    prescale = val;
                }
                catch (NumberFormatException e) {}
logger.info("      DataChannel Adapter: output prescale -> " + prescale);
            }

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
logger.info("      DataChannel Adapter: output ring buffer count (1/buildthread) = " + outputRingCount);

            // Create RingBuffers
            ringBuffersOut = new RingBuffer[outputRingCount];
            setupOutputRingBuffers();

            // Total capacity of all ring buffers
            totalRingCapacity = outputRingCount * outputRingItemCount;

            // Init arrays
            lastSequencesReleased = new long[outputRingCount];
            maxSequences = new long[outputRingCount];
            betweens = new int[outputRingCount];
            Arrays.fill(lastSequencesReleased, -1L);
            Arrays.fill(maxSequences, -1L);
            Arrays.fill(betweens, 0);
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
            ringBuffersOut[i] =
                createSingleProducer(new RingItemFactory(),
                                     outputRingItemCount,
          new SpinCountBackoffWaitStrategy(30000, new LiteBlockingWaitStrategy()));
          //new PhasedBackoffWaitStrategy(500L, 1000L, TimeUnit.NANOSECONDS, new LiteBlockingWaitStrategy()));
          //new LiteBlockingWaitStrategy());
          //new SleepingWaitStrategy(1000, 200000));   // 200 usec
          //new YieldingWaitStrategy());
          //new BlockingWaitStrategy());

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
        if (isFifo) {
            // If this is a fifo, the output channel creates the channel.
            // Don't need more than the # of internal byte buffers of the module on
            // side filling the fifo (output).
            inputRingItemCount = outputModuleInternalRingCount;
            inputRingItemCount = EmuUtilities.powerOfTwo(inputRingItemCount, true);
            logger.info("      DataChannel Adapter: setupInputRingBuffers, fifo, input ring item count = " + inputRingItemCount);
        }

        ringBufferIn = createSingleProducer(new RingItemFactory(),
                                            inputRingItemCount,
        new SpinCountBackoffWaitStrategy(30000, new LiteBlockingWaitStrategy()));
        //new PhasedBackoffWaitStrategy(500L, 1000L, TimeUnit.NANOSECONDS, new LiteBlockingWaitStrategy()));
        //new BusySpinWaitStrategy());
        //new SleepingWaitStrategy(1000, 200000));   // 200 usec
        //new LiteBlockingWaitStrategy());
        //new BlockingWaitStrategy());
        //new YieldingWaitStrategy());
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        // Need to set up a few things for all output channels
        if (input) return;

        // Get output channel count from module
        outputChannelCount = module.getOutputChannels().size();

        // Initialize the event number (first buildable event)
        nextEvent = outputIndex;

        // Initialize the ring number (of first buildable event)
        ringIndex = (int) (nextEvent % outputRingCount);

//System.out.println("      DataChannel Adapter: prestart, nextEv (" +
//                           nextEvent + "), ringIndex (" + ringIndex + ')' +
//                    ", output channel count = (" + outputChannelCount + ")");
    }


    /**
     * Set the index of the next buildable event to get from the module
     * and the ring it will appear on.<p>
     * NOTE: only called IFF outputRingCount > 1.
     *
     * @return index of ring that next event will be placed in
     */
    protected int setNextEventAndRing() {
//        System.out.print("      DataChannel Adapter:next ev (" + nextEvent + ") -> (" +
//                                   nextEvent + " + " + outputChannelCount + ") = ");
        nextEvent += outputChannelCount;
//System.out.println(nextEvent);

        ringIndex = (int) (nextEvent % outputRingCount);
//System.out.println("      DataChannel Adapter: set next ev (" + nextEvent + "), nextEv % bt -> (" +
//                           nextEvent + " % " + outputRingCount + ")");
        return ringIndex;
    }


    /** {@inheritDoc} */
    abstract public TransportType getTransportType();

    /** {@inheritDoc} */
    public EmuModule getModule() {return module;}

    /** {@inheritDoc} */
    public int getID() {return id;}

    /** {@inheritDoc} */
    public int getRecordId() {return recordId;}

    /** {@inheritDoc} */
    public void setRecordId(int recordId) {this.recordId = recordId;}

    /** {@inheritDoc} */
    public CODAStateIF state() {return channelState;}

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
    public void registerPrestartCallback(EmuEventNotify callback) {prestartCallback = callback;}

    /** {@inheritDoc} */
    public EmuEventNotify getPrestartCallback() {return prestartCallback;}

    public long getNextSequence(int ringIndex) {
        if (ringIndex < 0) return -1L;
        return nextSequences[ringIndex];
    }

    /** {@inheritDoc} */
    public int getInputLevel() {return 0;}

    /** {@inheritDoc} */
    public int getOutputLevel() {
        int count=0;

        for (int i=0; i < outputRingCount; i++) {
            // getCursor() does 1 volatile read to get max available sequence.
            // It's important to calculate the output channel level this way,
            // especially for the ET channel since it may get stuck waiting for
            // new ET events to become available and not be able to update ring
            // statistics (more specifically, availableSequences[]).
            //count += (int)(sequenceBarriers[i].getCursor() - nextSequences[i] + 1);

            count += (int)(ringBuffersOut[i].getCursor() - nextSequences[i] + 1);
        }

        // When (cursor(or avail) - next + 1) == ringSize, then the Q is full.
        return count*100/totalRingCapacity;
    }

    /** {@inheritDoc} */
    public void setDestinationIpList(String[] ipList) {ipAddrList = ipList;}

    /** {@inheritDoc} */
    public void setDestinationBaList(String[] baList) {bAddrList = baList;}

    /** {@inheritDoc} */
    public int getPrescale() {return prescale;}

    /** {@inheritDoc} */
    public void regulateOutputBufferRate(int eventsPerBuffer, double buffersPerSec) {
        this.buffersPerSec = buffersPerSec;
        this.eventsPerBuffer = eventsPerBuffer;
        nanoSecPerBuf = (long) (1000000000./ buffersPerSec);
        regulateBufferRate = true;
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
            // Only wait if necessary ...
            if (availableSequences[ringIndex] < nextSequences[ringIndex]) {
//System.out.println("getNextOutputRingITem: WAITING");
                availableSequences[ringIndex] =
                        sequenceBarriers[ringIndex].waitFor(nextSequences[ringIndex]);
            }
//System.out.println("getNextOutputRingITem: available seq[" + ringIndex + "] = " +
//                           availableSequences[ringIndex] +
//                        ", next seq = " + nextSequences[ringIndex] +
//" delta + 1 = " + (availableSequences[ringIndex] - nextSequences[ringIndex] +1));

            item = ringBuffersOut[ringIndex].get(nextSequences[ringIndex]);
//System.out.println("getNextOutputRingItem: got seq[" + ringIndex + "] = " + nextSequences[ringIndex]);
//System.out.println("Got ring item " + item.getRecordId());
        }
        catch (final com.lmax.disruptor.TimeoutException ex) {
            // never happen since we don't use timeout wait strategy
        }
        catch (final AlertException ex) {
            emu.setErrorState("Channel Adapter: ring buf alert");
            channelState = CODAState.ERROR;
            throw new EmuException("Channel Adapter: ring buf alert");
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
//        System.out.print("releaseCurrentAndGoToNextOutputRingItem: rel[" + ringIndex +
//                                 "] = " + nextSequences[ringIndex]);
        nextSequences[ringIndex]++;
//        System.out.println("  ->  " + nextSequences[ringIndex]);
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


    /**
     * Releases the items obtained by calling {@link #getNextOutputRingItem(int)},
     * so that it may be reused for writing into by the last module.
     * Must NOT be used in conjunction with {@link #releaseCurrentAndGoToNextOutputRingItem(int)}
     * or {@link #releaseOutputRingItem(int)}
     * and must be called after {@link #gotoNextRingItem(int)}.<p>
     *
     * This method <b>ensures</b> that sequences are released in order and is thread-safe.
     * Only works if each ring item is released individually.
     *
     * @param ringIndexes array of ring buffers to release item to
     * @param seqs array of sequences to release.
     * @param len number of array items to release
     */
    synchronized protected void sequentialReleaseOutputRingItem(byte[] ringIndexes, long[] seqs, int len) {
        long seq;
        int ringIndex;

        for (int i=0; i < len; i++) {
            seq = seqs[i];
            ringIndex = ringIndexes[i];

            // If we got a new max ...
            if (seq > maxSequences[ringIndex]) {
                // If the old max was > the last released ...
                if (maxSequences[ringIndex] > lastSequencesReleased[ringIndex]) {
                    // we now have a sequence between last released & new max
                    betweens[ringIndex]++;
                }

                // Set the new max
                maxSequences[ringIndex] = seq;
//System.out.println("    set max seq = " + seq + " for ring " + ringIndex);
            }
            // If we're < max and > last, then we're in between
            else if (seq > lastSequencesReleased[ringIndex]) {
                betweens[ringIndex]++;
//System.out.println("    add in between seq = " + seq + " for ring " + ringIndex);
            }

            // If we now have everything between last & max, release it all.
            // This way higher sequences are never released before lower.
            if ( (maxSequences[ringIndex] - lastSequencesReleased[ringIndex] - 1L) == betweens[ringIndex]) {
//System.out.println("    release seq " + sequences[ringIndex] + " for ring " + ringIndex);
                sequences[ringIndex].set(maxSequences[ringIndex]);
                lastSequencesReleased[ringIndex] = maxSequences[ringIndex];
                betweens[ringIndex] = 0;
            }
        }
    }


}
