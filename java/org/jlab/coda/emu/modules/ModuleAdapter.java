/*
 * Copyright (c) 2014, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.modules;

import com.lmax.disruptor.RingBuffer;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuEventNotify;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODAStateIF;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


/**
 * This class contains boilerplate code for implementing a module.
 *
 * @author timmer
 * Mar 20, 2014
 */
public class ModuleAdapter implements EmuModule {


    /** ID number of this module obtained from config file. */
    protected int id;

    /** Number of event producing threads in operation. Each
     *  must match up with its own output channel ring buffer. */
    protected int eventProducingThreads;

    /** Were the number of event producing threads explicitly set in config file? */
    protected boolean epThreadsSetInConfig;

    /** Name of this module. */
    protected final String name;

    /**
     * Possible error message. reset() sets it back to null.
     * Making this an atomically settable String ensures that only 1 thread
     * at a time can change its value. That way it's only set once per error.
     */
    protected AtomicReference<String> errorMsg = new AtomicReference<>();

    /** Emu this module belongs to. */
    protected final Emu emu;

    /** Logger used to log messages to debug console. */
    protected final Logger logger;

    /** State of this module. */
    protected volatile CODAStateIF moduleState = CODAState.BOOTED;

    /** Map containing attributes of this module given in config file. */
    protected final Map<String,String> attributeMap;

    /**
     * ArrayList of DataChannel objects for this module that are inputs.
     * It is only modified in the {@link #addInputChannels(ArrayList)} and
     * {@link #clearChannels()} methods and then only by the main EMU thread
     * in prestart. However, other threads (such as the EMU's statistics reporting
     * thread) call methods which use its iterator or getters.
     */
    protected ArrayList<DataChannel> inputChannels = new ArrayList<>(16);

    /** ArrayList of DataChannel objects that are outputs. Only modified in prestart
     *  but used during go when writing module output. */
    protected ArrayList<DataChannel> outputChannels = new ArrayList<>(4);

    /** Number of output channels. */
    protected int inputChannelCount;

    /** Number of output channels. */
    protected int outputChannelCount;

    /** User hit PAUSE button if {@code true}. */
    protected boolean paused;

    /** Object used by Emu to be notified of END event arrival. */
    protected EmuEventNotify endCallback;

    /** Object used by Emu to be notified of PRESTART event arrival. */
    protected EmuEventNotify prestartCallback;

    /** Do we produce big or little endian output in ByteBuffers? */
    protected ByteOrder outputOrder;

    /** If <code>true</code>, data to be built is from a streaming (not triggered) source. */
    protected boolean streamingData;


    //---------------------------
    // For generating statistics
    //---------------------------

    /** Array containing, for each input channel, the percentage (0-100)
     *  of filled ring space. */
    protected int[] inputChanLevels;

    /** Array containing, for each output channel, the percentage (0-100)
     *  of filled ring space. */
    protected int[] outputChanLevels;

    /** Array containing names for each input channel. */
    protected String[] inputChanNames;

    /** Array containing names for each output channel. */
    protected String[] outputChanNames;

    /** Total number of evio events written to the outputs. */
    protected long eventCountTotal;

    /** Sum of the sizes, in 32-bit words, of all evio events written to the outputs. */
    protected long wordCountTotal;

    /** Total number of time slice frames (timestamps) written to the outputs if streaming. */
    protected long frameCountTotal;

    /** Instantaneous event rate in Hz over the last time period of length {@link #statGatheringPeriod}. */
    protected float eventRate;

    /** Instantaneous word rate in Hz over the last time period of length {@link #statGatheringPeriod}. */
    protected float wordRate;

    /** Instantaneous frame rate in Hz over the last time period of length {@link #statGatheringPeriod}, if streaming. */
    protected float frameRate;

    /** Maximum-sized built event in bytes. */
    protected int maxEventSize;

    /** Minimum-sized built event in bytes. */
    protected int minEventSize;

    /** Average-sized built event in bytes. */
    protected int avgEventSize;

    /** For ET output channels, a suggested value of chunk * ET-buffer-size (bytes)
     *  which would be a good match for the current size events being built. */
    protected int goodChunk_X_EtBufSize;

    /** Histogram of time to build 1 event in nanoseconds
     * (metadata in first 5 elements). */
    protected int[] timeToBuild;

    /** Targeted time period in milliseconds over which instantaneous rates will be calculated. */
    protected static final int statGatheringPeriod = 2000;

    /** If {@code true}, this module's statistics represents that of the EMU. */
    protected boolean representStatistics;

    /** If true, collect statistics on event build times (performance drag). */
    protected boolean timeStatsOn;

    /** Handle histogram of event build times. */
    protected Statistics statistics;

    /** Thread to calculate event and data rates. */
    protected Thread RateCalculator;

    // ---------------------------------------------------


    /**
     * Default constructor for fake TS.
     * @param name name of module.
     * @param emu Emu this module belongs to.
     */
    public ModuleAdapter(String name, Emu emu) {
        this.emu = emu;
        this.name = name;

        logger = null;
        attributeMap = null;
    }


    /**
     * Constructor creates a new EventRecording instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     * @param emu Emu this module belongs to.
     */
    public ModuleAdapter(String name, Map<String, String> attributeMap, Emu emu) {
        this.emu = emu;
        this.name = name;
        this.attributeMap = attributeMap;
        logger = emu.getLogger();


        try {
            id = Integer.parseInt(attributeMap.get("id"));
            if (id < 0)  id = 0;
        }
        catch (NumberFormatException e) { /* default to 0 */ }
        emu.setCodaid(id);


        // Set number of event-producing threads
        eventProducingThreads = 1;
        try {
            eventProducingThreads = Integer.parseInt(attributeMap.get("threads"));
            if (eventProducingThreads < 1) {
                eventProducingThreads = 1;
            }
            else {
                // # of threads explicitly (& properly) set in config file
                // Need this when setting the # of threads in event builder
                // since we want default to be 2 in that case.
                epThreadsSetInConfig = true;
            }
        }
        catch (NumberFormatException e) {}


        // Is output written in big or little endian?
        outputOrder = ByteOrder.BIG_ENDIAN;
        String str = attributeMap.get("endian");
        if (str != null && str.equalsIgnoreCase("little"))   {
            outputOrder = ByteOrder.LITTLE_ENDIAN;
        }
logger.info("  Module Adapter: output byte order = " + outputOrder);


        // Does this module accurately represent the whole EMU's stats?
        representStatistics = true;
        str = attributeMap.get("repStats");
        if (str != null) {
            if (str.equalsIgnoreCase("false") ||
                str.equalsIgnoreCase("off")   ||
                str.equalsIgnoreCase("no"))   {
                representStatistics = false;
            }
        }

        // In EB, make histogram of time to build event.
        // Default is NOT to collect stats of time to build events
        // since it's a real performance killer.
        timeStatsOn = false;
        str = attributeMap.get("timeStats");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                timeStatsOn = true;
            }
        }

        // For FPGAs, fake ROCs, or EBs: is data in streaming format?
        streamingData = false;
        str = attributeMap.get("streaming");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                streamingData = true;
            }
        }

    }


    /**
     * This method is used to place an item onto a specified ring buffer of a
     * single, specified output channel.
     *
     * @param itemOut    the event to place on output channel
     * @param channelNum index of output channel to place item on
     * @param ringNum    index of output channel ring buffer to place item on
     * @throws InterruptedException it thread interrupted.
     */
    protected void eventToOutputChannel(RingItem itemOut, int channelNum, int ringNum)
                        throws InterruptedException{

        // Have any output channels?
        if (outputChannelCount < 1) {
//logger.info("  Module Adapter: no output channel so release event w/o publishing");
            itemOut.releaseByteBuffer();
            return;
        }
//logger.info("  Module Adapter: publishing events in out chan ring");

        RingBuffer rb = outputChannels.get(channelNum).getRingBuffersOut()[ringNum];
        long nextRingItem = rb.nextIntr(1);

        RingItem ri = (RingItem) rb.get(nextRingItem);
        ri.copy(itemOut);
        rb.publish(nextRingItem);
    }

    /**
     * This method is used to place an item onto a specified ring buffer of a
     * single, specified output channel.
     *
     * @param itemOut    the event to place on output channel
     * @param channel    which output channel to place item on
     * @param ringNum    index of output channel ring buffer to place item on
     * @throws InterruptedException it thread interrupted.
     */
    protected void eventToOutputChannel(RingItem itemOut, DataChannel channel, int ringNum)
        throws InterruptedException{

        // Have any output channels?
        if (outputChannelCount < 1) {
//            logger.info("  Module Adapter: no output channel so release event w/o publishing");
            itemOut.releaseByteBuffer();
            return;
        }

        RingBuffer rb = channel.getRingBuffersOut()[ringNum];
        long nextRingItem = rb.nextIntr(1);

        RingItem ri = (RingItem) rb.get(nextRingItem);
        ri.copy(itemOut);
        rb.publish(nextRingItem);
    }


    /** {@inheritDoc} */
    public String[] getInputNames() {
        // The values in this array are set in the EB & ER modules
        return inputChanNames;
    }


    /** {@inheritDoc} */
    public String[] getOutputNames() {
        // The values in this array are set in the EB & ER modules
        return outputChanNames;
    }


    /** {@inheritDoc} */
    public int[] getOutputLevels() {
        // The values in this array need to be obtained from each output channel
        if (outputChanLevels != null) {
            int i=0;
            for (DataChannel chan : outputChannels) {
                outputChanLevels[i++] = chan.getOutputLevel();
            }
        }
        return outputChanLevels;
    }


    /** {@inheritDoc} */
    public int[] getInputLevels() {
        // The values in this array need to be obtained from each input channel
        if (inputChanLevels != null) {
            int i=0;
            for (DataChannel chan : inputChannels) {
                inputChanLevels[i++] = chan.getInputLevel();
            }
        }
        return inputChanLevels;
    }


    //-----------------------------------------------------------
    // For CODAStateMachine interface
    //
    // In general, go(), end(), prestart(), download(), and reset()
    // will be overridden in modules which extend this class.
    //-----------------------------------------------------------

    /** {@inheritDoc} */
    public void go()       throws CmdExecException {moduleState = CODAState.ACTIVE;}
    /** {@inheritDoc} */
    public void end()      throws CmdExecException {moduleState = CODAState.DOWNLOADED;}
    /** {@inheritDoc} */
    public void pause()    {paused = true;}
    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {moduleState = CODAState.PAUSED;}
    /** {@inheritDoc} */
    public void download() throws CmdExecException {moduleState = CODAState.DOWNLOADED;}
    /** {@inheritDoc} */
    public void reset()   {moduleState = CODAState.CONFIGURED;}

    /** {@inheritDoc} */
    public void registerEndCallback(EmuEventNotify callback) {endCallback = callback;}
    /** {@inheritDoc} */
    public EmuEventNotify getEndCallback() {return endCallback;}

    /** {@inheritDoc} */
    public void registerPrestartCallback(EmuEventNotify callback) {prestartCallback = callback;}
    /** {@inheritDoc} */
    public EmuEventNotify getPrestartCallback() {return prestartCallback;}

    //-----------------------------------------------------------
    // For StatedObject interface
    //-----------------------------------------------------------

    /** {@inheritDoc} */
    public CODAStateIF state() {return moduleState;}
    /** {@inheritDoc} */
    public String getError() {return errorMsg.get();}

    //-----------------------------------------------------------
    // For EmuModule interface
    //-----------------------------------------------------------

    /** {@inheritDoc} */
    public String getAttr(String name) throws DataNotFoundException {
        String attribute = attributeMap.get(name);
        if (attribute == null) throw new DataNotFoundException("attribute " +
                                         name + " not found in config for " + name());
        return attribute;
    }

    /** {@inheritDoc} */
    public int getIntAttr(String name) throws DataNotFoundException,
                                              NumberFormatException {
        return Integer.valueOf(getAttr(name));
    }

    /** {@inheritDoc} */
    public String name() {return name;}

    /** {@inheritDoc} */
    public int getInternalRingCount() {return 0;}

    /** {@inheritDoc} */
    public boolean representsEmuStatistics() {return representStatistics;}

    /** {@inheritDoc} */
    synchronized public Object[] getStatistics() {

        Object[] stats = new Object[11];

        // If we're not active, keep the accumulated
        // totals and sizes, but the rates are zero.
        if (moduleState != CODAState.ACTIVE) {
            stats[0] = eventCountTotal;
            stats[1] = wordCountTotal;
            stats[2] = 0F;
            stats[3] = 0F;

            stats[4] = maxEventSize;
            stats[5] = minEventSize;
            stats[6] = avgEventSize;
            stats[7] = goodChunk_X_EtBufSize;
            stats[8] = timeToBuild;

            stats[9]  = frameCountTotal;
            stats[10] = frameRate;
        }
        else {
            if (timeStatsOn && statistics != null) {
                timeToBuild = statistics.fillHistogram();
            }

            stats[0] = eventCountTotal;
            stats[1] = wordCountTotal;
            stats[2] = eventRate;
            stats[3] = wordRate;

            stats[4] = maxEventSize;
            stats[5] = minEventSize;
            stats[6] = avgEventSize;
            stats[7] = goodChunk_X_EtBufSize;
            stats[8] = timeToBuild;

            stats[9]  = frameCountTotal;
            stats[10] = frameRate;
        }

        return stats;
    }

    /** {@inheritDoc} */
    public void adjustStatistics(long eventsAdded, long wordsAdded) {
        eventCountTotal += eventsAdded;
        wordCountTotal  += wordsAdded;
    }

    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {
        if (input_channels == null) return;
        inputChannels.addAll(input_channels);
        inputChannelCount  = inputChannels.size();
    }

    /** {@inheritDoc} */
    public void addOutputChannels(ArrayList<DataChannel> output_channels) {
        if (output_channels == null) return;
        outputChannels.addAll(output_channels);
        outputChannelCount = outputChannels.size();
    }

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getInputChannels() {return inputChannels;}

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getOutputChannels() {return outputChannels;}

    /** {@inheritDoc} */
    public void clearChannels() {
        inputChannels.clear();
        outputChannels.clear();
        inputChannelCount = outputChannelCount = 0;

        outputChanLevels = null;
        outputChanNames  = null;
        inputChanLevels  = null;
        inputChanNames   = null;
    }

    /** {@inheritDoc} */
    public int getEventProducingThreadCount() {return eventProducingThreads;}

    /** {@inheritDoc} */
    public ByteOrder getOutputOrder() {return outputOrder;}

    /** {@inheritDoc} */
    public boolean isStreamingData() {return streamingData;}

    //----------------------------------------------------------------

// TODO: This thread is run for modules, but results are NEVER used!!

    /**
     * This class defines a thread that makes instantaneous rate calculations
     * once every few seconds. Rates can be sent to run control
     * (or stored in local xml config file).
     */
    final class RateCalculatorThread extends Thread {
        /**
         * Method run is the action loop of the thread.
         * Suggested creation and start on PRESTART.
         * Suggested exit on END or RESET.
         */
        @Override
        public void run() {

            // variables for instantaneous stats
            long deltaT, t1, t2, prevEventCount=0L, prevWordCount=0L, prevFrameCount=0L;

            while ((moduleState == CODAState.ACTIVE) || paused) {
                try {
                    // In the paused state only wake every two seconds.
                    sleep(2000);

                    t1 = System.currentTimeMillis();

                    while (moduleState == CODAState.ACTIVE) {
                        sleep(statGatheringPeriod);

                        t2 = System.currentTimeMillis();
                        deltaT = t2 - t1;

                        // calculate rates
                        frameRate = (frameCountTotal - prevFrameCount)*1000F/deltaT;
                        eventRate = (eventCountTotal - prevEventCount)*1000F/deltaT;
                        wordRate  = (wordCountTotal  - prevWordCount)*1000F/deltaT;

                        prevFrameCount = frameCountTotal;
                        prevEventCount = eventCountTotal;
                        prevWordCount  = wordCountTotal;
                        t1 = t2;
//                        System.out.println("event rate = " + eventRate + ", byteRate = " + 4*wordRate);
//                        System.out.println("frame rate = " + frameRate + ", byteRate = " + 4*wordRate);

                        // The following was in the old RateCalculatorThread thread ...
//                        try {
//                            Configurer.setValue(emu.parameters(), "status/eventCount", Long.toString(eventCountTotal));
//                            Configurer.setValue(emu.parameters(), "status/wordCount", Long.toString(wordCountTotal));
//                        }
//                        catch (DataNotFoundException e) {}
                    }

                } catch (InterruptedException e) {
                    logger.info("  Module Adapter: rate calculating thread for " + name() + " exiting");
                    return;
                }
            }
        }
    }


}