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
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


/**
 * This class contains boilerplate code for implementing a module.
 *
 * @author timmer
 * Mar 20, 2014
 */
public class ModuleAdapter implements EmuModule {


    /** ID number of this event recorder obtained from config file. */
    protected int id;

    /** Number of event producing threads in operation. Each
     *  must match up with its own output channel ring buffer. */
    protected int eventProducingThreads;

    /** Were the number of event producing threads explicitly set in config file? */
    protected boolean epThreadsSetInConfig;

    /**
     * If multiple final event builders (SEBs or PEBs) exist, since all DCs are connected
     * to all SEBs, each DC must send the same number of contiguous buildable events to
     * each SEB in the proper sequence for building to take place.
     * This value should be set in the config file by jcedit.
     */
    protected int sebChunk;

    /** When the END event arrives this is its index, starting with 0 for prestart. */
    protected long endEventIndex;

    /** When the END event arrives this the output channel ring it is placed on. */
    protected int endEventRingIndex;

    /**
     * True if we're outputting from DC to multiple SEBs and so we need to chunk up events
     * sent into batches of "sebChunk".
     */
    protected boolean chunkingForSebs;

    /** Name of this event recorder. */
    protected final String name;

    /**
     * Possible error message. reset() sets it back to null.
     * Making this an atomically settable String ensures that only 1 thread
     * at a time can change its value. That way it's only set once per error.
     */
    protected AtomicReference<String> errorMsg = new AtomicReference<String>();

    /** Emu this module belongs to. */
    protected final Emu emu;

    /** Logger used to log messages to debug console. */
    protected final Logger logger;

    /** State of this module. */
    protected volatile State state = CODAState.BOOTED;

    /** Map containing attributes of this module given in config file. */
    protected final Map<String,String> attributeMap;

    /** ArrayList of DataChannel objects that are inputs. */
    protected ArrayList<DataChannel> inputChannels = new ArrayList<DataChannel>();

    /** ArrayList of DataChannel objects that are outputs. */
    protected ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    /** Number of output channels. */
    protected int inputChannelCount;

    /** Number of output channels. */
    protected int outputChannelCount;

    /** User hit PAUSE button if {@code true}. */
    protected boolean paused;

    /** Object used by Emu to be notified of END event arrival. */
    protected EmuEventNotify endCallback;

    /** Comparator which tells queue how to sort elements. */
    protected AttachComparator<Attached> comparator = new AttachComparator<Attached>();

    /** Do we produce big or little endian output in ByteBuffers? */
    protected ByteOrder outputOrder;

    //---------------------------
    // For generating statistics
    //---------------------------

    /** Total number of evio events written to the outputs. */
    protected long eventCountTotal;

    /** Sum of the sizes, in 32-bit words, of all evio events written to the outputs. */
    protected long wordCountTotal;

    /** Instantaneous event rate in Hz over the last time period of length {@link #statGatheringPeriod}. */
    protected float eventRate;

    /** Instantaneous word rate in Hz over the last time period of length {@link #statGatheringPeriod}. */
    protected float wordRate;

    /** Maximum-sized built event in bytes. */
    protected int maxEventSize;

    /** Minimum-sized built event in bytes. */
    protected int minEventSize;

    /** Average-sized built event in bytes. */
    protected int avgEventSize;

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

    /** Thread to calculate event & data rates. */
    protected Thread RateCalculator;

    // ---------------------------------------------------


    /**
     * Constructor creates a new EventRecording instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
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


        // If this is a DC and there are multiple SEBs, set the number of
        // evio events (all of the very same block level) to be sent,
        // in sequence, to a single SEB before sending the same amount
        // to the next SEB. Each SEB must get the same # of events from
        // each DC.
        sebChunk = 1;
        str = attributeMap.get("sebChunk");
        if (str != null) {
            try {
                // Note that the boolean "chunkingforSebs" is set in prestart
                // by adding more than one output channel (see the method
                // addOutputChannels()). Only when that is true, does sebChunk
                // do anything.
                int val = Integer.parseInt(str);
                if (val > 0) {
                    sebChunk = val;
logger.info("  Module Adapter: SEB chunk = " + sebChunk);
                }
            }
            catch (NumberFormatException e) {}
        }


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
    }


    /**
     * This method is used to place an item onto a specified ring buffer of a
     * single, specified output channel.
     *
     * @param itemOut    the event to place on output channel
     * @param channelNum which output channel to place item on
     * @param ringNum    which output channel ring buffer to place item on
     */
    protected void eventToOutputChannel(RingItem itemOut, int channelNum, int ringNum) {

        // Have any output channels?
        if (outputChannelCount < 1) {
            ByteBufferSupply supply = itemOut.getByteBufferSupply();
            if (supply != null) {
                supply.release(itemOut.getByteBufferItem());
            }
            return;
        }

        RingBuffer rb = outputChannels.get(channelNum).getRingBuffersOut()[ringNum];
        long nextRingItem = rb.next();

        RingItem ri = (RingItem) rb.get(nextRingItem);
        ri.copy(itemOut);
        rb.publish(nextRingItem);
    }



    //-----------------------------------------------------------
    // For CODAStateMachine interface
    //
    // In general, go(), end(), prestart(), download(), and reset()
    // will be overridden in modules which extend this class.
    //-----------------------------------------------------------

    /** {@inheritDoc} */
    public void go()       throws CmdExecException {}
    /** {@inheritDoc} */
    public void end()      throws CmdExecException {}
    /** {@inheritDoc} */
    public void pause()    {paused = true;}
    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {}
    /** {@inheritDoc} */
    public void download() throws CmdExecException {}
    /** {@inheritDoc} */
    public void reset() {}


    /** {@inheritDoc} */
    public void registerEndCallback(EmuEventNotify callback) {endCallback = callback;}

    /** {@inheritDoc} */
    public EmuEventNotify getEndCallback() {return endCallback;}

    //-----------------------------------------------------------
    // For StatedObject interface
    //-----------------------------------------------------------

    /** {@inheritDoc} */
    public State state() {return state;}
    /** {@inheritDoc} */
    public String getError() {return errorMsg.get();}

    //-----------------------------------------------------------
    // For EmuModule interface
    //-----------------------------------------------------------

    /** {@inheritDoc} */
    public String getAttr(String name) throws DataNotFoundException {
        String attribute = attributeMap.get(name);
        if (attribute == null) throw new DataNotFoundException("attribute " + name + " not found in config for " + name());
        return attribute;
    }

    /** {@inheritDoc} */
    public int getIntAttr(String name) throws DataNotFoundException {
        return Integer.valueOf(getAttr(name));
    }

    /** {@inheritDoc} */
    public String name() {return name;}

    /** {@inheritDoc} */
    public ModuleIoType getInputRingItemType() {return ModuleIoType.PayloadBank;}

    /** {@inheritDoc} */
    public ModuleIoType getOutputRingItemType() {return ModuleIoType.PayloadBank;}

    /** {@inheritDoc} */
    public boolean representsEmuStatistics() {return representStatistics;}

    /** {@inheritDoc} */
    synchronized public Object[] getStatistics() {

        Object[] stats = new Object[8];

        // If we're not active, keep the accumulated
        // totals and sizes, but the rates are zero.
        if (state != CODAState.ACTIVE) {
            stats[0] = eventCountTotal;
            stats[1] = wordCountTotal;
            stats[2] = 0F;
            stats[3] = 0F;

            stats[4] = maxEventSize;
            stats[5] = minEventSize;
            stats[6] = avgEventSize;
            stats[7] = timeToBuild;
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
            stats[7] = timeToBuild;
        }

        return stats;
    }

    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {
        if (input_channels == null) return;
        this.inputChannels.addAll(input_channels);
        inputChannelCount  = inputChannels.size();
    }

    /** {@inheritDoc} */
    public void addOutputChannels(ArrayList<DataChannel> output_channels) {
        if (output_channels == null) return;
        this.outputChannels.addAll(output_channels);
        outputChannelCount = outputChannels.size();

        if (outputChannelCount > 1) {
            // This is a DC outputting to multiple SEBs (output channels).
            // Since this is the case, we need to write a fixed, contiguous
            // # of events to a single SEB before moving on to the next and
            // writing the same amount of events there.
//if (emu.getCodaClass().isEventBuilder()) {
            if (emu.getCodaClass() == CODAClass.DC) {
System.out.println("Set chunking FOR SEB, chunk = " + sebChunk);
                chunkingForSebs = true;
            }
        }
        System.out.println("So far, chunkingForSebs = " + chunkingForSebs);
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
    }

    /** {@inheritDoc} */
    public int getEventProducingThreadCount() {return eventProducingThreads;}

    /** {@inheritDoc} */
    public ByteOrder getOutputOrder() {return outputOrder;}

    /** {@inheritDoc} */
    public int getSebChunk() {return sebChunk;}

    /** {@inheritDoc} */
    public boolean getSebChunking() {return chunkingForSebs;}

    //----------------------------------------------------------------

    /** Keep some data together and store as an event attachment.
     *  This class helps write events in the desired order.*/
    final class EventOrder {
        /** Output channel to use. */
        DataChannel outputChannel;
        /** Index into arrays for this output channel. */
        int index;
        /** Place of event in output order of this output channel. */
        int inputOrder;
        /** Lock to use for output to this output channel. */
        Object lock;
        /** If {@code true}, then its output order is not important. */
        boolean aSync;
    }

    /**
     * Class defining comparator which tells priority queue how to sort elements.
     * @param <T> Must be PayloadBank or PayloadBuffer in this case
     */
    final class AttachComparator<T> implements Comparator<T> {
        public int compare(T o1, T o2) throws ClassCastException {
            Attached a1 = (Attached) o1;
            Attached a2 = (Attached) o2;
            EventOrder eo1 = (EventOrder) (a1.getAttachment());
            EventOrder eo2 = (EventOrder) (a2.getAttachment());

            if (eo1 == null || eo2 == null) {
                return 0;
            }

            return (eo1.inputOrder - eo2.inputOrder);
        }
    }


    /**
     * This class defines a thread that makes instantaneous rate calculations
     * once every few seconds. Rates can be sent to run control
     * (or stored in local xml config file).
     */
    final class RateCalculatorThread extends Thread {
        /**
         * Method run is the action loop of the thread.
         * Suggested creation & start on PRESTART.
         * Suggested exit on END or RESET.
         */
        @Override
        public void run() {

            // variables for instantaneous stats
            long deltaT, t1, t2, prevEventCount=0L, prevWordCount=0L;

            while ((state == CODAState.ACTIVE) || paused) {
                try {
                    // In the paused state only wake every two seconds.
                    sleep(2000);

                    t1 = System.currentTimeMillis();

                    while (state == CODAState.ACTIVE) {
                        sleep(statGatheringPeriod);

                        t2 = System.currentTimeMillis();
                        deltaT = t2 - t1;

                        // calculate rates
                        eventRate = (eventCountTotal - prevEventCount)*1000F/deltaT;
                        wordRate  = (wordCountTotal  - prevWordCount)*1000F/deltaT;

                        prevEventCount = eventCountTotal;
                        prevWordCount  = wordCountTotal;
                        t1 = t2;
//                        System.out.println("evRate = " + eventRate + ", byteRate = " + 4*wordRate);

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