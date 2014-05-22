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

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuEventNotify;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.Attached;
import org.jlab.coda.emu.support.data.QueueItemType;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;

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

    /** User hit PAUSE button if {@code true}. */
    protected boolean paused;

    /** Object used by Emu to be notified of END event arrival. */
    protected EmuEventNotify endCallback;

    /** Flag used to kill eventMovingThread. */
    protected volatile boolean killThread;

    /** Comparator which tells queue how to sort elements. */
    protected AttachComparator<Attached> comparator = new AttachComparator<Attached>();

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

    /** Targeted time period in milliseconds over which instantaneous rates will be calculated. */
    protected static final int statGatheringPeriod = 2000;

    /** If {@code true}, this module's statistics represents that of the EMU. */
    protected boolean representStatistics;

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
            eventProducingThreads = Integer.parseInt(attributeMap.get("epThreads"));
            if (eventProducingThreads < 1) eventProducingThreads = 1;
        }
        catch (NumberFormatException e) {}

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
    public QueueItemType getInputQueueItemType() {return QueueItemType.PayloadBank;}

    /** {@inheritDoc} */
    public QueueItemType getOutputQueueItemType() {return QueueItemType.PayloadBank;}

    /** {@inheritDoc} */
    public boolean representsEmuStatistics() {return representStatistics;}

    /** {@inheritDoc} */
    synchronized public Object[] getStatistics() {
        Object[] stats = new Object[4];

        // If we're not active, keep the accumulated
        // totals, but the rates are zero.
        if (state != CODAState.ACTIVE) {
            stats[0] = eventCountTotal;
            stats[1] = wordCountTotal;
            stats[2] = 0F;
            stats[3] = 0F;
        }
        else {
            stats[0] = eventCountTotal;
            stats[1] = wordCountTotal;
            stats[2] = eventRate;
            stats[3] = wordRate;
        }

        return stats;
    }

    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {
        this.inputChannels.addAll(input_channels);
    }

    /** {@inheritDoc} */
    public void addOutputChannels(ArrayList<DataChannel> output_channels) {
        this.outputChannels.addAll(output_channels);
    }

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getInputChannels() {return inputChannels;}

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getOutputChannels() {return outputChannels;}

    /** {@inheritDoc} */
    public void clearChannels() {
        inputChannels.clear();
        outputChannels.clear();
    }

    /** {@inheritDoc} */
    public int getEventProducingThreadCount() {return eventProducingThreads;}


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
                    logger.info("EventBuilding thread " + name() + " interrupted");
                }
            }
        }
    }


}