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
import org.jlab.coda.emu.support.data.QueueItemType;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;

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


    /** ID number of this event recorder obtained from config file. */
    protected int id;

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

    /** Total number of DataBank objects written to the outputs. */
    protected long eventCountTotal;

    /** Sum of the sizes, in 32-bit words, of all DataBank objects written to the outputs. */
    protected long wordCountTotal;

    /** Instantaneous event rate in Hz over the last time period of length {@link #statGatheringPeriod}. */
    protected float eventRate;

    /** Instantaneous word rate in Hz over the last time period of length {@link #statGatheringPeriod}. */
    protected float wordRate;

    /** Targeted time period in milliseconds over which instantaneous rates will be calculated. */
    protected static final int statGatheringPeriod = 2000;

    /** If <code>true</code>, this module's statistics
     * accurately represent the statistics of the EMU. */
    protected boolean representStatistics;

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
    //-----------------------------------------------------------

    /** {@inheritDoc} */
    public void go()       throws CmdExecException {}
    /** {@inheritDoc} */
    public void end()      throws CmdExecException {}
    /** {@inheritDoc} */
    public void pause()    throws CmdExecException {}
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

    //----------------------------------------------------------------


}