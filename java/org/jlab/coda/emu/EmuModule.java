/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu;

import org.jlab.coda.emu.support.codaComponent.CODAStateMachine;
import org.jlab.coda.emu.support.codaComponent.StatedObject;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.transport.DataChannel;

import java.nio.ByteOrder;
import java.util.ArrayList;

/**
 * Interface EmuModule a simple interface to a modular part of
 * the EMU.
 *
 * @author heyes
 * @author timmer
 * Sep 17, 2008
 */
public interface EmuModule extends StatedObject, CODAStateMachine {
    /**
     * The name of the module
     *
     * @return the name
     */
    public String name();


    public String getAttr(String name) throws DataNotFoundException;

    public int getIntAttr(String name) throws DataNotFoundException;


    /**
     * Add the given input channels to this EmuModule object.
     * @param input_channels the input channels to add to this EmuModule object
     */
    public void addInputChannels(ArrayList<DataChannel> input_channels);

    /**
     * Add the given output channels to this EmuModule object.
     * @param output_channels the output channels to add to this EmuModule object
     */
    public void addOutputChannels(ArrayList<DataChannel> output_channels);

    /**
      * Set the input channels of this EmuModule object.
      * @return ArrayList containing the input channels of this EmuModule object
      */
     public ArrayList<DataChannel> getInputChannels();

     /**
      * Set the output channels of this EmuModule object.
      * @return ArrayList containing the output channels of this EmuModule object
      */
     public ArrayList<DataChannel> getOutputChannels();

    /**
     * Get the number of items this EmuModule object has in its internal rings.
     * @return number of items this EmuModule object has in its internal rings.
     */
    public int getInternalRingCount();

    /**
     * Remove all channels from this EmuModule object.
     */
    public void clearChannels();

    /**
     * Get the <b>output</b> statistics of this EmuModule object. The output statistics
     * consists of an array of 2 Longs, 2 Floats, 4 Integers, and 1 int array:<p>
     * <ol>
     * <li>event count (Long)<p>
     * <li>word count  (Long)<p>
     * <li>event rate in Hz (Float<p>
     * <li>data rate in kBytes/sec  (Float)<p>
     * <li>max event size in bytes (Integer) if module is an EB<p>
     * <li>min event size in bytes (Integer) if module is an EB<p>
     * <li>avg event size in bytes (Integer) if module is an EB<p>
     * <li>suggested value for chunk*EtBufSize (Integer) if have ET output channel<p>
     * <li>if EB & switched on, histogram of time to build 1 event in nanoseconds (int array)<p>
     * </ol>
     *
     * @return array of objects containing in order: 1) event count (Long),
     *          2) word count (Long), 3) event rate(Float), 4) data rate (Float), optionally
     *          5) max event byte size (Integer), 6) min event byte size (Integer),
     *          7) average event byte size (Integer), and
     *          8) suggested value for chunk*EtBufSize (Integer) if have ET output channel, or
     *          9) histogram of time to build 1 event in nanosec (int array), or
     *          null if no statistics reported for this module.
     */
    public Object[] getStatistics();

    /**
     * If an EMU has more than one module, which module's statistics represent the EMU
     * as a whole needs to be determined. This method returns true if this module's
     * statistics may represent the EMU as a whole. This may be specified in an EMU's
     * xml configuration file by including the attribute statistics="on" in the module
     * definition.
     *
     * @return <code>true</code> if this module's statistics represents the EMU, else <code>false</code>.
     */
    public boolean representsEmuStatistics();

    /**
     * Get the number of threads which produce events to be placed in output channels.
     * @return number of threads which produce events to be placed in output channels.
     */
    public int getEventProducingThreadCount();

    /** Get the byte order of the module's output. Defaults to big endian. */
    public ByteOrder getOutputOrder();

    /**
     * Get the number of contiguous evio events to send to one output channel before
     * switching to the next. Used when sending events from multiple final event
     * builders (SEBs & PEBs).
     */
    public int getSebChunk();

    /**
     * Get the whether this module is doing seb chunking or not.
     * Used when sending events from multiple final event
     * builders (SEBs & PEBs).
     */
    public boolean getSebChunking();

}
