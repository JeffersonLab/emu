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

import org.jlab.coda.emu.support.codaComponent.StatedObject;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.control.RcCommand;
import org.jlab.coda.emu.support.transport.DataChannel;

import java.util.ArrayList;

/**
 * Interface EmuModule a simple interface to a modular part of
 * the EMU.
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public interface EmuModule extends StatedObject {
    /**
     * The name of the module
     *
     * @return the name
     */
    public String name();

    /**
     * Method execute When passed a Command object executes the command
     * in the context of the receiving module.
     *
     * @param cmd of type Command
     * @throws CmdExecException
     *
     */
    public void execute(RcCommand cmd) throws CmdExecException;

    /**
     * Set the input channels of this EmuModule object.
     * @param input_channels the input channels of this EmuModule object
     */
    public void setInputChannels(ArrayList<DataChannel> input_channels);

    /**
     * Set the output channels of this EmuModule object.
     * @param output_channels the output channels of this EmuModule object
     */
    public void setOutputChannels(ArrayList<DataChannel> output_channels);

    /**
     * Get the <b>output</b> statistics of this EmuModule object. The output statistics
     * consists of an array of 2 longs and 2 floats in object form:<p>
     * <ol>
     * <li>event count<p>
     * <li>word (32 bit int) count<p>
     * <li>event rate (Hz)<p>
     * <li>word rate (Hz)<p>
     * </ol>
     *
     * @return array of objects containing in order: 1) event count, 2) word count,
     *         3) event rate and, 4) word rate, or<p>
     *         null if no statistics reported for this module.
     */
    public Object[] getStatistics();

    /**
     * If an EMU has more than one module, which module's statistics represent the EMU
     * as a whole needs to be determined. This method returns true if this module's
     * statistics may represent the EMU as a whole. This may be specified in an EMU's
     * xml configuration file by including the attribute statistics="on" in the module
     * definition.
     *
     * @return <coda>true</code> if this module's statistics represents the EMU, else <code>false</code>.
     */
    public boolean representsEmuStatistics();

}
