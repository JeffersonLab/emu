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
import org.jlab.coda.emu.support.control.Command;
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
    public void execute(Command cmd) throws CmdExecException;

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

}
