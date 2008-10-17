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

import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.transport.DataChannel;

import java.util.HashMap;

/**
 * Interface EmuModule a simple interface to a modular part of
 * the EMU.
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public interface EmuModule {
    /**
     * The name of the module
     *
     * @return the name
     */
    public String name();

    /** @return the state */
    public State state();

    /**
     * Method channels returns a hashmap providing named access to the datachannels
     * associated with this module.
     *
     * @return HashMap<String, DataChannel>
     */
    public HashMap<String, DataChannel> channels();

    /**
     * Method execute When passed a Command object executes the command
     * in the context of the receiving module.
     *
     * @param cmd of type Command
     */
    public void execute(Command cmd) throws CmdExecException;
}
