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

package org.jlab.coda.support.transport;

import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.data.DataBank;

import java.util.HashMap;

/**
 * Interface DataTransport ...
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public interface DataTransport {
    /**
     * Method getType returns the type of this DataTransport object.
     *
     * @return the type (type String) of this DataTransport object.
     */
    // field manipulation
    public String getTransportClass();

    /**
     * Method setName sets the name of this DataTransport object.
     *
     * @param pname the name of this DataTransport object.
     */
    public void setName(String pname);

    /**
     * Method state ...
     *
     * @return State
     */
    public State state();

    /**
     * Method execute When passed a Command object executes the command
     * in the context of the receiving module.
     *
     * @param cmd of type Command
     * @throws org.jlab.coda.support.control.CmdExecException
     *
     */
    public void execute(Command cmd) throws CmdExecException;

    /**
     * @return the name
     * @see org.jlab.coda.emu.EmuModule#name()
     */
    public String name();

    /**
     * Method setAttr ...
     *
     * @param pname of type String
     * @param value of type String
     */
    // Attribute management
    public void setAttr(String pname, String value);

    /**
     * Method getAttr ...
     *
     * @param pname of type String
     * @return String
     * @throws org.jlab.coda.support.configurer.DataNotFoundException
     *
     */
    public String getAttr(String pname) throws DataNotFoundException;

    /**
     * Method getIntAttr ...
     *
     * @param pname of type String
     * @return int
     * @throws DataNotFoundException when
     */
    public int getIntAttr(String pname) throws DataNotFoundException;

    /**
     * Method createChannel ...
     *
     * @param name    of type String
     * @param isInput
     * @return DataChannel
     */
    // Data Transport control
    public DataChannel createChannel(String name, boolean isInput) throws DataTransportException;

    /**
     * Method channels ...
     *
     * @return HashMap<String, DataChannel>
     */
    public HashMap<String, DataChannel> channels();

    /**
     * Method isConnected returns the connected of this DataTransport object.
     *
     * @return the connected (type boolean) of this DataTransport object.
     */
    public boolean isConnected();

    /**
     * Method send ...
     *
     * @param channel of type DataChannel
     * @param data    of type long[]
     */
    public void send(DataChannel channel, DataBank data);

    /**
     * Method receive ...
     *
     * @param channel of type DataChannel
     * @return int[]
     */
    public DataBank receive(DataChannel channel) throws InterruptedException;

    /**
     * Method isServer returns the server of this DataTransport object.
     *
     * @return the server (type boolean) of this DataTransport object.
     */
    public boolean isServer();

    public void close();
}
