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

import org.jlab.coda.emu.EmuModule;

import java.util.HashMap;

/**
 * Interface DataTransport ...
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public interface DataTransport extends EmuModule {
    /**
     * Method getType returns the type of this DataTransport object.
     *
     * @return the type (type String) of this DataTransport object.
     */
    // field manipulation
    public String getType();

    /**
     * Method setName sets the name of this DataTransport object.
     *
     * @param pname the name of this DataTransport object.
     */
    public void setName(String pname);

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
     */
    public String getAttr(String pname);

    /**
     * Method createChannel ...
     *
     * @param name of type String
     * @return DataChannel
     */
    // Data Transport control
    public DataChannel createChannel(String name);

    /**
     * Method channels ...
     *
     * @return HashMap<String, DataChannel>
     * @see org.jlab.coda.emu.EmuModule#channels()
     */
    public HashMap<String, DataChannel> channels();

    /**
     * Method isConnected returns the connected of this DataTransport object.
     *
     * @return the connected (type boolean) of this DataTransport object.
     */
    public boolean isConnected();

    /**
     * Method startServer ...
     *
     * @throws TransportException when
     */
    public void startServer() throws TransportException;

    /** Method connect ... */
    public void connect();

    /**
     * Method closeChannel ...
     *
     * @param channel of type DataChannel
     */
    public void closeChannel(DataChannel channel);

    /** Method close ... */
    public void close();

    /**
     * Method send ...
     *
     * @param channel of type DataChannel
     * @param data    of type long[]
     */
    public void send(DataChannel channel, long[] data);

    /**
     * Method receive ...
     *
     * @param channel of type DataChannel
     * @return int[]
     */
    public int[] receive(DataChannel channel);

    /**
     * Method isServer returns the server of this DataTransport object.
     *
     * @return the server (type boolean) of this DataTransport object.
     */
    public boolean isServer();
}
