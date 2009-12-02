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
import org.jlab.coda.support.codaComponent.StatedObject;
import org.jlab.coda.jevio.EvioBank;

import java.util.HashMap;

/**
 * This interface refers to how data is transported
 * and does <b>not</b> refer to a particular data connection.
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
@SuppressWarnings({"RedundantThrows"})
public interface DataTransport extends StatedObject {
    /**
     * Get the name of the transport implementation class implementing this class.
     * @return the name of the transport implementation class implementing this class
     */
    public String getTransportClass();

    // public State state(); from StatedObject
    // public String name(); from StatedObject

    /**
     * This method sets the name of this DataTransport object.
     * @param pname the name of this DataTransport object.
     */
    public void setName(String pname);

    /**
     * This method is run when passed a Command object
     * in the context of the receiving module.
     *
     * @param cmd of type Command
     * @throws CmdExecException if exception processing command
     */
    @SuppressWarnings({"RedundantThrows"})
    public void execute(Command cmd) throws CmdExecException;

    /**
     * This method sets an attribute.
     *
     * @param pname name of attribute (key)
     * @param value value of attribute (val)
     */
    public void setAttr(String pname, String value);

    /**
     * This method gets an attribute as a String object.
     *
     * @param pname name of attribute
     * @return value of attribute
     * @throws DataNotFoundException if couldn't find named attribute
     */
    public String getAttr(String pname) throws DataNotFoundException;

    /**
     * This method gets an attribute as an int.
     *
     * @param pname name of attribute
     * @return value of attribute
     * @throws DataNotFoundException if couldn't find named attribute
     */
    public int getIntAttr(String pname) throws DataNotFoundException;

    /**
     * This method creates a DataChannel object.
     *
     * @param name name of DataChannel
     * @param isInput true if this channel reads data into the Emu, else false
     * @return DataChannel object
     * @throws DataTransportException if transport problem
     */
    public DataChannel createChannel(String name, boolean isInput) throws DataTransportException;

    /**
     * This method gets all the DataChannel objects contained in a DataTransport object.
     *
     * @return hashmap containing the data channels (HashMap<String, DataChannel>)
     */
    public HashMap<String, DataChannel> channels();

    /**
     * This method tells if this DataTransport object is connected.
     *
     * @return true if this object is connected, else false
     */
    public boolean isConnected();

    /**
     * This method sends a EvioBank object to some output (fifo, another process, etc.)
     *
     * @param channel DataChannel to send data through
     * @param data EvioBank to send, containing data
     */
    public void send(DataChannel channel, EvioBank data);

    /**
     * This method returns true if this DataTransport object
     * is a server (sends data) or false if it is a client
     * (receives data).
     *
     * @return true if this DataTransport object sends data, else false
     */
    public boolean isServer();

    /** Close this DataTransport object and all its channels. */
    public void close();
}
