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

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAStateMachine;
import org.jlab.coda.emu.support.codaComponent.StatedObject;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;

import java.util.Map;

/**
 * This interface defines how data is transported
 * and does <b>not</b> refer to a particular data connection.
 *
 * @author heyes
 * @author timmer
 * Sep 17, 2008
 */
public interface DataTransport extends CODAStateMachine, StatedObject {

    /**
     * This method gets the name of this DataTransport object.
     *
     * @return the name of this DataTransport object.
     */
    String name();

    /**
     * This method sets the name of this DataTransport object.
     *
     * @param name the name of this DataTransport object.
     */
    void setName(String name);

    /**
     * This method sets an attribute.
     *
     * @param name  name  of attribute (key)
     * @param value value of attribute (val)
     */
    void setAttr(String name, String value);

    /**
     * This method gets an attribute as a String object.
     *
     * @param name name of attribute
     * @return value of attribute
     * @throws DataNotFoundException if cannot find named attribute
     */
    String getAttr(String name) throws DataNotFoundException;

    /**
     * This method gets an attribute as an int.
     *
     * @param pname name of attribute
     * @return value of attribute
     * @throws DataNotFoundException if couldn't find named attribute
     */
    int getIntAttr(String pname) throws DataNotFoundException;

    /**
     * This method creates a DataChannel object.
     *
     * @param name          name of DataChannel
     * @param attributeMap  hashmap of attributes taken from XML config file
     * @param isInput       true if this channel puts data into the Emu, else false
     * @param emu           Emu owning this transport
     * @param module        module creating this channel
     * @param outputIndex   order in which module's events will be sent to this
     *                      output channel (0 for first output channel, 1 for next, etc.).
     *
     * @return DataChannel object
     * @throws DataTransportException if transport problem
     */
    DataChannel createChannel(String name, Map<String, String> attributeMap,
                                     boolean isInput, Emu emu, EmuModule module,
                                     int outputIndex)
            throws DataTransportException;

    /**
     * This method tells if this DataTransport object is connected.
     *
     * @return true if this object is connected, else false
     */
    boolean isConnected();

    /**
     * This method returns true if this DataTransport object
     * is a server (sends data) or false if it is a client
     * (receives data).
     *
     * @return true if this DataTransport object sends data, else false
     */
    boolean isServer();

    /**
     * This method gets the name of the transport class implementing this interface.
     * @return the name of the transport class implementing this interface
     */
    String getTransportClass();
}
