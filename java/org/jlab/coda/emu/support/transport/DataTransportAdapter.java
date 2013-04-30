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
import org.jlab.coda.emu.EmuStateMachineAdapter;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.logger.Logger;

import java.util.Map;

/**
 * This class provides an empty implementation of the DataTransport interface
 * (which includes the EmuStateMachine interface).
 * Extending this class implements the DataTransport interface and frees
 * any subclass from having to implement methods that aren't used.
 * The only exception is the abstract {@link DataTransport#createChannel} method
 * which must be provided in subclasses.
 *
 * @author heyes
 * @author timmer
 */
public abstract class DataTransportAdapter extends EmuStateMachineAdapter implements DataTransport {

    /** Name of this DataTransport object. */
    protected String name;

    /** Emu object that created this transport. */
    protected Emu emu;

    /** Logging object associated with the emu that created this transport. */
    protected Logger logger;

    /** Does this DataTransport object receive data (server = true) or does it send data (server = false)? */
    protected boolean server;

    /** Is this object connected? */
    protected boolean connected;

    /** Name of class extending this class. */
    protected String transportClass = "unknown";

    /** Map of attributes. */
    protected final Map<String, String> attr;



    /**
     * Constructor.
     *
     * @param name    name of this transport object
     * @param attrib  Map of attributes of this transport
     * @param emu     emu that created this transport
     *
     * @throws DataNotFoundException when
     */
    public DataTransportAdapter(String name, Map<String, String> attrib, Emu emu)
            throws DataNotFoundException {

        this.name = name;

        attr = attrib;
        this.emu = emu;
        // emu = null in case of fifo implementation being statically initialized
        if (emu != null) {
            this.logger = emu.getLogger();
        }

        transportClass = getAttr("class");
        if (transportClass == null) {
            throw new DataNotFoundException("Transport : " + name() + ", missing class attribute");
        }

        String serverS = getAttr("server");
        if (serverS == null) {
            throw new DataNotFoundException("Transport : " + name() + ", missing server attribute");
        }
        server = Boolean.valueOf(serverS);

//        if (logger != null) {
//            logger.debug("INSTANCE " + pname + " of class " + this.getClass() + " : " + myInstance);
//        }
    }

    // createChannel needs to be defined in subclasses

    /**
     * Get the name of the transport implementation class extending this class.
     * @return the name of the transport implementation class extending this class
     */
    public String getTransportClass() {
        return transportClass;
    }

    /**
     * Set the name of this object.
     * @param name the name of this object
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the name of this object.
     * @return the name of this object
     */
    public String name() {
        return name;
    }

    /**
     * This method sets an attribute.
     *
     * @param name name of attribute (key)
     * @param value value of attribute (val)
     */
    public void setAttr(String name, String value) {
        attr.put(name, value);
    }

    /**
     * This method gets an attribute as a String object.
     * Includes: (queue)capacity, size, ...
     *
     * @param name name of attribute
     * @return value of attribute
     * @throws DataNotFoundException if couldn't find named attribute
     */
    public String getAttr(String name) throws DataNotFoundException {
        String attribute = attr.get(name);

        if (attribute == null) throw new DataNotFoundException("attribute " + name + " not found in config for " + name());
        return attribute;
    }

    /**
     * This method gets an attribute as an int.
     * Includes: (queue)capacity, size, ...
     *
     * @param name name of attribute
     * @return value of attribute
     * @throws DataNotFoundException if couldn't find named attribute
     */
    public int getIntAttr(String name) throws DataNotFoundException {
        return Integer.valueOf(getAttr(name));
    }

    /**
     * This method returns true if this DataTransport object
     * is a server (sends data) or false if it is a client
     * (receives data).
     *
     * @return true if this DataTransport object sends data, else false
     */
    public boolean isServer() {
        return server;
    }

    /**
     * This method tells if this DataTransport object is connected.
     *
     * @return true if this object is connected, else false
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * This method sets if this DataTransport object is connected or not.
     *
     * @param connected boolean: true if connected else false
     */
    public void setConnected(boolean connected) {
        this.connected = connected;
    }


    /** Close this DataTransport object's channels.  */
    public void reset() {
//System.out.println("Transport Core's reset() being called");
        setConnected(false);
    }

}
