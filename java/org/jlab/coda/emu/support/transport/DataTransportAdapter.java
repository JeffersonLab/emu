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
import org.jlab.coda.emu.support.codaComponent.CODAStateMachineAdapter;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.logger.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class provides a bare-bones implementation of the DataTransport interface
 * (which includes the CODAStateMachine interface).
 * Extending this class implements the DataTransport interface and frees
 * any subclass from having to implement common methods or those that aren't used.
 * The only exception is the abstract {@link DataTransport#createChannel} method
 * which must be provided in subclasses.
 *
 * @author heyes
 * @author timmer
 */
public abstract class DataTransportAdapter extends CODAStateMachineAdapter implements DataTransport {

    /** Name of this DataTransport object. */
    protected String name;

    /** DataTransport object's state. */
    protected State state;

    /**
     * Possible error message. reset() sets it back to null.
     * Making this an atomically settable String ensures that only 1 thread
     * at a time can change its value. That way it's only set once per error.
     */
    protected AtomicReference<String> errorMsg = new AtomicReference<String>();

    /** Emu object that created this transport. */
    protected Emu emu;

    /** Logging object associated with the emu that created this transport. */
    protected Logger logger;

    /** Does this DataTransport object receive data (server = true) or does it send data (server = false)? */
    protected boolean isServer;

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
     * @throws DataNotFoundException when "server" and "class" attributes
     *                               are missing from attrib map.
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

        String serverS = getAttr("server");
        isServer = Boolean.valueOf(serverS);
    }

    // createChannel needs to be defined in subclasses

    /** {@inheritDoc} */
    public String getTransportClass() {return transportClass;}

    /** {@inheritDoc} */
    public void setName(String name) {this.name = name;}

    /** {@inheritDoc} */
    public String name() {return name;}

    /** {@inheritDoc} */
    public State state() {return state;}

    /** {@inheritDoc} */
    public String getError() {return errorMsg.get();}

    /** {@inheritDoc} */
    public void setAttr(String name, String value) {attr.put(name, value);}

    /** {@inheritDoc} */
    public String getAttr(String name) throws DataNotFoundException {
        String attribute = attr.get(name);

        if (attribute == null) throw new DataNotFoundException("attribute " + name + " not found in config for " + name());
        return attribute;
    }

    /** {@inheritDoc} */
    public int getIntAttr(String name) throws DataNotFoundException {
        return Integer.parseInt(getAttr(name));
    }

    /** {@inheritDoc} */
    public boolean isServer() {return isServer;}

    /** {@inheritDoc} */
    public boolean isConnected() {return connected;}

    /**
     * This method sets if this DataTransport object is connected or not.
     *
     * @param connected boolean: true if connected else false
     */
    public void setConnected(boolean connected) {this.connected = connected;}


    /** {@inheritDoc} */
    public void reset() {setConnected(false);}

}
