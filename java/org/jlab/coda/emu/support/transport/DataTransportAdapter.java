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
import org.jlab.coda.emu.support.control.State;
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

    /** DataTransport object's state. */
    protected State state;

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

    /** {@inheritDoc} */
    public String getTransportClass() {return transportClass;}

    /** {@inheritDoc} */
    public void setName(String name) {this.name = name;}

    /** {@inheritDoc} */
    public String name() {return name;}

    /** {@inheritDoc} */
    public State state() {return state;}

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
        return Integer.valueOf(getAttr(name));
    }

    /** {@inheritDoc} */
    public boolean isServer() {
        return server;
    }

    /** {@inheritDoc} */
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


    /** {@inheritDoc} */
    public void reset() {
//System.out.println("Transport Core's reset() being called");
        setConnected(false);
    }

}
