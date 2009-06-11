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

import org.jlab.coda.support.codaComponent.CODAState;
import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.data.DataBank;
import org.jlab.coda.support.logger.Logger;

import java.util.HashMap;
import java.util.Map;

/** @author heyes */
@SuppressWarnings({"WeakerAccess"})
public class DataTransportCore {
    /** Field state */
    protected State state = CODAState.UNCONFIGURED;

    /** Field connected */
    private boolean connected = false;

    /** Field transports */
    private static final HashMap<String, DataTransportCore> transports = new HashMap<String, DataTransportCore>();

    /** Field type */
    private String transportClass = "unknown";

    /** Field name */
    private String name = null;

    /** Field server */
    protected boolean server = false;

    /** Field attr */
    public final Map<String, String> attr;

    /** Field channels */
    protected final HashMap<String, DataChannel> channels = new HashMap<String, DataChannel>();

    protected static int instanceCount = 0;

    protected int myInstance = -1;

    /**
     * Method tester ...
     *
     * @return long
     */
    public native long tester();

    /**
     * Method fifo_list ...
     *
     * @param arg of type long
     * @return long
     */
    public static native long fifo_list(long arg);

    /**
     * Constructor DataTransportCore creates a new DataTransportCore instance.
     *
     * @param pname  of type String
     * @param attrib of type Map
     * @throws DataNotFoundException when
     */
    public DataTransportCore(String pname, Map<String, String> attrib) throws DataNotFoundException {

        name = pname;
        transports.put(pname, this);

        attr = attrib;

        transportClass = getAttr("class");
        if (transportClass == null) {
            throw new DataNotFoundException("Transport : " + name() + ", missing class attribute");
        }
        String serverS = getAttr("server");
        if (serverS == null) {
            throw new DataNotFoundException("Transport : " + name() + ", missing server attribute");
        }
        server = Boolean.valueOf(serverS);
        myInstance = instanceCount++;

        Logger.debug("INSTANCE " + pname + "of " + this.getClass() + " : " + myInstance);
    }

    /** Method test ... */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.support.transport.EmuDataTransport#test()
    */
    public void test() {
    }

    /**
     * Method getType returns the type of this DataTransportCore object.
     *
     * @return the type (type String) of this DataTransportCore object.
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.support.transport.EmuDataTransport#getName()
    */
    public String getTransportClass() {
        return transportClass;
    }

    /**
     * Method setName sets the name of this DataTransportCore object.
     *
     * @param pname the name of this DataTransportCore object.
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.support.transport.EmuDataTransport#setName(java.lang.String)
    */
    public void setName(String pname) {
        name = pname;
    }

    /**
     * Method name ...
     *
     * @return String
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.support.transport.EmuDataTransport#getName()
    */
    public String name() {
        return name;
    }

    /**
     * Method setAttr ...
     *
     * @param pname of type String
     * @param value of type String
     */
    public void setAttr(String pname, String value) {
        attr.put(pname, value);
    }

    /**
     * Method getAttr ...
     *
     * @param pname of type String
     * @return String
     */
    public String getAttr(String pname) throws DataNotFoundException {
        String attribute = attr.get(pname);

        if (attribute == null) throw new DataNotFoundException("attribute " + pname + " not found in config for " + name());
        return attribute;
    }

    /**
     * Method getIntAttr ...
     *
     * @param pname of type String
     * @return int
     */
    public int getIntAttr(String pname) throws DataNotFoundException {
        return Integer.valueOf(getAttr(pname));
    }

    /**
     * Method channels ...
     *
     * @return HashMap<String, DataChannel>
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.emu.EmuModule#getChannels()
    */
    public HashMap<String, DataChannel> channels() {
        return channels;
    }

    public boolean isServer() {
        return server;
    }

    /**
     * Method isConnected returns the connected of this DataTransport object.
     *
     * @return the connected (type boolean) of this DataTransport object.
     */
    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    /** Method close ... */
    public void close() {
        setConnected(false);
        // close remaining channels.
        Logger.debug("close transport " + name());

        if (!channels().isEmpty()) {
            synchronized (channels()) {
                for (DataChannel c : channels().values()) {
                    c.close();
                }
                channels().clear();
            }
        }
    }

    /** @return the state */
    public State state() {
        return state;
    }

    /* cMsg related methods - implements callback interface */

    /**
     * Method maySkipMessages ...
     *
     * @return boolean
     */
    public boolean maySkipMessages() {
        return false;
    }

    /**
     * Method mustSerializeMessages ...
     *
     * @return boolean
     */
    public boolean mustSerializeMessages() {
        return true;
    }

    /**
     * Method getMaximumCueSize returns the maximumCueSize of this TransportImplCMsg object.
     *
     * @return the maximumCueSize (type int) of this TransportImplCMsg object.
     */
    public int getMaximumQueueSize() {
        return 100;
    }

    /**
     * Method getSkipSize returns the skipSize of this TransportImplCMsg object.
     *
     * @return the skipSize (type int) of this TransportImplCMsg object.
     */
    public int getSkipSize() {
        return 1;
    }

    /**
     * Method getMaximumThreads returns the maximumThreads of this TransportImplCMsg object.
     *
     * @return the maximumThreads (type int) of this TransportImplCMsg object.
     */
    public int getMaximumThreads() {
        return 200;
    }

    /**
     * Method getMessagesPerThread returns the messagesPerThread of this TransportImplCMsg object.
     *
     * @return the messagesPerThread (type int) of this TransportImplCMsg object.
     */
    public int getMessagesPerThread() {
        return 1;
    }

    /**
     * Method receive ...
     *
     * @param channel of type DataChannel
     * @return int[]
     */
    public DataBank receive(DataChannel channel) throws InterruptedException {
        return channel.getQueue().take();
    }

    /**
     * Method send ...
     *
     * @param channel of type DataChannel
     * @param data    of type long[]
     */
    public void send(DataChannel channel, DataBank data) {
        channel.getQueue().add(data);
    }
}
