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
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.jevio.EvioBank;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to contain all the boilerplate code
 * for classes that implement the DataTransport interface.
 * The classes that conform to that interface also extend
 * this class.
 * @author heyes
 */
@SuppressWarnings({"WeakerAccess", "SameReturnValue", "SameReturnValue", "SameReturnValue", "SameReturnValue", "SameReturnValue", "SameReturnValue"})
public class DataTransportCore {

    /** State of this DataTransport object. */
    protected State state = CODAState.UNCONFIGURED;

    /** Is this object connected? */
    private boolean connected;

    /** Name of class extending this class. */
    private String transportClass = "unknown";

    /** Name of this DataTransport object. */
    private String name;

    /** Does this DataTransport object receive data (server = true) or does it send data (server = false)? */
    protected boolean server;

    /** Map of attributes. */
    public final Map<String, String> attr;

    /** Map of all DataChannel channels. */
    protected final HashMap<String, DataChannel> allChannels = new HashMap<String, DataChannel>();

    /** Map of DataChannel input channels. */
    protected final HashMap<String, DataChannel> inChannels = new HashMap<String, DataChannel>();

    /** Map of DataChannel output channels. */
    protected final HashMap<String, DataChannel> outChannels = new HashMap<String, DataChannel>();

    /** How many DataTransport objects are there? */
    protected static int instanceCount;

    /** This object is the Nth DataTransport object created. */
    protected int myInstance = -1;

    protected Emu emu;

    protected Logger logger;
    

    /**
     * Constructor.
     *
     * @param pname  of type String
     * @param attrib of type Map
     * @param emu emu that created this object
     *
     * @throws DataNotFoundException when
     */
    public
    DataTransportCore(String pname, Map<String, String> attrib, Emu emu) throws DataNotFoundException {

        name = pname;
        // TODO transports.put(pname, this);

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
        myInstance = instanceCount++;

        if (logger != null) {
            logger.debug("INSTANCE " + pname + " of class " + this.getClass() + " : " + myInstance);
        }
    }

    /**
     * Get the name of the transport implementation class extending this class.
     * @return the name of the transport implementation class extending this class
     */
    public String getTransportClass() {
        return transportClass;
    }

    /**
     * Set the name of this object.
     * @param pname the name of this object
     */
    public void setName(String pname) {
        name = pname;
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
     * @param pname name of attribute (key)
     * @param value value of attribute (val)
     */
    public void setAttr(String pname, String value) {
        attr.put(pname, value);
    }

    /**
     * This method gets an attribute as a String object.
     * Includes: (queue)capacity, size, ...
     *
     * @param pname name of attribute
     * @return value of attribute
     * @throws DataNotFoundException if couldn't find named attribute
     */
    public String getAttr(String pname) throws DataNotFoundException {
        String attribute = attr.get(pname);

        if (attribute == null) throw new DataNotFoundException("attribute " + pname + " not found in config for " + name());
        return attribute;
    }

    /**
     * This method gets an attribute as an int.
     * Includes: (queue)capacity, size, ...
     *
     * @param pname name of attribute
     * @return value of attribute
     * @throws DataNotFoundException if couldn't find named attribute
     */
    public int getIntAttr(String pname) throws DataNotFoundException {
        return Integer.valueOf(getAttr(pname));
    }

    /**
     * This method gets all the DataChannel objects contained in a DataTransport object.
     *
     * @return hashmap containing the data channels (HashMap<String, DataChannel>)
     */
    public HashMap<String, DataChannel> allChannels() {
        return allChannels;
    }

    /**
     * This method gets all the input DataChannel objects contained in a DataTransport object.
     *
     * @return hashmap containing the input data channels (HashMap<String, DataChannel>)
     */
    public HashMap<String, DataChannel> inChannels() {
        return inChannels;
    }

    /**
     * This method gets all the input DataChannel objects contained in a DataTransport object.
     *
     * @return hashmap containing the input data channels (HashMap<String, DataChannel>)
     */
    public HashMap<String, DataChannel> outChannels() {
        return outChannels;
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

    /** Close this DataTransport object and all its channels. */
    public void close() {
System.out.println("Transport Core's close() being called");
        setConnected(false);

        if (logger != null) {
            logger.debug("close transport " + name());
        }

        // close channels
        if (!allChannels().isEmpty()) {
            //synchronized (allChannels()) {
                for (DataChannel c : allChannels().values()) {
                    c.close();
                }
                allChannels.clear();
                outChannels.clear();
                inChannels.clear();
            //}
        }
    }

    /** Close this DataTransport object's channels.  */
    public void reset() {
System.out.println("Transport Core's reset() being called");
        setConnected(false);

        if (logger != null) {
            logger.debug("reset transport " + name());
        }

        // close channels
        if (!allChannels().isEmpty()) {
            //synchronized (allChannels()) {
                for (DataChannel c : allChannels().values()) {
                    c.reset();
                }
                allChannels.clear();
                outChannels.clear();
                inChannels.clear();
            //}
        }
    }

    /**
     * Get the state of this object.
     * @return the state of this object
     */
    public State state() {
        return state;
    }

    /**
     * This method receives or gets EvioBank objects from the given DataChannel object.
     *
     * @return EvioBank object containing data
     * @throws InterruptedException on wakeup with no data.
     */
    public EvioBank receive(DataChannel channel) throws InterruptedException {
        return channel.getQueue().take();
    }

    /**
     * This method sends a EvioBank object to the given DataChannel object.
     *
     * @param channel DataChannel to send data through
     * @param data EvioBank to send, containing data
     */
    public void send(DataChannel channel, EvioBank data) {
        channel.getQueue().add(data);
    }

    //---------------------------------------------------------------------------------------
    // cMsg related methods - implements cMsgCallbackInterface EXCEPT for the callback method
    //---------------------------------------------------------------------------------------


     /**
      * Method telling whether messages may be skipped or not.
      * @return true if messages can be skipped without error, false otherwise
      */
     public boolean maySkipMessages() {
        return false;
    }

     /**
      * Method telling whether messages must serialized -- processed in the order
      * received.
      * @return true if messages must be processed in the order received, false otherwise
      */
     public boolean mustSerializeMessages() {
        return true;
    }

     /**
      * Method to get the maximum number of messages to queue for the callback.
      * @return maximum number of messages to queue for the callback
      */
     public int getMaximumQueueSize() {
        return 100;
    }

     /**
      * Method to get the maximum number of messages to skip over (delete) from
      * the cue for the callback when the cue size has reached it limit.
      * This is only used when the "maySkipMessages" method returns true.
      * @return maximum number of messages to skip over from the cue
      */
     public int getSkipSize() {
        return 1;
    }

     /**
      * Method to get the maximum number of worker threads to use for running
      * the callback if "mustSerializeMessages" returns false.
      * @return maximum number of worker threads to start
      */
     public int getMaximumThreads() {
        return 10;
    }

     /**
      * Method to get the maximum number of unprocessed messages per worker thread.
      * This number is a target for dynamically adjusting server.
      * This is only used when the "mustSerializeMessages" method returns false.
      * @return maximum number of messages per worker thread
      */
     public int getMessagesPerThread() {
        return 10;
    }

}
