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
    private boolean connected = false;

    // TODO Field transports - not used
    //private static final HashMap<String, DataTransportCore> transports = new HashMap<String, DataTransportCore>();

    /** Name of class extending this class. */
    private String transportClass = "unknown";

    /** Name of this DataTransport object. */
    private String name = null;

    /** Does this DataTransport object receive data (server = true) or does it send data (server = false)? */
    protected boolean server = false;

    /** Map of attributes. */
    public final Map<String, String> attr;

    /** Map of DataChannel channels. */
    protected final HashMap<String, DataChannel> channels = new HashMap<String, DataChannel>();

    /** How many DataTransport objects are there? */
    protected static int instanceCount = 0;

    /** This object is the Nth DataTransport object created. */
    protected int myInstance = -1;
    

    /**
     * Method tester ...  NOT USED
     *
     * @return long
     */
    public native long tester();

    /**
     * Method fifo_list ... NOT USED
     *
     * @param arg of type long
     *
     * @return long
     */
    public static native long fifo_list(long arg);

    /**
     * Constructor.
     *
     * @param pname  of type String
     * @param attrib of type Map
     *
     * @throws DataNotFoundException when
     */
    public DataTransportCore(String pname, Map<String, String> attrib) throws DataNotFoundException {

        name = pname;
        // TODO transports.put(pname, this);

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

        Logger.debug("INSTANCE " + pname + " of class " + this.getClass() + " : " + myInstance);
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
    public HashMap<String, DataChannel> channels() {
        return channels;
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

    /** Close this DataTransport object and all its channels.  */
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

    /**
     * Get the state of this object.
     * @return the state of this Cobject
     */
    public State state() {
        return state;
    }

    /**
     * This method receives or gets DataBank objects from the given DataChannel object.
     *
     * @return DataBank object containing int[]
     * @throws InterruptedException on wakeup with no data.
     */
    public DataBank receive(DataChannel channel) throws InterruptedException {
        return channel.getQueue().take();
    }

    /**
     * This method sends a DataBank object to the given DataChannel object.
     *
     * @param channel DataChannel to send data through
     * @param data DataBank to send, containing long[]
     */
    public void send(DataChannel channel, DataBank data) {
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
