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

import org.jlab.coda.support.config.DataNotFoundException;

import java.util.HashMap;
import java.util.Map;

/** @author heyes */
@SuppressWarnings({"WeakerAccess"})
public class DataTransportCore {

    /** Field transports */
    private static final HashMap<String, DataTransportCore> transports = new HashMap<String, DataTransportCore>();

    /** Field type */
    private String type = "unknown";

    /** Field name */
    private String name = null;

    /** Field server */
    protected boolean server = false;

    /** Field attr */
    public final Map<String, String> attr;

    /** Field channels */
    protected final HashMap<String, DataChannel> channels = new HashMap<String, DataChannel>();

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

        type = getAttr("type");
        if (type == null) {
            throw new DataNotFoundException("Transport : " + name() + ", missing type attribute");
        }
        String serverS = getAttr("server");
        if (serverS == null) {
            throw new DataNotFoundException("Transport : " + name() + ", missing server attribute");
        }
        server = Boolean.valueOf(serverS);

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
    public String getType() {
        return type;
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
    public String getAttr(String pname) {
        return attr.get(pname);
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
}
