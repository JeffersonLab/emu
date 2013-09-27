/*
 * Copyright (c) 2009, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;

import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.QueueItemType;
import org.jlab.coda.emu.support.logger.Logger;

import java.util.Map;

/**
 * This class specifies a single cMsg server.
 * Connects to and disconnects from the server.
 *
 * @author timmer
 * Dec 2, 2009
 */
public class DataTransportImplCmsg extends DataTransportAdapter {

    /** Connection to cmsg server. */
    private cMsg cmsgConnection;

    private Logger logger;

    /**
     * Get the cmsg connection object.
     * @return the cmsg connection object.
     */
    public cMsg getCmsgConnection() {
        return cmsgConnection;
    }

    /**
     * Constructor.
     *
     * @param pname  name of transport
     * @param attrib transport's attribute map from config file
     * @param emu  emu object this transport belongs to
     *
     * @throws DataNotFoundException
     *          when udl not given or cannot connect to cmsg server
     */
    public DataTransportImplCmsg(String pname, Map<String, String> attrib, Emu emu) throws DataNotFoundException {
        // pname is the "name" entry in the attrib map
        super(pname, attrib, emu);
        this.logger = emu.getLogger();

        // Which udl do we connect to?
        String udl = attrib.get("udl");
        if (udl == null) throw new DataNotFoundException("Cannot find udl");

        // create cmsg connection object (does NOT create connection yet)
        try {
            cmsgConnection = new cMsg(udl, pname , "");
        }
        catch (cMsgException e) {
            throw new DataNotFoundException("Cannot connect to cmsg server",e);
        }
    }


    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                     boolean isInput, Emu emu,
                                     QueueItemType queueItemType)
                throws DataTransportException {

        return new DataChannelImplCmsg(name, this, attributeMap, isInput, emu, queueItemType);
    }


    /** {@inheritDoc}. Disconnect from cMsg server. */
    public void reset() {
        try {
            logger.debug("    DataTransportImplCmsg.reset(): cmsg disconnect : " + name());
            cmsgConnection.disconnect();
        } catch (Exception e) {}
    };


    /** {@inheritDoc}. Connect to cMsg server. */
    public void prestart() throws CmdExecException {
        try {
            logger.debug("    DataTransportImplCmsg.prestart(): cmsg connect : " + name());
            cmsgConnection.connect();

        } catch (cMsgException e) {
            errorMsg.compareAndSet(null, "cannot connect to cMsg server (bad UDL or network)");
            state = CODAState.ERROR;
            emu.sendStatusMessage();
            logger.debug("    DataTransportImplCmsg.prestart(): cannot connect to cMsg server (bad UDL or network) : " + name());
            throw new CmdExecException("cannot connect to cMsg server (bad UDL or network)", e);
        }

        return;
    }


    /** {@inheritDoc}. Allow sending messages to callbacks. */
    public void go() {cmsgConnection.start();}


    /** {@inheritDoc}. Stop sending messages to callbacks. */
    public void pause() {cmsgConnection.stop();}


    /** {@inheritDoc}. Disconnect from cMsg server. */
    public void end() {
        try {
            logger.debug("    DataTransportImplCmsg.end(): cmsg disconnect : " + name());
            cmsgConnection.disconnect();
        } catch (Exception e) { }
    }

 }
