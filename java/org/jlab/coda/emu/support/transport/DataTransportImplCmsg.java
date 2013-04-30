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
import org.jlab.coda.emu.support.configurer.DataNotFoundException;

import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.logger.Logger;

import java.util.Map;

/**
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
     * @param pname  of type String
     * @param attrib of type Map
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


    public DataChannel createChannel(String name, Map<String,String> attributeMap, boolean isInput, Emu emu) throws DataTransportException {
        return new DataChannelImplCmsg(name, this, attributeMap, isInput, emu);
    }

    public void reset() {
        try {
logger.debug("    DataTransportImplCmsg.reset(): cmsg disconnect : " + name());
            cmsgConnection.disconnect();
        } catch (Exception e) {}
    };


    public void prestart() throws CmdExecException {

        try {
            logger.debug("    DataTransportImplCmsg.execute PRESTART: cmsg connect : " + name());
            cmsgConnection.connect();

        } catch (cMsgException e) {
            throw new CmdExecException("cannot connect to cMsg server", e);
        }

        return;
    }


    public void go() {
        cmsgConnection.start(); // allow message flow to callbacks
    }

    public void pause() {
        cmsgConnection.stop(); // stop message flow to callbacks
    }

    public void end() {
        try {
logger.debug("    DataTransportImplCmsg.end(): cmsg disconnect : " + name());
            cmsgConnection.disconnect();
        } catch (Exception e) { }
    }

 }
