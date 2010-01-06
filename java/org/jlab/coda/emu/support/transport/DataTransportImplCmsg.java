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
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.codaComponent.RunControl;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.control.Command;

import java.util.Map;

/**
 * @author timmer
 * @Date Dec 2, 2009
 */
public class DataTransportImplCmsg extends DataTransportCore implements DataTransport {

    /** Connection to cmsg server. */
    private cMsg cmsgConnection;

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
     * @throws org.jlab.coda.emu.support.configurer.DataNotFoundException
     *          when udl not given or cannot connect to cmsg server
     */
    public DataTransportImplCmsg(String pname, Map<String, String> attrib) throws DataNotFoundException {
        // pname is the "name" entry in the attrib map
        super(pname, attrib);

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
    public DataChannel createChannel(String name, Map<String,String> attributeMap, boolean isInput) throws DataTransportException {
        DataChannel c = new DataChannelImplCmsg(name, this, attributeMap, isInput);
        channels().put(name, c);
        return c;
    }

    /** {@inheritDoc} */
    public void execute(Command cmd) {
Logger.debug("    DataTransportImplCmsg.execute : " + cmd);

        if (cmd.equals(CODATransition.PRESTART)) {

            try {
                Logger.debug("    DataTransportImplCmsg.execute PRESTART: cmsg connect : " + name() + " " + myInstance);
                cmsgConnection.connect();

            } catch (cMsgException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            state = cmd.success();
            return;
        }
        else if (cmd.equals(CODATransition.GO)) {
            cmsgConnection.start(); // allow message flow to callbacks

            if (!channels().isEmpty()) {
                synchronized (channels()) {
                    for (DataChannel c : channels().values()) {
                        ((DataChannelImplCmsg)c).resumeOutputHelper();
                    }
                }
            }
        }
        else if (cmd.equals(CODATransition.PAUSE)) {
            cmsgConnection.stop(); // stop message flow to callbacks

            if (!channels().isEmpty()) {
                synchronized (channels()) {
                    for (DataChannel c : channels().values()) {
                        ((DataChannelImplCmsg)c).pauseOutputHelper();
                    }
                }
            }
        }
        else if ((cmd.equals(CODATransition.END)) || (cmd.equals(RunControl.RESET))) {

            try {
                Logger.debug("    DataTransportImplCmsg.execute END/RESET: cmsg disconnect : " + name() + " " + myInstance);
                cmsgConnection.disconnect();

            } catch (Exception e) {
                // ignore
            }
            state = cmd.success();
            return;
        }

        // We don't implement other commands so assume success.
        if (state != CODAState.ERROR) state = cmd.success();

    }

 }
