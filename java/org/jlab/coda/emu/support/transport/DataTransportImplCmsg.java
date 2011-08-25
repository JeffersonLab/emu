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
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.codaComponent.CODAState;

import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.logger.Logger;

import java.util.Map;

/**
 * @author timmer
 * Dec 2, 2009
 */
public class DataTransportImplCmsg extends DataTransportCore implements DataTransport {

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
        DataChannel c = new DataChannelImplCmsg(name, this, attributeMap, isInput, emu);
        allChannels().put(name, c);
        if (isInput) {
            inChannels().put(name, c);
        }
        else {
            outChannels().put(name, c);
        }
        return c;
    }

    public void execute(Command cmd, boolean forInput) {
        CODACommand emuCmd = cmd.getCodaCommand();
        logger.debug("    DataTransportImplCmsg.execute : " + emuCmd);

        if (emuCmd == PRESTART) {

            try {
                logger.debug("    DataTransportImplCmsg.execute PRESTART: cmsg connect : " + name() + " " + myInstance);
                cmsgConnection.connect();

            } catch (cMsgException e) {
                emu.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            state = cmd.success();
            return;
        }
        else if (emuCmd == GO) {
            cmsgConnection.start(); // allow message flow to callbacks

            if (!allChannels.isEmpty()) {
                synchronized (allChannels) {
                    for (DataChannel c : allChannels.values()) {
                        ((DataChannelImplCmsg)c).resumeOutputHelper();
                    }
                }
            }
        }
        else if (emuCmd == PAUSE) {
            cmsgConnection.stop(); // stop message flow to callbacks

            if (!allChannels.isEmpty()) {
                synchronized (allChannels) {
                    for (DataChannel c : allChannels.values()) {
                        ((DataChannelImplCmsg)c).pauseOutputHelper();
                    }
                }
            }
        }
        else if ((emuCmd == END) || (emuCmd == RESET)) {

            try {
                logger.debug("    DataTransportImplCmsg.execute END/RESET: cmsg disconnect : " + name() + " " + myInstance);
                cmsgConnection.disconnect();

            } catch (Exception e) {
                // ignore
            }
            state = cmd.success();
            return;
        }

        // We don't implement other commands so assume success.
        if (state != CODAState.ERROR) state = cmd.success();
System.out.println("    DataTransportImplCmsg.execute: final state = " + state);

    }

 }
