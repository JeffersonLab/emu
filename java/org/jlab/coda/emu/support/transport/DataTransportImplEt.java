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

import org.jlab.coda.et.*;
import org.jlab.coda.et.exception.EtException;
import org.jlab.coda.et.exception.EtTooManyException;
import org.jlab.coda.et.exception.EtExistsException;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.control.Command;

import java.util.Map;
import java.util.Collection;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;

/**
 * @author timmer
 * Jun 4, 2010
 */
public class DataTransportImplEt extends DataTransportCore implements DataTransport {


    private EtSystemOpenConfig openConfig;


    public EtSystemOpenConfig getOpenConfig() {
        return openConfig;
    }


    /**
     * Constructor.
     *
     * @param pname  name of transport
     * @param attrib transport's attribute map from config file
     *
     * @throws DataNotFoundException when cannot configure an ET system
     */
    public DataTransportImplEt(String pname, Map<String, String> attrib) throws DataNotFoundException {
        // pname is the "name" entry in the attrib map
        super(pname, attrib);

        //---------------------------------
        // Which ET sytem do we connect to?
        //---------------------------------

        String etName = attrib.get("etName");
        if (etName == null) {
            // default name is EMU name in /tmp directory
            etName = "/tmp/" +  System.getProperty("name");
        }

        // How do we connect to it? By default, assume it's anywhere and we need to broadcast.
        int method = EtConstants.broadcast;
        String methodString = attrib.get("method");
        if (methodString.equalsIgnoreCase("cast")) {
            method = EtConstants.broadAndMulticast;
        }
        else if (methodString.equalsIgnoreCase("bcast")) {
            method = EtConstants.broadcast;
        }
        else if (methodString.equalsIgnoreCase("mcast")) {
            method = EtConstants.multicast;
        }
        else if (methodString.equalsIgnoreCase("direct")) {
            method = EtConstants.direct;
        }

        // Where do we look for it?
        String host = attrib.get("host");
        if (host == null) {
            host = EtConstants.hostAnywhere;
        }
        else if (host.equalsIgnoreCase("local")) {
            host = EtConstants.hostLocal;
        }
        else if (host.equalsIgnoreCase("anywhere")) {
            host = EtConstants.hostAnywhere;
        }
        else if (host.equalsIgnoreCase("remote")) {
            host = EtConstants.hostRemote;
        }

        // broadcasting or direct connection port
        int port = EtConstants.broadcastPort;
        String portString = attrib.get("port");
        if (portString != null) {
            try {
                port = Integer.parseInt(portString);
            }
            catch (NumberFormatException e) {}
        }

        // multicasting port & addr
        int mport = EtConstants.multicastPort;
        String maddr;
        String[] mAddrs = null;
        Collection<String> mAddrList = null;

        if (method == EtConstants.broadAndMulticast ||
            method == EtConstants.multicast) {

            mport = EtConstants.multicastPort;
            portString = attrib.get("mPort");
            if (portString != null) {
                try {
                    mport = Integer.parseInt(portString);
                }
                catch (NumberFormatException e) {}
            }

            maddr = attrib.get("mAddr");
            if (maddr == null) {
                maddr = EtConstants.multicastAddr;
            }
            mAddrs = new String[] {maddr};
            mAddrList = Arrays.asList(mAddrs);
        }

        //---------------------------------
        // misc parameters
        //---------------------------------

        // Do we wait for a connection?
        int wait = 0;
        String waitString = attrib.get("wait");
        if (waitString != null) {
            try {
                wait = Integer.parseInt(waitString);
            }
            catch (NumberFormatException e) {}
        }


        //------------------------------------------------
        // Configure ET system
        //------------------------------------------------
        try {
            // configuration of a new connection
            openConfig = new  EtSystemOpenConfig(etName, host, mAddrList,
                                                 false, method, port, port, mport,
                                                 EtConstants.multicastTTL,
                                                 EtConstants.policyError);
            openConfig.setWaitTime(wait);
        }
        catch (EtException e) {
            //e.printStackTrace();
            throw new DataNotFoundException("Bad station parameters in config file", e);
        }
    }


    public DataChannel createChannel(String name, Map<String,String> attributeMap, boolean isInput) throws DataTransportException {
        DataChannel c = new DataChannelImplEt(name, this, attributeMap, isInput);
        channels().put(name, c);
System.out.println("Created channel " + name + ", channels size = " + channels().size());
        return c;
    }

    public void execute(Command cmd) {
Logger.debug("    DataTransportImplEt.execute : " + cmd);

        if (cmd.equals(CODATransition.PRESTART)) {

            try {
                Logger.debug("    DataTransportImplEt.execute PRESTART: ET open : " + name() + " " + myInstance);

            } catch (Exception e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            state = cmd.success();
            return;
        }
        else if (cmd.equals(CODATransition.GO)) {
            if (!channels().isEmpty()) {
                synchronized (channels()) {
                    for (DataChannel c : channels().values()) {
                        ((DataChannelImplEt)c).resumeHelper();
                    }
                }
            }
        }
        else if (cmd.equals(CODATransition.PAUSE)) {
            // have input channels stop reading events

            if (!channels().isEmpty()) {
                synchronized (channels()) {
                    for (DataChannel c : channels().values()) {
                        ((DataChannelImplEt)c).pauseHelper();
                    }
                }
            }
        }
        else if ((cmd.equals(CODATransition.END)) || (cmd.equals(CODATransition.RESET))) {
            // TransportFactory already calls close on each channel for these transitions
            state = cmd.success();
            return;
        }

        // We don't implement other commands so assume success.
        if (state != CODAState.ERROR) state = cmd.success();

    }

 }