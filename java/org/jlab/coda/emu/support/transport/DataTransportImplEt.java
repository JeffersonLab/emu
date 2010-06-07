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
import java.io.IOException;

/**
 * @author timmer
 * Jun 4, 2010
 */
public class DataTransportImplEt extends DataTransportCore implements DataTransport {

    /** Connection to ET system. */
    private EtSystem etSystem;

    private EtStation station;

    private EtSystemOpenConfig openConfig;

    private EtStationConfig stationConfig;

    private String stationName;

    private int stationPosition;

    private EtAttachment attachment;


    /**
     * Get the ET connection object.
     * @return the ET connection object.
     */
    public EtSystem getEtConnection() {
        return etSystem;
    }

    /**
     * Constructor.
     *
     * @param pname  of type String
     * @param attrib of type Map
     *
     * @throws DataNotFoundException
     *         when udl not given or cannot connect to cmsg server
     */
    public DataTransportImplEt(String pname, Map<String, String> attrib) throws DataNotFoundException {
        // pname is the "name" entry in the attrib map
        super(pname, attrib);

        //---------------------------------
        // Which ET sytem do we connect to?
        //---------------------------------

        String etName = attrib.get("etName");
        if (etName == null) throw new DataNotFoundException("Cannot find ET system name");

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
        int mport = 0;
        String maddr;
        String[] mAddrs = null;

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
        }

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
        // Which station do we create or attach to?
        //------------------------------------------------
        stationName = attrib.get("statName");
        if (stationName == null) throw new DataNotFoundException("Cannot find station name");

        // Where do we put the station?
        stationPosition = 1;
        String positionString = attrib.get("statPos");
        if (positionString != null) {
            try {
                stationPosition = Integer.parseInt(positionString);
            }
            catch (NumberFormatException e) {}
        }

        // create ET connection object (does create connection NOW)
        try {
            // configuration of a new connection
            openConfig = new  EtSystemOpenConfig(etName, host, Arrays.asList(mAddrs),
                                                 false, method, port, port, mport,
                                                 EtConstants.multicastTTL,
                                                 EtConstants.policyError);
            openConfig.setWaitTime(wait);
        }
        catch (EtException e) {
            //e.printStackTrace();
            throw new DataNotFoundException("Bad station parameters in config file", e);
        }


        // configuration of a new station
        stationConfig = new EtStationConfig();
        try {
            stationConfig.setUserMode(EtConstants.stationUserSingle);
        }
        catch (EtException e) { /* never happen */}

        // create ET system object
        etSystem = new EtSystem(openConfig, EtConstants.debugNone);

        // create station
        try {
            station = etSystem.createStation(stationConfig, stationName, stationPosition);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (EtException e) {
            e.printStackTrace();
        }
        catch (EtExistsException e) {
            try {
                station = etSystem.stationNameToObject(stationName);
            }
            catch (IOException e1) {
                e1.printStackTrace();
            }
            catch (EtException e1) {
                e1.printStackTrace();
            }
        }
        catch (EtTooManyException e) {
            e.printStackTrace();
        }


    }


    public DataChannel createChannel(String name, Map<String,String> attributeMap, boolean isInput) throws DataTransportException {
        DataChannel c = new DataChannelImplEt(name, this, attributeMap, isInput);
        channels().put(name, c);
        return c;
    }

    public void execute(Command cmd) {
Logger.debug("    DataTransportImplEt.execute : " + cmd);

        if (cmd.equals(CODATransition.PRESTART)) {

            try {
                Logger.debug("    DataTransportImplEt.execute PRESTART: ET open : " + name() + " " + myInstance);

                // open ET system
                etSystem.open();

                // create station if it does not already exists
                try {
                    station = etSystem.createStation(stationConfig, stationName, stationPosition);
                }
                catch (EtExistsException e) {
                    station = etSystem.stationNameToObject(stationName);
                }

                // attach to station
                attachment = etSystem.attach(station);
                
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
                        ((DataChannelImplEt)c).resumeOutputHelper();
                    }
                }
            }
        }
        else if (cmd.equals(CODATransition.PAUSE)) {
            if (!channels().isEmpty()) {
                synchronized (channels()) {
                    for (DataChannel c : channels().values()) {
                        ((DataChannelImplEt)c).pauseOutputHelper();
                    }
                }
            }
        }
        else if ((cmd.equals(CODATransition.END)) || (cmd.equals(CODATransition.RESET))) {
            Logger.debug("    DataTransportImplEt.execute END/RESET: ET disconnect : " + name() + " " + myInstance);
            etSystem.close();
            state = cmd.success();
            return;
        }

        // We don't implement other commands so assume success.
        if (state != CODAState.ERROR) state = cmd.success();

    }

 }