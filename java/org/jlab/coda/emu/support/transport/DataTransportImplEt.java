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
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.control.Command;

import java.util.Map;
import java.io.IOException;

/**
 * @author timmer
 * Jun 4, 2010
 */
public class DataTransportImplEt extends DataTransportCore implements DataTransport {

    /** Connection to ET system. */
    private SystemUse etSystem;

    private Station station;

    private SystemOpenConfig openConfig;

    private StationConfig stationConfig;

    /**
     * Get the ET connection object.
     * @return the ET connection object.
     */
    public org.jlab.coda.et.SystemUse getEtConnection() {
        return etSystem;
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
    public DataTransportImplEt(String pname, Map<String, String> attrib) throws DataNotFoundException {
        // pname is the "name" entry in the attrib map
        super(pname, attrib);

        // Which ET sytem do we connect to?
        String etName = attrib.get("etName");
        if (etName == null) throw new DataNotFoundException("Cannot find udl");

        // create ET connection object (does create connection NOW)
        try {
            SystemOpenConfig config = new SystemOpenConfig(etName, Constants.hostAnywhere);
            // configuration of a new station
            stationConfig = new StationConfig();
            //statConfig.setFlowMode(Constants.stationParallel);
            //statConfig.setCue(100);

            // create station
            Station station = etSystem.createStation(stationConfig, statName, position, pposition);
        } catch (EtException e) {
            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (EtTooManyException e) {
//            e.printStackTrace();
//        }
    }


    public DataChannel createChannel(String name, Map<String,String> attributeMap, boolean isInput) throws DataTransportException {
        DataChannel c = new DataChannelImplEt(name, this, attributeMap, isInput);
        channels().put(name, c);
        return c;
    }

    public void execute(Command cmd) {
Logger.debug("    DataTransportImplCmsg.execute : " + cmd);

        if (cmd.equals(CODATransition.PRESTART)) {

            try {
                Logger.debug("    DataTransportImplCmsg.execute PRESTART: cmsg connect : " + name() + " " + myInstance);
                try {
                    etSystem = new SystemUse(openConfig, Constants.debugNone);
                    etSystem.attach(station);
                } catch (EtException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (EtTooManyException e) {
                    e.printStackTrace();
                }

            } catch (Exception e) {
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
        else if ((cmd.equals(CODATransition.END)) || (cmd.equals(CODATransition.RESET))) {

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