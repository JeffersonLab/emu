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

import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.et.*;
import org.jlab.coda.et.system.SystemConfig;
import org.jlab.coda.et.exception.EtException;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.codaComponent.CODAState;

import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;
import org.jlab.coda.emu.support.logger.Logger;

import java.util.Map;
import java.util.Collection;
import java.util.Arrays;
import java.io.IOException;
import java.io.File;

/**
 * @author timmer
 * Jun 4, 2010
 */
public class DataTransportImplEt extends DataTransportCore implements DataTransport {

    /** Does the ET system get created by this object if it does not already exist? */
    private boolean tryToCreateET;

    /** Did this object actually create the ET system? */
    private boolean createdET;

    /** Configuration for opening the associated ET system. */
    private EtSystemOpenConfig openConfig;

    /** Configuration for creating the associated ET system. */
    private SystemConfig systemConfig;

    /** Running ET system process if any. */
    private Process processET;

    private Emu emu;
    private Logger logger;

    /**
     * Get whether the ET system should be created by the EMU if it does not exist.
     * @return whether the ET system should be created by the EMU if it does not exist
     */
    public boolean tryToCreateET() {
        return tryToCreateET;
    }


    /**
     * Gets the configuration for opening the ET system.
     * @return configuration for opening the ET system
     */
    public EtSystemOpenConfig getOpenConfig() {
        return openConfig;
    }


    /**
     * Gets the configuration for creating the ET system.
     * @return configuration for creating the ET system
     */
    public SystemConfig getSystemConfig() {
        return systemConfig;
    }


    /**
     * Constructor.
     *
     * @param pname  name of transport
     * @param attrib transport's attribute map from config file
     *
     * @throws DataNotFoundException when cannot configure an ET system
     */
    public DataTransportImplEt(String pname, Map<String, String> attrib, Emu emu)
            throws DataNotFoundException {

        // pname is the "name" entry in the attrib map
        super(pname, attrib, emu);
        this.emu = emu;
        this.logger = emu.getLogger();

        String etName = attrib.get("etName");
        if (etName == null) {
            // default name is EMU name in /tmp directory
            etName = "/tmp/" +  emu.name();
        }

        //--------------------------------------
        // Read in attributes applicable whether
        // we are creating an ET system or not
        //--------------------------------------

        // direct connection port
        int port = EtConstants.serverPort;
        String str = attrib.get("port");
        if (str != null) {
            try {
                port = Integer.parseInt(str);
            }
            catch (NumberFormatException e) {}
        }

        // broadcasting/multicasting (udp) port
        int uport = EtConstants.broadcastPort;
        str = attrib.get("uPort");
        if (str != null) {
            try {
                uport = Integer.parseInt(str);
            }
            catch (NumberFormatException e) {}
        }

        // multicast address
        String maddr;
        String[] mAddrs;
        Collection<String> mAddrList = null;

        maddr = attrib.get("mAddr");
        if (maddr != null) {
            mAddrs = new String[] {maddr};
            mAddrList = Arrays.asList(mAddrs);
        }


        // Are we willing to wait for an ET system to appear?
        int wait = 0;
        str = attrib.get("wait");
        if (str != null) {
            try {
                // wait is in milliseconds, str is in seconds
                wait = 1000*Integer.parseInt(str);
            }
            catch (NumberFormatException e) {}
        }

        //---------------------------------------------
        // Do we create the ET system if there is none?
        //---------------------------------------------
        tryToCreateET = false;
        str = attrib.get("create");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                tryToCreateET = true;
            }
        }

        // How do we contact the ET system?
        int method = EtConstants.direct;

        // Where is the host the ET system is running on?
        String host = EtConstants.hostLocal;

        if (tryToCreateET) {
            // If we're creating the ET system, it
            // must be local, we know its TCP port,
            // and we'll make a direct connection to it.

            // But we must know how many and what size
            // events to make as well as the desired number
            // of groups (we assume all groups are of equal
            // size.
            systemConfig = new SystemConfig();

            // number of events
            int eventNum = 0;
            str = attrib.get("eventNum");
            if (str != null) {
                try {
                    eventNum = Integer.parseInt(str);
                }
                catch (NumberFormatException e) {}
            }

            // size of events
            int eventSize = 0;
            str = attrib.get("eventSize");
            if (str != null) {
                try {
                    eventSize = Integer.parseInt(str);
                }
                catch (NumberFormatException e) {}
            }

            // groups of events
            int groups = 1;
            str = attrib.get("groups");
            if (str != null) {
                try {
                    groups = Integer.parseInt(str);
                }
                catch (NumberFormatException e) {}
            }

            try {
                systemConfig.setNumEvents(eventNum);
                systemConfig.setEventSize(eventSize);
                systemConfig.setServerPort(port);
                systemConfig.setUdpPort(uport);
                systemConfig.setMulticastPort(uport);
                if (maddr != null) {
                    systemConfig.addMulticastAddr(maddr);
                }

                if (groups > 1) {
                    int[] g = new int[groups];
                    for (int i=0; i < eventNum; i++) {
                        g[i%groups]++;
                    }
                    systemConfig.setGroups(g);
                }
            }
            catch (EtException e) {
                throw new DataNotFoundException("incomplete specification of ET system", e);
            }

        }
        else {
            // How do we connect to it? By default, assume
            // it's anywhere and we need to broadcast.
            method = EtConstants.broadcast;
            str = attrib.get("method");
            if (str.equalsIgnoreCase("cast")) {
                method = EtConstants.broadAndMulticast;
            }
            else if (str.equalsIgnoreCase("bcast")) {
                method = EtConstants.broadcast;
            }
            else if (str.equalsIgnoreCase("mcast")) {
                method = EtConstants.multicast;
            }
            else if (str.equalsIgnoreCase("direct")) {
                method = EtConstants.direct;
            }

            // Where do we look for it? By default assume
            // it can be anywhere (local or remote).
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
        }


        //------------------------------------------------
        // Configure ET system connection
        //------------------------------------------------
        try {
            openConfig = new EtSystemOpenConfig(etName, host, mAddrList,
                                                false, method, port, uport, uport,
                                                EtConstants.multicastTTL,
                                                EtConstants.policyError);
            openConfig.setWaitTime(wait);
        }
        catch (EtException e) {
            //e.printStackTrace();
            throw new DataNotFoundException("Bad station parameters in config file", e);
        }
    }


    /** Close this DataTransport object and all its channels. */
    public void close() {
        closeChannels();

        // kill any ET system this object started
        if (processET != null && createdET) {
System.out.println("Kill the ET system " + openConfig.getEtName());
            processET.destroy();
            try {
                processET.waitFor();
            }
            catch (InterruptedException e) { }
            // remove the ET system file
            File etFile = new File(openConfig.getEtName());
            boolean deleted = etFile.delete();
            System.out.println("     and removed the file (" + deleted + ")");
        }
    }


    public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                     boolean isInput, Emu emu) throws DataTransportException {
        DataChannel c = new DataChannelImplEt(name, this, attributeMap, isInput, emu);
        channels().put(name, c);
System.out.println("Created channel " + name + ", channels size = " + channels().size());
        return c;
    }

    public void execute(Command cmd) {
logger.debug("    DataTransportImplEt.execute : " + cmd);
        CODACommand emuCmd = cmd.getCodaCommand();

        if (emuCmd == DOWNLOAD) {
            createdET = false;
            boolean existingET = false;
            boolean incompatibleET = false;

            // Create the ET system if it does not exist and config file requests it.
            if (tryToCreateET) {
System.out.println("Try to create the ET system " + openConfig.getEtName());
                // First check to see if there is an existing
                // system by trying to open a connection to it.
                EtSystem etSystem = new EtSystem(getOpenConfig());
                try {
                    //etSystem.setDebug(EtConstants.debugInfo);
                    etSystem.open();
                    existingET = true;
System.out.println("ET system " + openConfig.getEtName() + " already exists");
                }
                catch (Exception e) {
                    // Any existing local ET might be different name or TCP port, or isn't local.
                    // A name conflict will spell doom for us later when we try to create it.
System.out.println("Cannot open ET system " + openConfig.getEtName() + ", none there?");
                }

                // If one exists, see if it's compatible
                if (existingET) {
                    // If we're here, we've managed to open the ET system so we
                    // must have the name & TCP port right, and it is local.

                    // But is it compatible?
                    if (etSystem.getEventSize() != systemConfig.getEventSize() ||
                        etSystem.getNumEvents() != systemConfig.getNumEvents())  {
                        incompatibleET = true;
                    }

                    if (!incompatibleET) {
                        // Compare event groups
                        try {
                            int[] groupsReal = etSystem.getGroups();
                            int[] groupsWant = systemConfig.getGroups();
                            if (groupsReal.length != groupsWant.length) {
                                incompatibleET = true;
                            }
                            if (!incompatibleET) {
                                for (int i=0; i < groupsReal.length; i++) {
                                    if (groupsReal[i] != groupsWant[i]) {
                                        incompatibleET = true;
                                    }
                                }
                            }
                        }
                        catch (EtException e) {
                            // not happening
                        }
                        catch (IOException e) {
                            // problems talking to the ET
                            incompatibleET = true;
                        }
                    }

                    // done messing with existing ET
                    etSystem.close();

                    // error if incompatible ET system exists
                    if (incompatibleET) {
System.out.println("Incompatible ET system, " + openConfig.getEtName());
                        logger.debug("    DataTransportImplEt.execute DOWNLOAD: imcompatible ET system exists : " + name() + " " + myInstance);
                        state = CODAState.ERROR;
                        return;
                    }
                }
                // else try to create a new ET system
                else {
                    String etCmd = "et_start -f " + openConfig.getEtName() +
                            " -s " + systemConfig.getEventSize() +
                            " -n " + systemConfig.getNumEvents() +
                            " -g " + systemConfig.getGroups().length +
                            " -p " + systemConfig.getServerPort() +
                            " -u " + systemConfig.getUdpPort();

                    if (systemConfig.getMulticastAddrs().size() > 0) {
                        etCmd += " -a " + systemConfig.getMulticastStrings()[0];
                    }

                    try {
System.out.println("Create ET system, " + openConfig.getEtName() + " with cmd \n" + etCmd);
                        processET = Runtime.getRuntime().exec(etCmd);
                        createdET = true;
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else if (emuCmd == PRESTART) {

            try {
                logger.debug("    DataTransportImplEt.execute PRESTART: ET open : " + name() + " " + myInstance);

            } catch (Exception e) {
                emu.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            state = cmd.success();
            return;
        }
        else if (emuCmd == GO) {
            if (!channels().isEmpty()) {
                synchronized (channels()) {
                    for (DataChannel c : channels().values()) {
                        ((DataChannelImplEt)c).resumeHelper();
                    }
                }
            }
        }
        else if (emuCmd == PAUSE) {
            // have input channels stop reading events

            if (!channels().isEmpty()) {
                synchronized (channels()) {
                    for (DataChannel c : channels().values()) {
                        ((DataChannelImplEt)c).pauseHelper();
                    }
                }
            }
        }
        else if ((emuCmd == END) || (emuCmd == RESET)) {
            // TransportFactory already calls close on each channel for these transitions
            state = cmd.success();
            return;
        }

        // We don't implement other commands so assume success.
        if (state != CODAState.ERROR) state = cmd.success();

    }

}