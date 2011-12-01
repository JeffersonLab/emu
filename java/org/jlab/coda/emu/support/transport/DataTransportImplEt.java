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

import java.util.HashMap;
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
     * @param emu  emu object this transport belongs to
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

        // broadcast address to use to connect to ET (not to listen on)
        String baddr;
        String[] bAddrs;
        Collection<String> bAddrList = null;

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

            // size of TCP send buffer (0 means use operating system default)
            int tcpSendBuf = 0;
            str = attrib.get("sendBuf");
            if (str != null) {
                try {
                    tcpSendBuf = Integer.parseInt(str);
                }
                catch (NumberFormatException e) {}
            }

            // size of TCP receive buffer (0 means use operating system default)
            int tcpRecvBuf = 0;
            str = attrib.get("recvBuf");
            if (str != null) {
                try {
                    tcpRecvBuf = Integer.parseInt(str);
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
                systemConfig.setTcpSendBufSize(tcpSendBuf);
                systemConfig.setTcpRecvBufSize(tcpRecvBuf);
                if (maddr != null) {
                    systemConfig.addMulticastAddr(maddr);
                }

                if (groups > 1) {
                    int[] g = new int[groups];
                    for (int i=0; i < eventNum; i++) {
                        g[i%groups]++;
                    }
                    /*
                    System.out.println("GROUPS:");
                    for (int i=0 ; i < g.length; i++) {
                        System.out.println("  events in group " + i + " = " + g[i]);
                    }
                    */

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
            str = attrib.get("method");
            if (str == null) {
                method = EtConstants.broadcast;
            }
            else if (str.equalsIgnoreCase("cast")) {
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
            host = attrib.get("host");
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

            // broadcast address to use to connect to ET
            baddr = attrib.get("bAddr");
            if (baddr != null) {
                bAddrs = new String[] {baddr};
                bAddrList = Arrays.asList(bAddrs);
            }
        }


        //------------------------------------------------
        // Configure ET system connection
        //------------------------------------------------
        try {
            openConfig = new EtSystemOpenConfig(etName, host, bAddrList, mAddrList,
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


    /**
     * Close this DataTransport object.
     * This method does nothing as closing channels is handled
     * in {@link #execute(org.jlab.coda.emu.support.control.Command, boolean)}
     * and we don't shutdown the ET system for an
     * "END" command (which calls this close() eventually).
     */
    public void close() {}


    /**
     * Reset or hard close this DataTransport object.
     * The resetting of channels is done in
     * {@link #execute(org.jlab.coda.emu.support.control.Command, boolean)}.
     * All this method does is remove any created ET system.
     */
    public void reset() {
        // kill any ET system this object started
        if (processET != null && createdET) {
logger.debug("    DataTransport Et: tell the ET system process to die - " + openConfig.getEtName());
            processET.destroy();
            try {
logger.debug("    DataTransport Et: wait for the ET system process to die ...");
                processET.waitFor();
logger.debug("    DataTransport Et: ET is dead");
            }
            catch (InterruptedException e) { }
            // remove the ET system file
            File etFile = new File(openConfig.getEtName());
            etFile.delete();
        }
    }

    public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                     boolean isInput, Emu emu) throws DataTransportException {
        DataChannel c = new DataChannelImplEt(name, this, attributeMap, isInput, emu);

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
logger.debug("    DataTransport Et execute : " + cmd.name());
        CODACommand emuCmd = cmd.getCodaCommand();

        if (emuCmd == DOWNLOAD) {
            createdET = false;
            boolean existingET = false;
            boolean incompatibleET = false;

            // Create the ET system if it does not exist and config file requests it.
            if (tryToCreateET) {
//System.out.println("Try to create the ET system " + openConfig.getEtName());
                // First check to see if there is an existing
                // system by trying to open a connection to it.
                // We don't want to wait for it here, so remove any wait.
                EtSystem etSystem = null;
                EtSystemOpenConfig openConfig = new EtSystemOpenConfig(getOpenConfig());
                openConfig.setWaitTime(0);
                try {
                    etSystem = new EtSystem(openConfig);
                    //etSystem.setDebug(EtConstants.debugInfo);
//System.out.println("  First try opening existing ET system " + openConfig.getEtName());
                    etSystem.open();
                    existingET = true;
//System.out.println("  ET system " + openConfig.getEtName() + " already exists");
                }
                catch (EtException e) {
//System.out.println("  Cannot open ET system, config is not self consistent");
                    logger.debug("    DataTransport Et execute DOWNLOAD: self-contradictory ET system config : " + name() + " " + myInstance);
                    state = CODAState.ERROR;
                    return;
                }
                catch (Exception e) {
//                    e.printStackTrace();
                    // Any existing local ET might be different name or TCP port, or isn't local.
                    // A name conflict will spell doom for us later when we try to create it.
//System.out.println("  Cannot open ET system " + openConfig.getEtName() + ", not there?");
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
logger.debug("    DataTransport Et execute DOWNLOAD: incompatible ET system exists : " + name() + " " + myInstance);
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

                    if (systemConfig.getTcpRecvBufSize() > 0) {
                        etCmd += " -rb " + systemConfig.getTcpRecvBufSize();
                    }

                    if (systemConfig.getTcpSendBufSize() > 0) {
                        etCmd += " -sb " + systemConfig.getTcpSendBufSize();
                    }

                    if (systemConfig.isNoDelay()) {
                        etCmd += " -nd";
                    }

                    try {
logger.debug("    DataTransport Et: create ET system, " + openConfig.getEtName() + " with cmd \n" + etCmd);
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
            // channels created by EmuModuleFactory in PRESTART
            state = cmd.success();
            return;
        }
        else if (emuCmd == GO) {
            HashMap<String, DataChannel> channels;
            if (forInput) {
                channels = inChannels;
            }
            else {
                channels = outChannels;
            }

            if (!channels.isEmpty()) {
                //synchronized (channels) {
                    for (DataChannel c : channels.values()) {
                        ((DataChannelImplEt)c).resumeHelper();
                    }
                //}
            }
        }
        else if (emuCmd == PAUSE) {
            // The timing of pausing the input & output channels
            // is not so critical.
            if (!inChannels.isEmpty()) {
                //synchronized (inChannels) {
                    for (DataChannel c : inChannels.values()) {
                        ((DataChannelImplEt)c).pauseHelper();
                    }
                //}
            }
            if (!outChannels.isEmpty()) {
                //synchronized (outChannels) {
                    for (DataChannel c : outChannels.values()) {
                        ((DataChannelImplEt)c).pauseHelper();
                    }
                //}
            }
        }
        else if (emuCmd == END) {
            setConnected(false);

//            if (logger != null) {
//                logger.debug("close transport " + name());
//            }

            HashMap<String, DataChannel> channels;
            if (forInput) {
                channels = inChannels;
            }
            else {
                channels = outChannels;
            }

            // close channels
            if (!channels.isEmpty()) {
                //synchronized (channels) {
                    for (DataChannel c : channels.values()) {
                        c.close();
                        allChannels.remove(c.getName());
                    }
                    channels.clear();
                //}
            }

            state = cmd.success();
            return;
        }
        else if (emuCmd == RESET) {
            setConnected(false);

//            if (logger != null) {
//                logger.debug("reset transport " + name());
//            }

            // reset channels
            if (!allChannels.isEmpty()) {
                //synchronized (allChannels) {
                    for (DataChannel c : allChannels.values()) {
                        c.reset();
                    }
                // all transport objects are closed and removed so don't bother clearing maps
                    //allChannels.clear();
                    //inChannels.clear();
                    //outChannels.clear();
                //}
            }

            state = cmd.success();
            return;
        }

        // We don't implement other commands so assume success.
        if (state != CODAState.ERROR) state = cmd.success();

    }

}