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


import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.QueueItemType;
import org.jlab.coda.et.*;
import org.jlab.coda.et.exception.EtClosedException;
import org.jlab.coda.et.exception.EtTooManyException;
import org.jlab.coda.et.system.SystemConfig;
import org.jlab.coda.et.exception.EtException;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;

import org.jlab.coda.emu.support.logger.Logger;

import java.io.*;
import java.util.Map;
import java.util.Collection;
import java.util.Arrays;

/**
 * This class specifies a single ET system to connect to.
 * The ET system may be created and destroyed by this class.
 *
 * @author timmer
 * Jun 4, 2010
 */
public class DataTransportImplEt extends DataTransportAdapter {

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
            // default name is <EXPID>_<EMU name> in /tmp directory
            etName = "/tmp/" +  emu.getExpid() + "_" + emu.name();
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

            // TCP NODELAY parameter (false by default)
            boolean noDelay = false;
            str = attrib.get("noDelay");
            if (str != null) {
                if (str.equalsIgnoreCase("true") ||
                    str.equalsIgnoreCase("on")   ||
                    str.equalsIgnoreCase("yes"))   {
                    noDelay = true;
                }
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
                systemConfig.setNoDelay(noDelay);
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
     * Get whether the ET system should be created by the EMU if it does not exist.
     * @return whether the ET system should be created by the EMU if it does not exist
     */
    public boolean tryToCreateET() {return tryToCreateET;}


    /**
     * Gets the configuration for opening the ET system.
     * @return configuration for opening the ET system
     */
    public EtSystemOpenConfig getOpenConfig() {return openConfig;}


    /**
     * Get the configuration for creating the ET system.
     * @return configuration for creating the ET system
     */
    public SystemConfig getSystemConfig() {return systemConfig;}


    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                     boolean isInput, Emu emu,
                                     QueueItemType queueItemType)
                    throws DataTransportException {

        return new DataChannelImplEt(name, this, attributeMap, isInput, emu, queueItemType);
    }


    /**
     * Get the output of a process - either error or regular output
     * depending on the input stream.
     *
     * @param inputStream get process output from this stream
     * @return String of process output
     */
    private String getProcessOutput(InputStream inputStream) {
        String line;
        StringBuilder sb = new StringBuilder(300);
        BufferedReader brErr = new BufferedReader(new InputStreamReader(inputStream));

        try {
            // read each line of output
            while ((line = brErr.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }
        }
        catch (IOException e) {
            // probably best to ignore this error
        }

        if (sb.length() > 0) {
            // take off last \n we put in buffer
            sb.deleteCharAt(sb.length()-1);
            return sb.toString();
        }

        return null;
    }


    /**
     * Get regular output (if monitor true) and error output
     * of Process and return both as strings.
     *
     * @param monitor <code>true</code> if we store regular output, else <code>false</code>.
     * @return array with both regular output (first element) and error output (second).
     */
    private String[] gatherAllOutput(Process process, boolean monitor) {
        String output;
        String[] strs = new String[2];

        // Grab regular output if requested.
        if (monitor) {
            output = getProcessOutput(process.getInputStream());
            if (output != null) {
                strs[0] = output;
            }
        }

        // Always grab error output.
        output = getProcessOutput(process.getErrorStream());
        if (output != null) {
            strs[1] = output;
        }

        return strs;
    }


    /**
     * {@inheritDoc}.
     * All this method does is remove any created ET system.
     */
    public void reset() {
        setConnected(false);

        // kill any ET system this object started
        if (processET != null && createdET) {
logger.debug("    DataTransport Et: tell the ET system process to die - " + openConfig.getEtName());
            processET.destroy();
            try {
                processET.waitFor();
logger.debug("    DataTransport Et: ET is dead");
            }
            catch (InterruptedException e) { }
            // remove the ET system file
            File etFile = new File(openConfig.getEtName());
            etFile.delete();
        }
    }


    /** {@inheritDoc} */
    public void download() throws CmdExecException {

        if (!tryToCreateET) {
            return;
        }

        createdET = false;
        boolean existingET = false;
        boolean incompatibleET = false;

        // Create the ET system if it does not exist and config file requests it.
        //
        // First check to see if there is an existing
        // system by trying to open a connection to it.
        // We don't want to wait for it here, so remove any wait.
        EtSystem etSystem = null;
        EtSystemOpenConfig openConfig = new EtSystemOpenConfig(getOpenConfig());
        openConfig.setWaitTime(0);

        try {
            etSystem = new EtSystem(openConfig);
        }
        catch (EtException e) {
            errorMsg.compareAndSet(null, "self-contradictory ET system config");
            state = CODAState.ERROR;
            emu.sendStatusMessage();
            logger.debug("    DataTransport Et execute DOWNLOAD: self-contradictory ET system config : " + name());
            throw new CmdExecException("Self-contradictory ET system config", e);
        }

        try {
            etSystem.open();
            existingET = true;
//System.out.println("  ET system " + openConfig.getEtName() + " already exists");
        }
        catch (EtTooManyException e) {
            errorMsg.compareAndSet(null, "multiple ET systems responding to open()");
            state = CODAState.ERROR;
            emu.sendStatusMessage();
            logger.debug("    DataTransport Et execute DOWNLOAD: multiple ET systems responding to open() : " + name());
            throw new CmdExecException("multiple ET systems responding to open()", e);
        }
        catch (Exception e) {
            // There are reasons why opening an ET system fails besides not existing.
            // An existing ET might have a different name or TCP port, or isn't local.
            // A name conflict will spell doom for us later when we try to create it.
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
                catch (EtClosedException e) {
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
                errorMsg.compareAndSet(null, "incompatible ET system exists");
                state = CODAState.ERROR;
                emu.sendStatusMessage();
                logger.debug("    DataTransport Et execute DOWNLOAD: incompatible ET system exists : " + name());
                throw new CmdExecException("incompatible ET system exists");
            }
        }
        // else try to create a new ET system
        else {
            String etCmd = "et_start -f " + openConfig.getEtName() +
                    " -s " + systemConfig.getEventSize() +
                    " -n " + systemConfig.getNumEvents() +
                    " -g " + systemConfig.getGroups().length +
                    " -p " + systemConfig.getServerPort() +
                    " -u " + systemConfig.getUdpPort() +
                    " -d";

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

                // Allow process a chance to run before testing if its terminated.
                Thread.yield();
                try {Thread.sleep(1000);}
                catch (InterruptedException e) {}

                // Figure out if process has already terminated.
                boolean terminated = true;
                try { processET.exitValue(); }
                catch (IllegalThreadStateException e) {
                    terminated = false;
                }

                if (terminated) {
                    String errorOut = null;
                    // grab any output
                    String[] retStrings = gatherAllOutput(processET, true);
                    if (retStrings[0] != null) {
                        errorOut += retStrings[0];
                    }
                    if (retStrings[1] != null) {
                        errorOut += "\n" + retStrings[0];
                    }

                    logger.debug(errorOut);
                    errorMsg.compareAndSet(null, errorOut);
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();
                    throw new CmdExecException(errorOut);
                }

                // There is no feedback mechanism to tell if
                // this ET system actually started.
                // So try for a few seconds to connect to it.
                // If we can't, then there must have been an error trying
                // to start it up (like another ET system using the same ports).
                openConfig.setWaitTime(2000);
//logger.debug("    DataTransport Et: try for 2 secs to connect to it with config = \n" +
//                     openConfig.toString());
                try {
                    etSystem = new EtSystem(openConfig);
                    etSystem.open();
                    try {etSystem.close();} catch (Exception e) {}
                }
                catch (Exception e) {
                    logger.debug("    DataTransport Et: created system " + openConfig.getEtName() + ", cannot connect");
                    errorMsg.compareAndSet(null, "created ET system but cannot connect");
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();
                    throw new CmdExecException("created ET, " + openConfig.getEtName() + ", but cannot connect");
                }

                createdET = true;
            }
            catch (IOException e) {
                e.printStackTrace();
                errorMsg.compareAndSet(null, "cannot run ET system");
                state = CODAState.ERROR;
                emu.sendStatusMessage();
                throw new CmdExecException("cannot run ET system", e);
            }
        }
    }



}