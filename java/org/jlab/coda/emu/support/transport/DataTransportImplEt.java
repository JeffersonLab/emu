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


import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.et.*;
import org.jlab.coda.et.exception.EtClosedException;
import org.jlab.coda.et.system.SystemConfig;
import org.jlab.coda.et.exception.EtException;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;

import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.et.system.SystemCreate;

import java.io.*;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


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

    /** Do we want a Java-based ET system run in this JVM (true) or
     *  do we want a to connect to a C-based system in another process (false). */
    private boolean isJavaSystem;

    /** ET system this object created. */
    private EtSystem etSystem;

    /** Configuration for opening the associated ET system. */
    private EtSystemOpenConfig openConfig;

    /** Configuration for creating the associated ET system. */
    private SystemConfig systemConfig;

    /** Local, running, java ET system. */
    private SystemCreate etSysLocal;

    /** Running ET system process if any. */
    private Process processET;

    /** Emu which created this object. */
    private Emu emu;

    private Logger logger;


    /** Thread used to kill ET and remove file if JVM exited by control-C. */
    private ControlCThread shutdownThread = new ControlCThread();

    /** Class describing thread to be used for killing ET
     *  and removing file if JVM exited by control-C. */
    static private class ControlCThread extends Thread {
        EtSystem etSys;
        String etFileName;

        ControlCThread() {}

        ControlCThread(EtSystem etSys,String etFileName) {
            this.etSys = etSys;
            this.etFileName = etFileName;
        }

        void reset(EtSystem etSys,String etFileName) {
            this.etSys = etSys;
            this.etFileName = etFileName;
        }

        public void run() {

//System.out.println("    DataTransport Et: HEY, I'm running control-C handling thread!\n");
            // Try killing ET system (should also delete file)
            if (etSys != null && etSys.alive()) {
                try {
//System.out.println("    DataTransport Et: try killing ET");
                    etSys.kill();
                }
                catch (Exception e) {}
            }

            // Try deleting any ET system file
            if (etFileName != null) {
                try {
                    File etFile = new File(etFileName);
                    if (etFile.exists() && etFile.isFile()) {
//System.out.println("    DataTransport Et: try deleting file");
                        etFile.delete();
                    }
                }
                catch (Exception e) {}
            }
        }
    }


    /**
     * Get the output of a process - either error or regular output
     * depending on the input stream.
     *
     * @param inputStream get process output from this stream
     * @return String of process output
     */
    static private String getProcessOutput(InputStream inputStream) {
        String line;
        StringBuilder sb = new StringBuilder(300);
        BufferedReader brErr = new BufferedReader(new InputStreamReader(inputStream));

        try {
            // read each line of output
            while ((line = brErr.readLine()) != null) {
                sb.append(line);
                sb.append('\n');
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
    static private String[] gatherAllOutput(Process process, boolean monitor) {
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
            etName = "/tmp/" +  emu.getExpid() + '_' + emu.name();
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
        int uport = EtConstants.udpPort;
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

        String preferredSubnet = null;

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

            // type of ET system, written in C or Java (C by default)
            isJavaSystem = false;
            str = attrib.get("type");
            if (str != null) {
                if (str.equalsIgnoreCase("java"))   {
                    isJavaSystem = true;
                }
            }
if (isJavaSystem) System.out.println("    DataTransport Et: create Java ET in this JVM, but not yet");

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
                transportState = CODAState.ERROR;
                emu.setErrorState("Transport et: incomplete specification of ET system, " + e.getMessage());
                throw new DataNotFoundException("incomplete specification of ET system", e);
            }

            // Since we're creating the ET system, add thread to
            // kill ET and remove file if EMU dies by control-C.
            Runtime.getRuntime().addShutdownHook(shutdownThread);
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

            // Use a preferred subnet for all communication?
            preferredSubnet = attrib.get("subnet");

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
                                                false, method, port, uport,
                                                EtConstants.multicastTTL,
                                                EtConstants.policyError);
            if (preferredSubnet != null) {
                try {
                    openConfig.setNetworkInterface(preferredSubnet);
System.out.println("    DataTransport Et: preferred subnet set to " + preferredSubnet);
                }
                catch (EtException e) {
                    /* not in dot-decimal format or unknown host so ignore */
System.out.println("    DataTransport Et: ignoring preferred subnet of " + preferredSubnet);
                }
            }
            openConfig.setWaitTime(wait);
        }
        catch (EtException e) {
            transportState = CODAState.ERROR;
            emu.setErrorState("Transport et: bad station parameters in config file, " + e.getMessage());
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


    /**
     * Get the locally created ET system object, if any.
     * @return locally created ET system object, if any.
     */
    public SystemCreate getLocalEtSystem() {return etSysLocal;}


    /**
      * Gets the number of events per group in the opened ET system.
      * @return  number of events per group in the opened ET system.
      */
     public int getEventsInGroup() {
         if (systemConfig == null) return 0;
         return (systemConfig.getNumEvents()/systemConfig.getGroups().length);
     }


    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                     boolean isInput, Emu emu, EmuModule module,
                                     int outputIndex)
                    throws DataTransportException {

        return new DataChannelImplEt(name, this, attributeMap, isInput, emu, module, outputIndex);
    }


    /**
     * {@inheritDoc}.
     * All this method does is remove any created ET system.
     */
    public void reset() {
        setConnected(false);

        // Kill any ET system this object started
        if (createdET) {
            try {
                killEtSystem();
            }
            catch (Exception e) {}

            try {
                // Remove any shutdown handler
                Runtime.getRuntime().removeShutdownHook(shutdownThread);
            }
            catch (Exception e) {}
        }
    }


    /**
     * Kill the ET system process and remove the file.
     * @return {@code true} if ET killed, else {@code false}.
     */
    private boolean killEtSystem() {

logger.info("    DataTransport Et: tell ET to die - " + openConfig.getEtName());

        boolean killedIt = false;

        // Stop ET system running in this JVM
        if (etSysLocal != null) {
logger.info("    DataTransport Et: shutdown local Java ET system");
            etSysLocal.shutdown();
        }
        // Stop ET system running in a separate process
        else {
logger.info("    DataTransport Et: try killing local C ET system");
            if (etSystem != null) {
                try {
                    // Tell ET to die directly and remove file, if we're still attached.
                    etSystem.kill();
logger.info("    DataTransport Et: told ET directly to die");
                    return true;
                }
                catch (IOException e) {
                }
                catch (EtClosedException e) {
                }
            }

            // If we couldn't command ET to die, try jvm method to kill ET process
            if (processET != null) {
                processET.destroy();

                try {
                    processET.waitFor();
logger.info("    DataTransport Et: used java process handle to kill ET");
                    killedIt = true;
                    processET = null;
                }
                catch (InterruptedException e) {
                }
            }
        }

        // Remove the ET system file
        File etFile = new File(openConfig.getEtName());
        etFile.delete();

        return killedIt;
    }


    /** {@inheritDoc} */
    public void download() throws CmdExecException {

        if (!tryToCreateET) {
            return;
        }

        createdET = false;
        EtSystemOpenConfig etOpenConfig;

        // Here is where the ET system is created.
        // If it does NOT exist, we create it now.
        //
        // We need to be careful here! The power of CODA 3 is that EMUs can be
        // started on any host and work just fine. It is possible that an identically
        // named EMU was previously run on a different host and left an operating,
        // identically named ET system as the one we're going to create. This can lead to
        // problems for other CODA components that need to attach to it - they may
        // find & attach to the wrong ET system. Therefore, after a configure or download
        // transition, we will start by trying to multicast on the local subnet and open
        // all ET systems with that name. Each will be opened and then killed.
        // Of course, any identically named local ET will be killed along with the others.
        // This way there are no legacy ET systems left to interfere.
        //


        // There should be NO ET system(s) running.
        // Kill any existing systems both on this host and elsewhere.
        //
        // Check to see if there are any existing ET systems running
        // on the local subnet by trying to open a connection to them.
        // We don't want to wait for any system.
        // We also want to connect as a remote user so no memory-mapping
        // is needlessly taking place.
        try {
            ArrayList<String> mAddrs = new ArrayList<>(6);
            mAddrs.add(EtConstants.multicastAddr);

            // multicasting constructor
            etOpenConfig = new EtSystemOpenConfig(openConfig.getEtName(),
                    EtConstants.hostAnywhere, mAddrs,
                    openConfig.getUdpPort(), 32);

            etOpenConfig.setWaitTime(2000);
            etOpenConfig.setConnectRemotely(true);
            etSystem = new EtSystem(etOpenConfig);
            //etSystem.setDebug(EtConstants.debugInfo);
        }
        catch (EtException e) {
            transportState = CODAState.ERROR;
            emu.setErrorState("Transport et: self-contradictory ET system config");
            System.out.println("    DataTransport Et: self-contradictory ET system config : " + name());
            throw new CmdExecException("Self-contradictory ET system config", e);
        }

        try {
            while (true) {
                etSystem.open();
                logger.info("    DataTransport Et: opened existing ET system, " + name() +
                            " on " + etSystem.getHost() + ", try to kill it ...");
                killEtSystem();
            }
        }
        catch (Exception e) {/* Not able to open ET so none are left running */}

        etSystem = null;

        // Create a new ET system
        // If we're here, no interfering ET systems are running.

        try {
            if (isJavaSystem) {
                // Create a Java ET system as threads in this JVM
                logger.debug("    DataTransport Et: create local Java ET system, " + etOpenConfig.getEtName());
                etSysLocal = new SystemCreate(openConfig.getEtName(), systemConfig);
            }
            else {
                // After creating the ET, we want to connect in a different way,
                // directly to a local system as a remote client (so no memory mapping).
                etOpenConfig = new EtSystemOpenConfig(openConfig);
                etOpenConfig.setWaitTime(30000);
                etOpenConfig.setConnectRemotely(true);

                String etCmd = "et_start -v -f " + etOpenConfig.getEtName() +
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

logger.debug("    DataTransport Et: create local C ET system, " + etOpenConfig.getEtName() +
                     " with cmd:\n" + etCmd);

//                Calendar cal = Calendar.getInstance();
//                String now = cal.get(Calendar.HOUR_OF_DAY) + ":" +
//                             cal.get(Calendar.MINUTE) + ':' +
//                             cal.get(Calendar.SECOND);
//
//                // Pipe output to log file
//                String cmds[] = new String[5];
//                cmds[0] = "script";
//                cmds[1] = "-c" ;
//                cmds[2] = etCmd;
//                cmds[3] = "-f" ;
//                cmds[4] = etOpenConfig.getEtName() + '-' + now + ".log";
//
//logger.debug("    DataTransport Et: create local C ET system, " + etOpenConfig.getEtName() + " with cmd:\n" +
//             "script -c \"" + etCmd + "\" -f " + etOpenConfig.getEtName()+ '-' + now + ".log");

                // Unfortunately, the "script" command spawns a csh which can redefine
                // various environmental variables being used back to another, more basic
                // source. Running script here caused an issue with Hall D in which the ET
                // system used one lib and the ET calls in emu used another.
                // Script can be set to use bash if set with SHELL env var. Bash will not
                // redefine these variables.
                // Scrap this for now!
                //processET = Runtime.getRuntime().exec(etCmds);

                String[] cmd = etCmd.split(" ");
                // Make ET io and error streams into local streams
                ProcessBuilder pb = new ProcessBuilder(cmd).inheritIO();
                // Start up ET system
                processET = pb.start();

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
                    logger.debug("    DataTransport Et:  ET system process was terminated");
                    String errorOut = "";
                    // grab any output
                    String[] retStrings = gatherAllOutput(processET, true);
                    if (retStrings[0] != null) {
                        errorOut += retStrings[0];
                    }
                    if (retStrings[1] != null) {
                        if (retStrings[0] != null) {
                            errorOut += "\n";
                        }
                        errorOut += retStrings[1];
                    }

                    try {
                        // Check to see if it bombs here in which case there's
                        // another ET system running on this port on this host.
                        ServerSocket socketserv = new ServerSocket(systemConfig.getServerPort());

                        transportState = CODAState.ERROR;
                        emu.setErrorState("Transport et: " + errorOut);
                        logger.debug(errorOut);
                        throw new CmdExecException(errorOut);
                    }
                    catch (IOException ex) {
                        transportState = CODAState.ERROR;
                        emu.setErrorState("cannot run ET system, port " +
                                           systemConfig.getServerPort() + " already in use");
                        logger.debug("cannot run ET system, port " +
                                     systemConfig.getServerPort() + " already in use");
                        throw new CmdExecException(errorOut);
                    }
                }

                // There is no feedback mechanism to tell if
                // this ET system actually started.
                // So try to connect to it.
                // If we can't, then there must have been an error trying
                // to start it up (like another ET system using the same ports).
                try {
                    etSystem = new EtSystem(etOpenConfig);
                    etSystem.setDebug(EtConstants.debugInfo);
                    etSystem.open();
                    // Leave the ET open so we can kill it after a reset/configure
                }
                catch (Exception e) {
                    etSystem = null;
                    transportState = CODAState.ERROR;
                    emu.setErrorState("Transport et: created ET system " + etOpenConfig.getEtName() + ", but cannot connect");
                    throw new CmdExecException("created ET, " + etOpenConfig.getEtName() + ", but cannot connect", e);
                }

                // Thread to run in case of control-C
                shutdownThread.reset(etSystem, etOpenConfig.getEtName());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            etSystem = null;
            transportState = CODAState.ERROR;
            emu.setErrorState("Transport et: cannot run ET system, " + e.getMessage());
            throw new CmdExecException("cannot run ET system", e);
        }

        createdET = true;
    }



}