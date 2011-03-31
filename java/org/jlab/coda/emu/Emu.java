/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu;

import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgPayloadItem;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.emu.support.codaComponent.*;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.control.Command;

import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.messaging.CMSGPortal;
import org.jlab.coda.emu.support.messaging.RCConstants;
import org.jlab.coda.emu.support.ui.DebugFrame;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.io.File;
import java.net.InetAddress;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This is the main class of the EMU (Event Management Unit) program.
 * It implements the CODAComponent interface which allows communication
 * with Run Control and implements a state machine.
 *
 * @athor heyes
 * @author timmer
 */
public class Emu implements CODAComponent {

    /**
     * The Emu can display a window containing debug information, a message log
     * and toolbars that allow commands to be issued without Run Control.
     * This is implemented by the DebugFrame class.
     */
    private DebugFrame debugGUI;

    /** The Emu starts all of it's threads in one thread group. */
    private final ThreadGroup threadGroup;

    /**
     * Most of the data manipulation in the Emu is done by plug-in modules.
     * The modules are specified in an XML configuration file and managed
     * by an object of the EmuModuleFactory class.
     */
    private final EmuModuleFactory moduleFactory;

    /** Name of the Emu, initially "unconfigured". */
    private String name = "unconfigured";

    /** The Emu monitors it's own status via a thread. */
    private Thread statusMonitor;

    /**
     * Commands from cMsg are converted into objects of
     * class Command that are then posted in this mailbox queue.
     */
    private final LinkedBlockingQueue<Command> mailbox;

    /** Vector of exception causes. */
    private final Vector<Throwable> causes = new Vector<Throwable>();

    /**
     * Configuration data can come from 4 sources:
     * run control string, run control file name,
     * debug gui file name, and command line file name.
     */
    private enum ConfigSource {
        CMD_LINE_FILE,
        RC_STRING,
        RC_FILE,
        GUI_FILE;
    }

    /** Which of the 4 sources does our config data come from? */
    private ConfigSource configSource;

    /**
     * Configure can be done by the debug GUI or Run Control command.
     * Keep track of when debug GUI last did it so we know if a configuration
     * from RC was already loaded or not.
     */
    private long configFileModifiedTime;

    /**
     * LoadedConfig is the XML document loaded when the configure command is executed.
     * It may change from run to run and tells the Emu which modules to load, which
     * data transports to start and what data channels to open.
     */
    private Document loadedConfig;

    /** Name of the file containing the Emu configuration (if any). */
    private String cmdLineConfigFile;

    /** Name of the file containing the Emu configuration (if any). */
    private String msgConfigFile;

    /**
     * LocalConfig is an XML document loaded when the configure command is executed.
     * This config contains all of the status variables that change from run to run.
     */
    private Document localConfig;

    /** A CMSGPortal object encapsulates all cMsg communication with Run Control. */
    private final CMSGPortal cmsgPortal;

    /** The UDL of the cMsg server. */
    private String cmsgUDL;

    /** An object used to log error and debug messages. */
    private final Logger logger;

    /** The experiment id. */
    private String expid;

    /** The name of the current DAQ session. */
    private String session;

    /** The name of the host this Emu is running on. */
    private String hostName;

    /** The name of the user account the Emu is running under. */
    private String userName;

    /**
     * Type of CODA object this is. Initially this is an EMU,
     * but it may be set later by the module(s) loaded.
     */
    private CODAClass codaClass = CODAClass.EMU;

    /** Which CODA version is this object designed for? */
    private String objectType = "coda3";

    /** The run number. */
    private volatile int runNumber;

    /** The numeric code representing the run type. */
    private int runType;

    /** A unique numeric identifier for this Emu. */
    private int codaid;

    /** The path to the directory that the EMU software is installed in. */
    private String installDir;



    /** Exit this Emu and the whole JVM. */ // TODO: only quit EMU threads?
    void quit() {
        try {
            cmsgPortal.shutdown();
        } catch (cMsgException e) {
            // ignore
        }

        if (debugGUI != null) debugGUI.dispose();
        statusMonitor.interrupt();
        System.exit(0);
    }

    /** This method sends the loaded XML configuration to the logger. */
    public void list() {
        if (moduleFactory.state() != CODAState.UNCONFIGURED) {
            logger.info("Dump of configuration", Configurer.serialize(loadedConfig));
        }
        else logger.warn("cannot list configuration, not configured");
    }

    /** {@inheritDoc} */
    public void postCommand(Command cmd) throws InterruptedException {
        mailbox.put(cmd);
    }

    /**
     * This method sets the name of this CODAComponent object.
     * @param name the name of this CODAComponent object.
     */
    private void setName(String name) {
        this.name = name;
        if (debugGUI != null) debugGUI.setTitle(name);
    }

    /**
     * {@inheritDoc}
     * This method actually returns the state of the modules in this EMU.
     */
    public State state() {
        return moduleFactory.state();
    }

    /**
     * Get the vector containing the causes of any exceptions
     * of an attempted transition from this state.
     * @return vector(type Vector<Throwable>) of causes of any exceptions
     *         of an attempted transition from this state.
     */
    public Vector<Throwable> getCauses() {
        return causes;
    }

    /**
     * Get the debug GUI object.
     * @return debug gui.
     */
    public DebugFrame getFramework() {
        return debugGUI;
    }

    /** {@inheritDoc} */
    public Document configuration() {
        return loadedConfig;
    }

    /** {@inheritDoc} */
    public Document parameters() {
        return localConfig;
    }

    /** {@inheritDoc} */
    public String name() {
        return name;
    }

    /**
     * Get the ThreadGroup this emu's threads are part of.
     * @return ThreadGroup this emu's threads are part of.
     */
    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    /**
     * Get the Logger this emu uses.
     * @return Logger this emu uses.
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Get the cmsgPortal object.
     * @return the cmsgPortal object of this emu.
     */
    public CMSGPortal getCmsgPortal() {
        return cmsgPortal;
    }

    /** {@inheritDoc} */
    public int getCodaid() {
        return codaid;
    }

    /** {@inheritDoc} */
    public String getSession() {
        return session;
    }

    /** {@inheritDoc} */
    public String getExpid() {
        return expid;
    }

    /** {@inheritDoc} */
    public String getHostName() {
        return hostName;
    }

    /** {@inheritDoc} */
    public String getUserName() {
        return userName;
    }

    /** {@inheritDoc} */
    public String getCodaClass() {
        return codaClass.name();
    }

    /**
     * Method to set the codaClass member.
     * @param codaClass
     */
    public void setCodaClass(CODAClass codaClass) {
        this.codaClass = codaClass;
    }

    /** {@inheritDoc} */
    public int getRunNumber() {
        return runNumber;
    }

    /** {@inheritDoc} */
    public int getRunType() {
        return runType;
    }

    /** {@inheritDoc} */
    public String getCmsgUDL() {
        return cmsgUDL;
    }

    /**
     * {@inheritDoc}
     * @see CODAComponent#setRunNumber(int)
     */
    public void setRunNumber(int runNumber) {
        this.runNumber = runNumber;
    }

    /**
     * {@inheritDoc}
     * @see CODAComponent#setRunType(int)
     */
    public void setRunType(int runType) {
        this.runType = runType;
    }

    /**
     * {@inheritDoc}
     * @see CODAComponent#setCodaid(int)
     */
    public void setCodaid(int codaid) {
        this.codaid = codaid;
    }

    //-----------------------------------------------------
    //              status reporting stuff
    //-----------------------------------------------------

    /** Thread which reports the EMU status to Run Control. */
    private Thread statusReportingThread;

    /** Time in milliseconds of the period of the reportingStatusThread. */
    private int statusReportingPeriod = 2000;

    /** If true, the status reporting thread is actively reporting status to Run Control. */
    private volatile boolean statusReportingOn;

    
    /** Class defining thread which reports the EMU status to Run Control. */
    class StatusReportingThread extends Thread {

        cMsgMessage reportMsg;

        StatusReportingThread() {
            reportMsg = new cMsgMessage();
            reportMsg.setSubject(name);
            reportMsg.setType(RCConstants.reportStatus);

            setDaemon(true);
        }

        public void run() {
//System.out.println("STATUS REPORTING THREAD: STARTED +++");

            while (!Thread.interrupted()) {
                if (statusReportingOn &&
                   (cmsgPortal.getServer() != null) &&
                   (cmsgPortal.getServer().isConnected())) {
                    
                    String state = moduleFactory.state().name().toLowerCase();

                    // clear stats
                    long  eventCount=0L, wordCount=0L;
                    float eventRate=0.F, wordRate=0.F;

                    // get new statistics from a single representative module
                    EmuModule statsModule = moduleFactory.getStatisticsModule();
                    if (statsModule != null) {
                        Object[] stats = statsModule.getStatistics();
                        if (stats != null) {
                            eventCount = (Long) stats[0];
                            wordCount  = (Long) stats[1];
                            eventRate  = (Float)stats[2];
                            wordRate   = (Float)stats[3];
//System.out.println("Stats for module " + statsModule.name() + ": count = " + eventCount +
//                   ", words = " + wordCount + ", eventRate = " + eventRate + ", wordRate = " + wordRate);
                        }
                    }

                    try {
                        // over write any previously defined payload items
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.state, state));
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.codaClass, codaClass.name()));
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventNumber, (int)eventCount));
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventRate, eventRate));
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.numberOfLongs, wordCount));
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.dataRate, (double)wordRate));

                        // send msg
//System.out.println("Emu " + name + " sending STATUS REPORTING Msg:");
//                        System.out.println("   " + RCConstants.state + " = " + state);
//                        System.out.println("   " + RCConstants.codaClass + " = " + codaClass.name());
//                        System.out.println("   " + RCConstants.eventNumber + " = " + (int)eventCount);
//                        System.out.println("   " + RCConstants.eventRate + " = " + eventRate);
//                        System.out.println("   " + RCConstants.numberOfLongs + " = " + wordCount);
//                        System.out.println("   " + RCConstants.dataRate + " = " + (double)wordRate);

                        cmsgPortal.getServer().send(reportMsg);
                    }
                    catch (cMsgException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    Thread.sleep(statusReportingPeriod);
                }
                catch (InterruptedException e) {
//System.out.println("STATUS REPORTING THREAD: DONE xxx");
                    return;
                }
            }
//System.out.println("STATUS REPORTING THREAD: DONE xxx");

        }

    };



    /**
     * Constructor.
     * This class is not executable. To create and run an Emu, use the {@link EmuFactory} class.<p/>
     * A thread is started to monitor the state.
     * The emu loads local.xml which contains a specification of status parameters.
     * The emu starts up a connection to the cMsg server.
     * By the end of the constructor several threads have been started.
     *
     * @param name            name of Emu
     * @param type            CODA component type of Emu
     * @param cmdLineConfigFile  name of Emu configuration file given on command line
     * @param loadedConfig    parsed XML document object of Emu configuration file
     * @param cmsgUDL         UDL used to connect to cMsg server to receive run control commands
     * @param debugUI         start a debug GUI
     * @throws EmuException   if name is null
     */
    public Emu(String name, String type, String cmdLineConfigFile, Document loadedConfig,
               String cmsgUDL, boolean debugUI) throws EmuException {

        if (name == null) {
            throw new EmuException("Emu name not defined");
        }

        if (type != null) {
            CODAClass cc = CODAClass.get(type);
            if (cc != null) {
                codaClass = cc;
            }
        }

        this.name = name;
        this.cmsgUDL = cmsgUDL;  // may be null
        this.loadedConfig = loadedConfig;
        this.cmdLineConfigFile = cmdLineConfigFile;

        // The source of our config info is the file given on the emu command line
        if (cmdLineConfigFile != null) {
            configSource = Emu.ConfigSource.CMD_LINE_FILE;
        }

        // Set the name of this EMU
        setName(name);

        // Each emu has its own logger
        logger = new Logger();
        Configurer.setLogger(logger);

        // Define thread group so all threads can be handled together
        threadGroup = new ThreadGroup(name);

        // Create object which manages all modules
        moduleFactory = new EmuModuleFactory(this);

        // Start up a GUI to control the EMU
        if (debugUI) {
            debugGUI = new DebugFrame(this);
        }

        // Define place to put incoming commands
        mailbox = new LinkedBlockingQueue<Command>();

        // Put this (which is a CODAComponent and therefore Runnable)
        // into a thread group and keep track of this object's thread.
        // This thread is started when statusMonitor.start() is called.
        statusMonitor = new Thread(threadGroup, this, "State monitor");
        statusMonitor.start();

        // Start up status reporting thread (which needs cmsg to send msgs)
        statusReportingThread = new Thread(threadGroup, new StatusReportingThread(), "Statistics reporting");
        statusReportingThread.start();

        // Check to see if LOCAL (static) config file given on command line
        String localConfigFile = System.getProperty("lconfig");
        if (localConfigFile == null) {
            // Must define the INSTALL_DIR env var in order to find config files
            installDir = System.getenv("INSTALL_DIR");
            if (installDir == null) {
                logger.error("CODAComponent exit - INSTALL_DIR is not set");
                System.exit(-1);
            }
            localConfigFile = installDir + File.separator + "emu/conf" + File.separator + "local.xml";
        }

        // Parse LOCAL XML-format config file and store
        try {
            localConfig = Configurer.parseFile(localConfigFile);
        } catch (DataNotFoundException e) {
            e.printStackTrace();
            logger.error("CODAComponent exit - " + localConfigFile + " not found");
            System.exit(-1);
        }

        // Put LOCAL config info into GUI
        if (debugGUI != null) {
            debugGUI.addDocument(localConfig);
            debugGUI.generateInputPanel();
        } else {
            Node node = localConfig.getFirstChild();
            // Puts node & children (actually their associated DataNodes) into GUI
            // and returns DataNode associated with node.
            Configurer.treeToPanel(node,0);
        }

        // Create object responsible for communication w/ runcontrol through cMsg server.
        cmsgPortal = new CMSGPortal(this);

        Configurer.setLogger(null);

        // Need the following info for this object's getter methods
        String tmp = System.getProperty("expid");
        if (tmp != null) expid = tmp;

        tmp = System.getProperty("session");
        if (tmp != null) session = tmp;

        // Get the user name which is added to the payload of logging messages
        tmp = System.getProperty("user.name");
        if (tmp != null) userName = tmp;

        // Get the local hostname which is added to the payload of logging messages
        try {
            InetAddress localMachine = java.net.InetAddress.getLocalHost();
            hostName = localMachine.getHostName();
        } catch (java.net.UnknownHostException uhe) {
            // Ignore this.
        }
    }


    /**
     * This method monitors the mailbox for incoming commands and monitors the state of the emu
     * (actually the state of the moduleFactory) to detect any error conditions.
     * @see CODAComponent#run()
     */
    public void run() {
        logger.info("CODAComponent state monitor thread started");
        State oldState = null;
        State state;
        // Thread.currentThread().getThreadGroup().list();
        do {

            try {
                // do NOT block forever here
                Command cmd = mailbox.poll(1, TimeUnit.SECONDS);

                if (!Thread.interrupted()) {
                    if (cmd != null) {
                        try {
                            this.execute(cmd);

                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                            // This just means that the command was not supported
                            logger.info("command " + cmd + " not supported by " + this.name());
                            continue;
                        }
                    }
                    // If modules are not loaded then our state is either
                    // unconfigured, configured, or error.

                    state = moduleFactory.state();

                    if ((state != null) && (state != oldState)) {
System.out.println("Emu: state changed to " + state.name());
                        if (debugGUI != null) {
                            // Enable/disable transition GUI buttons depending on
                            // which transitions are allowed out of our current state.
                            debugGUI.getToolBar().updateButtons(state);
                        }

                        logger.info("State Change to : " + state.toString());

                        try {
                            //Configurer.setValue(localConfig, "component/status/state", state.toString());
                            Configurer.setValue(localConfig, "status/state", state.toString());
                        } catch (DataNotFoundException e) {
                            // This is almost impossible but catch anyway
System.out.println("ERROR in setting value in local config !!!");
                            logger.error("CODAComponent thread failed to set state");
                        }

                        if (state == CODAState.ERROR) {
                            for (Throwable cause : causes) {
                                logger.error(cause.getMessage());
                            }

                            causes.clear();
                        }
                        oldState = state;
                    }
//                    else {
//                        System.out.println("Emu: state did NOT change, still in " + state.name());
//                    }
                }

            } catch (InterruptedException e) {
                break;
            }

        } while (!Thread.interrupted());

        // if this thread is ending, stop reporting status thread too
        statusReportingThread.interrupt();

        logger.info("Status monitor thread exit now");
    }


    /**
     * This method takes a Command object and attempts to execute it.
     *
     * @param cmd of type Command
     * @see EmuModule#execute(org.jlab.coda.emu.support.control.Command)
     */
    synchronized void execute(Command cmd) {
System.out.println("EXECUTING cmd = " + cmd.name());

        CODACommand codaCommand = cmd.getCodaCommand();

        if (codaCommand == START_REPORTING) {
            statusReportingOn = true;
            // Some commands are for the EMU itself and not all
            // the EMU subcomponents, so return immediately.
            return;
        }
        else if (codaCommand == STOP_REPORTING) {
            statusReportingOn = false;
            return;
        }
        // Run Control tells us our run number
        else if (codaCommand == SET_RUN_NUMBER) {
            // get the new run number and store it
            try {
                //cMsgPayloadItem item = cmd.getMessage().getPayloadItem("RUNNUMBER");

                cMsgPayloadItem item = cmd.getArg("RUNNUMBER");
                if (item != null) {
                    System.out.println("SET RUN NUMBER to " + item.getInt());
                    setRunNumber(item.getInt());
                }
                else {
                    System.out.println("Got RUN NUMBER command but no run #");
                }
            }
            catch (cMsgException e) {
                e.printStackTrace();
            }
            return;
        }
        // Send back our state
        else if (codaCommand == GET_STATE) {
            if ( (cmsgPortal.getServer() != null) &&
                 (cmsgPortal.getServer().isConnected())) {

                // need to reply to sendAndGet msg from Run Control
                cMsgMessage msg = null;
                cMsgMessage rcMsg = cmd.getMessage();
                if (!rcMsg.isGetRequest()) {
                    return;
                }
                try {
                    msg = rcMsg.response();
                }
                catch (cMsgException e) {/* never happen */}
                String state = moduleFactory.state().name().toLowerCase();
//System.out.println("     send STATE text field = " + state);
                msg.setText(state);
                try {
                    cmsgPortal.getServer().send(msg);
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
            }

            return;
        }
        // Send back our CODA class
        else if (codaCommand == GET_CODA_CLASS) {
            if ( (cmsgPortal.getServer() != null) &&
                 (cmsgPortal.getServer().isConnected())) {

                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(name);
                msg.setType(RCConstants.rcGetCodaClassResponse);
                msg.setText(getCodaClass());  // CODA class set in module constructors

                try {
                    cmsgPortal.getServer().send(msg);
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
            }

            return;
        }
        // Send back our object type
        else if (codaCommand == GET_OBJECT_TYPE) {
            if ( (cmsgPortal.getServer() != null) &&
                 (cmsgPortal.getServer().isConnected())) {

                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(name);
                msg.setType(RCConstants.getObjectType);
                msg.setText(objectType);

                try {
                    cmsgPortal.getServer().send(msg);
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
            }

            return;
        }


        // When we are told to CONFIGURE, the EMU handles this even though
        // this command is still passed on down to the modules. Read the
        // (if any) config file and update debug GUI.
        if (codaCommand == CONFIGURE) {
            // save a reference to any previously used config
            Document oldConfig = loadedConfig;

            try {
                // A msg from RC or a press of a debug GUI button can
                // both create a CONFIGURE command. In one case we have a
                // cMsg message from the callback, in the other we don't.
                cMsgMessage msg = cmd.getMessage();
                cMsgPayloadItem pItem;
                String rcConfigString = null, rcConfigFile = null;
                boolean isNewConfig = false;

                if (msg != null) {
                    try {
                        // May have an xml configuration string.
                        pItem = cmd.getArg(RCConstants.configPayloadFileContent);
                        if (pItem != null) {
                            rcConfigString = pItem.getString();
                            // Only get this if we have file content.
                            // This tells us if it changed since last configure.
                            pItem = cmd.getArg(RCConstants.configPayloadFileChanged);
                            if (pItem != null) {
                                isNewConfig = pItem.getInt() == 1;
                            }
                        }

                        // May have configuration file name.
                        pItem = cmd.getArg(RCConstants.configPayloadFileName);
                        if (pItem != null) {
                            rcConfigFile = pItem.getString();
                        }
                    }
                    catch (cMsgException e) {/* never happen */}
                }

                // If this config is sent as a string from Run Control...
                if (rcConfigString != null) {
                    // If it was NOT loaded before, load it now.
                    // If we have a debug GUI and it was used to last load
                    // the configuration, or if rc send a filename which this
                    // emu read and loaded, then reconfigure.
                    if (configSource != Emu.ConfigSource.RC_STRING || isNewConfig) {
System.out.println("LOADING NEW string config = \n" + rcConfigString);
                        Configurer.setLogger(logger);
                        // Parse XML config string into Document object.
                        loadedConfig = Configurer.parseString(rcConfigString);
                        Configurer.removeEmptyTextNodes(loadedConfig.getDocumentElement());
                    }
                    else {
System.out.println("NO CHANGE to string config");
                    }
                    configSource = Emu.ConfigSource.RC_STRING;
                }
                // If config file name is sent (either from Run Control or debug gui) ...
                else if (rcConfigFile != null) {
                    File file = new File(rcConfigFile);
                    if (!file.exists() || !file.isFile()) {
                        throw new DataNotFoundException("File " + rcConfigFile + " cannot be found");
                    }

                    boolean loadFile = true;
                    long modTime = file.lastModified();

                    // If we configured by file name sent in msg last time, and
                    // source is same (rc or debug gui), might not have to reload.
                    if ((configSource == Emu.ConfigSource.RC_FILE  && !cmd.isFromDebugGui()) ||
                        (configSource == Emu.ConfigSource.GUI_FILE &&  cmd.isFromDebugGui()) ) {
                        if (rcConfigFile.equals(msgConfigFile) &&
                            (modTime == configFileModifiedTime)) {
                            loadFile = false;
                        }
                    }

                    // reload
                    if (loadFile) {
System.out.println("LOADING FILE " + rcConfigFile);
                        Configurer.setLogger(logger);
                        // Parse XML config file into Document object.
                        loadedConfig = Configurer.parseFile(rcConfigFile);
                        Configurer.removeEmptyTextNodes(loadedConfig.getDocumentElement());
                        // store name of file loaded & its mod time
                        msgConfigFile = rcConfigFile;
                        configFileModifiedTime = modTime;
                    }
                    else {
System.out.println("ALREADY LOADED " + rcConfigFile);
                    }

                    if (cmd.isFromDebugGui()) {
                        configSource = Emu.ConfigSource.GUI_FILE;
                    }
                    else {
                        configSource = Emu.ConfigSource.RC_FILE;
                    }

                }
                // If we have no config from Run Control or debug gui, use one
                // provided locally on command line (if any).
                // Don't do any fancy stuff here like only reload if
                // file creation date changed, because RC & debug GUI
                // can interfere otherwise.
                else if (cmdLineConfigFile != null) {
                    File file = new File(cmdLineConfigFile);
                    if (!file.exists() || !file.isFile()) {
                        throw new DataNotFoundException("File " + cmdLineConfigFile + " cannot be found");
                    }

                    boolean loadFile = true;
                    long modTime = file.lastModified();

                    // If we configured by cmd line file last time
                    // and file is same, might not have to reload.
                    if ((configSource == Emu.ConfigSource.CMD_LINE_FILE) &&
                        (modTime == configFileModifiedTime)) {
                        loadFile = false;
                    }

                    if (!loadFile) {
System.out.println("ALREADY LOADED CMD LINE FILE " + cmdLineConfigFile);
                    }
                    else {
System.out.println("LOADING CMD LINE FILE " + cmdLineConfigFile);
                        Configurer.setLogger(logger);
                        // Parse XML config file and turn it into Document object.
                        loadedConfig = Configurer.parseFile(cmdLineConfigFile);
                        Configurer.removeEmptyTextNodes(loadedConfig.getDocumentElement());
                        configSource = ConfigSource.CMD_LINE_FILE;
                        configFileModifiedTime = modTime;

                        // Check that name and CODA type have NOT changed.
                        Node modulesConfig = Configurer.getNode(loadedConfig, "component");

                        // Get attributes of the top ("component") node
                        NamedNodeMap nm = modulesConfig.getAttributes();

                        // Get name of component from node
                        Node attr = nm.getNamedItem("name");
                        if (attr == null) {
                            throw new DataNotFoundException("No \"name\" attr in component element of config file");
                        }

                        System.out.println("Cmd line config: found component " + attr.getNodeValue());
                        // Get name in config file and compare to our name - should be same.
                        if (!attr.getNodeValue().equals(name)) {
                            throw new DataNotFoundException("Name in config file (" + attr.getNodeValue() +
                                                             ") conflicts with existing name (" + name + ")");
                        }

                        // Get type of component, if any - must be same as emu type.
                        attr = nm.getNamedItem("type");
                        if (attr != null) {
    System.out.println("\n\n\nExec configure: type = " + attr.getNodeValue());
    System.out.println("              : old codaClass = " + codaClass);
                            CODAClass cc = CODAClass.get(attr.getNodeValue());
                            if (cc != null) {
                                if (cc != codaClass) {
                                    throw new DataNotFoundException("CODA type in config file (" + attr.getNodeValue() +
                                                                     ") conflicts with existing type (" + codaClass + ")");
                                }
                            }
                            else {
                                throw new DataNotFoundException("Unsupported CODA component type in config file (" +
                                                                 attr.getNodeValue() + ")");
                            }
                        }
                    }
                }
                else {
                    // We were told to configure, but no config file or string provided.
                    throw new DataNotFoundException("No config file provided from RC or emu cmd line");
                }
            }
            // parsing XML error
            catch (DataNotFoundException e) {
                logger.error("CONFIGURE FAILED", e.getMessage());
e.printStackTrace();
                causes.add(e);
                moduleFactory.ERROR();
                return;
            }
            finally {
                Configurer.setLogger(null);
            }

            // update (or add to) GUI, window with non-local config info (static info)
            if (debugGUI != null) {
                if (oldConfig != null) debugGUI.removeDocument(oldConfig);
                debugGUI.addDocument(loadedConfig);
            }

        }


        // All commands are passed down to the modules here.
        // Note: the "CODACommand.CONFIGURE" command does nothing in the MODULE_FACTORY
        // except to set its state to "CODAState.CONFIGURED".
        try {
            moduleFactory.execute(cmd);
            logger.info("command " + codaCommand + " executed, state " + cmd.success());
        } catch (CmdExecException e) {
            causes.add(e);
            moduleFactory.ERROR();
        }

        // If given the "reset" command, do that after the modules have reset.
        // Go back to "configured" state
        if (codaCommand == RESET) {
        }
        // If given the "exit" command, do that after the modules have exited
        else if (codaCommand == EXIT) {
            quit();
        }

    }


}
