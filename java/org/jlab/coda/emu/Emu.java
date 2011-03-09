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

import org.jlab.coda.cMsg.cMsgConstants;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgPayloadItem;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.emu.support.codaComponent.*;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.messaging.CMSGPortal;
import org.jlab.coda.emu.support.messaging.RCConstants;
import org.jlab.coda.emu.support.ui.DebugFrame;
import org.w3c.dom.Document;
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
 */
public class Emu implements CODAComponent {

    /**
     * The Emu can display a window containing debug information, a message log
     * and toolbars that allow commands to be issued without Run Control.
     * This is implemented by the DebugFrame class.
     */
    private DebugFrame debugGUI;

    /** The Emu attempts to start all of it's threads in one thread group. */
    private final ThreadGroup threadGroup;

    /**
     * Most of the data manipulation in the Emu is done by plug-in modules.
     * The modules are specified in an XML configuration file and managed by an object of the
     * EmuModuleFactory class.
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

    /**
     * LoadedConfig is the XML document loaded when the configure command is executed.
     * It may change from run to run and tells the Emu which modules to load, which
     * data transports to start and what data channels to open.
     */
    private Document loadedConfig;

    /** Name of the file containing the Emu configuration (if any). */
    private String configFileName;

    /**
     * LocalConfig is an XML document loaded when the configure command is executed.
     * This config contains all of the status variables that change from run to run.
     */
    private Document localConfig;

    /** A CMSGPortal object encapsulates the cMsg API. */
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

    /** The name of the current configuration, passed via the configure command. */
    private String config = "unconfigured";

    /**
     * Type of CODA object this is. Initially this is an EMU,
     * but it may be set later by the module(s) loaded.
     */
    private CODAClass codaClass = CODAClass.EMU;

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

    /**
     * This method sends the loaded XML configuration to the logger.
     */
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
    public String getConfig() {
        return config;
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
     * @see CODAComponent#setConfig(String)
     */
    public void setConfig(String config) {
        this.config = config;
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

    /** Thread which reports the EMU status to run control. */
    private Thread statusReportingThread;

    /** Time in milliseconds of the period of the reportingStatusThread. */
    private int statusReportingPeriod = 2000;

    /** If true, the status reporting thread is actively reporting status to run control. */
    private volatile boolean statusReportingOn;

    
    /** Class defining thread which reports the EMU status to run control. */
    class StatusReportingThread extends Thread {

        StatusReportingThread() {
            setDaemon(true);
        }

        public void run() {
//System.out.println("STATUS REPORTING THREAD: STARTED +++");

            while (!Thread.interrupted()) {
                if (statusReportingOn &&
                   (cmsgPortal.getServer() != null) &&
                   (cmsgPortal.getServer().isConnected())) {
                    
                    cMsgMessage msg = new cMsgMessage();
                    msg.setSubject(name);
                    msg.setType(RCConstants.reportStatus);
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
System.out.println("Stats for module " + statsModule.name() + ": count = " + eventCount +
                   ", words = " + wordCount + ", eventRate = " + eventRate + ", wordRate = " + wordRate);
                        }
                    }

                    try {
                        msg.addPayloadItem(new cMsgPayloadItem(RCConstants.state, state));
                        msg.addPayloadItem(new cMsgPayloadItem(RCConstants.codaClass, "CDEB"));
                        msg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventNumber, eventCount));
                        msg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventRate, eventRate));
                        msg.addPayloadItem(new cMsgPayloadItem(RCConstants.numberOfLongs, wordCount));
                        msg.addPayloadItem(new cMsgPayloadItem(RCConstants.dataRate, wordRate));
                        cmsgPortal.getServer().send(msg);
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
     * @param configFileName  name of Emu configuration file
     * @param loadedConfig    parsed XML document object of Emu configuration file
     * @param cmsgUDL         UDL used to connect to cMsg server to receive run control commands
     * @param debugUI         start a debug GUI
     * @throws EmuException   if name is null
     */
    public Emu(String name, String configFileName, Document loadedConfig,
               String cmsgUDL, boolean debugUI) throws EmuException {

        if (name == null) {
            throw new EmuException("Emu name not defined");
        }

        this.name = name;
        this.cmsgUDL = cmsgUDL;  // may be null
        this.loadedConfig = loadedConfig;
        this.configFileName = configFileName;

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
        } else {
            Node node = localConfig.getFirstChild();
            // Puts node & children (actually their associated DataNodes) into GUI
            // and returns DataNode associated with node.
            Configurer.treeToPanel(node,0);  // TODO: ignoring returned DataNode object
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

        if (debugGUI != null) debugGUI.getToolBar().update();
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
                        }
                    }
                    // If modules are not loaded then our state is either unconfigured, configured
                    // or error.

                    state = moduleFactory.state();

                    if ((state != null) && (state != oldState)) {
                        // Allows all transitions given by state.allowed().
                        // The "allow" method should be static, but is simpler to
                        // just pick a particular enum (in the case, GO)
                        // and use that to allow various transitions.
                        CODATransition.GO.allow(state.allowed());

                        // enable/disable GUI buttons according to our current state
                        if (debugGUI != null) {
                            debugGUI.getToolBar().update();
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
                            Vector<Throwable> causes = CODAState.ERROR.getCauses();
                            for (Throwable cause : causes) {
                                logger.error(cause.getMessage());
                            }

                            causes.clear();
                        }
                        oldState = state;
                    }
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
     * Method execute takes a Command object and attempts to execute it.
     *
     * @param cmd of type Command
     * @see EmuModule#execute(Command)
     */
    synchronized void execute(Command cmd) {
System.out.println("EXECUTING cmd = " + cmd.name());

        // Some commands are for the EMU itself and not all the EMU subcomponents, so return immediately
        if (cmd.equals(SessionControl.START_REPORTING)) {
            statusReportingOn = true;
            // we are done so clean the cmd (necessary since this command object is static & is reused)
            cmd.clearArgs();
            return;
        }
        else if (cmd.equals(SessionControl.STOP_REPORTING)) {
            statusReportingOn = false;
            cmd.clearArgs();
            return;
        }
        // runcontrol tells us our run number
        else if (cmd.equals(SessionControl.SET_RUN_NUMBER)) {
            // get the new run number and store it
            try {
                cMsgPayloadItem item = (cMsgPayloadItem) cmd.getArg("runNumber");
                if (item != null) setRunNumber(item.getInt());
            }
            catch (cMsgException e) {
                e.printStackTrace();
            }
            cmd.clearArgs();
            return;
        }
        // send back our state
        else if (cmd.equals(InfoControl.GET_STATE)) {
            if ( (cmsgPortal.getServer() != null) &&
                 (cmsgPortal.getServer().isConnected())) {

                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(name);
                msg.setType(RCConstants.rcGetStateResponse);
                String state = moduleFactory.state().name().toLowerCase();
                msg.setText(state);  //TODO:correct ??

                try {
                    cmsgPortal.getServer().send(msg);
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
            }

            cmd.clearArgs();
            return;
        }
        // send back our state
        else if (cmd.equals(InfoControl.GET_CODA_CLASS)) {
            if ( (cmsgPortal.getServer() != null) &&
                 (cmsgPortal.getServer().isConnected())) {

                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(name);
                msg.setType(RCConstants.rcGetCodaClassResponse);
                String state = moduleFactory.state().name().toLowerCase();
                msg.setText(getCodaClass());  //TODO: this is not set anywhere!!

                try {
                    cmsgPortal.getServer().send(msg);
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
            }

            cmd.clearArgs();
            return;
        }
        // send back our state    // TODO: is this obsolete??
        else if (cmd.equals(RunControl.GET_STATE)) {
            if ( (cmsgPortal.getServer() != null) &&
                 (cmsgPortal.getServer().isConnected())) {

                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(name);
                // TODO: set type to proper string
                msg.setType(RCConstants.reportStatus);
                String state = moduleFactory.state().name().toLowerCase();

                try {
                    msg.addPayloadItem(new cMsgPayloadItem(RCConstants.state, state));
                    cmsgPortal.getServer().send(msg);
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
            }

            cmd.clearArgs();
            return;
        }


        // When we are told to CONFIGURE, the EMU handles this even though
        // this command is still passed on down to the modules. Read or
        // re-read the config file and update debug GUI.
        if (cmd.equals(CODATransition.CONFIGURE)) {

            // save a reference to any previously used config
            Document oldConfig = loadedConfig;

            try {
                // If a "config" button was pressed, there are no args, but
                // if we received a config command over cMsg, there may be a
                // config file specified. Find out what it is and load 'er up.
                if (cmd.hasArgs() && (cmd.getArg("config") != null)) {
                    Configurer.setLogger(logger);
                    cMsgPayloadItem arg = (cMsgPayloadItem) cmd.getArg("config");
                    if (arg.getType() == cMsgConstants.payloadStr) {
                        // Parse a string containing an XML configuration
                        // and turn it into a Document object.
                        loadedConfig = Configurer.parseString(arg.getString());
                    } else {
                        throw new DataNotFoundException("cMsg: configuration argument for configure is not a string");
                    }

                    Configurer.removeEmptyTextNodes(loadedConfig.getDocumentElement());
                }
            // parsing XML error
            }
            catch (DataNotFoundException e) {
                logger.error("Configure FAILED", e.getMessage());
                CODAState.ERROR.getCauses().add(e);
                moduleFactory.ERROR();
                return;
            // "config" payload item has no string (should never happen)
            }
            catch (cMsgException e) {
                logger.error("Configure FAILED", e.getMessage());
                CODAState.ERROR.getCauses().add(e);
                moduleFactory.ERROR();
                return;
            }
            finally {
                Configurer.setLogger(null);
            }
            
//System.out.println("Here in execute");
            // update (or add to) GUI, window with nonlocal config info (static info)
            if (debugGUI != null) {
                if (oldConfig != null) debugGUI.removeDocument(oldConfig);
                debugGUI.addDocument(loadedConfig);
            }
        }

        // All commands are passed down to the modules here.
        // The MODULE_FACTORY is created as a "static" singleton
        // created upon the loading of this (EMU) class.
        // Note: the "RunControl.CONFIGURE" command does nothing in the MODULE_FACTORY
        // except to set its state to "CODAState.CONFIGURED".
        try {
            moduleFactory.execute(cmd);
            logger.info("command " + cmd + " executed, state " + cmd.success());
        } catch (CmdExecException e) {
            CODAState.ERROR.getCauses().add(e);
            moduleFactory.ERROR();
        }

        // if given the "reset" command, do that after the modules have reset
        if (cmd.equals(CODATransition.RESET)) {
//            if ((FRAMEWORK != null) && (loadedConfig != null)) FRAMEWORK.removeDocument(loadedConfig);
//            loadedConfig = null;
        }
        // if given the "exit" command, do that after the modules have exited
        else if (cmd.equals(RunControl.EXIT)) {
            quit();
        }

        // we are done so clean the cmd (necessary since this command object is static & is reused)
        cmd.clearArgs();

    }


}
