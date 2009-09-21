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
import org.jlab.coda.support.codaComponent.CODAComponent;
import org.jlab.coda.support.codaComponent.CODAState;
import org.jlab.coda.support.codaComponent.CODATransition;
import org.jlab.coda.support.codaComponent.RunControl;
import org.jlab.coda.support.configurer.Configurer;
import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.keyboardControl.ApplicationConsole;
import org.jlab.coda.support.keyboardControl.KbdHandler;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.support.messaging.CMSGPortal;
import org.jlab.coda.support.transport.DataChannel;
import org.jlab.coda.support.ui.DebugFrame;
import org.jlab.coda.support.w3.CmdListener;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * This is the main class of the EMU (Event Management Unit) program.
 * It implements the CODAComponent interface which allows communication
 * with Run Control and implements a state machine. The class also
 * implements the KbdHandler interface which allows the EMU program to
 * accept commands from the keyboard.
 * </p>
 * This class is a singleton which means that only one EMU per JVM is
 * allowed by design.
 */
public class Emu implements KbdHandler, CODAComponent {

    /** Field INSTANCE, there is only one instance of the Emu class and it is stored in INSTANCE. */
    public static CODAComponent INSTANCE;

    /**
     * Field FRAMEWORK, the Emu can display a window containing debug information, a message log
     * and toolbars that allow commands to be issued without Run Control. This is implemented by
     * the DebugFrame class. If the DebugFrame is displayed it's instance is stored in FRAMEWORK.
     */
    private static DebugFrame FRAMEWORK;

    /**
     * Field THREAD_GROUP, the Emu attempts to start all of it's threads in one thread group.
     * the thread group is stored in THREAD_GROUP.
     */
    public static ThreadGroup THREAD_GROUP;

    /**
     * Field MODULE_FACTORY, most of the data manipulation in the Emu is done by plug-in modules.
     * The modules are specified in an XML configuration file and managed by an object of the
     * EmuModuleFactory class. The single instance of EmuModuleFactory is stored in MODULE_FACTORY.
     */
    private final static EmuModuleFactory MODULE_FACTORY = new EmuModuleFactory();

    /** Field name is the name of the Emu, initially "unconfigured" */
    private String name = "unconfigured";

    /** Field listener, a thread listens for commands and is of class CmdListener */
    private CmdListener listener;

    /** Field statusMonitor, the Emu monitors it's own status via a thread. */
    private Thread statusMonitor;

    /**
     * Field mailbox, commands from the keyboard or cMsg are converted into objects
     * of class Command that are then posted in the queue called mailbox.
     */
    private final LinkedBlockingQueue<Command> mailbox;

    /**
     * Field loadedConfig is the XML document loaded when the configure command is executed.
     * It may change from run to run and tells the Emu which modules to load, which data transports to
     * start and what data channels to open.
     */
    private Document loadedConfig;

    /**
     * Field localConfig is an XML document loaded when the configure command is executed.
     * This config contains all of the status variables that change from run to run.
     */
    private Document localConfig;

    /** Field cmsgPortal, a CMSGPortal object encapsulates the cMsg API. There is one instance. */
    private final CMSGPortal cmsgPortal;

    /** Field cmsgUDL, the UDL of the cMsg server */
    private String cmsgUDL;

    /** Field session, the name of the current DAQ session */
    private String session;
    /** Field expid, the numberic code corresponding to session */
    private String expid;
    /** Field hostName, the name of the host this Emu is running on. */
    private String hostName;
    /** Field userName, the name of the user account the Emu is running under. */
    private String userName;
    /** Field config, the name of the current configuration, passed via the configure command. */
    private String config = "unconfigured";
    /** Field codaClass, this is an EMU therefore the class of CODA component is EMU. */
    private final String codaClass = "EMU";

    /** Field runNumber, the run number. */
    private int runNumber;
    /** Field runType, the numeric code representing the run type. */
    private int runType;
    /** Field codaid, a unique numeric identifier for this Emu. */
    private int codaid;
    /** Field installDir, the path to the directory that the EMU software is installed in. */
    private String installDir;

    private static boolean debug;


    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private static void decodeCommandLine(String[] args) {

        // loop over all args
        for (String arg : args) {
            if (arg.equalsIgnoreCase("-h")) {
                usage();
                System.exit(-1);
            } else if (arg.equalsIgnoreCase("-debug")) {
                debug = true;
            } else {
                usage();
                System.exit(-1);
            }
        }
    }


    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage:\n\n" +
                "   java Emu\n" +
                "        [-h]                 print this help\n" +
                "        [-debug]             turn on printout\n" +
                "        [-Dname=xxx]         set name of EMU\n"+
                "        [-Dconfig=xxx]       set config file name to be loaded at configuration\n"+
                "        [-Dlconfig=xxx]      set local config file name for loading static info\n"+
                "        [-Dexpid=xxx]        set experiment ID\n"+
                "        [-Dsession=xxx]      set experimental session name\n"+
                "        [-user.name=xxx]     set user's name (defaults to expid, then session)\n"+
                "        [-DcmsgUDL=xxx]      set UDL to connect to cMsg server\n"+
                "        [-DDebugUI]          display a control GUI\n"+
                "        [-DKBDControl]       allow keyboard control\n");
    }


    /**
     * Method main, entry point for this program simply creates an object of class Emu.
     * The Emu gets arguments from the environment via environment variables and Java properties.
     *
     * @param args of type String[]
     */
    @SuppressWarnings({"UnusedParameters"})
    public static void main(String[] args) {
        decodeCommandLine(args);
        // This gets called only once
        new Emu();
    }

    
    /**
     * Constructor
     * The constructor is only called once and the created object is stored in INSTANCE.
     * A thread is started to monitor the state field.
     * Java system properties ar read and if required a debug GUI or keyboard command line
     * interface are stated.
     * <p/>
     * The emu is named from the "name" property.
     * <p/>
     * The emu loads local.xml which contains a specification of status parameters.
     * <p/>
     * The emu starts up a connecton to the cMsg server.
     * <p/>
     * By the end of the constructor several threads have been started  and the static
     * method main will not exit while they are running.
     */
    protected Emu() {
        // singleton reference
        INSTANCE = this;

        // set the name of this EMU temporarily (sets GUI title & thread group name)
        setName("EMUComponent");

        THREAD_GROUP = new ThreadGroup(name);

        // Put this singleton (which is a CODAComponent and therefore Runnable)
        // into a thread group and keep track of this object's thread. This
        // thread is started when statusMonitor.start() is called (below).
        statusMonitor = new Thread(THREAD_GROUP, INSTANCE, "State monitor");

        ///////////////////////////////////////////////////////////////
        // Properties are set with -D option to java interpreter (java)
        ///////////////////////////////////////////////////////////////

        // Start up a GUI to control the EMU
        if (System.getProperty("DebugUI") != null) {
            FRAMEWORK = new DebugFrame();
        }

        // Add keyboard control over EMU
        if (System.getProperty("KBDControl") != null) {
            // add this EMU object to the list of keyboard handlers
            ApplicationConsole.add(INSTANCE);
            // start monitor threads for all of the keyboard handlers
            ApplicationConsole.monitorSysin();
            //
            listener = new CmdListener();
        }

        Logger.info("CODAComponent constructor called.");

        mailbox = new LinkedBlockingQueue<Command>();

        // This object has a self-starting thread
        statusMonitor.start();

        // Must set the name of this EMU
        String emuName = System.getProperty("name");
        if (emuName == null) {
            Logger.error("CODAComponent exit - name not defined");
            System.exit(-1);
        }
        setName(emuName);

        // Check to see if LOCAL (static) config file given on command line
        String configFile = System.getProperty("lconfig");
        if (configFile == null) {
            // Must define the INSTALL_DIR env var in order to find config files
            installDir = System.getenv("INSTALL_DIR");
            if (installDir == null) {
                Logger.error("CODAComponent exit - INSTALL_DIR is not set");
                System.exit(-1);
            }
            configFile = installDir + File.separator + "conf" + File.separator + "local.xml";
        }

        // Parse LOCAL XML-format config file and store
        try {
System.out.println("Try parsing the file -> " + configFile);
            localConfig = Configurer.parseFile(configFile);
System.out.println("Done parsing the file -> " + configFile);
        } catch (DataNotFoundException e) {
            e.printStackTrace();
            Logger.error("CODAComponent exit - " + configFile + " not found");
            System.exit(-1);
        }

        // Put LOCAL config info into GUI
        if (FRAMEWORK != null) {
            FRAMEWORK.addDocument(localConfig);
        } else {
            Node node = localConfig.getFirstChild();
            // Puts node & children (actually their associated DataNodes) into GUI
            // and returns DataNode associated with node.
            Configurer.treeToPanel(node,0);
        }

        // Prints out an XML document representing all the
        // properties contained in the Properties table.
        if (debug) {
            try {
                System.getProperties().storeToXML(System.out, "test");
            } catch (IOException e) { }
        }

        // Get the UDL for connection to the cMsg server. If none given,
        // cMSGPortal defaults to using UDL = "cMsg:cMsg://localhost/cMsg/test".
        cmsgUDL = System.getProperty("cmsgUDL");

        // Get the singleton object responsible for communication with cMsg server
        cmsgPortal = CMSGPortal.getCMSGPortal(this);

        String tmp = System.getProperty("expid");
        if (tmp != null) expid = tmp;

        tmp = System.getProperty("session");
        if (tmp != null) session = tmp;

        // Get the local hostname which is added to the payload of logging messages
        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            hostName = localMachine.getHostName();
        } catch (java.net.UnknownHostException uhe) {
            // Ignore this.
        }

        // Get the user name which is added to the payload of logging messages
        tmp = System.getProperty("user.name");
        if (tmp != null) userName = tmp;

        if (FRAMEWORK != null) FRAMEWORK.getToolBar().update();
    }

    /**
     * Method setName sets the name of this ComponentImpl object.
     *
     * @param pname the name of this ComponentImpl object.
     */
    private void setName(String pname) {
        name = pname;
        if (FRAMEWORK != null) FRAMEWORK.setTitle(pname);
    }

    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.support.codaComponent.CODAComponent#list()
    */
    public void list() {
        if (MODULE_FACTORY.state() != CODAState.UNCONFIGURED) Logger.info("Dump of configuration", Configurer.serialize(loadedConfig));
        else Logger.warn("cannot list configuration, not configured");
    }

    /**
     * Method postCommand puts a command object into a mailbox that is periodically checked by the main thread
     * of the emu.
     *
     * @param cmd of type Command
     *
     * @throws InterruptedException when
     */
    public void postCommand(Command cmd) throws InterruptedException {
        mailbox.put(cmd);
    }

    /**
     * This method monitors the mailbox for incoming commands and monitors the state of the emu
     * (actually the state of the MODULE_FACTORY) to detect any error conditions.
     * @see org.jlab.coda.support.codaComponent.CODAComponent#run()
     */
    public void run() {
        Logger.info("CODAComponent state monitor thread started");
        State oldState = null;
        State state;
        // Thread.currentThread().getThreadGroup().list();
        do {

            try {

                Command cmd = mailbox.poll(1, TimeUnit.SECONDS);

                if (!Thread.interrupted()) {
                    if (cmd != null) {
                        try {
                            this.execute(cmd);

                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                            // This just means that the command was not supported
                            Logger.info("command " + cmd + " not supported by " + this.name());
                        }
                    }
                    // If modules are not loaded then our state is either unconfigured, configured
                    // or error.

                    state = MODULE_FACTORY.state();

                    if ((state != null) && (state != oldState)) {
                        CODATransition.RESUME.allow(state.allowed());
                        if (FRAMEWORK != null) {
                            FRAMEWORK.getToolBar().update();
                        }
                        Logger.info("State Change to : " + state.toString());

                        try {
                            //Configurer.setValue(localConfig, "component/status/state", state.toString());
                            Configurer.setValue(localConfig, "status/state", state.toString());
                        } catch (DataNotFoundException e) {
                            // This is almost impossible but catch anyway
System.out.println("ERROR in setting value in local config !!!");
                            Logger.error("CODAComponent thread failed to set state");
                        }

                        if (state == CODAState.ERROR) {
                            Vector<Throwable> causes = CODAState.ERROR.getCauses();
                            for (Throwable cause : causes) {
                                Logger.error(cause.getMessage());
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
        Logger.info("Status monitor thread exit now");
    }

    /**
     * Method the Emu class implements the KbdHandler interface which allows the emu to
     * respond to text commands.
     *
     * @param command of type String is the incoming command
     * @param out     of type PrintWriter
     *
     * @return boolean
     *
     * @see org.jlab.coda.support.keyboardControl.KbdHandler#keyHandler(String,java.io.PrintWriter,Object)
     */
    public boolean keyHandler(String command, PrintWriter out, Object argument) {
        try {
            CODATransition cmd = CODATransition.valueOf(command.toUpperCase());
            this.postCommand(cmd);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            // Ignore, Command.valueOf generates this
            // exception if the command is not found.
            return false;
        }

        return true;
    }

    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.support.kbd.KbdHandler#printHelp(char)
    */
    public void printHelp(PrintWriter out) {
        out.println("CODAComponent");
        CODATransition[] commands = CODATransition.values();

        for (CODATransition c : commands) {
            out.println(" \t" + c + " - " + c.description());
        }

    }

    /**
     * {@inheritDoc}
     * This actually returns the state of the modules in this EMU.
     */
    public State state() {
        return MODULE_FACTORY.state();
    }

    /** @return the debug gui */
    public static DebugFrame getFramework() {
        return FRAMEWORK;
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
     * Method execute takes a Command object and attempts to execute it.
     *
     * @param cmd of type Command
     *
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    synchronized void execute(Command cmd) {

        // When we are told to CONFIGURE, the EMU handles this even though
        // this command is still passed on down to the modules. Read or
        // re-read the config file and update debug GUI.
        if (cmd.equals(RunControl.CONFIGURE)) {

            // First find our config file (not local config) defined
            // on the command line, using a default if none given.
            String configF = System.getProperty("config");
            if (configF == null) {
                configF = installDir + File.separator + "conf" + File.separator + name + ".xml";
            }
            // save a reference to any previously used config
            Document oldConfig = loadedConfig;

            try {
                // If a "config" button was pressed, there are no args, but
                // if we received a config command over cMsg, there may be a
                // config file specified. Find out what it is and load 'er up.
                if (cmd.hasArgs() && (cmd.getArg("config") != null)) {
                    cMsgPayloadItem arg = (cMsgPayloadItem) cmd.getArg("config");
                    if (arg.getType() == cMsgConstants.payloadStr) {
                        // Parse a string containing an XML configuration
                        // and turn it into a Document object.
                        loadedConfig = Configurer.parseString(arg.getString());
                    } else {
                        throw new DataNotFoundException("cMsg: configuration argument for configure is not a string");
                    }
                } else {
                    // Parse a file containing an XML configuration
                    // and turn it into a Document object.
                    loadedConfig = Configurer.parseFile(configF);
                }

                Configurer.removeEmptyTextNodes(loadedConfig.getDocumentElement());

            // parsing XML error
            } catch (DataNotFoundException e) {
                Logger.error("Configure FAILED", e.getMessage());
                CODAState.ERROR.getCauses().add(e);
                MODULE_FACTORY.ERROR();
                return;
            // "config" payload item has no string (should never happen)
            } catch (cMsgException e) {
                Logger.error("Configure FAILED", e.getMessage());
                CODAState.ERROR.getCauses().add(e);
                MODULE_FACTORY.ERROR();
                return;
            }
            
//System.out.println("Here in execute");
            // update (or add to) GUI, window with nonlocal config info (static info)
            if (FRAMEWORK != null) {
                if (oldConfig != null) FRAMEWORK.removeDocument(oldConfig);
                FRAMEWORK.addDocument(loadedConfig);
            }
        }

        // All commands are passed down to the modules here.
        // The MODULE_FACTORY is created as a "static" singleton
        // created upon the loading of this (EMU) class.
        // Note: the "RunControl.CONFIGURE" command does nothing in the MODULE_FACTORY
        // except to set its state to "CODAState.CONFIGURED".
        try {
            MODULE_FACTORY.execute(cmd);
            Logger.info("command " + cmd + " executed, state " + cmd.success());
        } catch (CmdExecException e) {
            CODAState.ERROR.getCauses().add(e);
            MODULE_FACTORY.ERROR();
        }

        // if given the "reset" command, do that after the modules have reset
        if (cmd.equals(RunControl.RESET)) {
            if ((FRAMEWORK != null) && (loadedConfig != null)) FRAMEWORK.removeDocument(loadedConfig);
            loadedConfig = null;
        }
        // if given the "exit" command, do that after the modules have exited
        else if (cmd.equals(RunControl.EXIT)) {
            quit();
        }

        // we are done so clean the cmd (necessary since this command object is static & is reused)
        cmd.clearArgs();

    }

    /** Does what it says. */
    void quit() {

        ApplicationConsole.closeAll();

        if (listener != null) listener.close();

        try {
            cmsgPortal.shutdown();
        } catch (cMsgException e) {
            // ignore
        }

        if (FRAMEWORK != null) FRAMEWORK.dispose();
        statusMonitor.interrupt();
        System.exit(0);
    }

    /**
     * Method channels ...
     *
     * @return HashMap<String, DataChannel>
     *
     * @see org.jlab.coda.emu.Emu#channels()
     */
    @SuppressWarnings({"SameReturnValue"})
    public HashMap<String, DataChannel> channels() {
        return null;
    }

    /**
     * Method getCodaid returns the codaid.
     *
     * @return the codaid (type int).
     */
    public int getCodaid() {
        return codaid;
    }

    /**
     * Method getSession returns the session.
     *
     * @return the session (type String).
     */
    public String getSession() {
        return session;
    }

    /**
     * Method getExpid returns the expid.
     *
     * @return the expid (type String).
     */
    public String getExpid() {
        return expid;
    }

    /**
     * Method getHostName returns the hostName.
     *
     * @return the hostName (type String).
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Method getUserName returns the userName.
     *
     * @return the userName (type String).
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Method getConfig returns the config string.
     *
     * @return the config (type String).
     */
    public String getConfig() {
        return config;
    }

    /**
     * Method getCodaClass returns the codaClass.
     *
     * @return the codaClass (type String).
     */
    public String getCodaClass() {
        return codaClass;
    }

    /**
     * Method getRunNumber returns the runNumber.
     *
     * @return the runNumber (type int).
     */
    public int getRunNumber() {
        return runNumber;
    }

    /**
     * Method getRunType returns the runType.
     *
     * @return the runType (type int).
     */
    public int getRunType() {
        return runType;
    }

    /**
     * Method getCmsgUDL returns the cmsgUDL.
     *
     * @return the cmsgUDL (type String).
     */
    public String getCmsgUDL() {
        return cmsgUDL;
    }

    /**
     * Method setConfig sets the config of this object.
     *
     * @param config the new config string.
     *
     * @see org.jlab.coda.support.codaComponent.CODAComponent#setConfig(String)
     */
    public void setConfig(String config) {
        this.config = config;
    }

    /**
     * Method setRunNumber sets the runNumber.
     *
     * @param runNumber the new runNumber.
     *
     * @see org.jlab.coda.support.codaComponent.CODAComponent#setRunNumber(int)
     */
    public void setRunNumber(int runNumber) {
        this.runNumber = runNumber;
    }

    /**
     * Method setRunType sets the runType.
     *
     * @param runType the new runType.
     *
     * @see org.jlab.coda.support.codaComponent.CODAComponent#setRunType(int)
     */
    public void setRunType(int runType) {
        this.runType = runType;
    }

    /**
     * Method setCodaid sets the codaid.
     *
     * @param codaid the new codaid.
     *
     * @see org.jlab.coda.support.codaComponent.CODAComponent#setCodaid(int)
     */
    public void setCodaid(int codaid) {
        this.codaid = codaid;
    }

    /**
     * Method getCmsgPortal returns the cmsgPortal.
     *
     * @return the cmsgPortal (type CMSGPortal) of this EMUComponentImpl object.
     */
    public CMSGPortal getCmsgPortal() {
        return cmsgPortal;
    }
}
