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
 * This is the main class of the EMU (Event Management Unit) program. It implements the CODAComponent interface
 * which allows communication with Run Control and implements a state machine. The class also implemnts the
 * KbdHandler interface which allows the EMU program to accept commands from the keyboard.
 */
public class Emu implements KbdHandler, CODAComponent {

    /** Field INSTANCE, there is only one instance of the Emu class and it is stored in INSTANCE. */
    public static CODAComponent INSTANCE = null;

    /**
     * Field FRAMEWORK, the Emu can display a window containing debug information, a message log
     * and toolbars that allow commands to be issued without Run Control. This is implemented by
     * the DebugFrame class. If the DebugFrame is displayed it's instance is stored in FRAMEWORK.
     */
    private static DebugFrame FRAMEWORK = null;

    /**
     * Field THREAD_GROUP, the Emu attempts to start all of it's threads in one thread group.
     * the thread group is stored in THREAD_GROUP.
     */
    public static ThreadGroup THREAD_GROUP = null;

    /**
     * Field MODULE_FACTORY, most of the data manipulation in the Emu is done by plug-in modules.
     * The modules are specified in an XML configuration file and managed by an object of the
     * EmuModuleFactory class. The single instance of EmuModuleFactory is stored in MODULE_FACTORY.
     */
    private final static org.jlab.coda.emu.EmuModuleFactory MODULE_FACTORY = new org.jlab.coda.emu.EmuModuleFactory();

    /** Field name is the name of the Emu, initially "unconfigured" */
    private String name = "unconfigured";

    /** Field listener, a thread listens for commands and is of class CmdListener */
    private CmdListener listener = null;

    /** Field statusMonitor, the Emu monitors it's own status via a thread. */
    private Thread statusMonitor = null;

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
    private Document loadedConfig = null;

    /**
     * Field localConfig is an XML document loaded when the configure command is executed.
     * This config contains all of the status variables that change from run to run.
     */
    private Document localConfig = null;

    /** Field cmsgPortal, a CMSGPortal object encapsulates the cMsg API. There is one instance. */
    private final CMSGPortal cmsgPortal;

    /** Field cmsgUDL, the UDL of the cMsg server */
    private String cmsgUDL = null;

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
    private final String installDir;

    /**
     * Method main, entry point for this program simply creates an object of class Emu.
     * The Emu gets arguments from the environment via environment variables and Java properties.
     *
     * @param args of type String[]
     */
    @SuppressWarnings({"UnusedParameters"})
    public static void main(String[] args) {
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
        INSTANCE = this;
        setName("EMUComponent");

        THREAD_GROUP = new ThreadGroup(name);

        statusMonitor = new Thread(THREAD_GROUP, INSTANCE, "State monitor");

        if (System.getProperty("DebugUI") != null) {
            FRAMEWORK = new DebugFrame();
        }

        if (System.getProperty("KBDControl") != null) {
            ApplicationConsole.add(INSTANCE);
            ApplicationConsole.monitorSysin();
            listener = new CmdListener();
        }

        Logger.info("CODAComponent constructor called.");

        mailbox = new LinkedBlockingQueue<Command>();

        statusMonitor.start();

        String emuName = System.getProperty("name");
        if (emuName == null) {
            Logger.error("CODAComponent exit - name not defined");
            System.exit(-1);
        }

        setName(emuName);
        installDir = System.getenv("INSTALL_DIR");
        if (installDir == null) {
            Logger.error("CODAComponent exit - INSTALL_DIR is not set");
            System.exit(-1);
        }

        String configF = installDir + File.separator + "conf" + File.separator + "local.xml";
        try {
            localConfig = Configurer.parseFile(configF);
        } catch (DataNotFoundException e) {
            e.printStackTrace();
            Logger.error("CODAComponent exit - $INSTALL_DIR/conf/local.xml not found");
            System.exit(-1);
        }
        if (FRAMEWORK != null) {
            FRAMEWORK.addDocument(localConfig);
        } else {
            Node node = localConfig.getFirstChild();
            Configurer.getDataNodes(node);
        }

        try {
            System.getProperties().storeToXML(System.out, "test");
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        cmsgUDL = System.getProperty("cmsgUDL");

        cmsgPortal = CMSGPortal.getCMSGPortal(this);

        String tmp = System.getProperty("expid");
        if (tmp != null) expid = tmp;

        tmp = System.getProperty("session");
        if (tmp != null) session = tmp;

        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            hostName = localMachine.getHostName();
        } catch (java.net.UnknownHostException uhe) {
            // Ignore this.
        }

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
     * Method run stored in field statusMonitor this thread monitors the mailbox for incoming commands
     * and monitors the state of the emu (actually the state of the MODULE_FACTORY) to detect any
     * error conditions.
     */
    /*
    * (non-Javadoc)
    *
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
                            // This just means that the command was not supported
                            Logger.info("command " + cmd + " not supported by " + this.name());
                        }
                    }
                    // If modules are not loaded then our state is either unconfigured, configured
                    // or error.

                    state = MODULE_FACTORY.state();

                    if ((state != null) && (state != oldState)) {
                        CODATransition.resume.allow(state.allowed());
                        if (FRAMEWORK != null) {
                            FRAMEWORK.getToolBar().update();
                        }
                        Logger.info("State Change to : " + state.toString());

                        try {
                            Configurer.setValue(localConfig, "status/state", state.toString());
                        } catch (DataNotFoundException e) {
                            // This is almost impossible but catch anyway
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
            CODATransition cmd = CODATransition.valueOf(command);
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

    /** @see org.jlab.coda.emu.EmuModule#state() */
    public State state() {
        return MODULE_FACTORY.state();
    }

    /** @return the debug gui */
    public static DebugFrame getFramework() {
        return FRAMEWORK;
    }

    /** @see CODAComponent#configuration() */
    public Document configuration() {
        return loadedConfig;
    }

    /** @see CODAComponent#parameters() */
    public Document parameters() {
        return localConfig;
    }

    /**
     * @return the name
     *
     * @see org.jlab.coda.emu.EmuModule#name()
     */

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
        if (cmd.equals(RunControl.configure)) {
            String configF = System.getProperty("config");
            if (configF == null) {
                configF = installDir + File.separator + "conf" + File.separator + name + ".xml";
            }
            Document oldConfig = loadedConfig;

            try {
                if (cmd.hasArgs() && (cmd.getArg("config") != null)) {
                    cMsgPayloadItem arg = (cMsgPayloadItem) cmd.getArg("config");
                    if (arg.getType() == cMsgConstants.payloadStr) {
                        loadedConfig = Configurer.parseString(arg.getString());
                    } else {
                        throw new DataNotFoundException("cMsg: configuration argument for configure is not a string");
                    }
                } else {
                    loadedConfig = Configurer.parseFile(configF);
                }

                Configurer.removeEmptyTextNodes(loadedConfig.getDocumentElement());

            } catch (DataNotFoundException e) {
                Logger.error("Configure FAILED", e.getMessage());
                CODAState.ERROR.getCauses().add(e);
                MODULE_FACTORY.ERROR();
                return;
            } catch (cMsgException e) {
                Logger.error("Configure FAILED", e.getMessage());
                CODAState.ERROR.getCauses().add(e);
                MODULE_FACTORY.ERROR();
                return;
            }
            System.out.println("Here in execute");
            if (FRAMEWORK != null) {
                if (oldConfig != null) FRAMEWORK.removeDocument(oldConfig);

                FRAMEWORK.addDocument(loadedConfig);
            }

        }

        // All commands that are !done go to the modules.
        try {
            MODULE_FACTORY.execute(cmd);
            Logger.info("command " + cmd + " executed, state " + cmd.success());
        } catch (CmdExecException e) {
            CODAState.ERROR.getCauses().add(e);
            MODULE_FACTORY.ERROR();
        }

        if (cmd.equals(RunControl.reset)) {
            if ((FRAMEWORK != null) && (loadedConfig != null)) FRAMEWORK.removeDocument(loadedConfig);
            loadedConfig = null;
        }
        if (cmd.equals(RunControl.exit)) {
            quit();
        }
        // We are done so clean the cmd
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
