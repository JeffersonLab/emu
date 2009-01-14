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
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * -----------------------------------------------------
 * Copyright (c) 2008 Jefferson lab data acquisition group
 * Class ComponentImpl ...
 *
 * @author heyes
 *         Created on Sep 12, 2008
 */
public class Emu implements KbdHandler, CODAComponent {

    /** Field INSTANCE */
    public static CODAComponent INSTANCE = null;

    /** Field FRAMEWORK */
    protected static DebugFrame FRAMEWORK = null;

    /** Field THREAD_GROUP */
    public static ThreadGroup THREAD_GROUP = null;

    /** Field MODULE_FACTORY */
    private final static EmuModuleFactory MODULE_FACTORY = new EmuModuleFactory();

    /** Field name */
    private String name = "unconfigured";

    /** Field listener */
    private CmdListener listener = null;

    /** Field statusMonitor */
    private Thread statusMonitor = null;

    /** Field mailbox */
    private final LinkedBlockingQueue<Command> mailbox;

    /** Field loadedConfig */
    private Document loadedConfig = null;

    /** Field localConfig */
    private Document localConfig = null;

    /** Field cmsgPortal */
    private CMSGPortal cmsgPortal;

    /** Field cmsgUDL */
    private String cmsgUDL = null;

    /** Field session */
    private String session;
    /** Field expid */
    private String expid;
    /** Field hostName */
    private String hostName;
    /** Field userName */
    private String userName;
    /** Field config */
    private String config = "unconfigured";
    /** Field codaClass */
    private final String codaClass = "EMU";

    /** Field runNumber */
    private int runNumber;
    /** Field runType */
    private int runType;
    /** Field codaid */
    private int codaid;
    private String emuName;
    private String installDir;

    /**
     * Method main ...
     *
     * @param args of type String[]
     */
    public static void main(String[] args) {
        // This gets called only once
        new Emu();

    }

    /** Constructor ComponentImpl creates a new ComponentImpl instance. */
    // End of fields
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

        Logger.info("CODAComponent constructor called");

        mailbox = new LinkedBlockingQueue<Command>();

        statusMonitor.start();

        emuName = System.getProperty("name");
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
            Logger.error("CODAComponent exit - $INSTALL_DIR/conf/local.xml not found");
            System.exit(-1);
        }
        if (FRAMEWORK != null) {
            FRAMEWORK.addDocument(localConfig);
        } else {
            Node node = localConfig.getFirstChild();
            Configurer.getDataNodes(node);
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

    /** Method list ... */
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
     * Method postCommand ...
     *
     * @param cmd of type Command
     * @throws InterruptedException when
     */
    public void postCommand(Command cmd) throws InterruptedException {
        mailbox.put(cmd);
    }

    /** Method run ... */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.support.codaComponent.CODAComponent#run()
    */
    public void run() {
        Logger.info("CODAComponent state monitor thread started");
        State oldState = null;
        State state = null;
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
     * Method keyHandler ...
     *
     * @param s        of type String
     * @param out      of type PrintWriter
     * @param argument of type Object
     * @return boolean
     * @see org.jlab.coda.support.keyboardControl.KbdHandler#keyHandler(String, PrintWriter, Object)
     */
    public boolean keyHandler(String s, PrintWriter out, Object argument) {
        try {
            CODATransition cmd = CODATransition.valueOf(s);
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

    /**
     * Method printHelp ...
     *
     * @param out of type PrintWriter
     */
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

    /** @return the framework */
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
     * @see org.jlab.coda.emu.EmuModule#name()
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.support.codaComponent.CODAComponent#getName()
    */
    public String name() {
        return name;
    }

    /**
     * Method execute ...
     *
     * @param cmd of type Command
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    public synchronized void execute(Command cmd) {
        boolean done = false;
        if (cmd.equals(RunControl.configure)) {
            String configF = System.getProperty("config");
            if (configF == null) {
                configF = installDir + File.separator + "conf" + File.separator + emuName + ".xml";
            }
            Document oldConfig = loadedConfig;

            try {
                if (cmd.hasArgs()) {
                    cMsgPayloadItem arg = (cMsgPayloadItem) cmd.getArg("configuration");
                    if (arg.getType() == cMsgConstants.payloadStr) {
                        loadedConfig = Configurer.parseString(arg.getString());
                    } else {
                        Exception e = new DataNotFoundException("cMsg: configuration argument for configure is not a string");
                        Logger.error("Configure FAILED", e);
                        CODAState.ERROR.getCauses().add(e);
                        MODULE_FACTORY.ERROR();
                        return;
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
            Logger.error("Command " + cmd + " failed");

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

    /** Method quit ... */
/*
* (non-Javadoc)
*
* @see org.jlab.coda.emu.CODAComponent#reset()
*/
    public void quit() {

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
     * @see org.jlab.coda.emu.Emu#channels()
     */
    public HashMap<String, DataChannel> channels() {
        return null;
    }

    /**
     * Method getCodaid returns the codaid of this CODAComponent object.
     *
     * @return the codaid (type int) of this CODAComponent object.
     */
    public int getCodaid() {
        return codaid;
    }

    /**
     * Method getSession returns the session of this CODAComponent object.
     *
     * @return the session (type String) of this CODAComponent object.
     */
    public String getSession() {
        return session;
    }

    /**
     * Method getExpid returns the expid of this CODAComponent object.
     *
     * @return the expid (type String) of this CODAComponent object.
     */
    public String getExpid() {
        return expid;
    }

    /**
     * Method getHostName returns the hostName of this CODAComponent object.
     *
     * @return the hostName (type String) of this CODAComponent object.
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Method getUserName returns the userName of this CODAComponent object.
     *
     * @return the userName (type String) of this CODAComponent object.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Method getConfig returns the config of this CODAComponent object.
     *
     * @return the config (type String) of this CODAComponent object.
     */
    public String getConfig() {
        return config;
    }

    /**
     * Method getCodaClass returns the codaClass of this CODAComponent object.
     *
     * @return the codaClass (type String) of this CODAComponent object.
     */
    public String getCodaClass() {
        return codaClass;
    }

    /**
     * Method getRunNumber returns the runNumber of this CODAComponent object.
     *
     * @return the runNumber (type int) of this CODAComponent object.
     */
    public int getRunNumber() {
        return runNumber;
    }

    /**
     * Method getRunType returns the runType of this CODAComponent object.
     *
     * @return the runType (type int) of this CODAComponent object.
     */
    public int getRunType() {
        return runType;
    }

    /**
     * Method getCmsgUDL returns the cmsgUDL of this CODAComponent object.
     *
     * @return the cmsgUDL (type String) of this CODAComponent object.
     */
    public String getCmsgUDL() {
        return cmsgUDL;
    }

    /**
     * Method setConfig sets the config of this EMUComponentImpl object.
     *
     * @param config the config of this EMUComponentImpl object.
     * @see org.jlab.coda.support.codaComponent.CODAComponent#setConfig(String)
     */
    public void setConfig(String config) {
        this.config = config;
    }

    /**
     * Method setRunNumber sets the runNumber of this EMUComponentImpl object.
     *
     * @param runNumber the runNumber of this EMUComponentImpl object.
     * @see org.jlab.coda.support.codaComponent.CODAComponent#setRunNumber(int)
     */
    public void setRunNumber(int runNumber) {
        this.runNumber = runNumber;
    }

    /**
     * Method setRunType sets the runType of this EMUComponentImpl object.
     *
     * @param runType the runType of this EMUComponentImpl object.
     * @see org.jlab.coda.support.codaComponent.CODAComponent#setRunType(int)
     */
    public void setRunType(int runType) {
        this.runType = runType;
    }

    /**
     * Method setCodaid sets the codaid of this EMUComponentImpl object.
     *
     * @param codaid the codaid of this EMUComponentImpl object.
     * @see org.jlab.coda.support.codaComponent.CODAComponent#setCodaid(int)
     */
    public void setCodaid(int codaid) {
        this.codaid = codaid;
    }

    /**
     * Method getCmsgPortal returns the cmsgPortal of this EMUComponentImpl object.
     *
     * @return the cmsgPortal (type CMSGPortal) of this EMUComponentImpl object.
     */
    public CMSGPortal getCmsgPortal() {
        return cmsgPortal;
    }
}
