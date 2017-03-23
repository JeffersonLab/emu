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

import org.jlab.coda.emu.modules.*;

import org.jlab.coda.emu.support.codaComponent.*;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.messaging.CMSGPortal;
import org.jlab.coda.emu.support.messaging.RCConstants;
import org.jlab.coda.emu.support.transport.*;
import org.jlab.coda.emu.support.ui.DebugFrame;

import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;
import static org.jlab.coda.emu.support.codaComponent.CODAState.*;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is the main class of the EMU (Event Management Unit) program.
 * It implements the CODAComponent interface which allows communication
 * with Run Control and implements a state machine.
 *
 * @author heyes
 * @author timmer
 */
public class Emu implements CODAComponent {

    /** Name of the Emu, initially "booted". */
    private String name = "booted";

    /** The experiment id. */
    private String expid;

    /** The name of the current DAQ session. */
    private String session;

    /** The name of the current DAQ run type. */
    private String runType;

    /** The name of the host this Emu is running on. */
    private String hostName;

    /** A unique numeric identifier for this Emu. */
    private int codaid;

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
    private volatile int runTypeId;

    /** For a ROC, the smallest number of evio-events/et-buffer that DC/PEB found. */
    private volatile int bufferLevel;

    /** For a file output channel, flag (set at prestart) telling whether to write file or not. */
    private boolean fileWritingOn = true;

    /**
     * The Emu can display a window containing debug information, a message log
     * and toolbars that allow commands to be issued without Run Control.
     * This is implemented by the DebugFrame class.
     */
    private DebugFrame debugGUI;

    /** The Emu starts all of it's threads in one thread group. */
    private final ThreadGroup threadGroup;

    /** Maximum time to wait when commanded to END but no END event received. */
    private long endingTimeLimit = 60000;

    /** If true, stop executing commands coming from run control. Used while resetting. */
    private volatile boolean resetting;

    /**
     * Commands from cMsg are converted into objects of
     * class Command that are then posted in this mailbox queue.
     */
    private final ArrayBlockingQueue<Command> mailbox;

    /** A CMSGPortal object encapsulates all cMsg communication with Run Control. */
    private final CMSGPortal cmsgPortal;

    /** All the dot-decimal format IP addresses of the platform's host. */
    private String[] platformIpAddresses;

    /** The TCP port of the platform's cMsg domain server. */
    private int platformTcpPort;

    /** Path that the data takes through the parts of the emu. */
    private EmuDataPath dataPath;

    private boolean debug = false;

    //------------------------------------------------
    // State / error
    //------------------------------------------------

    /**
     * Error message. reset() sets it back to null.
     * Making this an atomically settable String ensures that only 1 thread
     * at a time can change its value. That way it's only set once per error.
     */
    protected AtomicReference<String> errorMsg = new AtomicReference<>();

    /**
     * Flag to ensure that a single error in this emu only sends
     * one (1) error message to run control. Gets set to false in
     * the reset() method.
     */
    private boolean errorSent;

    /** The Emu monitors it's own status via a thread. */
    private Thread statusMonitor;

    /** State of the emu. */
    private volatile State state = BOOTED;

    /** What was this emu's previous state? Useful when doing RESET transition. */
    private State previousState = BOOTED;

    /** An object used to log error and debug messages. */
    private final Logger logger;

    //-----------------------------------------------------
    // Status reporting
    //-----------------------------------------------------

    /** Destination of this emu's output (cMsg, ET name, or file name). */
    private String outputDestination;

    /** Thread which reports the EMU status to Run Control. */
    private StatusReportingThread statusReportingThread;

    /** Time in milliseconds of the period of the reportingStatusThread. */
    private int statusReportingPeriod = 2000;

    /** If true, the status reporting thread is actively reporting status to Run Control. */
    private volatile boolean statusReportingOn;

    //------------------------------------------------
    // Storage for channels, transports, and modules
    //------------------------------------------------

    /**
     * This object is thread-safe.
     * It is only modified in the {@link #execute(Command)} method and then
     * only by the main EMU thread. However, it is possible that other threads
     * (such as the EMU's statistics reporting thread) may call methods which use its
     * iterator ({@link #state()}, {@link #findModule(String)},
     * and {@link #getStatisticsModule()}) and therefore need to be synchronized.
     * Note that the CopyOnWriteArrayList is a thread-safe variant of the ArrayList
     * and should not be too "expensive" to use since its size will be very small
     * and add/remove methods will be seldom called.
     */
    private final CopyOnWriteArrayList<EmuModule> modules = new CopyOnWriteArrayList<>();

    /** List of input channels. */
    private final CopyOnWriteArrayList<DataChannel> inChannels = new CopyOnWriteArrayList<>();

    /** List of output channels. */
    private final CopyOnWriteArrayList<DataChannel> outChannels = new CopyOnWriteArrayList<>();

    /** Vector containing all DataTransport objects. */
    private final CopyOnWriteArrayList<DataTransport> transports = new CopyOnWriteArrayList<>();

    /** The Fifo transport is handled separately from the other transports. */
    private DataTransportImplFifo fifoTransport;


    //------------------------------------------------
    // Configuration Parameters
    //------------------------------------------------

    /**
     * In the case that a configuration has more than 1 data stream -
     * the number of ERs or final EBs - the components that write to a file
     * (usually the ERs) need to know how many data streams there are in the
     * configuration. This is necessary because when choosing a file name, the
     * stream number will need to be appended at the end to distinguish between
     * data files from different streams. This integer provides that information
     * and defaults to one with the actual value coming from a payload item in the
     * configuration command from run control.<p>
     *
     * Currently this is unused.
     */
    int dataStreamCount = 1;

    /**
     * If the current configuration has more than 1 data stream -
     * the number of ERs or final EBs - and this component is one that writes
     * to a file, this number identifies its stream uniquely.<p>
     *
     * Currently this is unused.
     */
    int dataStreamId;

    /**
     * Configuration data can come from 3 sources:
     * run control string, run control file name, and debug gui file name.
     */
    private enum ConfigSource {
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

    /** Name of the file containing the Emu configuration (if any) given in RC message. */
    private String msgConfigFile;

    /**
     * LocalConfig is an XML document loaded when the configure command is executed.
     * This config contains all of the status variables that change from run to run.
     */
    private Document localConfig;

    /** Instead of loading a local config from a file, just use this string as it's static. */
    private String localConfigXML =
    "<?xml version=\"1.0\"?>\n" +
    "<status state=\"\" eventCount=\"0\" wordCount=\"0\" run_number=\"0\" run_type=\"unknown\" " +
            "run_start_time=\"unknown\" run_end_time=\"unknown\"/>";

    /** If true, there was an error the last time the configure command was processed. */
    private boolean lastConfigHadError;


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
     * @param debugUI         start a debug GUI
     * @param debug           turn on debug printout
     * @throws EmuException   if name is null, or cannot connect to rc server
     */
    public Emu(String name, String type, boolean debugUI, boolean debug) throws EmuException {

        if (name == null) {
            throw new EmuException("Emu name not defined");
        }

        if (type != null) {
            CODAClass cc = CODAClass.get(type);
            if (cc != null) {
                codaClass = cc;
            }
        }
System.out.println("Emu created, name = " + name + ", type = " + codaClass);

        this.name = name;
        this.debug = debug;

        // Set the name of this EMU
        setName(name);

        // Each emu has its own logger
        logger = new Logger();
        Configurer.setLogger(logger);

        // Create the FIFO transport object
        HashMap<String, String> attrs = new HashMap<>(2);
        attrs.put("class", "Fifo");
        attrs.put("server", "false");
        try {
            fifoTransport = new DataTransportImplFifo("Fifo", attrs, null);
        }
        catch (DataNotFoundException e) {/* never happen */}

        // Define thread group so all threads can be handled together
        threadGroup = new ThreadGroup(name);

        // Start up a GUI to control the EMU
        if (debugUI) {
            debugGUI = new DebugFrame(this);
        }

        // Define place to put incoming commands
        mailbox = new ArrayBlockingQueue<>(100);

        // Put this (which is a CODAComponent and therefore Runnable)
        // into a thread group and keep track of this object's thread.
        // This thread is started when statusMonitor.start() is called.
        statusMonitor = new Thread(threadGroup, this, "RC command executor");
        statusMonitor.start();

        // Start up status reporting thread (which needs cmsg to send msgs)
        statusReportingThread = new StatusReportingThread();
        (new Thread(threadGroup, statusReportingThread, "Statistics reporting")).start();

        // Put LOCAL config info into GUI
        if (debugGUI != null) {
            // Parse XML-format config string
            try {
                localConfig = Configurer.parseString(localConfigXML);
            }
            catch (DataNotFoundException e) {/* Never happen */}
            debugGUI.addDocument(localConfig);
            debugGUI.generateInputPanel();
        }

        // Need the following info for this object's getter methods
        // and possibly for connecting to platform.
        String tmp = System.getProperty("expid");
        if (tmp != null) {
            expid = tmp;
        }
        else {
            expid = System.getenv("EXPID");
        }

        if (expid == null) {
            expid = "unknown";
        }

        tmp = System.getProperty("session");
        if (tmp != null) {
            session = tmp;
        }
        else {
            session = "unknown";
        }

        // Get the local hostname which is added to the payload of logging messages
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (java.net.UnknownHostException e) {
            hostName = "unknown";
        }

        // Create object for communication w/ run control through cMsg server
        cmsgPortal = new CMSGPortal(this);

        Configurer.setLogger(null);
    }


    /**
     * This method monitors the mailbox for incoming commands and
     * monitors the state of the emu to detect any error conditions.
     */
    public void run() {

        State oldState = null;

        try {
            while(true) {

                try {
                    // While resetting, stop executing rc commands.
                    // Wait for a bit then check flag again.
                    if (resetting) {
//logger.info("Emu " + name + ": stop executing commands");
                        Thread.sleep(100);
                        continue;
                    }

                    // Do NOT block forever here
                    final Command cmd = mailbox.poll(1, TimeUnit.SECONDS);

                    if (cmd != null) {
                        try {
                            this.execute(cmd);

                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                            // This just means that the command was not supported
                            logger.warn("Emu " + name + ": command, " + cmd + ", not supported ");
                            continue;
                        }
                    }

                    // If modules are not loaded then our state is either
                    // booted, configured, or error.

                    if ((state != null) && (state != oldState)) {
                        if (debugGUI != null) {
                            // Enable/disable transition GUI buttons depending on
                            // which transitions are allowed out of our current state.
                            debugGUI.getToolBar().updateButtons(state);
                        }

                        try {
                            Configurer.setValue(localConfig, "status/state", state.toString());
                        } catch (DataNotFoundException e) {
                            // This is almost impossible but catch anyway
                            logger.info("Emu " + name + ": failed to set state in local config");
                        }

                        oldState = state;
                    }

                } catch (InterruptedException e) {
                    Thread.interrupted(); // clear interrupt flag
                }
            }
        }
        finally {
            logger.info("Emu " + name + ": exit main thread!!!");
        }
    }

    //------------------------------------------------
    // Getters & Setters
    //------------------------------------------------

    /** {@inheritDoc} */
    public String name() {return name;}

    /** {@inheritDoc} */
    public int getCodaid() {return codaid;}

    /**
     * {@inheritDoc}
     * @see CODAComponent#setCodaid(int)
     */
    public void setCodaid(int codaid) {this.codaid = codaid;}

    /** {@inheritDoc} */
    public String getSession() {return session;}

    /** {@inheritDoc} */
    public String getExpid() {return expid;}

    /** {@inheritDoc} */
    public String getHostName() {return hostName;}

    /** {@inheritDoc} */
    public CODAClass getCodaClass() {return codaClass;}

    /** {@inheritDoc} */
    public int getRunNumber() {return runNumber;}

    /** {@inheritDoc} */
    public int getRunTypeId() {return runTypeId;}

    /** {@inheritDoc} */
    public String getRunType() {return runType;}

    /** {@inheritDoc} */
    public String getCmsgUDL() {return cmsgPortal.getRcUDL();}

    /**
     * Get the rc platform's IP addresses as dot-decimal strings.
     * @return rc platform's IP addresses as dot-decimal strings, null if none.
     */
    public String[] getPlatformIpAddresses() {
        return platformIpAddresses;
    }

    /**
     * Get the platform's cMsg domain server's TCP port.
     * @return platform's cMsg domain server's TCP port, 0 if none.
     */
    public int getPlatformTcpPort() {
        return platformTcpPort;
    }

    /**
     * Get the smallest number of evio-events/et-buffer that connected DC/PEB found.
     * Meaningful only for a ROC.
     * @return smallest number of evio-events/et-buffer that connected DC/PEB found.
     */
    public int getBufferLevel() {return bufferLevel;}

    /**
     * Set the smallest number of evio-events/et-buffer that connected DC/PEB found.
     * Meaningful only for a ROC.
     * @param bufferLevel smallest number of evio-events/et-buffer that connected DC/PEB found.
     */
    public void setBufferLevel(int bufferLevel) {this.bufferLevel = bufferLevel;}

    /** {@inheritDoc} */
    public Document configuration() {return loadedConfig;}

    /** {@inheritDoc} */
    public Document parameters() {return localConfig;}

    /**
     * {@inheritDoc}
     * @see CODAComponent#setRunNumber(int)
     */
    public void setRunNumber(int runNumber) {this.runNumber = runNumber;}

    /**
     * {@inheritDoc}
     * @see CODAComponent#setRunTypeId(int)
     */
    public void setRunTypeId(int runTypeId) {this.runTypeId = runTypeId;}

    /**
     * {@inheritDoc}
     * @see CODAComponent#setRunType(String)
     */
    public void setRunType(String runType) {this.runType = runType;}

    /**
     * Get the CODAClass of this emu.
     * @return CODAClass of this emu.
     */
    public CODAClass getCodaClassObject() {return codaClass;}

    /**
     * Method to set the CODAClass member.
     * @param codaClass
     */
    public void setCodaClass(CODAClass codaClass) {this.codaClass = codaClass;}

    /**
     * Set the flag to tell file output channel to actually write the file or not.
     * @return {@code true} if file output channel will actually write the file, else {@code false}.
     */
    public boolean isFileWritingOn() {
        return fileWritingOn;
    }

    /**
     * Get the debug GUI object.
     * @return debug gui.
     */
    public DebugFrame getFramework() {return debugGUI;}

    /**
     * Get the ThreadGroup this emu's threads are part of.
     * @return ThreadGroup this emu's threads are part of.
     */
    public ThreadGroup getThreadGroup() {return threadGroup;}

    /**
     * Get the Logger this emu uses.
     * @return Logger this emu uses.
     */
    public Logger getLogger() {return logger;}

    /**
     * Get the cmsgPortal object of this emu.
     * @return cmsgPortal object of this emu.
     */
    public CMSGPortal getCmsgPortal() {return cmsgPortal;}

    /**
     * This method gets the amount of milliseconds to wait for an
     * END command to succeed before going to an ERROR state.
     * @return amount of milliseconds to wait for an
     *         END command to succeed before going to an ERROR state.
     */
    public long getEndingTimeLimit() {return endingTimeLimit;}

    /**
     * This method sets the name of this CODAComponent object.
     * @param name the name of this CODAComponent object.
     */
    private void setName(String name) {
        this.name = name;
        if (debugGUI != null) debugGUI.setTitle(name);
    }

    /**
     * Get the data path object that directs how the run control
     * commands are distributed among the EMU parts.
     *
     * @return the data path object
     */
    EmuDataPath getDataPath() {return dataPath;}

    /**
     * Set the data path object that directs how the run control
     * commands are distributed among the EMU parts.
     *
     * @param dataPath the data path object
     */
    void setDataPath(EmuDataPath dataPath) {this.dataPath = dataPath;}

    /**
     * Get the module from which we gather statistics.
     * Used to report statistics to Run Control.
     *
     * @return the module from which statistics are gathered.
     */
    EmuModule getStatisticsModule() {
        synchronized(modules) {
            if (modules.size() < 1) return null;

            // Return first module that says its statistics represents EMU statistics
            for (EmuModule module : modules) {
                if (module.representsEmuStatistics()) {
                    return module;
                }
            }

            // If no modules claim to speak for EMU, choose last module in config file
            return modules.get(modules.size()-1);
        }
    }

    /**
     * Get list of input channels.
     * @return list of input channels
     */
    public List<DataChannel> getInChannels() {
        return inChannels;
    }

    /**
     * Get list of output channels.
     * @return list of output channels
     */
    public List<DataChannel> getOutChannels() {
        return outChannels;
    }

    /**
     * Get the id number of the data stream this emu is a part of.
     * Only relevant if this emu is an EB which outputs to a single ER,
     * or has an output file channel.
     * @return  data stream id.
     */
    public int getDataStreamId() {
        return dataStreamId;
    }

    /**
     * Get the total number of data streams in the configuration which contains this emu.
     * @return total number of data streams in this configuration.
     */
    public int getDataStreamCount() {
        return dataStreamCount;
    }



    //------------------------------------------------
    // State & Error methods
    //------------------------------------------------

    /**
     * This method returns the previous state of the modules in this Emu.
     * If the Emu has not undergone any transitions yet, it returns null.
     *
     * @return state before last transition
     * @return null if no transitions undergone yet
     */
    public State previousState() {return previousState;}

    /** {@inheritDoc} */
    public String getError() {return errorMsg.get();}

    /**
     * This method sets the state of this Emu, but not if emu is currently
     * resetting in which case it immediately returns.
     * Setting the state is synchronized on the emu object to ensure
     * that the state does not change while it's being read.
     * @param state desired state of this Emu.
     */
    public void setState(State state) {
        if (resetting) return;

        synchronized (this) {
            this.state = state;
        }
logger.info("Emu " + name + ": state set to " + state.name());
System.out.println("\n\n");
    }

    /**
     * This method sets the state of this Emu to ERROR, but not if the
     * emu is currently resetting in which case it immediately returns.
     * It sends an error message to run control if not already done.
     * Setting the state is synchronized on the emu object to ensure
     * that the state does not change while it's being read.
     * @param errorCause description of error's cause.
     */
    public void setErrorState(String errorCause) {
        if (resetting) return;

        synchronized (this) {
            this.state = ERROR;
            // The error message can be set only once until reset is called.
            // This prevents having to deal with a cascade of errors and
            // should capture the first.
            errorMsg.compareAndSet(null, errorCause);
            if (!errorSent) {
                sendRcErrorMessage(errorCause);
                sendStatusMessage();
                // errorSent is reset to false in reset() method
                errorSent = true;
            }
logger.error("Emu " + name + ": state set to ERROR");
System.out.println("\n\n");
        }
    }

    /** {@inheritDoc} */
    public State state() {return state;}

    //-----------------------------------------------------
    // Time & Status reporting methods
    //-----------------------------------------------------

    /**
     * Send run control an error message which gets displayed in its GUI.
     * @param error error message
     */
    public void sendRcErrorMessage(String error) {
//System.out.println("Emu " + name + " sending special RC display error Msg:\n *** " + error + " ***");
        cmsgPortal.rcGuiErrorMessage(error);
    }

// TODO: strictly speaking the EMU may have many output destinations, so which is right?
    /**
     * Set the output destination name, like a file or et system name, or
     * a string like "cMsg".
     * @param outputDestination name of this emu's output data destination
     */
    public void setOutputDestination(String outputDestination) {
        this.outputDestination = outputDestination;
    }

    /** Allow the "out-of-band" sending of a status message to run control. */
    public void sendStatusMessage() {
        statusReportingThread.sendStatusMessage();
    }

    /**
     * Get the time from System.currentTimeMillis(), but it's updated
     * only every second or thereabouts.
     * @return time from System.currentTimeMillis()
     */
    public long getTime() {
        return statusReportingThread.getTime();
    }

    /**
     * Class defining thread which reports the EMU status to Run Control.<p>
     *
     * Java calls to get the time are notoriously slow -
     * System.currentTimeMillis() & .nanoTime(). We implement a low resolution
     * clock here, for things like flushing output channels when there are
     * low rates, which allows other code to avoid calling these expensive methods
     * directly.
     */
    class StatusReportingThread extends Thread {

        /** Reuse this msg - overwriting fields each time. */
        private final cMsgMessage reportMsg;

        /** Time - updated every statusReportingPeriod milliseconds. */
        private volatile long time;

        /**
         * Get the time from System.currentTimeMillis(), but it's updated
         * only every second or thereabouts.
         * @return time from System.currentTimeMillis()
         */
        public long getTime() {
            return time;
        }

        StatusReportingThread() {
            reportMsg = new cMsgMessage();
            reportMsg.setSubject(name);
            reportMsg.setType(RCConstants.reportStatus);

            setDaemon(true);
        }

        /** Send a status message every 2 (statusReportingPeriod/1000) seconds. */
        public void run() {

            long lastMsgTime = 0L;
            time = System.currentTimeMillis();

            while (!Thread.interrupted()) {

                if (time - lastMsgTime >= statusReportingPeriod) {
                    sendStatusMessage();
                    lastMsgTime = time;
                }

                try {
                    Thread.sleep(250);
                    time = System.currentTimeMillis();
                }
                catch (InterruptedException e) {
                    return;
                }
            }
        }

        /**
         * Send a cMsg message with the status of this EMU to run control's cMsg server.
         * cMsg messages are not thread-safe when it comes to adding payloads so synchronize
         * this method. */
        synchronized void sendStatusMessage() {

            if (statusReportingOn &&
                    (cmsgPortal.getRcServer() != null) &&
                    (cmsgPortal.getRcServer().isConnected())) {

                // clear stats
                long  eventCount=0L, wordCount=0L;
                float eventRate=0.F, dataRate=0.F;
                int   maxEvSize=0, minEvSize=0, avgEvSize=0, chunk_X_EtBuf=0;
                int[] timeToBuild=null, inChanLevels=null, outChanLevels=null;
                String[] inChanNames=null, outChanNames=null;

                // get new statistics from a single representative module
                EmuModule statsModule = getStatisticsModule();
                if (statsModule != null) {
                    Object[] stats = statsModule.getStatistics();
                    if (stats != null) {
                        eventCount    = (Long)   stats[0];
                        wordCount     = (Long)   stats[1];
                        eventRate     = (Float)  stats[2];
                        dataRate      = (Float)  stats[3];

                        maxEvSize     = (Integer)stats[4];
                        minEvSize     = (Integer)stats[5];
                        avgEvSize     = (Integer)stats[6];
                        chunk_X_EtBuf = (Integer)stats[7];
                        timeToBuild   = (int[])  stats[8];
                    }

                    // Channel levels are only meaningful for EB & ER during go.
                    // Channels are created at prestart and go away at end which
                    // means that the returned arrays may be null at the beginning of
                    // prestart and become null sometime during end.
                    if (state == ACTIVE) {
                        inChanLevels  = statsModule.getInputLevels();
                        outChanLevels = statsModule.getOutputLevels();
                        inChanNames   = statsModule.getInputNames();
                        outChanNames  = statsModule.getOutputNames();
                    }
                    else {
                        inChanLevels  = null;
                        outChanLevels = null;
                        inChanNames   = null;
                        outChanNames  = null;
                    }
                }

                try {
                    // Over write any previously defined payload items
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.state, state().name().toLowerCase()));
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.codaClass, codaClass.name()));
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.objectType, "coda3"));

                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventCount, (int)eventCount));
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventCount64, eventCount));
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.numberOfLongs, wordCount));
                    // in Hz
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventRate, eventRate));
                    // in kBytes/sec
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.dataRate, (double)dataRate));
                    // in bytes
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.maxEventSize, maxEvSize));
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.minEventSize, minEvSize));
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.avgEventSize, avgEvSize));
                    reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.chunk_X_EtBuf, chunk_X_EtBuf));

                    // in/output channel ring levels (0-100)
                    if (inChanLevels != null && inChanLevels.length > 0) {
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.inputChanLevels,
                                                                     inChanLevels));
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.inputChanNames,
                                                                     inChanNames));
                    }
                    else {
                        reportMsg.removePayloadItem(RCConstants.inputChanLevels);
                        reportMsg.removePayloadItem(RCConstants.inputChanNames);
                    }

                    if (outChanLevels != null && outChanLevels.length > 0) {
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.outputChanLevels,
                                                                     outChanLevels));
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.outputChanNames,
                                                                     outChanNames));
                    }
                    else {
                        reportMsg.removePayloadItem(RCConstants.outputChanLevels);
                        reportMsg.removePayloadItem(RCConstants.outputChanNames);
                    }

                    // histogram in nanoseconds
                    if (timeToBuild != null && timeToBuild.length > 0) {
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.timeToBuild, timeToBuild));
                    }
                    else {
                        reportMsg.removePayloadItem(RCConstants.timeToBuild);
                    }

                    if (outputDestination != null) {
                        reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.filename, outputDestination));
                    }
                    else {
                        reportMsg.removePayloadItem(RCConstants.filename);
                    }

//                        System.out.println("Emu " + name + ": try sending STATUS REPORTING Msg:");
//                        System.out.println("   " + RCConstants.state + " = " + stateName);
//                        System.out.println("   " + RCConstants.codaClass + " = " + codaClass.name());
//                        System.out.println("   " + RCConstants.eventCount + " = " + eventCount);
//                        System.out.println("   " + RCConstants.eventRate + " = " + eventRate);
//                        System.out.println("   " + RCConstants.numberOfLongs + " = " + wordCount);
//                        System.out.println("   " + RCConstants.dataRate + " = " + (double)wordRate);
//                        System.out.println("   " + RCConstants.timeToBuild + " = " + timeToBuild);
//                        System.out.println("   " + RCConstants.maxEventSize + " = " + maxEvSize);
//                        System.out.println("   " + RCConstants.minEventSize + " = " + minEvSize);
//                        System.out.println("   " + RCConstants.avgEventSize + " = " + avgEvSize);

                    // Send msg
                    cmsgPortal.getRcServer().send(reportMsg);
                }
                catch (cMsgException e) {
                    logger.warn(e.getMessage());
                }
            }
        }

    };


    //-----------------------------------------------------


    /** {@inheritDoc} */
    public void postCommand(Command cmd) throws InterruptedException {
        mailbox.put(cmd);
    }


    /** Exit this Emu (there still may be other threads running in the JVM). */
    void quit() {
logger.info("Emu " + name + " quitting");
        // Shutdown all channel, module, & transport threads
        reset();

        // Get rid of thread watching cMsg connection
        try {
            cmsgPortal.shutdown();
        }
        catch (cMsgException e) {}

        // Get rid of any GUI
        if (debugGUI != null) debugGUI.dispose();

        // Interrupt both of Emu's threads
        statusReportingThread.interrupt();

        // This thread is currently stopping itself
        statusMonitor.stop();
    }


    /**
     * This method executes a RESET command.
     * We don't queued it up and possibly have it wait like a transition command.
     * RESET must always have top priority and is executed in the cMsg callback.
     * Synchronized on emu.
     */
    synchronized public void reset() {
logger.info("Emu " + name + " resetting");
        // Stop any more run control commands from being executed
        resetting = true;

        state = RESETTING;

        // Clear error until next one occurs
        errorSent = false;
        errorMsg.set(null);

        // Clear out any existing, un-executed commands
        mailbox.clear();

        // Interrupt the main emu thread which is processing transition commands
        // since it may be stuck in the middle of a transition. This thread
        // should recover. This method is executed from a cMsg callback thread
        // and is not affected by this call.
        statusMonitor.interrupt();

        // The most difficult situation in which to do a RESET is when
        // data is flowing. It's best to send RESET cmds to various components
        // in the same manner in which ENDs are sent.

        // Reset channels first
        if (inChannels.size() > 0) {
            for (DataChannel chan : inChannels) {
if (debug) System.out.println("Emu " + name + " reset: reset in chan " + chan.name());
                chan.reset();
            }
        }

        // Reset all modules
        for (EmuModule module : modules) {
if (debug) System.out.println("Emu " + name + " reset: try to reset module " + module.name());
            module.reset();
        }

        if (outChannels.size() > 0) {
            for (DataChannel chan : outChannels) {
if (debug) System.out.println("Emu " + name + ": reset out chan " + chan.name());
                chan.reset();
            }
        }

        // Reset transport objects
        for (DataTransport t : transports) {
if (debug) System.out.println("Emu " + name + " reset: reset transport " + t.name());
            t.reset();
        }

        // Reset Fifo transport (removes Fifo channels from its hash table)
        fifoTransport.reset();

        // Set state
        if (previousState == ERROR || previousState == BOOTED) {
            state = BOOTED;
        }
        else {
            state = CONFIGURED;
        }
logger.info("Emu " + name + " reset: done, setting state to " + state);

        // Allow run control commands to be executed once again
        resetting = false;
    }


    /**
     * This method finds the DataTransport object corresponding to the given name.
     *
     * @param name name of transport object
     * @return DataTransport object corresponding to given name
     * @throws DataNotFoundException when no transport object of that name can be found
     */
    private DataTransport findTransport(String name) throws DataNotFoundException {
        DataTransport t;

        // first look in non-fifo transports
        if (!transports.isEmpty()) {
            for (DataTransport transport : transports) {
                t = transport;
                if (t.name().equals(name)) return t;
            }
        }

        // now look at fifo transport
        if (fifoTransport.name().equals(name)) {
            return fifoTransport;
        }

        throw new DataNotFoundException("Data Transport not found");
    }


    /**
     * This method finds the EmuModule object corresponding to the given name.
     *
     * @param name of module object
     * @return EmuModule object corresponding to given name; null if none
     */
    private EmuModule findModule(String name) {
        synchronized(modules) {
            for (EmuModule module : modules) {
                if (module.name().equals(name)) {
                    return module;
                }
            }
        }
        return null;
    }


    /**
     * This method encapsulates a required response
     * to a run control command to send info.
     * Used if msg contains a String.
     *
     * @param type        type of cMsg message
     * @param payloadName name of String payload in cMsg message
     * @param val         value of payload
     * @param rcMsg       original message from run control
     */
    private void replyToRunControl(String type,
                                   String payloadName, String val,
                                   cMsgMessage rcMsg) {

        if (val == null) {
            System.out.println("Emu " + name + ": cannot send cMsg msg since type is null");
            return;
        }

        // Msg to reply to run control
        cMsgMessage msg = null;

        // If received msg is sendAndGet ...
        if (rcMsg.isGetRequest()) {
            try {
                msg = rcMsg.response();
            }
            catch (cMsgException e) {/* never happen */}
        }
        // else if just a regular msg ...
        else {
            msg = new cMsgMessage();
            msg.setSubject(name);
            msg.setType(type);
            return;
        }

        if (payloadName == null) {
            msg.setText(val);
        }
        else {
            try {
                cMsgPayloadItem item = new cMsgPayloadItem(payloadName, val);
                msg.addPayloadItem(item);
            }
            catch (cMsgException e) {/* never happen */}
        }

        try {
            cmsgPortal.getRcServer().send(msg);
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }
    }


    /**
     * This method encapsulates a required response
     * to a run control command to send info.
     * Used if msg contains an int.
     *
     * @param type        type of cMsg message
     * @param payloadName name of String payload in cMsg message
     * @param val         value of payload
     * @param rcMsg       original message from run control
     */
    private void replyToRunControl(String type,
                                   String payloadName, int val,
                                   cMsgMessage rcMsg) {

        // Msg to reply to run control
        cMsgMessage msg = null;

        // If received msg is sendAndGet ...
        if (rcMsg.isGetRequest()) {
            try {
                msg = rcMsg.response();
            }
            catch (cMsgException e) {/* never happen */}
        }
        // else if just a regular msg ...
        else {
            msg = new cMsgMessage();
            msg.setSubject(name);
            msg.setType(type);
            return;
        }

        if (payloadName == null) {
            msg.setUserInt(val);
        }
        else {
            try {
                cMsgPayloadItem item = new cMsgPayloadItem(payloadName, val);
                msg.addPayloadItem(item);
            }
            catch (cMsgException e) {/* never happen */}
        }

        try {
            cmsgPortal.getRcServer().send(msg);
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }
    }


    /**
     * This method encapsulates a required response
     * to a run control command to send info.
     * Used if msg contains a long and requires a payloadName.
     *
     * @param type        type of cMsg message
     * @param payloadName name of String payload in cMsg message
     * @param val         value of payload
     * @param rcMsg       original message from run control
     */
    private void replyToRunControl(String type,
                                   String payloadName, long val,
                                   cMsgMessage rcMsg) {

        if (payloadName == null) {
            return;
        }

        // Msg to reply to run control
        cMsgMessage msg = null;

        // If received msg is sendAndGet ...
        if (rcMsg.isGetRequest()) {
            try {
                msg = rcMsg.response();
            }
            catch (cMsgException e) {/* never happen */}
        }
        // else if just a regular msg ...
        else {
            msg = new cMsgMessage();
            msg.setSubject(name);
            msg.setType(type);
            return;
        }

        try {
            cMsgPayloadItem item = new cMsgPayloadItem(payloadName, val);
            msg.addPayloadItem(item);
        }
        catch (cMsgException e) {/* never happen */}

        try {
            cmsgPortal.getRcServer().send(msg);
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }
    }


    /**
     * This method takes a Command object and attempts to execute it.
     *
     * @param cmd of type Command
     */
    public void execute(Command cmd) {
System.out.println("Emu " + name + ": start executing cmd = " + cmd.name());

        if (resetting) {
System.out.println("Emu " + name + ": do not execute cmd = " + cmd.name() + ", resetting");
            return;
        }

        CODACommand codaCommand = cmd.getCodaCommand();

        // Some commands are for the EMU itself and not
        // the EMU subcomponents, so return immediately.

        if (codaCommand == START_REPORTING) {
            statusReportingOn = true;
            return;
        }

        else if (codaCommand == STOP_REPORTING) {
            statusReportingOn = false;
            return;
        }

        // Run Control tells us our session
        else if (codaCommand == SET_SESSION) {
            // Get the new session and store it
            cMsgPayloadItem pItem = cmd.getArg(RCConstants.sessionPayload);
            if (pItem != null) {
                try {
                    session = pItem.getString();
if (debug) System.out.println("Emu " + name + " SET_SESSION: set to " + session);
                }
                catch (cMsgException e) {
logger.error("Got SET_SESSION command but no session specified 1");
                }
            }
            else {
logger.error("Got SET_SESSION command but no session specified 2");
            }
            return;
        }

        // Run Control tells us our run type
        else if (codaCommand == SET_RUN_TYPE) {
            // Get the new run type and store it
            cMsgPayloadItem pItem = cmd.getArg(RCConstants.runTypePayload);
            if (pItem != null) {
                try {
                    String txt = pItem.getString();
                    runType = txt;
if (debug) System.out.println("Emu " + name + " SET_RUN_TYPE: set to " + txt);
                }
                catch (cMsgException e) {
logger.error("Emu " + name + ": got SET_RUN_TYPE command but no run type specified 1");
                }
            }
            else {
logger.error("Emu " + name + ": got SET_RUN_TYPE command but no run type specified 2");
            }
            return;
        }

        // Run Control tells us our ROC output buffer level
        else if (codaCommand == SET_BUF_LEVEL) {
            // Get the new run type and store it
            int bufLevel = cmd.getMessage().getUserInt();
            if (bufLevel > 0) {
//logger.info("Emu " + name + " SET_BUF_LEVEL: set to " + bufferLevel);
                bufferLevel = bufLevel;
            }
            else {
logger.error("Emu " + name + ": got SET_BUF_LEVEL command but bad value (" + bufLevel + ')');
            }
            return;
        }

        //------------------------------------
        // SENDING INFO BACK TO RUN CONTROL
        //------------------------------------

        // Send back our state
        else if (codaCommand == GET_STATE) {
            if ( (cmsgPortal != null) &&
                 (cmsgPortal.getRcServer() != null) &&
                 (cmsgPortal.getRcServer().isConnected())) {

                replyToRunControl(RCConstants.getStateResponse,
                                  null, state().name().toLowerCase(),
                                  cmd.getMessage());
            }
            return;
        }

        // Send back our CODA class
        else if (codaCommand == GET_CODA_CLASS) {
            if ( (cmsgPortal != null) &&
                 (cmsgPortal.getRcServer() != null) &&
                 (cmsgPortal.getRcServer().isConnected())) {

                // CODA class set in module constructors
                replyToRunControl(RCConstants.getCodaClassResponse,
                                  null, codaClass.name(),
                                  cmd.getMessage());
            }
            return;
        }

        // Send back our object type
        else if (codaCommand == GET_OBJECT_TYPE) {
            if ( (cmsgPortal != null) &&
                 (cmsgPortal.getRcServer() != null) &&
                 (cmsgPortal.getRcServer().isConnected())) {

                replyToRunControl(RCConstants.getObjectTypeResponse,
                                  null, objectType,
                                  cmd.getMessage());
            }
            return;
        }

        // Send back our session
        else if (codaCommand == GET_SESSION) {
            if ( (cmsgPortal != null) &&
                 (cmsgPortal.getRcServer() != null) &&
                 (cmsgPortal.getRcServer().isConnected())) {

                replyToRunControl(RCConstants.getSessionResponse,
                                  null, session,
                                  cmd.getMessage());
            }
            return;
        }

        // Send back our run number
        else if (codaCommand == GET_RUN_NUMBER) {
            if ( (cmsgPortal != null) &&
                 (cmsgPortal.getRcServer() != null) &&
                 (cmsgPortal.getRcServer().isConnected())) {

                replyToRunControl(RCConstants.getRunNumberResponse,
                                  RCConstants.runNumberPayload, (long)runNumber,
                                  cmd.getMessage());
            }
            return;
        }

        // Send back our run type
        else if (codaCommand == GET_RUN_TYPE) {
            if ( (cmsgPortal != null) &&
                 (cmsgPortal.getRcServer() != null) &&
                 (cmsgPortal.getRcServer().isConnected())) {

                replyToRunControl(RCConstants.getRunTypeResponse,
                                  null, runType,
                                  cmd.getMessage());
            }
            return;
        }

        // Send back our config id
        else if (codaCommand == GET_CONFIG_ID) {
            if ( (cmsgPortal != null) &&
                 (cmsgPortal.getRcServer() != null) &&
                 (cmsgPortal.getRcServer().isConnected())) {

                replyToRunControl(RCConstants.getConfigIdResponse,
                                  null, codaid,
                                  cmd.getMessage());
            }
            return;
        }

        // Send back our roc buffer level
        else if (codaCommand == GET_BUF_LEVEL) {
            if ( (cmsgPortal != null) &&
                 (cmsgPortal.getRcServer() != null) &&
                 (cmsgPortal.getRcServer().isConnected())) {

                replyToRunControl(RCConstants.getRocBufferLevelResponse,
                                  null, bufferLevel,
                                  cmd.getMessage());
            }
            return;
        }

        // If given the "exit" command, do that after the modules have exited
        if (codaCommand == EXIT) {
            quit();
            return;
        }

        // Save the current state if attempting a transition
        if (codaCommand.isTransition()) {
            previousState = state;
        }

        //--------------------------
        // CONFIGURE
        //--------------------------
        // When we are told to CONFIGURE, the EMU handles this even though
        // this command is still passed on down to the modules.
        if (codaCommand == CONFIGURE) {
            configure(cmd);
        }

        //--------------------------
        // DOWNLOAD
        //--------------------------
        else if (codaCommand == DOWNLOAD) {
            download(cmd);
        }

        //--------------------------
        // PRESTART
        //--------------------------
        else if (codaCommand == PRESTART) {
            prestart(cmd);
        }

        //--------------------------
        // GO
        //--------------------------
        else if (codaCommand == GO) {
            go();
        }

        //--------------------------
        // END
        //--------------------------
        else if (codaCommand == END) {
            end();
        }

        if (state == ERROR) {
logger.error("Emu " + name + ": transition NOT successful, state = ERROR");
        }
    }



    /**
     * Implement end command.
     */
    private void end() {
logger.info("Emu " + name + " end: change state to ENDING");
        setState(ENDING);

        // How long do we wait for the END event (in milliseconds)?
        long timeout = 30000;

        // Unit of time for waiting is milliseconds.
        TimeUnit timeUnits = TimeUnit.MILLISECONDS;

        try {
            // Fake TS does not have any I/O so handle it here
            if (codaClass == CODAClass.TS) {
                modules.get(0).end();
if (debug) System.out.println("Emu " + name + " end: END cmd to module " + modules.get(0).name());
                setState(DOWNLOADED);
                return;
            }

            LinkedList<EmuModule> mods = dataPath.getEmuModules();

            if (mods.size() < 1) {
                throw new CmdExecException("no modules in data path");
            }

            //--------------------------------------------------------
            // (1) Wait for END event to make its way through the Emu.
            //     Look at the end of the chain of channels & modules.
            //
            // Normally this is the correct behavior. However, in the
            // case of the RocSimulation module, the END command needs
            // to be sent to it FIRST in order for the END event to be
            // generated at all.
            //--------------------------------------------------------

            // Look for the RocSimulation module. If this emu is it, send the END cmd
            for (EmuModule mod : mods) {
                Class c = mod.getClass();
                if (c.getName().equals("org.jlab.coda.emu.modules.RocSimulation")) {
if (debug) System.out.println("Emu " + name + " end: call end() in fake ROC " + mod.name());
                    mod.end();
if (debug) System.out.println("Emu " + name + " end: end() done in fake ROC " + mod.name());
                    break;
                }
            }

            boolean gotEndEvent, gotAllEnds = true;

            // Look at the input channels for END first - NO TIMEOUT
            if (inChannels.size() > 0) {
                for (DataChannel chan : inChannels) {
                    try {
                        chan.getEndCallback().waitForEvent();
                        gotAllEnds = true;
                    }
                    catch (InterruptedException e) {}
                }
            }

            // Look at the last module (30 sec timeout)
            if (mods.size() > 0) {
                try {
if (debug) System.out.println("Emu " + name + " end: wait for END event in module " + mods.getLast().name());
                    gotEndEvent = mods.getLast().getEndCallback().waitForEvent(timeout, timeUnits);
if (debug) System.out.println("Emu " + name + " end: got END event in module " + mods.getLast().name());
                    if (!gotEndEvent) {
if (debug) System.out.println("Emu " + name + " end: timeout (30 sec) waiting for END event in module " + mods.getLast().name());
                        setErrorState("Emu " + name + " end: timeout waiting for END event in module " + mods.getLast().name());
                    }
                    gotAllEnds = gotEndEvent;
                }
                catch (InterruptedException e) {}
            }

            // Look at the output channels (30 sec timeout for each chan)
            if (gotAllEnds && outChannels.size() > 0) {
                for (DataChannel chan : outChannels) {
                    try {
if (debug) System.out.println("Emu " + name + " end: output chan " + chan.name() + " call waitForEvent()");
                        gotEndEvent = chan.getEndCallback().waitForEvent(timeout, timeUnits);
if (debug) System.out.println("Emu " + name + " end: output chan " + chan.name() + " gotEndEvent = " + gotEndEvent);
                        if (!gotEndEvent) {
if (debug) System.out.println("Emu " + name + " end: timeout (30 sec) waiting for END event in output chan " + chan.name());
                            setErrorState("Emu " + name + " end: timeout waiting for END event in output chan " + chan.name());
                        }
                        gotAllEnds = gotAllEnds && gotEndEvent;
                    }
                    catch (InterruptedException e) {}
                }
            }

            if (!gotAllEnds) {
                logger.error("Emu " + name + " end: END event did NOT make it through EMU");
            }

            // (2) END command to input channels (of FIRST module)
            if (inChannels.size() > 0) {
                for (DataChannel chan : inChannels) {
if (debug) System.out.println("Emu " + name + " end: END cmd to in chan " + chan.name());
                    chan.end();
                }
            }

            // (3) END command to all modules in normal order (starting with first)
            for (EmuModule mod : mods) {
                // Only use this code if there's 1 ROC and the run ends when hitting
                // run control's END button.
                // We already sent the END event to the RocSimulation module
                if (mod.getClass().getName().equals("org.jlab.coda.emu.modules.RocSimulation")) {
                    continue;
                }

if (debug) System.out.println("Emu " + name + " end: END cmd to module " + mod.name());
                mod.end();
            }

            // (4) END command to output channels (of LAST module)
            if (outChannels.size() > 0) {
                for (DataChannel chan : outChannels) {
if (debug) System.out.println("Emu " + name + " end: END cmd to out chan " + chan.name());
                    chan.end();
                }
            }

            // (5) END command to transport objects
            for (DataTransport transport : transports) {
if (debug) System.out.println("Emu " + name + " end: END cmd to transport " + transport.name());
                transport.end();
            }
            fifoTransport.end();

        }
        catch (OutOfMemoryError e) {
System.out.println("Emu " + name + " end: jvm out of memory, exiting");
            setErrorState("Emu " + name + " end: jvm out of memory, exiting");
            System.exit(-1);
        }
        catch (CmdExecException e) {
System.out.println("Emu " + name + " end: " + e.getMessage());
            setErrorState("Emu " + name + " end:" + e.getMessage());
            return;
        }

        if (state == ERROR) return;
        setState(DOWNLOADED);
    }



    /**
     * Implement go command.
     */
    private void go() {
logger.info("Emu " + name + " go: change state to GOING");
        setState(GOING);

        try {
            // Fake TS does not have any I/O so handle it here
            if (codaClass == CODAClass.TS) {
                modules.get(0).go();
if (debug) System.out.println("Emu " + name + " go: GO cmd to module " + modules.get(0).name());
                setState(ACTIVE);
                return;
            }

            LinkedList<EmuModule> mods = dataPath.getEmuModules();

            if (mods.size() < 1) {
                throw new CmdExecException("no modules in data path");
            }

            // (1) GO to transport objects
            for (DataTransport transport : transports) {
if (debug) System.out.println("Emu " + name + " go: GO cmd to transport " + transport.name());
                transport.go();
            }

            // (2) GO to output channels (of LAST module)
            if (outChannels.size() > 0) {
                for (DataChannel chan : outChannels) {
if (debug) System.out.println("Emu " + name + " go: GO cmd to out chan " + chan.name());
                    chan.go();
                }
            }

            // (3) GO to all modules in reverse order (starting with last)
            for (int i=mods.size()-1; i >= 0; i--) {
if (debug) System.out.println("Emu " + name + " go: GO cmd to module " + mods.get(i).name());
                mods.get(i).go();
            }

            // (4) GO to input channels (of FIRST module)
            if (inChannels.size() > 0) {
                for (DataChannel chan : inChannels) {
if (debug) System.out.println("Emu " + name + " go: GO cmd to in chan " + chan.name());
                    chan.go();
                }
            }
        }
        catch (OutOfMemoryError e) {
System.out.println("Emu " + name + " go: jvm out of memory, exiting");
            setErrorState("Emu " + name + " go: jvm out of memory, exiting");
            System.exit(-1);
        }
        catch (CmdExecException e) {
System.out.println("Emu " + name + " go: " + e.getMessage());
            setErrorState("Emu " + name + " go: " + e.getMessage());
            return;
        }

        if (state == ERROR) return;
        setState(ACTIVE);
    }



    /**
     * Implement prestart command.
     * @param cmd
     */
    private void prestart(Command cmd) {
logger.info("Emu " + name + " prestart: change state to PRESTARTING");
        setState(PRESTARTING);

        // Run Control tells us our run number & runType.
        // Get and store them.
        cMsgMessage msg = cmd.getMessage();
        cMsgPayloadItem pItem;

        if (msg != null) {
            try {
                // Should have run number
                pItem = cmd.getArg(RCConstants.runNumberPayload);
                if (pItem != null) {
                    runNumber = pItem.getInt();
                }
            }
            catch (cMsgException e) {/* never happen */}

            try {
                // Should have writing file flag
                pItem = cmd.getArg(RCConstants.fileWritingPayload);
                if (pItem != null) {
                    fileWritingOn = pItem.getInt() != 0;
                }
            }
            catch (cMsgException e) {/* never happen */}
        }


        try {
            // Fake TS does not have transport channels so handle it here
            if (codaClass == CODAClass.TS) {
                modules.get(0).prestart();
if (debug) System.out.println("Emu " + name + " prestart: PRESTART cmd to module " + modules.get(0).name());
                setState(PAUSED);
                return;
            }

            //------------------------------------------------
            // PRESTART to transport objects first
            //------------------------------------------------
            for (DataTransport transport : transports) {
if (debug) System.out.println("Emu " + name + " prestart: PRESTART cmd to " + transport.name());
                transport.prestart();
            }

            //------------------------------------------------
            // Create transportation channels for all modules
            //------------------------------------------------
            inChannels.clear();
            outChannels.clear();

            // modulesConfig never null cause checked in download transition
            Node modulesConfig = Configurer.getNode(configuration(), "component/modules");
            Node moduleNode = modulesConfig.getFirstChild();
            // For each module in the list of modules ...
            do {
                // Modules section present in config (no modules if no children)
                if ((moduleNode.getNodeType() == Node.ELEMENT_NODE) && moduleNode.hasChildNodes()) {

                    // Find module object associated with this config node
                    EmuModule module = findModule(moduleNode.getNodeName());
                    if (module == null) {
                        throw new DataNotFoundException("module corresponding to " +
                                                                moduleNode.getNodeName() + " not found");
                    }

                    // Clear out all channels created in previous PRESTART
                    module.clearChannels();

                    ArrayList<DataChannel> in = new ArrayList<>(16);
                    ArrayList<DataChannel> out = new ArrayList<>(4);
                    ArrayList<DataChannel> inFifo = new ArrayList<>(2);
                    ArrayList<DataChannel> outFifo = new ArrayList<>(2);

                    int outputChannelCount = 0;

                    // For each channel in (children of) the module ...
                    NodeList childList = moduleNode.getChildNodes();
                    for (int i = 0; i < childList.getLength(); i++) {
                        Node channelNode = childList.item(i);
                        if (channelNode.getNodeType() != Node.ELEMENT_NODE) continue;

//System.out.println("Emu " + name + " prestart: looking at channel node = " + channelNode.getNodeName());
                        // Get attributes of channel node
                        NamedNodeMap nnm = channelNode.getAttributes();
                        if (nnm == null) {
//System.out.println("Emu " + name + " prestart: junk in config file (no attributes), skip " + channelNode.getNodeName());
                            continue;
                        }

                        // Get "name" attribute node from map
                        Node channelNameNode = nnm.getNamedItem("name");

                        // If none (junk in config file) go to next channel
                        if (channelNameNode == null) {
//System.out.println("Emu " + name + " prestart: junk in config file (no name attr), skip " + channelNode.getNodeName());
                            continue;
                        }
//System.out.println("Emu " + name + " prestart: channel node of attribute \"name\" = " + channelNameNode.getNodeName());
                        // Get name of this channel
                        String channelName = channelNameNode.getNodeValue();
//System.out.println("Emu " + name + " prestart: found channel of name " + channelName);
                        // Get "transp" attribute node from map
                        Node channelTranspNode = nnm.getNamedItem("transp");
                        if (channelTranspNode == null) {
//System.out.println("Emu " + name + " prestart: junk in config file (no transp attr), skip " + channelNode.getNodeName());
                            continue;
                        }
                        // Get name of transport
                        String channelTransName = channelTranspNode.getNodeValue();
//System.out.println("Emu " + name + " prestart: module = " + module.name() + ", channel = " + channelName + ", transp = " + channelTransName);
                        // Look up transport object from name
                        DataTransport trans = findTransport(channelTransName);

                        // Store all attributes in a hashmap to pass to channel
                        Map<String, String> attributeMap = new HashMap<>(8);
                        for (int j = 0; j < nnm.getLength(); j++) {
                            Node a = nnm.item(j);
//System.out.println("Emu " + name + " prestart: Put (" + a.getNodeName() + "," + a.getNodeValue() + ") into attribute map for channel " + channelName);
                            attributeMap.put(a.getNodeName(), a.getNodeValue());
                        }

                        // If it's an input channel ...
                        if (channelNode.getNodeName().equalsIgnoreCase("inchannel")) {
                            // Create channel
                            DataChannel channel = trans.createChannel(channelName, attributeMap,
                                                                      true, this, module, 0);
                            // Add to list while keeping fifos separate
                            if (channelTransName.equals("Fifo")) {
                                // Fifo does NOT notify Emu when END event comes through
                                channel.registerEndCallback(null);
                                inFifo.add(channel);
                            }
                            else {
                                if (channel != null) {
                                    // Give it object to notify Emu when END event comes through
                                    channel.registerEndCallback(new EmuEventNotify());
                                    in.add(channel);
                                }
                            }
                        }
                        // If it's an output channel ...
                        else if (channelNode.getNodeName().equalsIgnoreCase("outchannel")) {
                            DataChannel channel = trans.createChannel(channelName, attributeMap,
                                                                      false, this, module,
                                                                      outputChannelCount++);
                            if (channelTransName.equals("Fifo")) {
                                channel.registerEndCallback(null);
                                outFifo.add(channel);
                            }
                            else {
                                if (channel != null) {
                                    channel.registerEndCallback(new EmuEventNotify());
                                    out.add(channel);
                                }
                            }
                        }
                        else {
//System.out.println("Emu " + name + "u prestart: channel type \"" + channelNode.getNodeName() + "\" is unknown");
                        }
                    }

                    // Set input and output channels of each module
                    module.addInputChannels(in);
                    module.addInputChannels(inFifo);

                    module.addOutputChannels(out);
                    module.addOutputChannels(outFifo);

                    // Keep local track of all channels created
                    inChannels.addAll(in);
                    outChannels.addAll(out);
                }
            } while ((moduleNode = moduleNode.getNextSibling()) != null);  // while another module exists ...

            //-------------------------
            // PRESTART to all:
            //-------------------------

            // Output channels
            for (DataChannel chan : outChannels) {
if (debug) System.out.println("Emu " + name + " prestart: PRESTART cmd to OUT chan " + chan.name());
                chan.prestart();
            }

            // Modules
            for (EmuModule module : modules) {
                // Reset the notification latch as it may have been used
                // if previous transition was "END"
                module.getEndCallback().reset();

if (debug) System.out.println("Emu " + name + " prestart: PRESTART cmd to module " + module.name());
                module.prestart();
            }

            // Input channels
            for (DataChannel chan : inChannels) {
if (debug) System.out.println("Emu " + name + " prestart: PRESTART cmd to IN chan " + chan.name());
                chan.prestart();
            }

        }
        catch (OutOfMemoryError e) {
System.out.println("Emu " + name + " prestart: jvm out of memory, exiting");
            setErrorState("Emu " + name + " prestart: jvm out of memory, exiting");
            System.exit(-1);
        }
        catch (Exception e) {
System.out.println("Emu " + name + " prestart: " + e.getMessage());
            setErrorState("Emu " + name + " prestart: " + e.getMessage());
            return;
        }

        if (state == ERROR) return;
        setState(PAUSED);
    }



    /**
     * Implement download command.
     * @param cmd
     */
    private void download(Command cmd) {
logger.info("Emu " + name + " download: change state to DOWNLOADING");
        setState(DOWNLOADING);

        try {

            // Get the config info again since it may have changed
            Node modulesConfig = Configurer.getNode(configuration(), "component/modules");

            // Check for config problems
            if (modulesConfig == null) {
                // Only happens if  emu.configuration() is null or config file is faulty
                throw new DataNotFoundException("config never loaded / faulty");
            }

            // Need modules to create an emu
            if (!modulesConfig.hasChildNodes()) {
                throw new DataNotFoundException("modules section present in config, but no modules");
            }

            //--------------------------
            // Create transport objects
            //--------------------------

            // Fake TS has no transports
            if (codaClass != CODAClass.TS) {
                try {
                    // If doing a download from the downloaded state,
                    // close the existing transport objects first
                    // (this step is normally done from RESET).
                    for (DataTransport t : transports) {
if (debug) System.out.println("Emu " + name + " download: transport " + t.name() + " reset");
                        t.reset();
                    }

                    // Remove all current data transport objects
                    transports.clear();

                    Node m = Configurer.getNode(configuration(), "component/transports");
                    if (m == null|| !m.hasChildNodes()) {
                        throw new DataNotFoundException("transport section or info missing in config");
                    }

                    NodeList l = m.getChildNodes();

                    //****************************************************
                    // TODO: only create transports if used by a channel!!
                    //****************************************************

                    // for each child node (under component/transports) ...
                    for (int ix = 0; ix < l.getLength(); ix++) {
                        Node n = l.item(ix);

                        if (n.getNodeType() == Node.ELEMENT_NODE) {
                            // type is "server" (send data to) or "client" (get data from)
                            String transportType = n.getNodeName();

                            // store all attributes in a hashmap
                            Map<String, String> attrib = new HashMap<>(8);
                            if (n.hasAttributes()) {
                                NamedNodeMap attr = n.getAttributes();

                                for (int jx = 0; jx < attr.getLength(); jx++) {
                                    Node a = attr.item(jx);
                                    attrib.put(a.getNodeName(), a.getNodeValue());
                                }
                            }

                            if (transportType.equalsIgnoreCase("server")) attrib.put("server", "true");
                            else attrib.put("server", "false");

                            // get the name used to access transport
                            String transportName = attrib.get("name");
                            if (transportName == null) {
                                throw new DataNotFoundException("transport name attribute missing in config");
                            }
if (debug) System.out.println("Emu " + name + " download: creating " + transportName);

                            // Generate a name for the implementation of this transport
                            // from the name passed from the configuration.
                            String transportClass = attrib.get("class");
                            if (transportClass == null) {
                                throw new DataNotFoundException("transport class attribute missing in config");
                            }
                            String implName = "org.jlab.coda.emu.support.transport.DataTransportImpl" + transportClass;

                            // Fifos are created internally, not by an Emu
                            if (transportClass.equals("Fifo")) {
                                //logger.warn("Emu " + name + " download: no need to specify FIFOs in transport section of config");
                                continue;
                            }

                            Class c;
                            try {
                                c = Emu.class.getClassLoader().loadClass(implName);
                                //logger.info("Emu " + name + " download: loaded class = " + c);
                            }
                            catch (Exception e) {
                                throw new CmdExecException("cannot load transport class", e);
                            }

                            try {
                                // 2 constructor args
                                Class[] parameterTypes = {String.class, Map.class, Emu.class};
                                Constructor co = c.getConstructor(parameterTypes);

                                // create an instance & store reference
                                Object[] args = {transportName, attrib, this};
                                transports.add((DataTransport) co.newInstance(args));
                            }
                            catch (Exception e) {
                                throw new CmdExecException("cannot create transport object", e);
                            }
                        } // if node is element
                    } // for each child node
                }
                catch (DataNotFoundException e) {
                    // If we're here, the transport section is missing from the config file.
                    // This is permissible if and only if Fifo is the only transport used.
logger.warn("Emu " + name + " download: transport section missing/incomplete from config");
                }

                // Pass command down to all transport objects
                for (DataTransport transport : transports) {
if (debug) System.out.println("Emu " + name + " download: pass download down to " + transport.name());
                    transport.download();
                }
            }

            //--------------------------
            // Create modules
            //--------------------------

            // Remove all existing modules from collection
            modules.clear();

            Node n = modulesConfig.getFirstChild();
            do {
                if (n.getNodeType() == Node.ELEMENT_NODE) {
                    NamedNodeMap nm2 = n.getAttributes();

                    // Store all attributes in a hashmap to pass to module
                    Map<String, String> attributeMap = new HashMap<>(8);
                    for (int j=0; j < nm2.getLength(); j++) {
                        Node a = nm2.item(j);
                        attributeMap.put(a.getNodeName(), a.getNodeValue());
                    }

                    Node typeAttr = nm2.getNamedItem("class");
                    if (typeAttr == null) {
                        throw new DataNotFoundException("module " + n.getNodeName() +
                                                        " has no class attribute");
                    }
                    String moduleClassName = typeAttr.getNodeValue();

                    // What type of module are we creating?
                    EmuModule module;

                    switch (moduleClassName) {
                        case "EventRecording":
                            module = new EventRecording(n.getNodeName(), attributeMap, this);
                            break;
                        case "EventBuilding":
                            module = new FastEventBuilder(n.getNodeName(), attributeMap, this);
                            break;
                        case "RocSimulation":
                            module = new RocSimulation(n.getNodeName(), attributeMap, this);
                            break;
                        case "FarmController":
                            module = new FarmController(n.getNodeName(), attributeMap, this);
                            break;
                        case "TsSimulation":
                            module = new TsSimulation(n.getNodeName(), attributeMap, this);
                            break;
                        default:

                            if (debug)
                                System.out.println("Emu " + name + " download: load module class " + moduleClassName +
                                                           " to create a module of name " + n.getNodeName() +
                                                           "\n  in classpath = " + System.getProperty("java.class.path"));

                            // Load the class using the JVM's standard class loader
                            Class c = Class.forName(moduleClassName);

                            // Constructor required to have a string, a map, and an emu as args
                            Class[] parameterTypes = {String.class, Map.class, Emu.class};
                            Constructor co = c.getConstructor(parameterTypes);

                            // Create an instance
                            Object[] args = {n.getNodeName(), attributeMap, this};
                            module = (EmuModule) co.newInstance(args);
//if (debug) System.out.println("Emu " + name + " download: loaded module " + moduleClassName);
                            break;
                    }

if (debug) System.out.println("Emu " + name + " download: create module " + module.name());

                    // Give it object to notify Emu when END event comes through
                    module.registerEndCallback(new EmuEventNotify());

                    dataPath.associateModule(module);
                    modules.add(module);
                }

            } while ((n = n.getNextSibling()) != null);

            // Pass DOWNLOAD to all the modules. "modules" is only
            // changed in this method so no synchronization is necessary.
            for (EmuModule module : modules) {
if (debug) System.out.println("Emu " + name + " download: pass download to module " + module.name());
                module.download();
            }
        }
        catch (OutOfMemoryError e) {
System.out.println("Emu " + name + " download: jvm out of memory, exiting");
            setErrorState("Emu " + name + " download: jvm out of memory, exiting");
            System.exit(-1);
        }
        // This includes ClassNotFoundException
        catch (Exception e) {
System.out.println("Emu " + name + " download error: " + e.getMessage());
            setErrorState("Emu " + name + " download: " + e.getMessage());
            return;
        }

        if (state == ERROR) return;
        setState(DOWNLOADED);
    }



    /**
     * Implement configure command.
     * @param cmd
     */
    private void configure(Command cmd) {
logger.info("Emu " + name + " config: change state to CONFIGURING");
        setState(CONFIGURING);

        // save a reference to any previously used config
        Document oldConfig = loadedConfig;
        boolean newConfigLoaded = false;

        // Clear out old data
        outputDestination = null;

        try {
            // A msg from RC or a press of a debug GUI button can
            // both create a CONFIGURE command. In one case we have a
            // cMsg message from the callback, in the other we don't.
            cMsgMessage msg = cmd.getMessage();
            cMsgPayloadItem pItem;
            String rcConfigString = null, rcConfigFile = null;
            boolean isNewConfig = false;

            // Should have run type
            try {
                pItem = cmd.getArg(RCConstants.prestartPayloadRunType);
                if (pItem != null) {
                    runTypeId = pItem.getInt();
                }
            }
            catch (cMsgException e) { }

            if (msg != null) {
                try {
                    // If this is a Ts/RocSimulation emu, this is how we
                    // get the xml configuration string.
                    pItem = cmd.getArg(RCConstants.configPayloadFileContentRoc);
                    if (pItem != null) {
                        rcConfigString = pItem.getString();
                        isNewConfig = true;
                    }
                    // May have an xml configuration string for other emus
                    else {
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
                    }

                    // May have configuration file name.
                    pItem = cmd.getArg(RCConstants.configPayloadFileName);
                    if (pItem != null) {
                        rcConfigFile = pItem.getString();
                    }

                    // May have number of data streams (final EB and ERs) in this configuration.
                    pItem = cmd.getArg(RCConstants.configPayloadStreamCount);
                    if (pItem != null) {
                        dataStreamCount = pItem.getInt();
System.out.println("Emu " + name + " config: data stream count = " + dataStreamCount);
                    }

                    // May have this data stream's id number.
                    pItem = cmd.getArg(RCConstants.configPayloadStreamId);
                    if (pItem != null) {
                        dataStreamId = pItem.getInt();
System.out.println("Emu " + name + " config: data stream ID = " + dataStreamId);
                    }

                    // May have all of platform's IP addresses, dot-decimal format,
                    // along with platform's cMsg domain server's TCP port
                    pItem = cmd.getArg(RCConstants.configPayloadPlatformHosts);
                    if (pItem != null) {
                        platformIpAddresses = pItem.getStringArray();
                        pItem = cmd.getArg(RCConstants.configPayloadPlatformPort);
                        if (pItem != null) {
                            platformTcpPort = pItem.getInt();
                        }
                        // Use the platform's host & port to connect to
                        // platform's cMsg domain server.
System.out.println("Emu " + name + " config: connect to cMsg server");
                        cmsgPortal.cMsgServerConnect();
                    }
                }
                catch (cMsgException e) {/* never happen */}
                catch (EmuException e) {
System.out.println("Emu " + name + " config: " + e.getMessage());
                    setErrorState("Emu " + name + " config: " + e.getMessage());
                    return;
                }
            }

            // If this config is sent as a string from Run Control...
            if (rcConfigString != null) {
                // If it was NOT loaded before, load it now.
                // If we have a debug GUI and it was used to last load
                // the configuration, or if rc sent a filename which this
                // emu read and loaded, then reconfigure.
                if (configSource != Emu.ConfigSource.RC_STRING || isNewConfig) {
System.out.println("Emu " + name + " config: loading new string config = \n" + rcConfigString);
                    Configurer.setLogger(logger);
                    // Parse XML config string into Document object.
                    loadedConfig = Configurer.parseString(rcConfigString);
                    Configurer.removeEmptyTextNodes(loadedConfig.getDocumentElement());
                    newConfigLoaded = true;
                }
                else {
if (debug) System.out.println("Emu " + name + " config: no change to string config");
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
System.out.println("Emu " + name + " config: loading file " + rcConfigFile);
                    Configurer.setLogger(logger);
                    // Parse XML config file into Document object.
                    loadedConfig = Configurer.parseFile(rcConfigFile);
                    Configurer.removeEmptyTextNodes(loadedConfig.getDocumentElement());
                    // store name of file loaded & its mod time
                    msgConfigFile = rcConfigFile;
                    configFileModifiedTime = modTime;
                    newConfigLoaded = true;
                }
//                    else {
//System.out.println("Emu " + name + " configure: already loaded " + rcConfigFile);
//                    }

                if (cmd.isFromDebugGui()) {
                    configSource = Emu.ConfigSource.GUI_FILE;
                }
                else {
                    configSource = Emu.ConfigSource.RC_FILE;
                }

            }
            else {
                // We were told to configure, but no config file or string provided.
                throw new DataNotFoundException("Emu config: No config file provided from RC or debug emu");
            }
        }
        // parsing XML error
        catch (DataNotFoundException e) {
System.out.println("Emu " + name + " config: " + e.getMessage());
            setErrorState("Emu " + name + " config: " + e.getMessage());
            return;
        }
        finally {
            Configurer.setLogger(null);
        }

        // If an error resulted from the last time a configure was done,
        // then the config needs to be reloaded even if the file or
        // string from RC has not changed.
        if (lastConfigHadError) newConfigLoaded = true;

        // update (or add to) GUI, window with non-local config info (static info)
        if (debugGUI != null) {
            if (oldConfig != null) debugGUI.removeDocument(oldConfig);
            debugGUI.addDocument(loadedConfig);
        }

        // WE NEED TO LOOK CAREFULLY AT THE NEWLY LOADED CONFIGURATION.
        // Each EMU may contain only ONE (1) data path. A data path may start
        // with a set of transport input channels (or none at all). The data
        // go through the channels to a single module which uses those channels.
        // From there the data may be passed through a fifo to another module,
        // so on and so forth, until it finally gets passed to a set of transport
        // output channels. In order to keep the data flow from getting
        // ridiculously complex, if a module has a fifo as its output channel,
        // then it may only have ONE output channel. Likewise, if a module has a
        // fifo as its input channel, then it may only have ONE input channel.
        //
        // The reason only one data path is allowed is simply because it prevents
        // complications that arise when the output channels of one path are the
        // input channels of another path. In such a situation, for example, an
        // END event may not reach the second path since both sets of input channels
        // are shutdown simultaneously.
        //
        // The reason all this is important is that RC instructions which end
        // data flow (END or PAUSE) must be sent first to the input  channel,
        // then to each succeeding module in the data flow until it
        // finally gets sent to the output channel. In this way, for example,
        // an END event may be watched for, beginning with the input channel and
        // allowed to pass through the entire data path, enabling the EMU to be
        // shut down in the proper sequence. For RC instructions that start a data
        // flow (RESUME, GO), they must be sent first to the output channel,
        // through the modules in the opposite direction of the data flow,
        // and finally to the input channel.
        //
        // The following code is for analyzing the configuration to find the details
        // of the data path so this EMU can distribute RC's commands in the proper
        // sequence to its components.
        if (newConfigLoaded) {
if (debug) System.out.println("Emu " + name + " config: LOAD NEW config, type = " + codaClass);
            try {
                // Before we look at data flow through the module,
                // it's possible the emu's type has not been defined yet.
                // Look through the new config to find the type and set it.

                // get the config info
                Node componentConfig = Configurer.getNode(loadedConfig, "component");

                // get attributes of the top ("component") node
                NamedNodeMap nm = componentConfig.getAttributes();

                // get type of component from node
                Node attr = nm.getNamedItem("type");
                if (attr != null) {
                    CODAClass myClass = CODAClass.get(attr.getNodeValue());
if (debug) System.out.println("Emu " + name + " config: Got config type = " + myClass + ", I was " + codaClass);
                    if (myClass != null) {
                        // See if it conflicts with what this EMU thinks it is.
                        // (Type EMU can be anything).
                        if (codaClass != null &&
                                codaClass != CODAClass.EMU &&
                                codaClass != myClass) {

                            setErrorState("Emu " + name + " config: conflicting CODA types: rc says " +
                                                  myClass + ", emu cmd line has " + codaClass);
                            lastConfigHadError = true;
                            return;
                        }
                        codaClass = myClass;
                    }
                }

                // Now, on to the modules.
                // We find the data paths by finding the modules
                // which have at least one non-fifo input channel.
                dataPath = null;
                int moduleCount = 0, usedModules = 0;
                int inputFifoCount = 0,  inputChannelCount = 0,
                        outputFifoCount = 0, outputChannelCount = 0;

                // Look in module section of config file ...
                Node modulesConfig = Configurer.getNode(loadedConfig, "component/modules");

                // Need at least 1 module in config file
                if (!modulesConfig.hasChildNodes()) {
                    throw new DataNotFoundException("modules section present in config, but no modules");
                }

                int dataPathCount = 0;

                // List of modules
                NodeList childList = modulesConfig.getChildNodes();

                // Look through all modules ...
                for (int j=0; j < childList.getLength(); j++) {
                    Node moduleNode = childList.item(j);
                    if (moduleNode.getNodeType() != Node.ELEMENT_NODE) continue;

                    moduleCount++;

                    // Name of module is its node name
                    String moduleName = moduleNode.getNodeName();

                    // Get attributes of module & look for codaID
                    int codaID = -1;
                    NamedNodeMap map = moduleNode.getAttributes();
                    if (map != null) {
                        // Get "id" attribute node from map
                        Node modIdNode = map.getNamedItem("id");
                        // If it exists, get its value
                        if (modIdNode != null) {
                            try {
                                codaID = Integer.parseInt(modIdNode.getNodeValue());
                                if (codaID < 0) codaID = -1;
                            }
                            catch (NumberFormatException e) { /* default to -`1 */ }
                        }
                    }

                    // List of channels in (children of) the module ...
                    NodeList childChannelList = moduleNode.getChildNodes();

                    inputFifoCount  =  inputChannelCount = 0;
                    outputFifoCount = outputChannelCount = 0;
                    String channelTransName = null, channelName = null,
                            inputFifoName = null, outputFifoName = null;

                    // First count channels & look for fifos
                    for (int i = 0; i < childChannelList.getLength(); i++) {
                        Node channelNode = childChannelList.item(i);
                        if (channelNode.getNodeType() != Node.ELEMENT_NODE) continue;

                        // Get attributes of channel node
                        NamedNodeMap nnm = channelNode.getAttributes();
                        if (nnm == null) continue;

                        // Get "name" attribute node from map
                        Node channelNameNode = nnm.getNamedItem("name");
                        // If none (junk in config file) go to next channel
                        if (channelNameNode == null) continue;

                        // Get name of this channel
                        channelName = channelNameNode.getNodeValue();

                        // Get "transp" attribute node from map
                        Node channelTranspNode = nnm.getNamedItem("transp");
                        if (channelTranspNode == null) continue;

                        // Get name of transport
                        channelTransName = channelTranspNode.getNodeValue();

                        // If it's an input channel ...
                        if (channelNode.getNodeName().equalsIgnoreCase("inchannel")) {
                            // Count input channels
                            inputChannelCount++;

                            // Count Fifo type input channels
                            if (channelTransName.equals("Fifo")) {
                                inputFifoCount++;
                                inputFifoName = channelName;
                            }
                        }
                        else if (channelNode.getNodeName().equalsIgnoreCase("outchannel")) {
                            outputChannelCount++;

                            if (channelTransName.equals("Fifo")) {
                                outputFifoCount++;
                                outputFifoName = channelName;
                            }

                            // Get attributes of channel & look for id which must match codaID
                            int chanID = -1;
                            // Get "id" attribute node from map
                            Node channelIdNode = nnm.getNamedItem("id");
                            if (channelIdNode != null) {
                                // If it exists, get its value
                                try {
                                    chanID = Integer.parseInt(channelIdNode.getNodeValue());
                                    if (chanID < 0) chanID = -1;
                                }
                                catch (NumberFormatException e) { /* default to -`1 */ }
                            }

                            // Make sure id's match
                            if (codaID > -1 && chanID > -1 && codaID != chanID) {
                                throw new DataNotFoundException("CODA id (" + codaID +
                                                                ") does not match config file output chan id (" +
                                                                 chanID + ")");
                            }
                        }
                    }

                    // Illegal configurations, look for:
                    // 1) more than 1 fifo in/out channel, and
                    // 2) 1 fifo together with a non-fifo channel - either in or out
                    if ( inputFifoCount > 1 || ( inputFifoCount == 1 &&  inputChannelCount > 1) ||
                            outputFifoCount > 1 || (outputFifoCount == 1 && outputChannelCount > 1))   {
                        throw new DataNotFoundException("only 1 input/output channel allowed with fifo in/out");
                    }
                    // 3) input and output fifos must be different
                    else if ((inputFifoCount == 1 && outputFifoCount == 1) &&
                            inputFifoName.equals(outputFifoName)) {
                        throw new DataNotFoundException("input & output fifos for " +
                                                        moduleName + " must be different");
                    }

                    // Find modules with non-fifo (or no) input channels which
                    // will be the beginning point of a data path.
                    if (inputFifoCount < 1) {
                        // Found the starting point of a data path
                        dataPathCount++;
                        // (module with non-fifo input channel)
                        dataPath = new EmuDataPath(moduleName, null, outputFifoName);
                        usedModules++;
                    }

                    // If there is more than one data path, reject the configuration.
                    if (dataPathCount > 1) {
                        throw new DataNotFoundException("only 1 data path allowed");
                    }
                }

                // A fifo may not start a data path
                if (dataPathCount < 1 && inputFifoCount > 0) {
                    throw new DataNotFoundException("fifo not allowed to start data path");
                }

                // No data path (should not happen)
                if (dataPath == null) {
                    throw new DataNotFoundException("no data path found");
                }

                // Now that we have the starting point of the data path
                // (list of connected modules and transports), we can
                // construct the whole path. This will allow us to
                // properly distribute RC commands to all EMU modules
                // & the data transports.

                // Look through all modules trying to add them to path
                again:
                while (true) {

                    // Iterate through all modules
                    for (int j=0; j < childList.getLength(); j++) {
                        Node moduleNode = childList.item(j);
                        if (moduleNode.getNodeType() != Node.ELEMENT_NODE) continue;

                        String moduleName = moduleNode.getNodeName();

                        if (dataPath.containsModuleName(moduleName)) {
                            // This module is already in data
                            // path so go to the next one.
                            continue;
                        }

                        //if (!moduleNode.hasChildNodes()) continue;

                        String channelTransName = null, channelName = null,
                                inputFifoName = null, outputFifoName = null;

                        // List of channels in this module
                        NodeList childChannelList = moduleNode.getChildNodes();

                        // Go through list of channels to pick out fifos
                        for (int i=0; i < childChannelList.getLength(); i++) {

                            Node channelNode = childChannelList.item(i);
                            if (channelNode.getNodeType() != Node.ELEMENT_NODE) continue;

                            NamedNodeMap nnm = channelNode.getAttributes();
                            if (nnm == null) continue;

                            Node channelNameNode = nnm.getNamedItem("name");
                            if (channelNameNode == null) continue;

                            channelName = channelNameNode.getNodeValue();

                            Node channelTranspNode = nnm.getNamedItem("transp");
                            if (channelTranspNode == null) continue;

                            channelTransName = channelTranspNode.getNodeValue();

                            // If it's an input channel ...
                            if (channelNode.getNodeName().equalsIgnoreCase("inchannel")) {
                                // Remember Fifo type input channels
                                if (channelTransName.equals("Fifo")) {
                                    inputFifoName = channelName;
                                }
                            }
                            else if (channelNode.getNodeName().equalsIgnoreCase("outchannel")) {
                                if (channelTransName.equals("Fifo")) {
                                    outputFifoName = channelName;
                                }
                            }
                        }

                        // If successfully added, go through list of modules again
                        // and try to add another.
                        if (dataPath.addModuleName(moduleName, inputFifoName, outputFifoName)) {
                            usedModules++;
                            continue again;
                        }
                    }

                    break;
                }

                // Check for any unused/stranded modules (have fifo input)
                if (moduleCount != usedModules) {
                    throw new DataNotFoundException("not all modules in data path");
                }

                // Check to see is last module's output is to a fifo (bad)
                if (dataPath.getModules().getLast().hasOutputFifo) {
                    throw new DataNotFoundException("last module cannot have output fifo");
                }

//System.out.println("DataPath -> " + dataPath);

            }
            catch (OutOfMemoryError e) {
System.out.println("Emu " + name + " config: jvm out of memory, exiting");
                setErrorState("Emu " + name + " config: jvm out of memory, exiting");
                System.exit(-1);
            }
            catch (DataNotFoundException e) {
System.out.println("Emu " + name + " config: " + e.getMessage());
                setErrorState("Emu " + name + " config: " + e.getMessage());
                lastConfigHadError = true;
                return;
            }

            // Successfully loaded new configuration
            lastConfigHadError = false;
        }

        if (state == ERROR) return;
        setState(CONFIGURED);
    }


}
