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


import com.lmax.disruptor.*;
import com.lmax.disruptor.TimeoutException;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.et.enums.Modify;
import org.jlab.coda.et.exception.*;
import org.jlab.coda.et.system.AttachmentLocal;
import org.jlab.coda.et.system.StationLocal;
import org.jlab.coda.et.system.SystemCreate;
import org.jlab.coda.jevio.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;


/**
 * This class implement a data channel which gets data from
 * or sends data to an ET system.
 *
 * @author timmer
 * (Dec 2, 2009)
 */
public class DataChannelImplEt extends DataChannelAdapter {

    /** Data transport subclass object for Et. */
    private final DataTransportImplEt dataTransportImplEt;

    /** Is this an input channel? */
    private final boolean input;

    /** Do we pause the dataThread? */
    private volatile boolean pause;

    /** Read END event from input ring. */
    private volatile boolean haveInputEndEvent;

    /** Got RESET command from Run Control. */
    private volatile boolean gotResetCmd;

    /** Got END or RESET command from Run Control and must stop thread getting events. */
    private volatile boolean stopGetterThread;

    // OUTPUT

    /** Thread used to output data. */
    private DataOutputHelper dataOutputThread;

    /** Is the EMU using this ET output channel as a simulated ROC ? */
    private boolean isROC;

    /** Is the EMU using this ET output channel as an event builder? */
    private boolean isEB;

    /** Is the EMU using this ET output channel as the last level event builder? */
    private boolean isFinalEB;

    /** Is the EMU using this ET output channel as the first level event builder? */
    private boolean isFirstEB;

    // INPUT

    /** Store locally whether this channel's module is an ER or not.
      * If so, don't parse incoming data so deeply - only top bank header. */
    private boolean isER;

    /** Thread used to input data. */
    private DataInputHelper dataInputThread;

    /** Node pools to use to get top-level EvioNode object.
     * Index is number of buffer in ByteBufferSupplys. */
    private EvioNodePool[] nodePools;

    //-------------------------------------------
    // ET Stuff
    //-------------------------------------------

    /** Number of events to ask for in an array. */
    private int chunk;

    /** Number of groups from which new ET events are taken. */
    private int group;

    /** If true, there will be a deadlock at prestart since putEvents is blocked
     * due to newEvents not returning in sleep mode due to too few events. */
    private boolean deadLockAtPrestart;

    /** Control words of each ET event written to output. */
    private int[] control;

    /** ET system connected to. */
    private EtSystem etSystem;

    /** Local, running, java ET system. */
    private SystemCreate etSysLocal;

    /** ET station attached to. */
    private EtStation station;

    /** ET station attached to. */
    private StationLocal stationLocal;

    /** Name of ET station attached to. */
    private String stationName;

    /** Position of ET station attached to. */
    private int stationPosition = 1;

    /** Attachment to ET station. */
    private EtAttachment attachment;

    /** Attachment to ET station. */
    private AttachmentLocal attachmentLocal;

    /** Configuration of ET station being created and attached to. */
    private EtStationConfig stationConfig;

    /** Time in microseconds to wait for the ET system to deliver requested events
     *  before throwing an EtTimeoutException. */
    private static final int etWaitTime = 500000;


    //-------------------------------------------
    // Disruptor (RingBuffer)  Stuff
    //-------------------------------------------
    private long nextRingItem;       // input

    /** Ring buffer holding ByteBuffers when using EvioCompactEvent reader for incoming events. */
    protected ByteBufferSupply bbSupply;   // input




    /**
     * This method copies a ByteBuffer as efficiently as possible.
     * It is <b>not</b> to be used in general since it assumes that the src data starts
     * at position 0 and the destination data starts at position 0 as well.
     * Any modification to srcBuf is ignored since the ET data buffer is not used
     * once this copy is done.
     *
     * @param srcBuf  buffer to copy.
     * @param destBuf buffer to copy into.
     * @param len     number of bytes to copy
     * @return destination buffer.
     */
    private static final ByteBuffer copyBuffer(ByteBuffer srcBuf, ByteBuffer destBuf, int len) {

        if (srcBuf.hasArray() && destBuf.hasArray()) {
            System.arraycopy(srcBuf.array(), 0, destBuf.array(), 0, len);
        }
        else {
            srcBuf.limit(len).position(0);
            destBuf.clear();
            destBuf.put(srcBuf);
        }
        destBuf.order(srcBuf.order());

        return (ByteBuffer)destBuf.position(0).limit(len);
    }



    /**
     * Constructor to create a new DataChannelImplEt instance. Used only by
     * {@link DataTransportImplEt#createChannel(String, Map, boolean, Emu, EmuModule, int)}
     * which is only used during PRESTART in {@link Emu}.
     *
     * @param name          the name of this channel
     * @param transport     the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     * @param emu           emu this channel belongs to
     * @param module        module this channel belongs to
     * @param outputIndex   order in which module's events will be sent to this
     *                      output channel (0 for first output channel, 1 for next, etc.).
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplEt(String name, DataTransportImplEt transport,
                         Map<String, String> attributeMap, boolean input, Emu emu,
                         EmuModule module, int outputIndex)
        throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module, outputIndex);

        this.input = input;
        dataTransportImplEt = transport;

        if (input) {
logger.info("      DataChannel Et: creating input channel " + name);
        }
        else {
logger.info("      DataChannel Et: creating output channel " + name);
        }

        // size of TCP send buffer (0 means use operating system default)
        int tcpSendBuf = 0;
        String attribString = attributeMap.get("sendBuf");
        if (attribString != null) {
            try {
                tcpSendBuf = Integer.parseInt(attribString);
                if (tcpSendBuf < 0) {
                    tcpSendBuf = 0;
                }
            }
            catch (NumberFormatException e) {}
        }

        // size of TCP receive buffer (0 means use operating system default)
        int tcpRecvBuf = 0;
        attribString = attributeMap.get("recvBuf");
        if (attribString != null) {
            try {
                tcpRecvBuf = Integer.parseInt(attribString);
                if (tcpRecvBuf < 0) {
                    tcpRecvBuf = 0;
                }
            }
            catch (NumberFormatException e) {}
        }

        // set TCP_NODELAY option off
        boolean noDelay = true;
        attribString = attributeMap.get("noDelay");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("false") ||
                attribString.equalsIgnoreCase("off")   ||
                attribString.equalsIgnoreCase("no"))   {
                noDelay = false;
            }
        }

        // create ET system object & info
        try {
            // copy transport's config
            EtSystemOpenConfig openConfig = new EtSystemOpenConfig(dataTransportImplEt.getOpenConfig());
            // set TCP options specific to this client only
            openConfig.setTcpRecvBufSize(tcpRecvBuf);
            openConfig.setTcpSendBufSize(tcpSendBuf);
            openConfig.setNoDelay(noDelay);
            // debug is set to error in constructor
            etSystem = new EtSystem(openConfig);
        }
        catch (EtException e) {
            throw new DataTransportException("", e);
        }

        // How may buffers do we grab at a time?
        chunk = 4;
        attribString = attributeMap.get("chunk");
        if (attribString != null) {
            try {
                chunk = Integer.parseInt(attribString);
                if (chunk < 1) chunk = 1;
            }
            catch (NumberFormatException e) {}
        }
//        chunk = 4;
logger.info("      DataChannel Et: chunk = " + chunk);

        // From which group do we grab new events? (default = 1)
        group = 1;
        attribString = attributeMap.get("group");
        if (attribString != null) {
            try {
                group = Integer.parseInt(attribString);
                if (group < 1) group = 1;
            }
            catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
//logger.info("      DataChannel Et: group = " + group);

        // Set station name. Use any defined in config file else use
        // this channel's name for input and "GRAND_CENTRAL" for output.
        stationName = attributeMap.get("stationName");
//logger.info("      DataChannel Et: station name = " + stationName);


        // Set station position. Use any defined in config file else use default (1)
        attribString = attributeMap.get("position");
        if (attribString != null) {
            try {
                stationPosition = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {  }
        }
//logger.info("      DataChannel Et: position = " + stationPosition);

        // if INPUT channel
        if (input) {

            try {
                // Rocs need feedback of minimum evio-events / et-buffer from
                // the DCs and PEBs.
                CODAClass emuClass = emu.getCodaClass();
                isFirstEB = (emuClass == CODAClass.PEB ||
                             emuClass == CODAClass.DC  ||
                             emuClass == CODAClass.PEBER);
                isER = (emuClass == CODAClass.ER);


                if (transport.tryToCreateET())  {
                    etSysLocal = transport.getLocalEtSystem();
                }

                // configuration of a new station
                stationConfig = new EtStationConfig();
                try {
                    stationConfig.setUserMode(EtConstants.stationUserSingle);
                }
                catch (EtException e) { /* never happen */}

                if (isER) {
                    // Make the station the ER connects to a parallel station
                    // so that if there is another ER connected, each will receive
                    // half the events.
                    stationName = emu.name();
                    stationConfig.setSelectMode(EtConstants.stationSelectRRobin);
                    stationConfig.setFlowMode(EtConstants.stationParallel);
                    stationConfig.setBlockMode(EtConstants.stationBlocking);
                    stationConfig.setRestoreMode(EtConstants.stationRestoreOut);
                    stationConfig.setPrescale(1);
                }
                else {
                    String idFilter = attributeMap.get("idFilter");
                    if (idFilter != null && idFilter.equalsIgnoreCase("on")) {
                        // Create filter for station so only events from a particular ROC
                        // (id as defined in config file) make it in.
                        // Station filter is the built-in selection function.
                        int[] selects = new int[EtConstants.stationSelectInts];
                        Arrays.fill(selects, -1);
                        selects[0] = id;
                        stationConfig.setSelect(selects);
                        stationConfig.setSelectMode(EtConstants.stationSelectMatch);
                    }

                    String controlFilter = attributeMap.get("controlFilter");
                    if (controlFilter != null && controlFilter.equalsIgnoreCase("on")) {
                        // Create filter for station so only control events make it in.
                        // Station filter is the built-in selection function.
                        int[] selects = new int[EtConstants.stationSelectInts];
                        Arrays.fill(selects, -1);
                        selects[0] = EventType.CONTROL.getValue();
                        stationConfig.setSelect(selects);
                        stationConfig.setSelectMode(EtConstants.stationSelectMatch);
                    }
                }

                // Note that controlFilter trumps idFilter

                // create station if it does not already exist
                if (stationName == null) {
                    // if this is an ER, make the station the ER's name
                    if (isER) {
                        stationName = emu.name();
                    }
                    // if this is an EB, make the station the input channel name
                    else {
                        stationName = name;
                    }
                }

                // Connect to ET system
                openEtSystem();

            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        // if OUTPUT channel
        else {
            // Tell emu what that output name is for stat reporting
            emu.addOutputDestination(transport.getOpenConfig().getEtName());

            if (stationName == null) {
                stationName = "GRAND_CENTRAL";
            }

            if (!stationName.equals("GRAND_CENTRAL")) {
                try {
                    // configuration of a new station
                    stationConfig = new EtStationConfig();
                    try {
                        stationConfig.setUserMode(EtConstants.stationUserSingle);
                    }
                    catch (EtException e) {/* never happen */}
                 }
                catch (Exception e) {/* never happen */}
            }

            // If this is an event building EMU, set the control array
            // for each outgoing ET buffer.
            CODAClass emuClass = emu.getCodaClass();
            isEB = emuClass.isEventBuilder();
            isROC = emuClass == CODAClass.ROC;
            if (isEB || isROC) {
                // The control array needs to be the right size.
                control = new int[EtConstants.stationSelectInts];

                // The first control word is this EB's coda id
                control[0] = id;
//System.out.println("      DataChannel Et: setting control[0] = " + id);
                // Is this the last level event builder (not a DC)?
                // In this case, we want the first control word to indicate
                // what type of event is being sent.
                //
                // Control events will be received and dealt with by the FCS
                // (Farm Control Supervisor).
                isFinalEB = emuClass.isFinalEventBuilder();
                // The value of control[0] will be set in the DataOutputHelper
            }

            // Connect to ET system
            openEtSystem();
        }

    }


    /**
     * Get the size of the ET system's events in bytes.
     * @return size of the ET system's events in bytes.
     */
    private long getEtEventSize() {
        if (etSysLocal != null) {
            return etSysLocal.getConfig().getEventSize();
        }
        return etSystem.getEventSize();
    }


    /**
     * Get the number of the ET system's events.
     * @return number of the ET system's events.
     */
    private int getEtEventCount() {
        if (etSysLocal != null) {
            return etSysLocal.getConfig().getNumEvents();
        }
        return etSystem.getNumEvents();
    }


    /**
     * Get the ET system object.                                                                         , e
     * @return the ET system object.
     */
    private void openEtSystem() throws DataTransportException {

        // If we have an ET system running in this JVM, take advantage of it
        if (etSysLocal != null)  {
            try {
                if (stationName.equals("GRAND_CENTRAL")) {
                    stationLocal = etSysLocal.stationNameToObject(stationName);
                }
                else {
                    try {
                        stationLocal = etSysLocal.createStation(stationConfig, stationName);
                        etSysLocal.setStationPosition(stationLocal.getStationId(), stationPosition, 0);
                    }
                    catch (EtExistsException e) {
                        stationLocal = etSysLocal.stationNameToObject(stationName);
                        etSysLocal.setStationPosition(stationLocal.getStationId(), stationPosition, 0);
                    }
                }

                // attach to station
                attachmentLocal = etSysLocal.attach(stationLocal.getStationId());
            }
            catch (Exception e) {
System.out.println("      DataChannel Et: can't create/attach to station " +
                                          stationName + "; " + e.getMessage());
                emu.setErrorState("DataChannel Et: can't create/attach to station " +
                                          stationName + "; " + e.getMessage());
                channelState = CODAState.ERROR;
                throw new DataTransportException("cannot create/attach to station " + stationName, e);
            }
        }
        // Otherwise open in usual manner, use sockets
        else {
            try {
                EtSystemOpenConfig config = etSystem.getConfig();
                logger.info("      DataChannel Et: try opening " + config.getEtName());

                int method = config.getNetworkContactMethod();
                if (method == EtConstants.direct) {
                    logger.info(" directly on " + config.getHost() + ", port " + config.getTcpPort());
                }
                else if (method == EtConstants.multicast) {
                    logger.info(" by multicasting to port " + config.getUdpPort());
                }
                else if (method == EtConstants.broadcast) {
                    logger.info(" by broadcasting to port " + config.getUdpPort());
                }
                else if (method == EtConstants.broadAndMulticast) {
                    logger.info(" by multi & broadcasting to port " + config.getUdpPort());
                }

                if (config.isConnectRemotely()) {
                    System.out.println(", using sockets only");
                }
                else {
                    System.out.println();
                }

                etSystem.open();

                logger.info("      DataChannel Et: SUCCESS opening ET from local addr, " +
                                    etSystem.getLocalAddress());
                if (etSystem.getLanguage() == EtConstants.langJava) {
                    System.out.println(", written in Java");
                }
                else {
                    System.out.println();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                // Send error msg that ends up in run control gui's list of messages.
                // This allows the user to quickly identify the ET problem
                // and fix it in the config file or wherever.
                EtSystemOpenConfig config = etSystem.getConfig();
                String errString = "cannot connect to ET " + config.getEtName() + ", ";
                int method = config.getNetworkContactMethod();
                if (method == EtConstants.direct) {
                    errString += " direct to " + config.getHost() + " on port " + config.getTcpPort();
                }
                else if (method == EtConstants.multicast) {
                    errString += " multicasting to port " + config.getUdpPort();
                }
                else if (method == EtConstants.broadcast) {
                    errString += " broadcasting to port " + config.getUdpPort();
                }
                else if (method == EtConstants.broadAndMulticast) {
                    errString += " multi/broadcasting to port " + config.getUdpPort();
                }

System.out.println("      DataChannel Et:" + errString + "; " + e.getMessage());
                emu.setErrorState("DataChannel Et: " + errString + "; " + e.getMessage());
                channelState = CODAState.ERROR;
                throw new DataTransportException(errString, e);
            }

            try {
                if (stationName.equals("GRAND_CENTRAL")) {
                    station = etSystem.stationNameToObject(stationName);
                }
                else {
                    try {
                        if (isER) {
                            // The guy who owns the ET system creates the head of the parallel stations
                            if (dataTransportImplEt.tryToCreateET()) {
System.out.println("      DataChannel Et: try creating station " + stationName + " at pos " + stationPosition +
                   ", at parallel pos = head");
                                station = etSystem.createStation(stationConfig, stationName, stationPosition, EtConstants.newHead);
                            }
                            else {
System.out.println("      DataChannel Et: try creating station " + stationName + " at pos " + stationPosition +
                   ", at parallel pos = end");
                                station = etSystem.createStation(stationConfig, stationName, stationPosition, EtConstants.end);
                            }
                        }
                        else {
                            station = etSystem.createStation(stationConfig, stationName, stationPosition, 0);
                        }
                    }
                    catch (EtExistsException e) {
System.out.println("      DataChannel Et: try creating station " + stationName + " at pos " + stationPosition +
                   ", but it exists so attach to existing");
                        station = etSystem.stationNameToObject(stationName);
                        etSystem.setStationPosition(station, stationPosition, 0);
                    }
                }

                // attach to station
                attachment = etSystem.attach(station);
            }
            catch (Exception e) {
System.out.println("      DataChannel Et: can't create/attach to station " +
                                   stationName + "; " + e.getMessage());
                emu.setErrorState("DataChannel Et: can't create/attach to station " +
                                   stationName + "; " + e.getMessage());
                channelState = CODAState.ERROR;
                throw new DataTransportException("cannot create/attach to station " + stationName, e);
            }
        }
    }


    private void closeEtSystem() throws DataTransportException {

        if (etSysLocal != null) {
            try {
                etSysLocal.detach(attachmentLocal);
            }
            catch (Exception e) {
                // Might be detached already or cannot communicate with ET
            }

            try {
                if (!stationName.equals("GRAND_CENTRAL")) {
                    etSysLocal.removeStation(stationLocal.getStationId());
                }
            }
            catch (Exception e) {
                // Station may not exist, may still have attachments, or
                // cannot communicate with ET
            }
            etSysLocal = null;
        }
        else if (etSystem != null) {
            try {
                etSystem.detach(attachment);
            }
            catch (Exception e) {
                // Might be detached already or cannot communicate with ET
            }

            try {
                if (!stationName.equals("GRAND_CENTRAL")) {
                    etSystem.removeStation(station);
                }
            }
            catch (Exception e) {
                // Station may not exist, may still have attachments, or
                // cannot communicate with ET
            }

            etSystem.close();
            etSystem = null;
        }
    }


    /** {@inheritDoc} */
    public TransportType getTransportType() {
        return TransportType.ET;
    }


    /** {@inheritDoc} */
    public int getInputLevel() {return bbSupply.getFillLevel();}

    
    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        super.prestart();

        pause = false;

        if (!input) {
            // Find out how many total events in ET system.
            // Useful for avoiding bad situation in output channel in which
            // putEvents() blocks due to newEvents() stuck in sleep mode.
            if (getEtEventCount() < 4*chunk) {
                deadLockAtPrestart = true;
logger.info("      DataChannel Et: newEvents() using timed mode to avoid deadlock");
            }
        }
        else {
            // At this point, all in & output channels have been created. All output channels
            // and modules have had their prestart() methods called. Input channels are prestarted
            // right after modules.
            // In other words, at this point we know if we're attached to an ER and if that ER
            // has one file output channel. If that's the case, then we can have a non-synchronized
            // byte buffer supply.
            // Since ER has only 1 recording thread, and every event is processed in order,
            // and since a file output channel does the same, the byte buffer supply does
            // not have to be synchronized as byte buffers are released in order.

            // RingBuffer supply of buffers to hold all ET event bytes
            // ET system parameters
            int etEventSize = (int) getEtEventSize();
logger.info("      DataChannel Et: eventSize = " + etEventSize);

            // For creating a reusable supply of ByteBuffer objects:
            // Put a limit on the amount of memory (140MB). That may be
            // the easiest way to figure out how many buffers to use.
            // Number of bufs must be a power of 2.
            // This will give 64, 2.1MB buffers.
            int numEtBufs = 140000000 / etEventSize;
            // Have at least 8 buffer
            numEtBufs = numEtBufs < 8 ? 8 : numEtBufs;
            // Have no more than 2048 buffers
            numEtBufs = numEtBufs > 2048 ? 2048 : numEtBufs;
            // Make power of 2
            if (Integer.bitCount(numEtBufs) != 1) {
                int newVal = numEtBufs / 2;
                numEtBufs = 1;
                while (newVal > 0) {
                    numEtBufs *= 2;
                    newVal /= 2;
                }
            }
logger.info("      DataChannel Et: # copy-ET-buffers in input supply -> " + numEtBufs);

            // One pool for each supply buffer. However,
            // the smaller the ET buffer, the smaller the number of nodes each can use.
            // Smallest pool size = 200 (when ET size = 0).
            // This linearly increases to 3500 at ET size = 4MB (y = mx + b).
            int poolSize = (3500 - 200)*(etEventSize/4000000) + 200;
            nodePools = new EvioNodePool[numEtBufs];
            // Create the EvioNode pools
            for (int i = 0; i < numEtBufs; i++) {
                nodePools[i] = new EvioNodePool(poolSize);
            }
logger.info("      DataChannel Et: done creating " + numEtBufs + " node pools, single pool size = " + poolSize);

            // If ER
            if (isER) {
                List<DataChannel> outChannels = emu.getOutChannels();
                // if (0 output channels or 1 file output channel) ...
                if (((outChannels.size() < 1) ||
                        (outChannels.size() == 1 &&
                                (outChannels.get(0).getTransportType() == TransportType.FILE)))) {

                    // Since ER has only 1 recording thread and every event is processed in order,
                    // and since the file output channel also processes all events in order,
                    // the byte buffer supply does not have to be synchronized as byte buffers are
                    // released in order. Will make things faster.

                    // UPDATE, user events coming over same channel as physics are COPIED and
                    // buffers from this supply are released. They are released while a previous
                    // physics buffer is still being used to write events (ie nodes in
                    // the process of being written). So there is NO sequential release.
                    bbSupply = new ByteBufferSupply(numEtBufs, etEventSize,
                                                    module.getOutputOrder(),
                                                    false, false, nodePools);
                }
                else {
                    // If ER has more than one output, buffers may not be released sequentially
                    bbSupply = new ByteBufferSupply(numEtBufs, etEventSize,
                                                    module.getOutputOrder(),
                                                    false, false, nodePools);
                }
            }
            else {
                // EBs all release these ByteBuffers in order in the ReleaseRingResourceThread thread
                bbSupply = new ByteBufferSupply(numEtBufs, etEventSize,
                                                module.getOutputOrder(),
                                                false, true, nodePools);
            }
        }

        // Start up threads for I/O
        startHelper();

        channelState = CODAState.PAUSED;
    }


    /** {@inheritDoc} */
    public void go() {
        pause = false;
        channelState = CODAState.ACTIVE;
    }

    /** {@inheritDoc} */
    public void pause() {
        pause = true;
        channelState = CODAState.PAUSED;
    }


    /**
     * Interrupt all threads.
     */
    private void interruptThreads() {
        // Don't close ET system until helper threads are done
        if (dataInputThread != null) {
            dataInputThread.interruptThreads();
            if (etSysLocal != null) {
                etSysLocal.detach(attachmentLocal);
            }
            else if (etSystem != null) {
                try {
                    etSystem.wakeUpAttachment(attachment);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if (dataOutputThread != null) {
            dataOutputThread.shutdown();
        }
    }


    /**
     * Try joining all threads, up to 1 sec each.
     */
    private void joinThreads() {
        // Don't close ET system until helper threads are done
        try {
            if (dataInputThread != null) {
                dataInputThread.join(1000);
            }
        }
        catch (InterruptedException e) {}

        if (dataOutputThread != null) {
            dataOutputThread.waitForThreadsToEnd(1000);
        }
    }

    
    /** {@inheritDoc}. Formerly this code was the close() method. */
    public void end() {
logger.info("      DataChannel Et: " + name + " - end threads & close ET system");

        gotEndCmd = true;
        gotResetCmd = false;
        stopGetterThread = true;

        // Do NOT interrupt threads which are communicating with the ET server.
        // This will mess up future communications !!!

        // The emu's emu.end() method first waits (up to 60 sec) for the END event to be read
        // by input channels, processed by the module, and finally to be sent by
        // the output channels. Then it calls everyone's end() method including this one.
        // Threads and sockets can be shutdown quickly, since we've already
        // waited for the END event.

//        logger.info("      DataChannel Et: interrupt threads");
        interruptThreads();
////        logger.info("      DataChannel Et: join threads");
        joinThreads();

        // At this point all threads should be done
        try {
//            logger.info("      DataChannel Et: close ET");
            closeEtSystem();
        }
        catch (DataTransportException e) {
            e.printStackTrace();
        }

        channelState = CODAState.DOWNLOADED;
logger.info("      DataChannel Et: end() done");
    }


    /**
     * {@inheritDoc}
     * Reset this channel by interrupting the data sending threads and closing ET system.
     */
    public void reset() {
logger.debug("      DataChannel Et: reset " + name + " channel");

        gotEndCmd   = false;
        gotResetCmd = true;
        stopGetterThread = true;

        interruptThreads();
        joinThreads();

        try {
            closeEtSystem();
        }
        catch (DataTransportException e) {}

        errorMsg.set(null);
        channelState = CODAState.CONFIGURED;
//logger.debug("      DataChannel Et: reset " + name + " - all done");
    }


    /**
     * For input channel, start the DataInputHelper thread which takes ET events,
     * parses each, puts the events back into the ET system, and puts the parsed
     * evio banks onto the ring.<p>
     * For output channel, start the DataOutputHelper thread which takes a bank from
     * the ring, puts it into a new ET event and puts that into the ET system.
     */
    private void startHelper() {
        if (input) {
            dataInputThread = new DataInputHelper(emu.getThreadGroup(),
                                                  name() + "et_in");
            dataInputThread.start();
            dataInputThread.waitUntilStarted();
        }
        else {
            dataOutputThread = new DataOutputHelper(emu.getThreadGroup(),
                                                               name() + "et_out");
            dataOutputThread.start();
            dataOutputThread.waitUntilStarted();
        }
    }


//
//    /**
//     * Class used to get ET events, parse them into Evio banks,
//     * and put them onto a single ring buffer.
//     */
//    private class DataInputHelper extends Thread {
//
//        /** Array of ET events to be gotten from ET system. */
//        private EtEvent[] events;
//
//        /** Array of ET events to be gotten from ET system. */
//        private EtEventImpl[] eventsDirect;
//
//        /** Variable to print messages when paused. */
//        private int pauseCounter = 0;
//
//        /** Let a single waiter know that the main thread has been started. */
//        private final CountDownLatch latch = new CountDownLatch(1);
//
//        /** The minimum amount of milliseconds between updates to the M value. */
//        private static final long timeBetweenMupdates = 500;
//
//
//        /** Constructor. */
//        DataInputHelper (ThreadGroup group, String name) {super(group, name);}
//
//
//        /** A single waiter can call this method which returns when thread was started. */
//        private void waitUntilStarted() {
//            try {
//                latch.await();
//            }
//            catch (InterruptedException e) {}
//        }
//
//
//        /** {@inheritDoc} */
//        @Override
//        public void run() {
//
//            // Tell the world I've started
//            latch.countDown();
//            String errorString = null;
//
//            try {
//                int sourceId, recordId;
//                BlockHeaderV4 header4;
//                EventType eventType;
//                ControlType controlType;
//                ByteBufferItem bbItem;
//                ByteBuffer buf;
//                EvioCompactReaderUnsync compactReader = null;
//                RingItem ri;
//                long t1, t2;
//                boolean delay = false;
//                boolean useDirectEt = (etSysLocal != null);
//                boolean etAlive = true;
//                boolean hasFirstEvent, isUser=false;
//
//                // This object is needed to use the new, garbage-free, sync-free ET interface
//                EtContainer etContainer = null;
//                // Number of valid events in event arrays inside etContainer
//                int validEvents;
//
//                if (!useDirectEt) {
//                    etAlive = etSystem.alive();
//                    // This object is needed to use the new, garbage-free, sync-free ET interface
//                    etContainer = new EtContainer(etSystem, chunk, (int)getEtEventSize());
//                }
//
//                t1 = System.currentTimeMillis();
//
//                while ( etAlive ) {
//
//                    if (delay) {
//                        Thread.sleep(5);
//                        delay = false;
//                    }
//
//                    if (pause) {
//                        if (pauseCounter++ % 400 == 0)
//logger.warn("      DataChannel Et in: " + name + " - PAUSED");
//                        Thread.sleep(5);
//                        continue;
//                    }
//
//                    // Get events while checking periodically to see if we must go away.
//                    // Do some work to get accurate error msgs back to run control.
//                    try {
////System.out.print("      DataChannel Et in: " + name + " getEvents() ...");
//                        if (useDirectEt) {
//                            events = eventsDirect = etSysLocal.getEvents(attachmentLocal,
//                                                                         Mode.TIMED.getValue(),
//                                                                         etWaitTime, chunk);
//                            validEvents = events.length;
//                        }
//                        else {
//                            //events = etSystem.getEvents(attachment, Mode.TIMED,
//                            //                            Modify.NOTHING, etWaitTime, chunk);
//                            etContainer.getEvents(attachment, Mode.TIMED, Modify.NOTHING, etWaitTime, chunk);
//                            etSystem.getEvents(etContainer);
//                            validEvents = etContainer.getEventCount();
//                            events = etContainer.getEventArray();
//                        }
//                    }
//                    catch (IOException e) {
//                        errorString = "DataChannel Et in: network communication error with Et";
//                        throw e;
//                    }
//                    catch (EtException e) {
//                        errorString = "DataChannel Et in: internal error handling Et";
//                        throw e;
//                    }
//                    catch (EtDeadException e) {
//                        errorString = "DataChannel Et in:  Et system dead";
//                        throw e;
//                    }
//                    catch (EtClosedException e) {
//                        errorString = "DataChannel Et in:  Et connection closed";
//                        throw e;
//                    }
//                    catch (EtWakeUpException e) {
//                        // Told to wake up because we're ending or resetting
//                        if (haveInputEndEvent) {
//                            logger.info("      DataChannel Et in: wake up " + name + ", other thd found END, quit");
//                        }
//                        else if (gotResetCmd) {
//                            logger.info("      DataChannel Et in: " + name + " got RESET cmd, quitting");
//                        }
//                        return;
//                    }
//                    catch (EtTimeoutException e) {
//                        if (haveInputEndEvent) {
//                            logger.info("      DataChannel Et in: timeout " + name + ", other thd found END, quit");
//                            return;
//                        }
//                        else if (gotResetCmd) {
//                            logger.info("      DataChannel Et in: " + name + " got RESET cmd, quitting");
//                            return;
//                        }
//
//                        // Want to delay before calling getEvents again
//                        // but don't do it here in a synchronized block.
//                        delay = true;
//
//                        continue;
//                    }
//
//                    //for (EtEvent ev : events) {
//                    for (int j=0; j < validEvents; j++) {
//
//                        // Get a local, reusable ByteBuffer
//                        bbItem = bbSupply.get();
//
//                        // Copy ET data into this buffer.
//                        // The reason we do this is because if we're connected to a local
//                        // C ET system, the ET event's data buffer points into shared memory.
//                        // Since the parsed evio events (which hold a reference to this buffer)
//                        // go onto a ring and then who knows where, it's best to copy the buffer
//                        // since ET buffers recirculate and get reused.
//                        // Was already burned by this once.
//
//                        // A single ET event may be larger than those initially created.
//                        // It may be a "temp" event which is created temporarily to hold
//                        // a larger amount of data. If this is the case, the ET event MAY
//                        // contain more data than the space available in buf. So we must
//                        // first ensure there's enough memory to do the copy.
//                        bbItem.ensureCapacity(events[j].getLength());
//                        buf = bbItem.getBuffer();
//                        copyBuffer(events[j].getDataBuffer(), buf, events[j].getLength());
//
//                        try {
//                            // These calls do not change buf position
//                            if (compactReader == null) {
//                                compactReader = new EvioCompactReaderUnsync(buf);
//                            }
//                            else {
//                                compactReader.setBuffer(buf);
//                            }
//                        }
//                        catch (EvioException e) {
//Utilities.printBuffer(buf, 0, 21, "BAD EVENT ");
//                            e.printStackTrace();
//                            errorString = "DataChannel Et in: ET data NOT evio v4 format";
//                            throw e;
//                        }
//
//                        // First block header in ET buffer
//                        header4 = compactReader.getFirstBlockHeader();
////System.out.println("      DataChannel Et in: blk header, order = " + header4.getByteOrder());
//                        if (header4.getVersion() < 4) {
//                            errorString = "DataChannel Et in: ET data NOT evio v4 format";
//                            throw new EvioException("Evio data needs to be written in version 4+ format");
//                        }
//
//                        hasFirstEvent = header4.hasFirstEvent();
//                        eventType     = EventType.getEventType(header4.getEventType());
//                        controlType   = null;
//                        // this only works from ROC !!!
//                        sourceId      = header4.getReserved1();
//                        if (eventType == EventType.PARTIAL_PHYSICS) {
//                            sourceId = events[j].getControl()[0];
//                        }
//                        // Record id is set in the DataOutputHelper so that it is incremented
//                        // once per non-user, non-control ET buffer. Each writer will only use
//                        // 1 block per 2.2MB or 10K events. Thus we can get away with only
//                        // looking at the very first block #.
//                        recordId = header4.getNumber();
//
//                        // Number of evio event associated with this buffer.
//                        int eventCount = compactReader.getEventCount();
//
//                        // When the emu is done processing all evio events,
//                        // this buffer is released, so use this to keep count.
//                        bbItem.setUsers(eventCount);
//
//                        // Send the # of (buildable) evio events / ET event for ROC feedback,
//                        // but only if this is the DC or PEB.
//                        t2 = emu.getTime();
//                        if (isFirstEB && eventType.isBuildable() && (t2-t1 > timeBetweenMupdates)) {
//                            emu.getCmsgPortal().sendMHandlerMessage(eventCount, "M");
//                            t1 = t2;
//                        }
////logger.info("      DataChannel Et in: isFirstEb = " + isFirstEB + ", eventCount = " + eventCount +
////", isBuildable = " + eventType.isBuildable());
//                        EvioNode node;
////logger.info("      DataChannel Et in: " + name + " block header, event type " + eventType +
////            ", src id = " + sourceId + ", recd id = " + recordId + ", event cnt = " + eventCount);
//
//                        for (int i=1; i < eventCount+1; i++) {
//                            if (isER) {
//                                // Don't need to parse all bank headers, just top level.
//                                node = compactReader.getEvent(i);
//                            }
//                            else {
//                                node = compactReader.getScannedEvent(i);
//                            }
//
//                            // Complication: from the ROC, we'll be receiving USER events
//                            // mixed in with and labeled as ROC Raw events. Check for that
//                            // and fix it.
//                            if (eventType == EventType.ROC_RAW) {
//                                if (Evio.isUserEvent(node)) {
//logger.info("      DataChannel Et in: " + name + " got USER event from ROC");
//                                    eventType = EventType.USER;
//                                    isUser = true;
//                                }
//                            }
//                            else if (eventType == EventType.CONTROL) {
//                                // Find out exactly what type of control event it is
//                                // (May be null if there is an error).
//                                // It may NOT be enough just to check the tag
//                                controlType = ControlType.getControlType(node.getTag());
//logger.info("      DataChannel Et in: " + name + " got CONTROL event, " + controlType);
//                                if (controlType == null) {
//                                    errorString = "DataChannel Et in:  found unidentified control event, tag = 0x" + Integer.toHexString(node.getTag());
//                                    throw new EvioException("Found unidentified control event, tag = 0x" + Integer.toHexString(node.getTag()));
//                                }
//                            }
//                            else if (eventType == EventType.USER) {
//                                isUser = true;
////                                if (hasFirstEvent) {
////logger.info("      DataChannel Et in: " + name + " got FIRST event");
////                                }
////                                else {
////logger.info("      DataChannel Et in: " + name + " got USER event");
////                                }
//                            }
//
//                            // Don't need to set controlType = null for each loop since each
//                            // ET event contains only one event type (CONTROL, PHYSICS, etc).
//
////System.out.println("      DataChannel Et in: wait for next ring buf for writing");
//                            nextRingItem = ringBufferIn.next();
////System.out.println("      DataChannel Et in: Got sequence " + nextRingItem);\
//
//                            ri = ringBufferIn.get(nextRingItem);
//
//                            // Set & reset all parameters of the ringItem
//                            if (eventType.isBuildable()) {
//                                ri.setAll(null, null, node, eventType, controlType,
//                                          isUser, hasFirstEvent, id, recordId, sourceId,
//                                          node.getNum(), name, bbItem, bbSupply);
//                            }
//                            else {
//                                ri.setAll(null, null, node, eventType, controlType,
//                                          isUser, hasFirstEvent, id, recordId,
//                                          sourceId, 1, name, bbItem, bbSupply);
//                            }
//
//                            // Only the first event of first block can be "first event"
//                            isUser = hasFirstEvent = false;
//
//                            ringBufferIn.publish(nextRingItem);
////System.out.println("      DataChannel Et in: published sequence " + nextRingItem);
//
//                            // Handle end event ...
//                            if (controlType == ControlType.END) {
//                                // There should be no more events coming down the pike so
//                                // go ahead write out existing events and then shut this
//                                // thread down.
//logger.info("      DataChannel Et in: " + name + " found END event");
//                                haveInputEndEvent = true;
//                                // run callback saying we got end event
//                                if (endCallback != null) endCallback.endWait();
//                                break;
//                            }
//                        }
//
//                        if (haveInputEndEvent) {
//                            break;
//                        }
//                    }
//
//                    // Put all events back in ET system - even those unused.
//                    // Do some work to get accurate error msgs back to run control.
//                    try {
//                        if (useDirectEt) {
//                            etSysLocal.putEvents(attachmentLocal, eventsDirect);
//                        }
//                        else {
//                            etContainer.putEvents(attachment, events, 0, validEvents);
//                            etSystem.putEvents(etContainer);
//                            //etSystem.putEvents(attachment, events);
//                            //etSystem.dumpEvents(attachment, events);
//                        }
//                    }
//                    catch (IOException e) {
//                        errorString = "DataChannel Et in: network communication error with Et";
//                        throw e;
//                    }
//                    catch (EtException e) {
//                        errorString = "DataChannel Et in: internal error handling Et";
//                        throw e;
//                    }
//                    catch (EtDeadException e) {
//                        errorString = "DataChannel Et in: Et system dead";
//                        throw e;
//                    }
//                    catch (EtClosedException e) {
//                        errorString = "DataChannel Et in: Et connection closed";
//                        throw e;
//                    }
//
//                    if (haveInputEndEvent) {
//logger.info("      DataChannel Et in: have END, " + name + " quit thd");
//                        return;
//                    }
//
//                    if (!useDirectEt) {
//                        etAlive = etSystem.alive();
//                    }
//                }
//
//            } catch (InterruptedException e) {
//logger.warn("      DataChannel Et in: " + name + "  interrupted thd, exiting");
//            } catch (Exception e) {
//                channelState = CODAState.ERROR;
//                // If we haven't yet set the cause of error, do so now & inform run control
//                if (errorString == null) errorString = e.getMessage();
//                emu.setErrorState(errorString);
//System.out.println("      DataChannel Et in: " + name + " exit thd: " + errorString);
//            }
//        }
//
//    }


    /**
      * Class used by the this input channel's internal RingBuffer
      * to populate itself with etContainer objects.
      */
     private final class ContainerFactory implements EventFactory<EtContainer> {
         public EtContainer newInstance() {
             try {
                 // This object holds an EtEvent array & more
                 return new EtContainer(chunk, (int)getEtEventSize());
             }
             catch (EtException e) {/* never happen */}
             return null;
         }
     }



    /**
     * Class used to get ET events, parse them into Evio banks,
     * and put them onto a single ring buffer.
     */
    private class DataInputHelper extends Thread {

        /** Array of ET events to be gotten from ET system. */
        private EtEvent[] events;

        /** Variable to print messages when paused. */
        private int pauseCounter = 0;

        /** Let a single waiter know that the main threads have been started. */
        private final CountDownLatch latch = new CountDownLatch(2);

        /** The minimum amount of milliseconds between updates to the M value. */
        private static final long timeBetweenMupdates = 500;

        /** Thread for getting new ET events. */
        private EvGetter getter;

        /** Internal ring buffer. */
        private RingBuffer<EtContainer> rb;

        /** Used by first consumer (DataOutputHelper) to get ring buffer items. */
        private Sequence etConsumeSequence;

        /** Used by first consumer to get ring buffer items. */
        private SequenceBarrier etConsumeBarrier;

        /** Keep track of record ids coming in to make sure they're sequential. */
        private int expectedRecordId = 1;


        /** Constructor. */
        DataInputHelper (ThreadGroup group, String name) {
            super(group, name);

            // Ring will have 2 slots. Each is an EtContainer object containing
            // "chunk" number of ET events.
            int ringSize = 2;

            // Create ring buffer used by 2 threads -
            //   1 to get existing events from ET system and place into ring
            //          (producer of ring items)
            //   1 to take these ET events, parse them into evio events and
            //   put them back into ET system
            //          (consumer of ring items)
            rb = createSingleProducer(new ContainerFactory(), ringSize,
                                      new SpinCountBackoffWaitStrategy(10000, new LiteBlockingWaitStrategy()));
                                      //new YieldingWaitStrategy());

            // Consumer barrier of ring buffer, used to get ET event
            // and parse it and put it back into ET.
            etConsumeBarrier = rb.newBarrier();
            // Consumer sequence of ring buffer
            etConsumeSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

            // Consumer sequence is last before producer can grab ring item
            rb.addGatingSequences(etConsumeSequence);

            // Start producer thread for getting new ET events
            getter = new EvGetter(group, name + "_Getter");
            getter.start();
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {}
        }


        /** Interrupt both input channel threads. */
        void interruptThreads() {
            stopGetterThread = true;
            getter.interrupt();
            this.interrupt();
        }


        /**
         * This class is a thread designed to get ET events from the ET system.
         * It runs simultaneously with the thread that parses these events
         * with evio data and the thread that puts them back.
         */
        final private class EvGetter extends Thread {

            /** Constructor. */
            EvGetter (ThreadGroup group, String name) {
                super(group, name);
            }

            /**
             * {@inheritDoc}<p>
             * Get the ET events.
             */
            public void run() {

                long sequence;
                boolean gotError;
                String errorString;
                EtContainer etContainer;
                boolean useDirectEt = (etSysLocal != null);

                // Tell the world I've started
                latch.countDown();

                try {

                    while (true) {
                        if (stopGetterThread) {
//logger.info("      DataChannel Et in: " + name + ", Getter thd stopped 1");
                            return;
                        }

                        // Will block here if no available slots in ring.
                        // It will unblock when ET events are put back by the other thread.
                        sequence = rb.nextIntr(1); // This in interruptible
                        etContainer = rb.get(sequence);

                        if (useDirectEt) {
                            while (true) {
                                try {
                                    // We need to do a timed WAIT since getEvents is NOT interruptible
                                    etContainer.getEvents(attachmentLocal, Mode.TIMED.getValue(), 500, chunk);
                                    etSysLocal.getEvents(etContainer);
                                }
                                catch (EtTimeoutException e) {}
                                if (stopGetterThread) {
//logger.info("      DataChannel Et in: " + name + ", Getter thd stopped 2");
                                   return;
                                }
                            }
                        }
                        else {
                            // Now that we have a free container, get events & store them in container
                            etContainer.getEvents(attachment, Mode.SLEEP, Modify.NOTHING, 0, chunk);
                            etSystem.getEvents(etContainer);
                        }

                        // Set local reference to null for error recovery
                        etContainer = null;

                        // Make container available for parsing/putting thread
                        rb.publish(sequence++);
                    }
                }
                catch (EtWakeUpException e) {
                    // Told to wake up because we're ending or resetting
                    if (haveInputEndEvent) {
                        logger.info("      DataChannel Et in: " + name + ", Getter thd woken up, got END event");
                    }
                    else if (gotResetCmd) {
                        logger.info("      DataChannel Et in: " + name + ", Getter thd woken up, got RESET cmd");
                    }
                    else {
                        logger.info("      DataChannel Et in: " + name + ", Getter thd woken up");
                    }
                    return;
                }
                catch (IOException e) {
                    gotError = true;
                    errorString = "DataChannel Et in: network communication error with Et";
                }
                catch (EtException e) {
                    gotError = true;
                    errorString = "DataChannel Et in: internal error handling Et";
                }
                catch (EtDeadException e) {
                    gotError = true;
                    errorString = "DataChannel Et in: Et system dead";
                }
                catch (EtClosedException e) {
                    gotError = true;
                    errorString = "DataChannel Et in: Et connection closed";
                }
                catch (Exception e) {
                    gotError = true;
                    errorString = "DataChannel Et in: " + e.getMessage();
                }

                // ET system problem - run will come to an end
                if (gotError) {
                    channelState = CODAState.ERROR;
                    System.out.println("      DataChannel Et in: " + name + ", " + errorString);
                    emu.setErrorState(errorString);
                }
System.out.println("      DataChannel Et in: GETTER is Quitting");
            }
        }


        /** {@inheritDoc} */
        @Override
        public void run() {

            // Tell the world I've started
            latch.countDown();
            String errorString = null;

            int sourceId, recordId;
            IBlockHeader header;
            EventType eventType;
            ControlType controlType;
            ByteBufferItem bbItem;
            ByteBuffer buf;
            EvioCompactReader compactReader = null;
            RingItem ri;
            long t1, t2;
            boolean delay = false;
            boolean useDirectEt = (etSysLocal != null);
            boolean etAlive = true;
            boolean hasFirstEvent, isUser=false;
            EvioNodeSource nodePool;

            EtContainer etContainer = null;
            long nextSequence = etConsumeSequence.get() + 1L;
            long availableSequence = -1L;
            int validEvents = 0;

            if (!useDirectEt) {
                etAlive = etSystem.alive();
            }


            try {

                t1 = System.currentTimeMillis();

                while ( etAlive ) {

                    if (delay) {
                        Thread.sleep(1);
                        delay = false;
                    }

                    if (pause) {
                    //    if (pauseCounter++ % 400 == 0)
                            logger.warn("      DataChannel Et in: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // Get events while checking periodically to see if we must go away.
                    // Do some work to get accurate error msgs back to run control.
                    try {
                        //System.out.print("      DataChannel Et in: " + name + " getEvents() ...");

                        // Will block here if no available slots in ring.
                        // It will unblock when more ET events are gotten by the evGetter thread.
                        if (availableSequence < nextSequence) {
                            availableSequence = etConsumeBarrier.waitFor(nextSequence);
                        }
                        etContainer = rb.get(nextSequence);

                        if (useDirectEt) {
                            events = etContainer.getEventArrayLocal();
                            validEvents = events.length;
                        }
                        else {
                            events = etContainer.getEventArray();
                            validEvents = etContainer.getEventCount();
                        }
                    }
                    catch (AlertException e) {/* never happen */}
                    catch (InterruptedException e) {
                        // Told to wake up because we're ending or resetting
                        if (haveInputEndEvent) {
                            logger.info("      DataChannel Et in: wake up " + name + ", other thd found END, quit");
                        }
                        else if (gotResetCmd) {
                            logger.info("      DataChannel Et in: " + name + " got RESET cmd, quitting");
                        }
                        return;
                    }
                    catch (TimeoutException e) {
                        if (haveInputEndEvent) {
                            logger.info("      DataChannel Et in: timeout " + name + ", other thd found END, quit");
                            return;
                        }
                        else if (gotResetCmd) {
                            logger.info("      DataChannel Et in: " + name + " got RESET cmd, quitting");
                            return;
                        }

                        // Want to delay before calling getEvents again
                        delay = true;
                        continue;
                    }

                    for (int j=0; j < validEvents; j++) {

                        // Get a local, reusable ByteBuffer
                        bbItem = bbSupply.get();

                        // Copy ET data into this buffer.
                        // The reason we do this is because if we're connected to a local
                        // C ET system, the ET event's data buffer points into shared memory.
                        // Since the parsed evio events (which hold a reference to this buffer)
                        // go onto a ring and then who knows where, it's best to copy the buffer
                        // since ET buffers recirculate and get reused.
                        // Was already burned by this once.

                        // A single ET event may be larger than those initially created.
                        // It may be a "temp" event which is created temporarily to hold
                        // a larger amount of data. If this is the case, the ET event MAY
                        // contain more data than the space available in buf. So we must
                        // first ensure there's enough memory to do the copy.
                        EtEvent ev = events[j];
                        int evSize = ev.getLength();
                        bbItem.ensureCapacity(evSize);
                        buf = bbItem.getBuffer();
                        copyBuffer(ev.getDataBuffer(), buf, evSize);

                        try {
                            nodePool = (EvioNodePool)bbItem.getMyObject();
                            nodePool.reset();

                            // These calls do not change buf position
                            if (compactReader == null) {
                                compactReader = new EvioCompactReader(buf, nodePool, false);
                            }
                            else {
                                compactReader.setBuffer(buf, nodePool);
                            }
                        }
                        catch (EvioException e) {
                            bbSupply.release(bbItem);
                            if (ignoreDataErrors) {
                                System.out.println("IGNORE ERROR: " + e.getMessage());
                                continue;
                            }
                            else {
                                Utilities.printBuffer(buf, 0, 21, "BAD EVENT ");
                                e.printStackTrace();
                                errorString = "DataChannel Et in: ET data NOT evio v4 format";
                                throw e;
                            }
                        }

                        // First block header in ET buffer
                        header = compactReader.getFirstBlockHeader();
//System.out.println("      DataChannel Et in: blk header, order = " + header4.getByteOrder());
                        if (header.getVersion() < 4) {
                            bbSupply.release(bbItem);
                            if (ignoreDataErrors) {
                                System.out.println("IGNORE ERROR: version = " + header.getVersion());
                                continue;
                            }
                            else {
                                errorString = "DataChannel Et in: ET data NOT evio 4+ format";
                                throw new EvioException("Evio data not in evio 4+ format");
                            }
                        }

                        controlType   = null;
                        hasFirstEvent = header.hasFirstEvent();

                        eventType = EventType.getEventType(header.getEventType());
                        if (eventType == null || !eventType.isEbFriendly()) {
                            bbSupply.release(bbItem);
                            if (ignoreDataErrors) {
                                System.out.println("IGNORE ERROR: event type  = " + eventType);
                                 continue;
                             }
                             else {
                                throw new EvioException("bad evio format or improper event type");
                            }
                        }
                        // this only works from ROC !!!
                        sourceId = header.getSourceId();
                        if (eventType == EventType.PARTIAL_PHYSICS) {
                            sourceId = ev.getControl()[0];
                        }
                        // Record id is set in the DataOutputHelper so that it is incremented
                        // once per non-user, non-control ET buffer. Each writer will only use
                        // 1 block per 2.2MB or 10K events. Thus we can get away with only
                        // looking at the very first block #.
                        recordId = header.getNumber();

                        // Check record for sequential record id for buildable events only
                        expectedRecordId = Evio.checkRecordIdSequence(recordId, expectedRecordId, false,
                                                                      eventType, DataChannelImplEt.this);

                        // Number of evio event associated with this buffer.
                        int eventCount = compactReader.getEventCount();

                        // When the emu is done processing all evio events,
                        // this buffer is released, so use this to keep count.
                        bbItem.setUsers(eventCount);

                        // Send the # of (buildable) evio events / ET event for ROC feedback,
                        // but only if this is the DC or PEB.
                        t2 = emu.getTime();
                        if (isFirstEB && eventType.isBuildable() && (t2-t1 > timeBetweenMupdates)) {
                            emu.getCmsgPortal().sendMHandlerMessage(eventCount, "M");
                            t1 = t2;
                        }
                        //logger.info("      DataChannel Et in: isFirstEb = " + isFirstEB + ", eventCount = " + eventCount +
                        //", isBuildable = " + eventType.isBuildable());
                        EvioNode node;
                        //logger.info("      DataChannel Et in: " + name + " block header, event type " + eventType +
                        //            ", src id = " + sourceId + ", recd id = " + recordId + ", event cnt = " + eventCount);

                        for (int i=1; i < eventCount+1; i++) {
                            if (isER) {
                                // Don't need to parse all bank headers, just top level.
                                node = compactReader.getEvent(i);
                            }
                            else {
                                node = compactReader.getScannedEvent(i, nodePool);
                             }

                            // Complication: from the ROC, we'll be receiving USER events
                            // mixed in with and labeled as ROC Raw events. Check for that
                            // and fix it.
                            if (eventType == EventType.ROC_RAW) {
                                if (Evio.isUserEvent(node)) {
                                    isUser = true;
                                    eventType = EventType.USER;
                                    if (hasFirstEvent) {
                                        logger.info("      DataChannel Et in: " + name + " got First event from ROC");
                                    }
                                    else {
                                        logger.info("      DataChannel Et in: " + name + " got USER event from ROC");
                                    }
                                }
                                else {
                                    // Pick this raw data event apart a little
                                    if (!node.getDataTypeObj().isBank()) {
                                        bbSupply.release(bbItem);
                                        if (ignoreDataErrors) {
                                             continue;
                                         }
                                         else {
                                            DataType eventDataType = node.getDataTypeObj();
                                            throw new EvioException("ROC raw record contains " + eventDataType +
                                                                            " instead of banks (data corruption?)");
                                        }
                                    }
                                }
                            }
                            else if (eventType == EventType.CONTROL) {
                                // Find out exactly what type of control event it is
                                // (May be null if there is an error).
                                // It may NOT be enough just to check the tag
                                controlType = ControlType.getControlType(node.getTag());
                                logger.info("      DataChannel Et in: " + name + " got CONTROL event, " + controlType);
                                if (controlType == null) {
                                    bbSupply.release(bbItem);
                                    if (ignoreDataErrors) {
                                         continue;
                                     }
                                     else {
                                        errorString = "DataChannel Et in:  unidentified control event, tag = 0x" + Integer.toHexString(node.getTag());
                                        throw new EvioException("unidentified control event, tag = 0x" + Integer.toHexString(node.getTag()));
                                    }
                                }
                            }
                            else if (eventType == EventType.USER) {
                                isUser = true;
                                if (hasFirstEvent) {
                                    logger.info("      DataChannel Et in: " + name + " got FIRST event");
                                }
                                else {
                                    logger.info("      DataChannel Et in: " + name + " got USER event");
                                    //Utilities.printBuffer(buf, 0, 30, "USER EVENT ");
                                }
                            }
                            else {
                                // Physics or partial physics event must have BANK as data type
                                if (!node.getDataTypeObj().isBank()) {
                                    bbSupply.release(bbItem);
                                    if (ignoreDataErrors) {
                                         continue;
                                     }
                                     else {
                                        DataType eventDataType = node.getDataTypeObj();
                                        throw new EvioException("physics record contains " + eventDataType +
                                                                        " instead of banks (data corruption?)");
                                    }
                                }
                            }

                            // Don't need to set controlType = null for each loop since each
                            // ET event contains only one event type (CONTROL, PHYSICS, etc).

                            //System.out.println("      DataChannel Et in: wait for next ring buf for writing");
                            nextRingItem = ringBufferIn.nextIntr(1);
                            //System.out.println("      DataChannel Et in: Got sequence " + nextRingItem);\

                            ri = ringBufferIn.get(nextRingItem);

                            // Set & reset all parameters of the ringItem
                            if (eventType.isBuildable()) {
                                ri.setAll(null, null, node, eventType, controlType,
                                          isUser, hasFirstEvent, id, recordId, sourceId,
                                          node.getNum(), name, bbItem, bbSupply);
                            }
                            else {
                                ri.setAll(null, null, node, eventType, controlType,
                                          isUser, hasFirstEvent, id, recordId,
                                          sourceId, 1, name, bbItem, bbSupply);
                            }

                            // Only the first event of first block can be "first event"
                            isUser = hasFirstEvent = false;

                            ringBufferIn.publish(nextRingItem);
                            //System.out.println("      DataChannel Et in: published sequence " + nextRingItem);

                            // Handle end event ...
                            if (controlType == ControlType.END) {
                                // There should be no more events coming down the pike so
                                // go ahead write out existing events and then shut this
                                // thread down.
                                logger.info("      DataChannel Et in: " + name + " found END event");
                                haveInputEndEvent = true;
                                break;
                            }
                        }

                        if (haveInputEndEvent) {
                            break;
                        }
                    }

                    // Put all events back in ET system - even those unused.
                    // Do some work to get accurate error msgs back to run control.
                    try {
                        if (useDirectEt) {
                            etContainer.putEvents(attachmentLocal);
                            etSysLocal.putEvents(etContainer);

                        }
                        else {
                            etContainer.putEvents(attachment, 0, validEvents);
                            etSystem.putEvents(etContainer);
                        }

                        etConsumeSequence.set(nextSequence++);
                    }
                    catch (IOException e) {
                        if (ignoreDataErrors) {
                             continue;
                         }
                         else {
                            errorString = "DataChannel Et in: network communication error with Et";
                            throw e;
                        }
                    }
                    catch (EtException e) {
                        if (ignoreDataErrors) {
                             continue;
                         }
                         else {
                            errorString = "DataChannel Et in: internal error handling Et";
                            throw e;
                        }
                    }
                    catch (EtDeadException e) {
                        if (ignoreDataErrors) {
                             continue;
                         }
                         else {
                            errorString = "DataChannel Et in: Et system dead";
                            throw e;
                        }
                    }
                    catch (EtClosedException e) {
                        if (ignoreDataErrors) {
                             continue;
                         }
                         else {
                            errorString = "DataChannel Et in: Et connection closed";
                            throw e;
                        }
                    }

                    if (haveInputEndEvent) {
                        // At this point we need to wake up that Getter which is sleeping on
                        // trying to get another event - which is not coming. If using JNI,
                        // this will block forever.
                        stopGetterThread = true;
                        if (useDirectEt) {
                            // NOT SURE WHAT TO DO HERE as there is no wakeup routine
                            System.out.println("Might be an issue waking up the GETTER thread which is sleeping");
                        }
                        else {
logger.info("      DataChannel Et in: wake up GETTER's getEvents() call so it can exit thread");
                            etSystem.wakeUpAttachment(attachment);
                        }

                        // Run callback saying we got end event.
                        // Need to wait until we put all events back and finish using ET system.
                        // Otherwise, the emu thread orchestrating END, will assume we're done
                        // and tell channels and modules to start executing the END commands.
                        // Basically, we need to avoid calling et system calls simultaneously.
                        if (endCallback != null) endCallback.endWait();
                        logger.info("      DataChannel Et in: have END, " + name + " quit parsing thd");
                        return;
                    }

                    if (!useDirectEt) {
                        etAlive = etSystem.alive();
                    }
                }

            }
            catch (InterruptedException e) {
                logger.warn("      DataChannel Et in: " + name + "  interrupted thd, exiting");
            }
            catch (Exception e) {
                channelState = CODAState.ERROR;
                // If we haven't yet set the cause of error, do so now & inform run control
                if (errorString == null) errorString = e.getMessage();
                emu.setErrorState(errorString);
                System.out.println("      DataChannel Et in: " + name + " exit thd: " + errorString);
            }
        }

    }



    /**
     * The DataChannelImplEt class has 1 ring buffer (RB) to accept output from each
     * event-processing thread of a module.
     * It takes Evio banks from these, writes them into ET events and puts them into an
     * ET system. It uses its another RB internally. Sequentially, this is what happens:
     *
     * The getter thread gets new ET buffers and puts each into a holder of the internal RB.
     *
     * The main, DataOutputHelper, thread gets a single holder from the internal RB.
     * It also takes evio events from the other RBs which contain module output. Each
     * evio event is immediately written into the ET buffer. Once the ET buffer is full or
     * some other limiting condition is met, it places the holder back into the internal RB.
     *
     * Finally, the putter thread takes holder and their data-filled ET buffers and puts
     * them back into the ET system.
     */
    private class DataOutputHelper extends Thread {

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Thread for getting new ET events. */
        private EvGetter getter;

        /** Thread for putting filled ET events back into ET system. */
        private EvPutter putter;

        /** Let a single waiter know that the main threads have been started. */
        private final CountDownLatch startLatch = new CountDownLatch(2);

        /** What state is this thread in? */
        private volatile ThreadState threadState;

        /** Place to store a bank off the ring for the next event out. */
        private RingItem unusedRingItem;

        /** Number of items in internal ring buffer. */
        private int ringSize;

        /** Internal ring buffer. */
        private RingBuffer<EtContainer> rb;

        /** Used by first consumer (DataOutputHelper) to get ring buffer items. */
        private Sequence etFillSequence;

        /** Used by first consumer to get ring buffer items. */
        private SequenceBarrier etFillBarrier;

        /** Maximum allowed number of evio ring items per ET event. */
        private static final int maxEvioItemsPerEtBuf = 10000;



        /** Constructor. */
        DataOutputHelper(ThreadGroup group, String name) {
            super(group, name);

            // Ring will have 4 slots. Each is an EtContainer object containing
            // "chunk" number of ET events.
            int ringSize = 4;

            // Create ring buffer used by 2 threads -
            //   1 to get new events from ET system and place into ring (producer of ring items)
            //   1 to get evio events, parse them into these ET events and
            //        put them back into ET system                  (consumer of ring items)
            rb = createSingleProducer(new ContainerFactory(), ringSize,
                                      new SpinCountBackoffWaitStrategy(10000, new LiteBlockingWaitStrategy()));
                                      //new YieldingWaitStrategy());

            // 1st consumer barrier of ring buffer, which gets evio
            // and writes it, depends on producer.
            etFillBarrier = rb.newBarrier();
            // 1st consumer sequence of ring buffer
            etFillSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

            // 2nd consumer to take filled ET buffers and put back into ET system,
            // depends on filling consumer.
            SequenceBarrier etPutBarrier = rb.newBarrier(etFillSequence);
            Sequence etPutSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
            // Last sequence before producer can grab ring item
            rb.addGatingSequences(etPutSequence);

            // Start consumer thread to put ET events back into ET system
            putter = new EvPutter(group, name+"_EvPutter", etPutSequence, etPutBarrier);
            putter.start();

            // Start producer thread for getting new ET events
            getter = new EvGetter(group, name+"_EvGetter");
            getter.start();
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {}
        }


        /**
         * Wait for all this object's threads to end, for the given time.
         * @param milliseconds
         * @return true if all threads ended, else false
         */
        private boolean waitForThreadsToEnd(int milliseconds) {
            int oneThreadWaitTime = milliseconds/(3);
            if (oneThreadWaitTime < 0) {
                oneThreadWaitTime = 0;
            }

            try {getter.join(oneThreadWaitTime);}
            catch (InterruptedException e) {}

            try {putter.join(oneThreadWaitTime);}
            catch (InterruptedException e) {}

            try {this.join(oneThreadWaitTime);}
            catch (InterruptedException e) {}

            return !(this.isAlive() || putter.isAlive() || getter.isAlive());
        }


        /** Stop all this object's threads from an external thread. */
        private void shutdown() {
            // If any EvGetter thread is stuck on etSystem.newEvents(), unstuck it
            try {
                // Wake up getter thread
                etSystem.wakeUpAttachment(attachment);
            }
            catch (Exception e) {}
        }


        /**
         * Main thread of ET output channel. This thread gets a "new" ET event,
         * gathers the appropriate number of evio events from a module's output
         * and writes them into that single ET buffer.
         */
        @Override
        public void run() {

            threadState = ThreadState.RUNNING;

            // Tell the world I've started
            startLatch.countDown();

            try {
                RingItem ringItem;

                EtEvent event;
                EventType pBankType = null;
                ControlType pBankControlType = null;

                // Time in milliseconds for writing if time expired
                long startTime;
                final long TIMEOUT = 2000L;

                // Always start out reading prestart & go events from ring 0
                int outputRingIndex = 0;

                // Create writer with some args that get overwritten later.
                // Make the block size bigger than the Roc's 4.2MB ET buffer
                // size so no additional block headers must be written.
                // It should contain less than 100 ROC Raw records,
                // but we'll allow 10000 such banks per block header.
                ByteBuffer etBuffer = ByteBuffer.allocate(128);
                etBuffer.order(byteOrder);

//                public EventWriterUnsync(ByteBuffer buf, int maxRecordSize, int maxEventCount,
//                                String xmlDictionary, int recordNumber,
//                                EvioBank firstEvent, int compressionType)
                EventWriterUnsync writer = new EventWriterUnsync(etBuffer, 4*1100000, maxEvioItemsPerEtBuf,
                                                                 null, 0, null, 0);
                writer.setSourceId(emu.getCodaid());
//                public EventWriterUnsync(ByteBuffer buf, int blockSizeMax, int blockCountMax,
//                                             String xmlDictionary, BitSet bitInfo, int reserved1,
//                                             int blockNumber) throws EvioException {
//                emu.getCodaid() == sourceId
                writer.close();

                int bytesToEtBuf, ringItemSize=0, banksInEtBuf, myRecordId;
                int etSize = (int) etSystem.getEventSize();
                boolean etEventInitialized, isUserOrControl=false;
                boolean isUser=false, isControl=false;
                boolean gotPrestart=false;

                // Variables for consuming ring buffer items
                long etNextFillSequence = etFillSequence.get() + 1L;
                long etAvailableFillSequence = -1L;

                EtContainer etContainer;
                EtEvent[] events;
                int validEvents;
                int itemCount;
                BitSet bitInfo = new BitSet(24);
                recordId = 1;

                top:
                while (true) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) Thread.sleep(5);
                        continue;
                    }

                    // Init variables
                    event = null;

                    // Get events
                    try {
                        //System.out.print("      DataChannel Et out: " + name + " getEvents() ...");

                        // Will block here if no available slots in ring.
                        // It will unblock when more new, unfilled ET events are gotten by the evGetter thread.
                        if (etAvailableFillSequence < etNextFillSequence) {
                            // Wait for next available ring slot
//logger.info("      DataChannel Et out (" + name + "): filler, wait for container");
                            etAvailableFillSequence = etFillBarrier.waitFor(etNextFillSequence);
                        }
                        etContainer = rb.get(etNextFillSequence);
                        events = etContainer.getEventArray();
                        validEvents = etContainer.getEventCount();
                        // All events have data unless otherwise specified
                        etContainer.setLastIndex(validEvents - 1);
//logger.info("      DataChannel Et out (" + name + "): filler, got container with " + validEvents +
//                    " events, lastIndex = " + (validEvents - 1) + ", id = " + etContainer.getId());
                    }
                    catch (InterruptedException e) {
                        // Told to wake up because we're ending or resetting
                        if (haveInputEndEvent) {
                            logger.info("      DataChannel Et out: wake up " + name + ", other thd found END, quit");
                        }
                        else if (gotResetCmd) {
                            logger.info("      DataChannel Et out: " + name + " got RESET cmd, quitting");
                        }
                        return;
                    }
                    catch (Exception e) {
                        throw new EmuException("Error getting events to fill", e);
                    }

                    // For each event ...
                    nextEvent:
                    for (int j=0; j < validEvents; j++) {
//logger.info("      DataChannel Et out (" + name + "): filler, got ET event " + j);

                        // Set time we started dealing with this ET event
                        startTime = emu.getTime();

                        // Init variables
                        bytesToEtBuf = 0;
                        banksInEtBuf = 0;
                        etEventInitialized = false;
                        itemCount = 0;

                        // Very first time through, event is null and we skip this
                        if (event != null) {
//logger.info("      DataChannel Et out (" + name + "): filler, close writer for event " + j);
                            // Finish up the writing for the current ET event
                            writer.close();

                            // Be sure to set length of ET event to bytes of data actually written
                            event.setLength((int) writer.getBytesWrittenToBuffer());
                        }

                        // Get another ET event from the container
                        event = events[j];

                        //------------------------------------------

                        while (true) {
                            //--------------------------------------------------------
                            // Get 1 item off of this channel's input rings which gets
                            // stuff from last module.
                            // (Have 1 ring for each module event-processing thread).
                            //--------------------------------------------------------

                            // If we already got a ring item (evio event) in
                            // the previous loop and have not used it yet, then
                            // don't bother getting another one right now.
                            if (unusedRingItem != null) {
                                ringItem = unusedRingItem;
                                unusedRingItem = null;
                            }
                            else {
                                try {
//System.out.println("      DataChannel Et out (" + name + "): filler, get ring item from ring " + outputRingIndex);
                                    ringItem = getNextOutputRingItem(outputRingIndex);
//System.out.println("      DataChannel Et out (" + name + "): filler, got ring item from ring " + outputRingIndex);

//                                if (isEB) sleep(1);
//System.out.println("     got evt from ring");
//System.out.println(outputIndex + " : " + outputRingIndex + " : " + nextEvent);
                                }
                                catch (InterruptedException e) {
                                    return;
                                }
//System.out.println("done");
                                pBankType = ringItem.getEventType();
                                pBankControlType = ringItem.getControlType();
                                isUser = pBankType.isUser();
                                isControl = pBankType.isControl();
                                isUserOrControl = pBankType.isUserOrControl();
//System.out.println("      DataChannel Et out (" + name + "): filler, isUserOrControl = " + isUserOrControl);

                                // If no prestart yet ...
                                if (!gotPrestart) {
                                    // and we have a control event ...
                                    if (pBankControlType != null) {
                                        // See if it's a prestart
                                        gotPrestart = pBankControlType.isPrestart();
                                        // If not, error
                                        if (!gotPrestart) {
                                            throw new EmuException("Prestart event must be first control event");
                                        }
                                    }
                                    // Else if not a user event, error
                                    else if (!isUser) {
                                        throw new EmuException("Only user or prestart event allowed to be first");
                                    }
                                }

                                // Allow for the possibility of having to write
                                // 2 block headers in addition to this evio event.
                                ringItemSize = ringItem.getTotalBytes() + 64;
                            }

                            //------------------------------------------------
//System.out.println("      DataChannel Et out: " + name + " etSize = " + etSize + " <? bytesToEtBuf(" +
//                           bytesToEtBuf + ") + ringItemSize (" + ringItemSize + ")" +
//", item count = " + itemCount);

                            // If this ring item will not fit into current ET buffer,
                            // either because there is no more room or there's a limit on
                            // the # of evio events in a single ET buffer ...
                            if ((bytesToEtBuf + ringItemSize > etSize) ||
                                    (itemCount >= maxEvioItemsPerEtBuf)) {
                                // If nothing written into ET buf yet ...
                                if (banksInEtBuf < 1) {
                                    // Get rid of this ET buf which is too small
                                    // Don't bother using the new methods for this as it likely never happens
                                    etSystem.dumpEvents(attachment, new EtEvent[]{event});

                                    // Get 1 bigger & better ET buf as a replacement
                                    logger.rcConsole("\n      DataChannel Et out (" + name + "): filler, using over-sized (temp) ET event", "DANGER !!!\n");

                                    EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, false,
                                                                        0, 1, ringItemSize, group);
                                    // Put the new ET buf into container (realEvents array if remote, else jniEvents)
                                    events[j] = event = evts[0];
                                }
                                // If data was previously written into this ET buf ...
                                else {
//System.out.println("      DataChannel Et out (" + name + "): filler, item doesn't fit cause other stuff in there, do write close, get another ET event");
//System.out.println("      DataChannel Et out (" + name + "): filler, banks in ET buf = " + banksInEtBuf + ", isUserOrControl = " + isUserOrControl);
                                    // Get another ET event to put this evio data into
                                    // and hope there is enough room for it.
                                    //
                                    // On the next time through this while loop, do not
                                    // grab another ring item since we already have this
                                    // one we're in the middle of dealing with.
                                    unusedRingItem = ringItem;

                                    // Grab a new ET event and hope it fits in there
                                    continue nextEvent;
                                }
                            }
                            // If this event is a user or control event ...
                            else if (isUserOrControl) {
                                // If data was previously written into this ET buf ...
                                if (banksInEtBuf > 0) {
                                    // We want to put all user & control events into their
                                    // very own ET events. This makes things much easier to
                                    // handle downstream.

                                    // Get another ET event to put this evio data into.
                                    //
                                    // On the next time through this while loop, do not
                                    // grab another ring item since we already have this
                                    // one we're in the middle of dealing with.
                                    unusedRingItem = ringItem;

                                    // Grab a new ET event and use it. Don't mix data types.
                                    continue nextEvent;
                                }

                                // store info on END event
                                if (pBankControlType == ControlType.END) {
                                    etContainer.setHasEndEvent(true);
                                    etContainer.setLastIndex(j);
//System.out.println("      DataChannel Et out (" + name + "): filler found END, last index = " + j);
                                }
                            }


                            //-------------------------------------------------------
                            // Do the following once per holder/ET-event
                            //-------------------------------------------------------
                            if (!etEventInitialized) {
                                // Set control words of ET event
                                //
                                // CODA owns the first ET event control int which contains source id.
                                // If a PEB or SEB, set it to event type.
                                // If a DC or ROC,  set this to coda id.
                                if (isFinalEB) {
                                    control[0] = pBankType.getValue();
                                    event.setControl(control);
                                }
                                else if (isEB || isROC) {
                                    event.setControl(control);
                                }

                                // Set byte order
                                event.setByteOrder(byteOrder);

                                // Encode event type into bits
                                bitInfo.clear();
                                EmuUtilities.setEventType(bitInfo, pBankType);

                                // Set recordId depending on what type this bank is
                                myRecordId = -1;
                                if (!isUserOrControl) {
                                    myRecordId = recordId;
                                }
                                // If user event which is to be the first event,
                                // mark it in the block header's bit info word.
                                else if (ringItem.isFirstEvent()) {
                                    EmuUtilities.setFirstEvent(bitInfo);
                                }
                                recordId++;

                                // Prepare ET event's data buffer
                                etBuffer = event.getDataBuffer();
                                etBuffer.clear();
                                etBuffer.order(byteOrder);

                                // Do init once per ET event
                                etEventInitialized = true;

                                // Initialize the writer which writes evio banks into ET buffer
                                writer.setBuffer(etBuffer, bitInfo, myRecordId);
                            }

                            //----------------------------------
                            // Write evio bank into ET buffer
                            //----------------------------------
                            EvioNode node = ringItem.getNode();
                            ByteBuffer buf = ringItem.getBuffer();

                            if (buf != null) {
                                writer.writeEvent(buf);
                            }
                            else if (node != null) {
                                // The last arg is do we need to duplicate node's backing buffer?
                                // Don't have to do that to keep our own lim/pos because the only
                                // nodes that make it to an output channel are the USER events.
                                // Even though it is almost never the case, if 2 USER events share
                                // the same backing buffer, there still should be no problem.
                                // Even if 2nd event is being scanned by CompactEventReader, while
                                // previous event is being written right here, it should still be OK
                                // since buffer lim or pos are not changed during scanning process.
                                writer.writeEvent(node, false, false);
                            }

                            itemCount++;

                            // Added evio event/buf to this ET event
                            banksInEtBuf++;
                            // This is just a max ESTIMATE for purposes of deciding
                            // when to switch to a new event.
                            bytesToEtBuf += ringItemSize;

                            // If this ring item's data is in a buffer which is part of a
                            // ByteBufferSupply object, release it back to the supply now.
                            // Otherwise call does nothing.
                            ringItem.releaseByteBuffer();

                            // FREE UP this channel's input rings' slots/items --
                            // for the module's event producing threads.
                            releaseCurrentAndGoToNextOutputRingItem(outputRingIndex);
//System.out.println("      DataChannel Et out (" + name + "): filler, released item on ring " +
//                           outputRingIndex  + ", go to next");

                            // Handle END & GO events
                            if (pBankControlType != null) {
                                if (pBankControlType == ControlType.END) {
                                    // Finish up the writing
                                    writer.close();
                                    event.setLength((int) writer.getBytesWrittenToBuffer());

                                    // Tell Getter thread to stop getting new ET events
                                    stopGetterThread = true;

                                    // Pass END and all unused new events after it to Putter thread.
                                    // Cursor is the highest published sequence in the ring.

                                    //etFillSequence.set(rb.getCursor());
                                    etFillSequence.set(etNextFillSequence);

                                    // Do not call shutdown() here since putter
                                    // thread must still do a putEvents().
                                    threadState = ThreadState.DONE;
                                    return;
                                }
                                else if (pBankControlType == ControlType.GO) {
                                    // If the module has multiple build threads, then it's possible
                                    // that the first buildable event (next one in this case)
                                    // will NOT come on ring 0. Make sure we're looking for it
                                    // on the right ring. It was set to the correct value in
                                    // DataChannelAdapter.prestart().
                                    outputRingIndex = ringIndex;
                                }
                            }

//System.out.println("      DataChannel Et out (" + name + "): go to item " + nextSequences[outputRingIndex] +
//" on ring " + outputRingIndex);

                            // Do not go to the next ring if we got a control or user event.
                            // All prestart, go, & users go to the first ring. Just keep reading
                            // from the same ring until we get to a buildable event. Then start
                            // keeping count so we know when to switch to the next ring.
                            if (outputRingCount > 1 && !isUserOrControl) {
                                outputRingIndex = setNextEventAndRing();
//System.out.println("      DataChannel Et out (" + name + "): for next ev " + nextEvent +
//                           " SWITCH TO ring " + outputRingIndex + ", outputRingCount (bt threads) = " +
//                           outputRingCount);
                            }

                            // Implement a timeout for low rates.
                            // We're done with this event and this container.
                            // Send the container to the putter thread and to ET system.

                            // Also switch to new ET event for user & control banks
                            if ((emu.getTime() - startTime > TIMEOUT) || isUserOrControl) {
                                // We want the PRESTART event to go right through without delay.
                                // So don't wait for all new events to be filled before sending this
                                // container to be put back into the ET system.
                                if (pBankControlType == ControlType.PRESTART) {
//System.out.println("      DataChannel Et out (" + name + "): control ev = " + pBankControlType +
//                           ", go to next container, last index = " + j);
                                    // Finish up the writing for the last ET event
                                    writer.close();
                                    event.setLength((int) writer.getBytesWrittenToBuffer());

                                    // Forget about any other unused events in the container
                                    etContainer.setLastIndex(j);

                                    // Release this container to putter thread
                                    etFillSequence.set(etNextFillSequence++);
                                    continue top;
                                }
//                                if (emu.getTime() - startTime > TIMEOUT) {
//                                    System.out.println("TIME FLUSH ******************");
//                                }
                                continue nextEvent;
                            }
                        }
                    }

//logger.info("      DataChannel Et out (" + name + "): filler, close writer for last event " + (validEvents - 1));
                    // Finish up the writing for the last ET event
                    writer.close();

                    // Be sure to set length of ET event to bytes of data actually written
                    event.setLength((int) writer.getBytesWrittenToBuffer());

                    //----------------------------------------
                    // Release container for putting thread
                    //----------------------------------------
                    etFillSequence.set(etNextFillSequence++);
                }
            }
            catch (InterruptedException e) {
                // Interrupted while waiting for ring item
//logger.warn("      DataChannel Et out: " + name + "  interrupted thd, exiting");
            }
            catch (Exception e) {
logger.warn("      DataChannel Et out: exit thd w/ error = " + e.getMessage());
                e.printStackTrace();
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel Et out: " + e.getMessage());
            }

            threadState = ThreadState.DONE;
        }


        /**
         * This class is a thread designed to put ET events that have been
         * filled with evio data, back into the ET system.
         * It runs simultaneously with the thread that fills these events
         * with evio data and the thread that gets them from the ET system.
         */
        private class EvPutter extends Thread {

            private Sequence sequence;
            private SequenceBarrier barrier;


            /**
             * Constructor.
             * @param group     thread group to be a part of.
             * @param name      name of thread.
             * @param sequence  ring buffer sequence to use.
             * @param barrier   ring buffer barrier to use.
             */
            EvPutter(ThreadGroup group, String name,
                     Sequence sequence, SequenceBarrier barrier) {

                super(group, name);
                this.barrier = barrier;
                this.sequence = sequence;
            }


            /** {@inheritDoc} */
            public void run() {

                // Tell the world I've started
                startLatch.countDown();

                EtContainer etContainer;

                int  lastIndex=0, validEvents, eventsToPut, eventsToDump;
                long availableSequence = -1L;
                long nextSequence = sequence.get() + 1L;
                boolean hasEnd;

                try {

                    while (true) {
                        if (gotResetCmd) {
                            return;
                        }

                        // Do we wait for next ring slot or do we already have something from last time?
                        if (availableSequence < nextSequence) {
                            // Wait for next available ring slot
//System.out.println("      DataChannel Et out (" + name + "): PUTTER try getting seq " + nextSequence);
                            availableSequence = barrier.waitFor(nextSequence);
                        }
                        etContainer = rb.get(nextSequence);

                        // Total # of events obtained by newEvents()
                        validEvents = etContainer.getEventCount();

                        // Index of last event containing data
                        lastIndex = etContainer.getLastIndex();

                        // Look for the END event
                        hasEnd = etContainer.hasEndEvent();

                        if (lastIndex + 1 < validEvents) {
                            eventsToPut = lastIndex + 1;
                            eventsToDump = validEvents - eventsToPut;
                        }
                        else {
                            eventsToPut = validEvents;
                            eventsToDump = 0;
                        }

//System.out.println("      DataChannel Et out (" + name + "): PUTTER got seq " + nextSequence +
//                   ", " + validEvents + " valid, hasEnd = " + hasEnd + ", lastIndex = " + lastIndex +
//                   ", toPut = " + eventsToPut + ", toDump = " + eventsToDump);

                        // Put all events with valid data back in ET system.
                        etContainer.putEvents(attachment, 0, eventsToPut);
                        etSystem.putEvents(etContainer);

                        if (eventsToDump > 0) {
                            // Dump all events with NO valid data. END is last valid event.
                            etContainer.dumpEvents(attachment, lastIndex+1, eventsToDump);
                            etSystem.dumpEvents(etContainer);
//System.out.println("      DataChannel Et out (" + name + "): PUTTER callED dumpEvents()");
                        }

                        sequence.set(nextSequence++);

                        // Checks the last event we're putting to see if it's the END event
                        if (hasEnd) {
                            // Run callback saying we got & have processed END event
                            if (endCallback != null) endCallback.endWait();
System.out.println("      DataChannel Et out (" + name + "): PUTTER got END event, quitting thread");
                            return;
                        }
                    }
                }
                catch (AlertException e) {
                    channelState = CODAState.ERROR;
                    emu.setErrorState("DataChannel Et out: ring buffer error");
                }
                catch (InterruptedException e) {
                    // Quit thread
System.out.println("      DataChannel Et out: " + name + " interrupted thread");
                }
                catch (TimeoutException e) {
                    // Never happen in our ring buffer
                    channelState = CODAState.ERROR;
                    emu.setErrorState("DataChannel Et out: time out in ring buffer");
                }
                catch (IOException e) {
                    channelState = CODAState.ERROR;
System.out.println("      DataChannel Et out: " + name + " network communication error with Et");
                    emu.setErrorState("DataChannel Et out: network communication error with Et");
                    e.printStackTrace();
                }
                catch (EtException e) {
                    channelState = CODAState.ERROR;
System.out.println("      DataChannel Et out: " + name + " internal error handling Et");
                    emu.setErrorState("DataChannel Et out: internal error handling Et");
                    e.printStackTrace();
                }
                catch (EtDeadException e) {
                    channelState = CODAState.ERROR;
System.out.println("      DataChannel Et out: " + name + " Et system dead");
                    emu.setErrorState("DataChannel Et out: Et system dead");
                }
                catch (EtClosedException e) {
                    channelState = CODAState.ERROR;
System.out.println("      DataChannel Et out: " + name + " Et connection closed");
                    emu.setErrorState("DataChannel Et out: Et connection closed");
                }
                finally {
                    logger.info("      DataChannel Et out: PUTTER is Quitting");
                }
            }
        }


        /**
         * This class is a thread designed to get new events from the ET system.
         * It runs simultaneously with the thread that fills these events
         * with evio data and the thread that puts them back.
         */
        final private class EvGetter extends Thread {

            /**
             * Constructor.
             * @param group  thread group to be a part of.
             * @param name   name of thread.
             */
            EvGetter(ThreadGroup group, String name) {
                super(group, name);
            }

            /**
             * {@inheritDoc}<p>
             * Get the new ET events.
             */
            public void run() {

                long sequence;
                boolean gotError;
                String errorString;
                EtContainer etContainer;
                int eventSize = (int)getEtEventSize();

                // Tell the world I've started
                startLatch.countDown();

                try {
                    // If there are too few events to avoid a deadlock while newEvents is
                    // called in sleep mode, use a timed mode ...
                    if (deadLockAtPrestart) {
                        while (true) {
                            if (stopGetterThread) {
                                return;
                            }

                            // Will block here if no available slots in ring.
                            // It will unblock when ET events are put back by the other thread.
                            sequence = rb.nextIntr(1); // This just spins on parkNanos
                            etContainer = rb.get(sequence);

                            // Now that we have a free container, get new events & store them in container.
                            // The reason this is timed and not in sleep mode is that if there are 6 or less
                            // events in the ET system. This thread will block here and not in rb.next();
                            // If we completely block here, then we tie up the mutex which the evPutter
                            // threads needs to use to put events back. Thus we block all event flow.
                            etContainer.newEvents(attachment, Mode.TIMED, 100000, chunk,
                                                  eventSize, group);
                            while (true) {
                                try {
//System.out.println("      DataChannel Et out (" + name + "): GETTER try getting new events");
                                    etSystem.newEvents(etContainer);
//System.out.println("      DataChannel Et out (" + name + "): GETTER got new events");
                                    break;
                                }
                                catch (EtTimeoutException e) {
                                    continue;
                                }
                            }

                            // Make container available for parsing/putting thread
                            rb.publish(sequence++);
                        }
                    }
                    else {
                        while (true) {
                            if (stopGetterThread) {
                                return;
                            }

                            sequence = rb.nextIntr(1);
                            etContainer = rb.get(sequence);

                            etContainer.newEvents(attachment, Mode.SLEEP, 0, chunk, eventSize, group);
                            etSystem.newEvents(etContainer);

                            rb.publish(sequence++);
                        }
                    }
                }
                catch (EtWakeUpException e) {
                    // Told to wake up because we're ending or resetting
                    if (haveInputEndEvent) {
                        logger.info("      DataChannel Et out: wake up " + name + ", other thd found END, quit");
                    }
                    else if (gotResetCmd) {
                        logger.info("      DataChannel Et out: " + name + " got RESET cmd, quitting");
                    }
                    return;
                }
                catch (IOException e) {
                    gotError = true;
                    errorString = "DataChannel Et out: network communication error with Et";
                    e.printStackTrace();
                }
                catch (EtException e) {
                    gotError = true;
                    errorString = "DataChannel Et out: internal error handling Et";
                    e.printStackTrace();
                }
                catch (EtDeadException e) {
                    gotError = true;
                    errorString = "DataChannel Et out: Et system dead";
                }
                catch (EtClosedException e) {
                    gotError = true;
                    errorString = "DataChannel Et out: Et connection closed";
                }
                catch (Exception e) {
                    gotError = true;
                    errorString = "DataChannel Et out: " + e.getMessage();
                }

                // ET system problem - run will come to an end
                if (gotError) {
                    channelState = CODAState.ERROR;
                    System.out.println("      DataChannel Et out: " + name + ", " + errorString);
                    emu.setErrorState(errorString);
                }
System.out.println("      DataChannel Et out: GETTER is Quitting");
            }
        }

    }



//
//
//    /**
//      * The DataChannelImplEt class has 1 ring buffer (RB) to accept output from each
//      * event-processing thread of a module.
//      * It takes Evio banks from these, writes them into ET events and puts them into an
//      * ET system. It uses its another RB internally. Sequentially, this is what happens:
//      *
//      * The getter thread gets new ET buffers and puts each into a holder of the internal RB.
//      *
//      * The main, DataOutputHelper, thread gets a single holder from the internal RB.
//      * It also takes evio events from the other RBs which contain module output. Each
//      * evio event is immediately written into the ET buffer. Once the ET buffer is full or
//      * some other limiting condition is met, it places the holder back into the internal RB.
//      *
//      * Finally, the putter thread takes holder and their data-filled ET buffers and puts
//      * them back into the ET system.
//      */
//     private class DataOutputHelper extends Thread {
//
//         /** Help in pausing DAQ. */
//         private int pauseCounter;
//
//         /** Thread for getting new ET events. */
//         private EvGetter getter;
//
//         /** Thread for putting filled ET events back into ET system. */
//         private EvPutter putter;
//
//         /** Let a single waiter know that the main thread has been started. */
//         private final CountDownLatch startLatch = new CountDownLatch(1);
//
//         /** What state is this thread in? */
//         private volatile ThreadState threadState;
//
//         /** Place to store a bank off the ring for the next event out. */
//         private RingItem unusedRingItem;
//
//         /** Number of items in internal ring buffer. */
//         private int ringSize;
//
//         /** Internal ring buffer. */
//         private RingBuffer<EventHolder> rb;
//
//         /** Used by first consumer (DataOutputHelper) to get ring buffer items. */
//         private Sequence etFillSequence;
//
//         /** Used by first consumer to get ring buffer items. */
//         private SequenceBarrier etFillBarrier;
//
//         /** Maximum allowed number of evio ring items per ET event. */
//         private static final int maxEvioItemsPerEtBuf = 10000;
//
//
//
//         /** Constructor. */
//         DataOutputHelper(ThreadGroup group, String name) {
//             super(group, name);
//
//             // Closest power of 2 to chunk, rounded up.
//             // Making it 2xchunk slows it down quite a bit.
//             // Making it chunk or chunk/2 is roughly the same on average
//             // although, chunk/2 has bigger top speed.
//             ringSize = EmuUtilities.powerOfTwo(chunk, true);
//
//             // Create ring buffer used by 3 threads -
//             //   1 to get new events from ET system and place into ring (producer of ring items)
//             //   1 to get evio events and write them into ET event  (1st consumer of ring items)
//             //   1 to put events back into ET system (2nd consumer of ring items)
//             rb = createSingleProducer(new HolderFactory(), ringSize,
//                                       new YieldingWaitStrategy());
//
//             // 1st consumer barrier of ring buffer, which gets evio
//             // and writes it, depends on producer.
//             etFillBarrier = rb.newBarrier();
//             // 1st consumer sequence of ring buffer
//             etFillSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
//
//             // 2nd consumer to take filled ET buffers and put back into ET system,
//             // depends on filling consumer.
//             SequenceBarrier etPutBarrier = rb.newBarrier(etFillSequence);
//             Sequence etPutSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
//             // Last sequence before producer can grab ring item
//             rb.addGatingSequences(etPutSequence);
//
//             // Start consumer thread to put ET events back into ET system
//             putter = new EvPutter(etPutSequence, etPutBarrier);
//             putter.start();
//
//             // Start producer thread for getting new ET events
//             getter = new EvGetter();
//             getter.start();
//         }
//
//
//         /** A single waiter can call this method which returns when thread was started. */
//         private void waitUntilStarted() {
//             try {
//                 startLatch.await();
//             }
//             catch (InterruptedException e) {}
//         }
//
//
//         /** Kill all this object's threads from an external thread. */
//         private void killFromOutside() {
//             // Kill all threads
//             getter.stop();
//             putter.stop();
//             this.stop();
//         }
//
//
//         /**
//          * Wait for all this object's threads to end, for the given time.
//          * @param milliseconds
//          * @return true if all threads ended, else false
//          */
//         private boolean waitForThreadsToEnd(int milliseconds) {
//             int oneThreadWaitTime = milliseconds/(3);
//             if (oneThreadWaitTime < 0) {
//                 oneThreadWaitTime = 0;
//             }
//
//             try {getter.join(oneThreadWaitTime);}
//             catch (InterruptedException e) {}
//
//             try {putter.join(oneThreadWaitTime);}
//             catch (InterruptedException e) {}
//
//             try {this.join(oneThreadWaitTime);}
//             catch (InterruptedException e) {}
//
//             return !(this.isAlive() || putter.isAlive() || getter.isAlive());
//         }
//
//
//         /** Stop all this object's threads from an external thread. */
//         private void shutdown() {
//             // If any EvGetter thread is stuck on etSystem.newEvents(), unstuck it
//             try {
//                 // Wake up getter thread
//                 etSystem.wakeUpAttachment(attachment);
//             }
//             catch (Exception e) {}
//         }
//
//
//         /** Object that holds an EtEvent and metadata in internal ring buffer. */
//         final private class EventHolder {
//             /** Place to hold ET event. */
//             private EtEvent event;
//
//             /** Is this the END event? */
//             private boolean isEnd;
//
//             /** Number of evio events in ET event. */
//             private int itemCount;
//
//             /** Bit info to set in EventWriter. */
//             private BitSet bitInfo = new BitSet(24);
//
//             /** Id to set in EventWriter. */
//             private int recordId;
//         }
//
//
//         /**
//          * Class used by the this output channel's internal RingBuffer
//          * to populate itself with holders of ET buffers.
//          */
//         final private class HolderFactory implements EventFactory<EventHolder> {
//             final public EventHolder newInstance() {
//                 // This object holds an EtEvent
//                 return new EventHolder();
//             }
//         }
//
//
//
//         /**
//          * Main thread of ET output channel. This thread gets a "new" ET event,
//          * gathers the appropriate number of evio events from a module's output
//          * and writes them into that single ET buffer.
//          */
//         @Override
//         public void run() {
//
//             threadState = ThreadState.RUNNING;
//
//             // Tell the world I've started
//             startLatch.countDown();
//
//             try {
//                 RingItem ringItem;
//                 EventHolder holder;
//
//                 EtEvent event = null;
//                 EventType pBankType = null;
//                 ControlType pBankControlType = null;
//
//                 // Time in milliseconds for writing if time expired
//                 long startTime;
//                 final long TIMEOUT = 2000L;
//
//                 // Always start out reading prestart & go events from ring 0
//                 int outputRingIndex = 0;
//
//                 // Create writer with some args that get overwritten later.
//                 // Make the block size bigger than the Roc's 4.2MB ET buffer
//                 // size so no additional block headers must be written.
//                 // It should contain less than 100 ROC Raw records,
//                 // but we'll allow 10000 such banks per block header.
//                 ByteBuffer etBuffer = ByteBuffer.allocate(128);
//                 etBuffer.order(byteOrder);
//                 EventWriterUnsync writer = new EventWriterUnsync(etBuffer, 1100000, maxEvioItemsPerEtBuf,
//                                                                  null, null, emu.getCodaid(), 0);
//                 writer.close();
//
//                 int bytesToEtBuf, ringItemSize=0, banksInEtBuf, myRecordId;
//                 int etSize = (int) etSystem.getEventSize();
//                 boolean etEventInitialized, isUserOrControl=false;
//                 boolean gotPrestart=false;
//
//                 // Variables for consuming ring buffer items
//                 long etNextFillSequence = etFillSequence.get() + 1L;
//                 long etAvailableFillSequence = -1L;
//
//
//                 top:
//                 while (true) {
//
//                     if (pause) {
//                         if (pauseCounter++ % 400 == 0) Thread.sleep(5);
//                         continue;
//                     }
//
//                     // Init variables
//                     myRecordId = 1;
//                     bytesToEtBuf = 0;
//                     banksInEtBuf = 0;
//                     etEventInitialized = false;
//
//                     // Set time we started dealing with this ET event
//                     startTime = emu.getTime();
//
//                     // Very first time through, event is null and we skip this
//                     if (event != null) {
//                         // Finish up the writing
//                         writer.close();
//
//                         // Be sure to set length of ET event to bytes of data actually written
//                         event.setLength((int)writer.getBytesWrittenToBuffer());
//
//                         //----------------------------------------
//                         // Release ET event for putting thread
//                         //----------------------------------------
// //System.out.println("      DataChannel Et out (" + name + "): filler, release ET event " + etNextFillSequence);
//                         etFillSequence.set(etNextFillSequence++);
//                     }
//
// //System.out.println("      DataChannel Et out (" + name + "): filler, try getting next seq (" + etNextFillSequence + ") for ET");
//                     // Do we wait for next ring slot or do we already have something from last time?
//                     if (etAvailableFillSequence < etNextFillSequence) {
//                         // Wait for next available ring slot
//                         etAvailableFillSequence = etFillBarrier.waitFor(etNextFillSequence);
//                     }
// //System.out.println("      DataChannel Et out (" + name + "): filler, available Seq = " + etAvailableFillSequence);
//
//                     //---------------------------------------------------------
//                     // Get the next holder object from internal ring's slot.
//                     // This will contain the new ET event to place data into.
//                     //---------------------------------------------------------
//                     holder = rb.get(etNextFillSequence);
//                     event = holder.event;
//                     //------------------------------------------
//
//                     while (true) {
//                         //--------------------------------------------------------
//                         // Get 1 item off of this channel's input rings which gets
//                         // stuff from last module.
//                         // (Have 1 ring for each module event-processing thread).
//                         //--------------------------------------------------------
//
//                         // If we already got a ring item (evio event) in
//                         // the previous loop and have not used it yet, then
//                         // don't bother getting another one right now.
//                         if (unusedRingItem != null) {
//                             ringItem = unusedRingItem;
//                             unusedRingItem = null;
//                         }
//                         else {
//                             try {
// //System.out.println("      DataChannel Et out (" + name + "): filler, get ring item from ring " + outputRingIndex);
//                                 ringItem = getNextOutputRingItem(outputRingIndex);
// //System.out.println("      DataChannel Et out (" + name + "): filler, got ring item from ring " + outputRingIndex);
//
// //                                if (isEB) sleep(1);
// //System.out.println("     got evt from ring");
// //System.out.println(outputIndex + " : " + outputRingIndex + " : " + nextEvent);
//                             }
//                             catch (InterruptedException e) {
//                                 return;
//                             }
// //System.out.println("done");
//                             pBankType = ringItem.getEventType();
//                             pBankControlType = ringItem.getControlType();
//                             isUserOrControl = pBankType.isUserOrControl();
//
//                             // If no prestart yet ...
//                             if (!gotPrestart) {
//                                 // and we have a control event ...
//                                 if (pBankControlType != null) {
//                                     // See if it's a prestart
//                                     gotPrestart = pBankControlType.isPrestart();
//                                     // If not, error
//                                     if (!gotPrestart) {
//                                         throw new EmuException("Prestart event must be first control event");
//                                     }
//                                 }
//                                 // Else if not a user event, error
//                                 else if (!isUserOrControl) {
//                                     throw new EmuException("Only user or prestart event allowed to be first");
//                                 }
//                             }
//
//                             // Allow for the possibility of having to write
//                             // 2 block headers in addition to this evio event.
//                             ringItemSize = ringItem.getTotalBytes() + 64;
//                         }
//
//                         //------------------------------------------------
// //System.out.println("      DataChannel Et out: " + name + " etSize = " + etSize + " <? bytesToEtBuf(" +
// //                           bytesToEtBuf + ") + ringItemSize (" + ringItemSize + ")" +
// //", holder item count = " + holder.itemCount);
//
//                         // If this ring item will not fit into current ET buffer,
//                         // either because there is no more room or there's a limit on
//                         // the # of evio events in a single ET buffer ...
//                         if ((bytesToEtBuf + ringItemSize > etSize) ||
//                                 (holder.itemCount >= maxEvioItemsPerEtBuf)) {
//                             // If nothing written into ET buf yet ...
//                             if (banksInEtBuf < 1) {
//                                 // Get rid of this ET buf which is too small
//                                 // Don't bother using the new methods for this as it likely never happens
//                                 etSystem.dumpEvents(attachment, new EtEvent[]{event});
//
//                                 // Get 1 bigger & better ET buf as a replacement
// logger.rcConsole("\n      DataChannel Et out (" + name + "): filler, using over-sized (temp) ET event", "DANGER !!!\n");
//
//                                 EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, false,
//                                                                     0, 1, ringItemSize, group);
//                                 event = evts[0];
//
//                                 // Put the new ET buf into ring slot's holder
//                                 holder.event = event;
//                             }
//                             // If data was previously written into this ET buf ...
//                             else {
// //System.out.println("      DataChannel Et out: " + name + " item doesn't fit cause other stuff in there, do write close, get another ET event");
// //System.out.println("      DataChannel Et out: " + name + " banks in ET buf = " + banksInEtBuf + ", isUserOrControl = " + isUserOrControl);
//                                 // Get another ET event to put this evio data into
//                                 // and hope there is enough room for it.
//                                 //
//                                 // On the next time through this while loop, do not
//                                 // grab another ring item since we already have this
//                                 // one we're in the middle of dealing with.
//                                 unusedRingItem = ringItem;
//
//                                 // Grab a new ET event and hope it fits in there
//                                 continue top;
//                             }
//                         }
//                         // If this event is a user or control event ...
//                         else if (isUserOrControl) {
//                             // If data was previously written into this ET buf ...
//                             if (banksInEtBuf > 0) {
//                                 // We want to put all user & control events into their
//                                 // very own ET events. This makes things much easier to
//                                 // handle downstream.
//
//                                 // Get another ET event to put this evio data into.
//                                 //
//                                 // On the next time through this while loop, do not
//                                 // grab another ring item since we already have this
//                                 // one we're in the middle of dealing with.
//                                 unusedRingItem = ringItem;
//
//                                 // Grab a new ET event and use it. Don't mix data types.
//                                 continue top;
//                             }
//
//                             // store info on END event
//                             if (pBankControlType == ControlType.END) {
//                                 holder.isEnd = true;
//                             }
//                         }
//
//
//                         //-------------------------------------------------------
//                         // Do the following once per holder/ET-event
//                         //-------------------------------------------------------
//                         if (!etEventInitialized) {
//                             // Set control words of ET event
//                             //
//                             // CODA owns the first ET event control int which contains source id.
//                             // If a PEB or SEB, set it to event type.
//                             // If a DC or ROC,  set this to coda id.
//                             if (isFinalEB) {
//                                 control[0] = pBankType.getValue();
//                                 event.setControl(control);
//                             }
//                             else if (isEB || isROC) {
//                                 event.setControl(control);
//                             }
//
//                             // Set byte order
//                             event.setByteOrder(byteOrder);
//
//                             // Encode event type into bits
//                             //holder.bitInfo.clear();
//                             EmuUtilities.setEventType(holder.bitInfo, pBankType);
//
//                             // Set recordId depending on what type this bank is
//                             if (!isUserOrControl) {
//                                 myRecordId = recordId++;
//                             }
//                             // If user event which is to be the first event,
//                             // mark it in the block header's bit info word.
//                             else if (ringItem.isFirstEvent()) {
//                                 EmuUtilities.setFirstEvent(holder.bitInfo);
//                             }
//
//                             // Values needed to initialize object which writes into ET buffer
//                             holder.recordId = myRecordId;
//
//                             // Prepare ET event's data buffer
//                             etBuffer = event.getDataBuffer();
//                             etBuffer.clear();
//                             etBuffer.order(byteOrder);
//
//                             // Do init once per ET event
//                             etEventInitialized = true;
//
//                             // Initialize the writer which writes evio banks into ET buffer
//                             writer.setBuffer(etBuffer, holder.bitInfo, holder.recordId);
//                         }
//
//                         //----------------------------------
//                         // Write evio bank into ET buffer
//                         //----------------------------------
//                         EvioNode node  = ringItem.getNode();
//                         ByteBuffer buf = ringItem.getBuffer();
//
//                         if (buf != null) {
//                             writer.writeEvent(buf);
//                         }
//                         else if (node != null) {
//                             // The last arg is do we need to duplicate node's backing buffer?
//                             // Don't have to do that to keep our own lim/pos because the only
//                             // nodes that make it to an output channel are the USER events.
//                             // Even though it is almost never the case, if 2 USER events share
//                             // the same backing buffer, there still should be no problem.
//                             // Even if 2nd event is being scanned by CompactEventReader, while
//                             // previous event is being written right here, it should still be OK
//                             // since buffer lim or pos are not changed during scanning process.
//                             writer.writeEvent(node, false, false);
//                         }
//
//                         holder.itemCount++;
//
//                         // Added evio event/buf to this ET event
//                         banksInEtBuf++;
//                         bytesToEtBuf += ringItemSize;
//
//                         // If this ring item's data is in a buffer which is part of a
//                         // ByteBufferSupply object, release it back to the supply now.
//                         // Otherwise call does nothing.
//                         ringItem.releaseByteBuffer();
//
//                         // FREE UP this channel's input rings' slots/items for next
//                         // user - the thread which puts ET events back.
//                         releaseCurrentAndGoToNextOutputRingItem(outputRingIndex);
// //System.out.println("      DataChannel Et out (" + name + "): filler, released item on ring " +
// //                           outputRingIndex  + ", go to next");
//
//                         // Handle END & GO events
//                         if (pBankControlType != null) {
// //System.out.println("      DataChannel Et out " + outputIndex + ": have " +  pBankControlType +
// //                   " event, ringIndex = " + outputRingIndex);
//                             if (pBankControlType == ControlType.END) {
//                                 // Finish up the writing
//                                 writer.close();
//
//                                 // Be sure to set length of ET event to bytes of data actually written
//                                 event.setLength((int)writer.getBytesWrittenToBuffer());
//
//                                 // Tell Getter thread to stop getting new ET events
//                                 stopGetterThread = true;
//
//                                 // Mark ring item as END event
//                                 holder.isEnd = true;
//
// //System.out.println("      DataChannel Et out " + outputIndex + ": organizer END seq = " +  (nextFillSequence) +
// //                   ", cursor seq = " + rb.getCursor());
//                                 // Pass END and all unused new events after it to Putter thread.
//                                 // Cursor is the highest published sequence in the ring.
//
//                                 //etFillSequence.set(rb.getCursor());
//                                 etFillSequence.set(etNextFillSequence);
//
//                                 // Do not call shutdown() here since putter
//                                 // thread must still do a putEvents().
//                                 threadState = ThreadState.DONE;
//                                 return;
//                             }
//                             else if (pBankControlType == ControlType.GO) {
//                                 // If the module has multiple build threads, then it's possible
//                                 // that the first buildable event (next one in this case)
//                                 // will NOT come on ring 0. Make sure we're looking for it
//                                 // on the right ring. It was set to the correct value in
//                                 // DataChannelAdapter.prestart().
//                                 outputRingIndex = ringIndex;
//                             }
//                         }
//
// //System.out.println("      DataChannel Et out " + outputIndex + ": go to item " + nextSequences[outputRingIndex] +
// //" on ring " + outputRingIndex);
//
//                         // Do not go to the next ring if we got a control or user event.
//                         // All prestart, go, & users go to the first ring. Just keep reading
//                         // from the same ring until we get to a buildable event. Then start
//                         // keeping count so we know when to switch to the next ring.
//                         if (outputRingCount > 1 && !isUserOrControl) {
//                             outputRingIndex = setNextEventAndRing();
// //System.out.println("      DataChannel Et out (" + name + "): for next ev " + nextEvent +
// //                           " SWITCH TO ring " + outputRingIndex + ", outputRingCount (bt threads) = " +
// //                           outputRingCount);
//                         }
//
//                         // Implement a timeout for low rates.
//                         // Also switch to new ET event for user & control banks
//                         if ((emu.getTime() - startTime > TIMEOUT) || isUserOrControl) {
// //                            if (isUserOrControl) {
// //System.out.println("\n\n      DataChannel Et out, " + name + ": control ev = " + pBankControlType);
// //                            }
// //                                if (emu.getTime() - startTime > timeout) {
// //                                    System.out.println("TIME FLUSH ******************");
// //                                }
//                             continue top;
//                         }
//                     }
//                 }
//             }
//             catch (InterruptedException e) {
//                 // Interrupted while waiting for ring item
// //logger.warn("      DataChannel Et out: " + name + "  interrupted thd, exiting");
//             }
//             catch (Exception e) {
// logger.warn("      DataChannel Et out: exit thd w/ error = " + e.getMessage());
//                 e.printStackTrace();
//                 channelState = CODAState.ERROR;
//                 emu.setErrorState("DataChannel Et out: " + e.getMessage());
//             }
//
//             threadState = ThreadState.DONE;
//         }
//
//
//         /**
//          * This class is a thread designed to put ET events that have been
//          * filled with evio data, back into the ET system.
//          * It runs simultaneously with the thread that fills these events
//          * with evio data and the thread that gets them from the ET system.
//          */
//         private class EvPutter extends Thread {
//
//             private Sequence sequence;
//             private SequenceBarrier barrier;
//
//
//             /**
//              * Constructor.
//              * @param sequence  ring buffer sequence to use
//              * @param barrier   ring buffer barrier to use
//              */
//             EvPutter(Sequence sequence, SequenceBarrier barrier) {
//                 this.barrier = barrier;
//                 this.sequence = sequence;
//             }
//
//
//             /** {@inheritDoc} */
//             public void run() {
//
//                 EtEvent[] events = new EtEvent[ringSize];
//                 EventHolder evHolder, endHolder = null;
//                 EtContainer etContainer = null;
//
//                 int  eventCount, eventsToPut;
//                 long availableSequence = -1L;
//                 long nextSequence = sequence.get() + 1L;
//
//                 try {
//
// //                    // This object is needed to use the new, garbage-free, sync-free ET interface
// //                    etContainer = new EtContainer(etSystem, chunk, (int)getEtEventSize());
//                     // Number of valid events in event arrays inside etContainer
//                     int validEvents;
//
//                     while (true) {
//                         if (gotResetCmd) {
//                             return;
//                         }
//
//                         // Do we wait for next ring slot or do we already have something from last time?
//                         if (availableSequence < nextSequence) {
//                             // Wait for next available ring slot
// //System.out.println("      DataChannel Et out: PUTTER try getting seq " + nextSequence);
//                             availableSequence = barrier.waitFor(nextSequence);
// //System.out.println("      DataChannel Et out: PUTTER got up to seq " + availableSequence);
//                         }
//
//                         //-------------------------------------------------------
//                         // Get all available ET events from ring & put in 1 call
//                         //-------------------------------------------------------
//
//                         // # of events available right now.
//                         // Warning: this may include extra, unused new ET events
//                         // which come after END event. Don't put those into ET sys.
//                         eventsToPut = eventCount = (int) (availableSequence - nextSequence + 1);
// //System.out.println("      DataChannel Et out: PUTTER count = " + eventCount);
//
//                         for (int i=0; i < eventCount; i++) {
//                             evHolder = rb.get(nextSequence++);
//                             events[i] = evHolder.event;
//
//                             // Don't go past the END event
//                             if (evHolder.isEnd) {
//                                 endHolder = evHolder;
//                                 eventsToPut = i+1;
// //System.out.println("      DataChannel Et out: PUTTER end seq = " + (nextSequence - 1L) +
// //                   ", available = " + availableSequence + ", eventCount = " + eventCount + ", eventsToPut = " + eventsToPut);
//                                 break;
//                             }
//                         }
//
//                         // Put events back into ET system
// //                        etContainer.putEvents(attachment, events, 0, eventsToPut);
// //                        etSystem.putEvents(etContainer);
//                         etSystem.putEvents(attachment, events, 0, eventsToPut);
//
//                         // Checks the last event we're putting to see if it's the END event
//                         if (endHolder != null) {
//                             // Empty the ring of additional unused events
//                             // and dump them back into ET sys.
//                             int index=0;
//                             for (long l = nextSequence; l <= rb.getCursor(); l++) {
//                                 events[index++] = rb.get(l).event;
//                             }
//
//                             if (index > 0) {
// //System.out.println("      DataChannel Et out: PUTTER dumping " + index + " ET events");
// //                                etContainer.dumpEvents(attachment, events, 0, index);
// //                                etSystem.dumpEvents(etContainer);
//                                 etSystem.dumpEvents(attachment, events, 0, index);
//                             }
//
// //System.out.println("      DataChannel Et out: " + name + " PUTTER releasing up to seq = " + rb.getCursor());
//                             // Release slots in internal ring
//                             sequence.set(rb.getCursor());
//
//                             // Run callback saying we got & have processed END event
//                             if (endCallback != null) endCallback.endWait();
//
// //System.out.println("      DataChannel Et out: " + name + " got END event, quitting PUTTER thread, ET len = " + endEvent.getLength());
//                             return;
//                         }
//
//                         // Tell ring we're done with these slots
//                         // and getter thread can use them now.
// //System.out.println("      DataChannel Et out: " + name + " PUTTER releasing up to seq = " + availableSequence);
//                         sequence.set(availableSequence);
//                     }
//                 }
//                 catch (AlertException e) {
//                     channelState = CODAState.ERROR;
//                     emu.setErrorState("DataChannel Et out: ring buffer error");
//                 }
//                 catch (InterruptedException e) {
//                     // Quit thread
// //System.out.println("      DataChannel Et out: " + name + " interrupted thread");
//                 }
//                 catch (TimeoutException e) {
//                     // Never happen in our ring buffer
//                     channelState = CODAState.ERROR;
//                     emu.setErrorState("DataChannel Et out: time out in ring buffer");
//                 }
//                 catch (IOException e) {
//                     channelState = CODAState.ERROR;
// System.out.println("      DataChannel Et out: " + name + " network communication error with Et");
//                     emu.setErrorState("DataChannel Et out: network communication error with Et");
//                 }
//                 catch (EtException e) {
//                     channelState = CODAState.ERROR;
// System.out.println("      DataChannel Et out: " + name + " internal error handling Et");
//                     emu.setErrorState("DataChannel Et out: internal error handling Et");
//                 }
//                 catch (EtDeadException e) {
//                     channelState = CODAState.ERROR;
// System.out.println("      DataChannel Et out: " + name + " Et system dead");
//                     emu.setErrorState("DataChannel Et out: Et system dead");
//                 }
//                 catch (EtClosedException e) {
//                     channelState = CODAState.ERROR;
// System.out.println("      DataChannel Et out: " + name + " Et connection closed");
//                     emu.setErrorState("DataChannel Et out: Et connection closed");
//                 }
//                 finally {
//                     logger.info("      DataChannel Et out: PUTTER is Quitting");
//                 }
//             }
//         }
//
//
//
//         /**
//          * This class is a thread designed to get new ET events from the ET system.
//          * It runs simultaneously with the thread that fills these events
//          * with evio data and the thread that puts them back.
//          */
//         private class EvGetter extends Thread {
//
//             /**
//              * {@inheritDoc}<p>
//              * Get the ET events.
//              */
//             public void run() {
//
//                 int evCount=0;
//                 long sequence;
//                 EtEvent[] events=null;
//                 EventHolder holder;
//                 boolean gotError = false;
//                 String errorString = null;
//                 EtContainer etContainer = null;
//
//                 try {
//                     // This object is needed to use the new, garbage-free, sync-free ET interface
//                     etContainer = new EtContainer(etSystem, chunk, (int)getEtEventSize());
//                     // Number of valid events in event arrays inside etContainer
//                     int validEvents;
//
//                     while (true) {
//                         if (stopGetterThread) {
//                             return;
//                         }
// //System.out.println("      DataChannel Et out: GETTER get new events");
//                         // The last arg (true) means allocate new internal mem for new events.
//                         // We need to do that since in this thread we never put or dump
//                         // but pass things on to another thread. Thus we don't want to
//                         // overwrite the new events we just obtained.
// //                        etContainer.newEvents(attachment, Mode.SLEEP, 0, chunk,
// //                                              (int)etSystem.getEventSize(), group, true);
// //                        etSystem.newEvents(etContainer);
// //                        validEvents = etContainer.getEventCount();
// ////System.out.println("      DataChannel Et out: GETTER got " + validEvents + " new events");
// //                        events = etContainer.getEventArray();
//
//                         events = etSystem.newEvents(attachment, Mode.SLEEP, false, 0,
//                                                     chunk, (int)etSystem.getEventSize(), group);
// //System.out.println("      DataChannel Et out: GETTER got " + validEvents + " new events");
//                         evCount = validEvents = events.length;
// //                        evCount = validEvents;
//
//                         // Place ET events, one-by-one, into ring buffer
//                         for  (int i=0; i < validEvents; i++) {
//                             if (stopGetterThread) {
//                                 return;
//                             }
//
//                             // Will block here if no space in ring.
//                             // But it should unblock when ET events
//                             // are put back by the Putter thread.
// //System.out.println("      DataChannel Et out: GETTER try getting event slot in ring");
//                             sequence  = rb.next(); // This just spins on parkNanos
//                             holder = rb.get(sequence);
//                             holder.event = events[i];
//                             holder.isEnd = false;
//                             holder.itemCount = 0;
//                             holder.bitInfo.clear();
//                             rb.publish(sequence);
// //System.out.println("      DataChannel Et out: GETTER inserted event into ring");
//                             evCount--;
//                             if (stopGetterThread) {
//                                 // DO SOMETHING;
//                             }
//                         }
//                     }
//                 }
//                 catch (EtWakeUpException e) {
//                     // Told to wake up because we're ending or resetting.
//                 }
//                 catch (IOException e) {
//                     gotError = true;
//                     errorString = "DataChannel Et out: network communication error with Et";
//                     e.printStackTrace();
//                 }
//                 catch (EtException e) {
//                     gotError = true;
//                     errorString = "DataChannel Et out: internal error handling Et";
//                     e.printStackTrace();
//                 }
//                 catch (EtDeadException e) {
//                     gotError = true;
//                     errorString = "DataChannel Et out: Et system dead";
//                 }
//                 catch (EtClosedException e) {
//                     gotError = true;
//                     errorString = "DataChannel Et out: Et connection closed";
//                 }
//                 catch (Exception e) {
//                     gotError = true;
//                     errorString = "DataChannel Et out: " + e.getMessage();
//                     e.printStackTrace();
//                 }
//                 finally {
//                     // Dump any left over events
//                     if (evCount > 0) {
//                         try {
// //                            if (etContainer != null) {
// //System.out.println("      DataChannel Et out: GETTER will dump " + evCount+ " ET events");
// //                                etContainer.dumpEvents(attachment, events, events.length - evCount, evCount);
// //                                etSystem.dumpEvents(etContainer);
//                                 etSystem.dumpEvents(attachment, events, events.length - evCount, evCount);
// //                            }
//                         }
//                         catch (Exception e1) {
//                             if (!gotError) {
//                                 gotError = true;
//                                 errorString = "DataChannel Et out: " + e1.getMessage();
//                             }
//                         }
//                     }
//                 }
//
//                 // ET system problem - run will come to an end
//                 if (gotError) {
//                     channelState = CODAState.ERROR;
// System.out.println("      DataChannel Et out: " + name + ", " + errorString);
//                     emu.setErrorState(errorString);
//                 }
// //System.out.println("      DataChannel Et out: GETTER is Quitting");
//             }
//         }
//
//     }
//


}
