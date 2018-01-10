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

    /** Use the evio block header's block number as a record id. */
    private int recordId;

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

    //-------------------------------------------
    // ET Stuff
    //-------------------------------------------

    /** Number of events to ask for in an array. */
    private int chunk;

    /** Number of group from which new ET events are taken. */
    private int group;

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
                isFirstEB = (emuClass == CODAClass.PEB || emuClass == CODAClass.DC);
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
            emu.setOutputDestination(transport.getOpenConfig().getEtName());

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
                isFinalEB = (emuClass == CODAClass.PEB || emuClass == CODAClass.SEB);
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
                        station = etSystem.createStation(stationConfig, stationName);
                        etSystem.setStationPosition(station, stationPosition, 0);
                    }
                    catch (EtExistsException e) {
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

logger.info("      DataChannel Et: closeEtSystem(), closed ET connection");
            etSystem.close();
            etSystem = null;
        }
    }


    /** {@inheritDoc} */
    public TransportType getTransportType() {
        return TransportType.ET;
    }


    /** {@inheritDoc} */
    public void prestart() {
        pause = false;

        if (input) {
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
            numEtBufs = numEtBufs < 8 ? 8 : numEtBufs;
            // Make power of 2
            if (Integer.bitCount(numEtBufs) != 1) {
                int newVal = numEtBufs / 2;
                numEtBufs = 1;
                while (newVal > 0) {
                    numEtBufs *= 2;
                    newVal /= 2;
                }
                logger.info("      DataChannel Et: # copy-ET-buffers in input supply -> " + numEtBufs);
            }

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
                    bbSupply = new ByteBufferSupply(numEtBufs, etEventSize, module.getOutputOrder(), false, true);
                }
                else {
                    // If ER has more than one output, buffers may not be released sequentially
                    bbSupply = new ByteBufferSupply(numEtBufs, etEventSize, module.getOutputOrder(), false);
                }
            }
            else {
                // EBs all release these ByteBuffers in order in the ReleaseRingResourceThread thread
                bbSupply = new ByteBufferSupply(numEtBufs, etEventSize, module.getOutputOrder(), false, true);
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

    /** {@inheritDoc}. Formerly this code was the close() method. */
    public void end() {
logger.info("      DataChannel Et: " + name + " - end threads & close ET system");

        gotEndCmd = true;
        gotResetCmd = false;
        stopGetterThread = true;

        // Do NOT interrupt threads which are communicating with the ET server.
        // This will mess up future communications !!!

        // How long do we wait for each input or output thread
        // to end before we just terminate them?
        // The total time for an emu to wait for the END transition
        // is emu.endingTimeLimit. Dividing that by the number of
        // in/output threads is probably a good guess.
        int waitTime;

        // Don't close ET system until helper threads are done
        try {
            waitTime = (int) emu.getEndingTimeLimit();
//System.out.println("      DataChannel Et: waiting for input thread to end ...");
            if (dataInputThread != null) {
                dataInputThread.join(waitTime);
                // kill it if not already dead since we waited as long as possible
                dataInputThread.interrupt();
                try {
                    dataInputThread.join(250);
                    if (dataInputThread.isAlive()) {
                        // kill it since we waited as long as possible
                        dataInputThread.stop();
                    }
                }
                catch (InterruptedException e) {}
            }

            if (dataOutputThread != null) {
                waitTime = (int) emu.getEndingTimeLimit();
                dataOutputThread.shutdown();
//System.out.println("      DataChannel Et: try joining output thread for " + (waitTime/1000) + " sec");
                if (!dataOutputThread.waitForThreadsToEnd(waitTime)) {
                    // Kill everything since we waited as long as possible
//System.out.println("      DataChannel Et: end(), kill all output threads");
                    dataOutputThread.killFromOutside();
                }
//System.out.println("      DataChannel Et: output thread done");
            }
//System.out.println("      DataChannel Et: all helper thds done");
        }
        catch (InterruptedException e) {
        }

        // At this point all threads should be done
        try {
            closeEtSystem();
        }
        catch (DataTransportException e) {
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

        if (dataInputThread != null) {
            dataInputThread.interrupt();
            try {
                dataInputThread.join(250);
                dataInputThread.stop();
            }
            catch (InterruptedException e) {}
        }

        if (dataOutputThread != null) {
            dataOutputThread.shutdown();
            dataOutputThread.waitForThreadsToEnd(250);
            // Kill everything since we waited as long as possible
            dataOutputThread.killFromOutside();
        }

        // At this point all threads should be done
        try {
            closeEtSystem();
        }
        catch (DataTransportException e) {
        }

        errorMsg.set(null);
        channelState = CODAState.CONFIGURED;
//logger.debug("      DataChannel Et: reset " + name + " - all done");
    }


    /**
     * If this is an output channel, it may be blocked on reading from a module
     * because the END event arrived on an unexpected ring
     * (possible if module has more than one event-producing thread
     * AND there is more than one output channel),
     * this method interrupts and allows this channel to read the
     * END event from the proper ring.
     *
     * @param eventIndex index of last buildable event before END event.
     * @param ringIndex  ring to read END event on.
     */
    public void processEnd(long eventIndex, int ringIndex) {

        eventIndexEnd = eventIndex;
        ringIndexEnd  = ringIndex;

        if (input || !dataOutputThread.isAlive()) {
            //thread already done
            return;
        }

        // Don't wait more than 1/2 second
        int loopCount = 20;
        while (dataOutputThread.threadState != ThreadState.DONE && (loopCount-- > 0)) {
            try {
                Thread.sleep(25);
            }
            catch (InterruptedException e) { break; }
        }

        if (dataOutputThread.threadState == ThreadState.DONE) {
            return;
        }

        // Probably stuck trying to get item from ring buffer,
        // so interrupt it and get it to read the END event from
        // the correct ring.
        dataOutputThread.interrupt();
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



    /**
     * Class used to get ET events, parse them into Evio banks,
     * and put them onto a single ring buffer.
     */
    private class DataInputHelper extends Thread {

        /** Array of ET events to be gotten from ET system. */
        private EtEvent[] events;

        /** Array of ET events to be gotten from ET system. */
        private EtEventImpl[] eventsDirect;

        /** Variable to print messages when paused. */
        private int pauseCounter = 0;

        /** Let a single waiter know that the main thread has been started. */
        private final CountDownLatch latch = new CountDownLatch(1);

        /** The minimum amount of milliseconds between updates to the M value. */
        private static final long timeBetweenMupdates = 500;


        /** Constructor. */
        DataInputHelper (ThreadGroup group, String name) {
            super(group, name);
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {}
        }


        /** {@inheritDoc} */
        @Override
        public void run() {

            // Tell the world I've started
            latch.countDown();
            String errorString = null;

            try {
                int sourceId, recordId;
                BlockHeaderV4 header4;
                EventType eventType;
                ControlType controlType;
                ByteBufferItem bbItem;
                ByteBuffer buf;
                EvioCompactReaderUnsync compactReader = null;
                RingItem ri;
                long t1, t2;
                boolean delay = false;
                boolean useDirectEt = (etSysLocal != null);
                boolean etAlive = true;
                boolean hasFirstEvent, isUser=false;

                if (!useDirectEt) {
                    etAlive = etSystem.alive();
                }

                t1 = System.currentTimeMillis();

                while ( etAlive ) {

                    if (delay) {
                        Thread.sleep(5);
                        delay = false;
                    }

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
logger.warn("      DataChannel Et in: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // Get events while checking periodically to see if we must go away.
                    // Do some work to get accurate error msgs back to run control.
                    try {
//System.out.print("      DataChannel Et in: " + name + " getEvents() ...");
                        if (useDirectEt) {
                            events = eventsDirect = etSysLocal.getEvents(attachmentLocal,
                                                                         Mode.TIMED.getValue(),
                                                                         etWaitTime, chunk);
                        }
                        else {
                            events = etSystem.getEvents(attachment, Mode.TIMED,
                                                        Modify.NOTHING, etWaitTime, chunk);
                        }
                    }
                    catch (IOException e) {
                        errorString = "DataChannel Et in: network communication error with Et";
                        throw e;
                    }
                    catch (EtException e) {
                        errorString = "DataChannel Et in: internal error handling Et";
                        throw e;
                    }
                    catch (EtDeadException e) {
                        errorString = "DataChannel Et in:  Et system dead";
                        throw e;
                    }
                    catch (EtClosedException e) {
                        errorString = "DataChannel Et in:  Et connection closed";
                        throw e;
                    }
                    catch (EtWakeUpException e) {
                        // Told to wake up because we're ending or resetting
                        if (haveInputEndEvent) {
                            logger.info("      DataChannel Et in: wake up " + name + ", other thd found END, quit");
                        }
                        else if (gotResetCmd) {
                            logger.info("      DataChannel Et in: " + name + " got RESET cmd, quitting");
                        }
                        return;
                    }
                    catch (EtTimeoutException e) {
                        if (haveInputEndEvent) {
                            logger.info("      DataChannel Et in: timeout " + name + ", other thd found END, quit");
                            return;
                        }
                        else if (gotResetCmd) {
                            logger.info("      DataChannel Et in: " + name + " got RESET cmd, quitting");
                            return;
                        }

                        // Want to delay before calling getEvents again
                        // but don't do it here in a synchronized block.
                        delay = true;

                        continue;
                    }

                    for (EtEvent ev : events) {

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
                        bbItem.ensureCapacity(ev.getLength());
                        buf = bbItem.getBuffer();
                        copyBuffer(ev.getDataBuffer(), buf, ev.getLength());

                        try {
                            // These calls do not change buf position
                            if (compactReader == null) {
                                compactReader = new EvioCompactReaderUnsync(buf);
                            }
                            else {
                                compactReader.setBuffer(buf);
                            }
                        }
                        catch (EvioException e) {
Utilities.printBuffer(buf, 0, 21, "BAD EVENT ");
                            e.printStackTrace();
                            errorString = "DataChannel Et in: ET data NOT evio v4 format";
                            throw e;
                        }

                        // First block header in ET buffer
                        header4 = compactReader.getFirstBlockHeader();
//System.out.println("      DataChannel Et in: blk header, order = " + header4.getByteOrder());
                        if (header4.getVersion() < 4) {
                            errorString = "DataChannel Et in: ET data NOT evio v4 format";
                            throw new EvioException("Evio data needs to be written in version 4+ format");
                        }

                        hasFirstEvent = header4.hasFirstEvent();
                        eventType     = EventType.getEventType(header4.getEventType());
                        controlType   = null;
                        // this only works from ROC !!!
                        sourceId      = header4.getReserved1();
                        if (eventType == EventType.PARTIAL_PHYSICS) {
                            sourceId = ev.getControl()[0];
                        }
                        // Record id is set in the DataOutputHelper so that it is incremented
                        // once per non-user, non-control ET buffer. Each writer will only use
                        // 1 block per 2.2MB or 10K events. Thus we can get away with only
                        // looking at the very first block #.
                        recordId = header4.getNumber();

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
                                node = compactReader.getScannedEvent(i);
                            }

                            // Complication: from the ROC, we'll be receiving USER events
                            // mixed in with and labeled as ROC Raw events. Check for that
                            // and fix it.
                            if (eventType == EventType.ROC_RAW) {
                                if (Evio.isUserEvent(node)) {
logger.info("      DataChannel Et in: " + name + " got USER event from ROC");
                                    eventType = EventType.USER;
                                    isUser = true;
                                }
                            }
                            else if (eventType == EventType.CONTROL) {
                                // Find out exactly what type of control event it is
                                // (May be null if there is an error).
                                // It may NOT be enough just to check the tag
                                controlType = ControlType.getControlType(node.getTag());
logger.info("      DataChannel Et in: " + name + " got CONTROL event, " + controlType);
                                if (controlType == null) {
                                    errorString = "DataChannel Et in:  found unidentified control event, tag = 0x" + Integer.toHexString(node.getTag());
                                    throw new EvioException("Found unidentified control event, tag = 0x" + Integer.toHexString(node.getTag()));
                                }
                            }
                            else if (eventType == EventType.USER) {
                                isUser = true;
//                                if (hasFirstEvent) {
//logger.info("      DataChannel Et in: " + name + " got FIRST event");
//                                }
//                                else {
//logger.info("      DataChannel Et in: " + name + " got USER event");
//                                }
                            }

                            // Don't need to set controlType = null for each loop since each
                            // ET event contains only one event type (CONTROL, PHYSICS, etc).

//System.out.println("      DataChannel Et in: wait for next ring buf for writing");
                            nextRingItem = ringBufferIn.next();
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
                                // run callback saying we got end event
                                if (endCallback != null) endCallback.endWait();
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
                            etSysLocal.putEvents(attachmentLocal, eventsDirect);
                        }
                        else {
                            etSystem.putEvents(attachment, events);
                            //etSystem.dumpEvents(attachment, events);
                        }
                    }
                    catch (IOException e) {
                        errorString = "DataChannel Et in: network communication error with Et";
                        throw e;
                    }
                    catch (EtException e) {
                        errorString = "DataChannel Et in: internal error handling Et";
                        throw e;
                    }
                    catch (EtDeadException e) {
                        errorString = "DataChannel Et in: Et system dead";
                        throw e;
                    }
                    catch (EtClosedException e) {
                        errorString = "DataChannel Et in: Et connection closed";
                        throw e;
                    }

                    if (haveInputEndEvent) {
logger.info("      DataChannel Et in: have END, " + name + " quit thd");
                        return;
                    }

                    if (!useDirectEt) {
                        etAlive = etSystem.alive();
                    }
                }

            } catch (InterruptedException e) {
logger.warn("      DataChannel Et in: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
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
     * The getter thread gets new ET buffers and puts each into a container of the internal RB.
     *
     * The main, DataOutputHelper, thread gets a single container from the internal RB.
     * It also takes evio events from the other RBs which contain module output. Each
     * evio event is immediately written into the ET buffer. Once the ET buffer is full or
     * some other limiting condition is met, it places the container back into the internal RB.
     *
     * Finally, the putter thread takes containers and their data-filled ET buffers and puts
     * them back into the ET system.
     */
    private class DataOutputHelper extends Thread {

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Thread for getting new ET events. */
        private EvGetter getter;

        /** Thread for putting filled ET events back into ET system. */
        private EvPutter putter;

        /** Let a single waiter know that the main thread has been started. */
        private final CountDownLatch startLatch = new CountDownLatch(1);

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

            // Closest power of 2 to chunk, rounded up.
            // Making it 2xchunk slows it down quite a bit.
            // Making it chunk or chunk/2 is roughly the same on average
            // although, chunk/2 has bigger top speed.
            ringSize = EmuUtilities.powerOfTwo(chunk, true);

            // Create ring buffer used by 3 threads -
            //   1 to get new events from ET system and place into ring (producer of ring items)
            //   1 to get evio events and write them into ET event  (1st consumer of ring items)
            //   1 to put events back into ET system (2nd consumer of ring items)
            rb = createSingleProducer(new ContainerFactory(), ringSize,
                                      new YieldingWaitStrategy());

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
            putter = new EvPutter(etPutSequence, etPutBarrier);
            putter.start();

            // Start producer thread for getting new ET events
            getter = new EvGetter();
            getter.start();
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {}
        }


        /** Kill all this object's threads from an external thread. */
        private void killFromOutside() {
            // Kill all threads
            getter.stop();
            putter.stop();
            this.stop();
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


        /** Object that holds an EtEvent and metadata in internal ring buffer. */
        final private class EtContainer {
            /** Place to hold ET event. */
            private EtEvent event;

            /** Is this the END event? */
            private boolean isEnd;

            /** Number of evio events in ET event. */
            private int itemCount;

            /** Bit info to set in EventWriter. */
            private BitSet bitInfo = new BitSet(24);

            /** Id to set in EventWriter. */
            private int recordId;
        }


        /**
         * Class used by the this output channel's internal RingBuffer
         * to populate itself with containers of ET buffers.
         */
        final private class ContainerFactory implements EventFactory<EtContainer> {
            final public EtContainer newInstance() {
                // This object holds an EtEvent
                return new EtContainer();
            }
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
                EtContainer container;

                EtEvent event = null;
                EventType pBankType = null;
                ControlType pBankControlType = null;

                // Time in milliseconds for writing if time expired
                long startTime;
                final long TIMEOUT = 2000L;

                // Always start out reading prestart & go events from ring 0
                int outputRingIndex = 0;

                // Create writer with some args that get overwritten later.
                // Make the block size bigger than the Roc's 2MB ET buffer
                // size so no additional block headers must be written.
                // It should contain less than 100 ROC Raw records,
                // but we'll allow 10000 such banks per block header.
                ByteBuffer etBuffer = ByteBuffer.allocate(128);
                etBuffer.order(byteOrder);
                EventWriterUnsync writer = new EventWriterUnsync(etBuffer, 550000, maxEvioItemsPerEtBuf,
                                                                 null, null, emu.getCodaid(), 0);
                writer.close();

                int bytesToEtBuf, ringItemSize=0, banksInEtBuf, myRecordId;
                int etSize = (int) etSystem.getEventSize();
                boolean etEventInitialized, isUserOrControl=false;
                boolean gotPrestart=false;

                // Variables for consuming ring buffer items
                long etNextFillSequence = etFillSequence.get() + 1L;
                long etAvailableFillSequence = -1L;


                top:
                while (true) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) Thread.sleep(5);
                        continue;
                    }

                    // Init variables
                    myRecordId = 1;
                    bytesToEtBuf = 0;
                    banksInEtBuf = 0;
                    etEventInitialized = false;

                    // Set time we started dealing with this ET event
                    startTime = emu.getTime();

                    // Very first time through, event is null and we skip this
                    if (event != null) {
                        // Finish up the writing
                        writer.close();

                        // Be sure to set length of ET event to bytes of data actually written
                        event.setLength((int)writer.getBytesWrittenToBuffer());

                        //----------------------------------------
                        // Release ET event for writings threads
                        //----------------------------------------
//System.out.println("      DataChannel Et out filler : release ET event " + etNextFillSequence);
                        etFillSequence.set(etNextFillSequence++);
                    }

//System.out.println("      DataChannel Et out filler: " + name + ", try getting next seq (" + etNextFillSequence + ") for ET");
                    // Do we wait for next ring slot or do we already have something from last time?
                    if (etAvailableFillSequence < etNextFillSequence) {
                        // Wait for next available ring slot
                        etAvailableFillSequence = etFillBarrier.waitFor(etNextFillSequence);
                    }
//System.out.println("      DataChannel Et out filler: available Seq = " + etAvailableFillSequence);

                    //---------------------------------------------------------
                    // Get the next container object from internal ring's slot.
                    // This will contain the new ET event to place data into.
                    //---------------------------------------------------------
                    container = rb.get(etNextFillSequence);
                    event = container.event;
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
                                ringItem = getNextOutputRingItem(outputRingIndex);
//                                if (isEB) sleep(1);
//System.out.println("     got evt from ring");
//System.out.println(outputIndex + " : " + outputRingIndex + " : " + nextEvent);
                            }
                            catch (InterruptedException e) {
                                threadState = ThreadState.INTERRUPTED;
                                // If we're here we were blocked trying to read the next
                                // (END) event from the wrong ring. We've had 1/4 second
                                // to read everything else so let's try reading END from
                                // given ring.
//System.out.println("\n      DataChannel Et out " + outputIndex + ": try again, read END from ringIndex " + ringIndexEnd +
//                       " not " + outputRingIndex);
                                ringItem = getNextOutputRingItem(ringIndexEnd);
                            }
//System.out.println("done");
                            pBankType = ringItem.getEventType();
                            pBankControlType = ringItem.getControlType();
                            isUserOrControl = pBankType.isUserOrControl();

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
                                else if (!isUserOrControl) {
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
//", container item count = " + container.itemCount);

                        // If this ring item will not fit into current ET buffer,
                        // either because there is no more room or there's a limit on
                        // the # of evio events in a single ET buffer ...
                        if ((bytesToEtBuf + ringItemSize > etSize) ||
                                (container.itemCount >= maxEvioItemsPerEtBuf)) {
                            // If nothing written into ET buf yet ...
                            if (banksInEtBuf < 1) {
                                // Get rid of this ET buf which is too small
                                etSystem.dumpEvents(attachment, new EtEvent[]{event});

                                // Get 1 bigger & better ET buf as a replacement
logger.rcConsole("      DataChannel Et out: " + name + " using over-sized (temp) ET event", "DANGER");

                                EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, false,
                                                                    0, 1, ringItemSize, group);
                                event = evts[0];

                                // Put the new ET buf into ring slot's container
                                container.event = event;
                            }
                            // If data was previously written into this ET buf ...
                            else {
//System.out.println("      DataChannel Et out: " + name + " item doesn't fit cause other stuff in there, do write close, get another ET event");
//System.out.println("      DataChannel Et out: " + name + " banks in ET buf = " + banksInEtBuf + ", isUserOrControl = " + isUserOrControl);
                                // Get another ET event to put this evio data into
                                // and hope there is enough room for it.
                                //
                                // On the next time through this while loop, do not
                                // grab another ring item since we already have this
                                // one we're in the middle of dealing with.
                                unusedRingItem = ringItem;

                                // Grab a new ET event and hope it fits in there
                                continue top;
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
                                continue top;
                            }

                            // store info on END event
                            if (pBankControlType == ControlType.END) {
                                container.isEnd = true;
                            }
                        }


                        //-------------------------------------------------------
                        // Do the following once per container/ET-event
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
                            //container.bitInfo.clear();
                            EmuUtilities.setEventType(container.bitInfo, pBankType);

                            // Set recordId depending on what type this bank is
                            if (!isUserOrControl) {
                                myRecordId = recordId++;
                            }
                            // If user event which is to be the first event,
                            // mark it in the block header's bit info word.
                            else if (ringItem.isFirstEvent()) {
                                EmuUtilities.setFirstEvent(container.bitInfo);
                            }

                            // Values needed to initialize object which writes into ET buffer
                            container.recordId = myRecordId;

                            // Prepare ET event's data buffer
                            etBuffer = event.getDataBuffer();
                            etBuffer.clear();
                            etBuffer.order(byteOrder);

                            // Do init once per ET event
                            etEventInitialized = true;

                            // Initialize the writer which writes evio banks into ET buffer
                            writer.setBuffer(etBuffer, container.bitInfo, container.recordId);
                        }

                        //----------------------------------
                        // Write evio bank into ET buffer
                        //----------------------------------
                        EvioNode node  = ringItem.getNode();
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

                        container.itemCount++;

                        // Added evio event/buf to this ET event
                        banksInEtBuf++;
                        bytesToEtBuf += ringItemSize;

                        // If this ring item's data is in a buffer which is part of a
                        // ByteBufferSupply object, release it back to the supply now.
                        // Otherwise call does nothing.
                       ringItem.releaseByteBuffer();

                        // FREE UP this channel's input rings' slots/items for next
                        // user - the thread which puts ET events back.
                        releaseCurrentAndGoToNextOutputRingItem(outputRingIndex);

                        // Handle END & GO events
                        if (pBankControlType != null) {
//System.out.println("      DataChannel Et out " + outputIndex + ": have " +  pBankControlType +
//                   " event, ringIndex = " + outputRingIndex);
                            if (pBankControlType == ControlType.END) {
                                // Finish up the writing
                                writer.close();

                                // Be sure to set length of ET event to bytes of data actually written
                                event.setLength((int)writer.getBytesWrittenToBuffer());

                                // Tell Getter thread to stop getting new ET events
                                stopGetterThread = true;

                                // Mark ring item as END event
                                container.isEnd = true;

//System.out.println("      DataChannel Et out " + outputIndex + ": organizer END seq = " +  (nextFillSequence) +
//                   ", cursor seq = " + rb.getCursor());
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

//System.out.println("      DataChannel Et out " + outputIndex + ": go to item " + nextSequences[outputRingIndex] +
//" on ring " + outputRingIndex);

                        // Do not go to the next ring if we got a control or user event.
                        // All prestart, go, & users go to the first ring. Just keep reading
                        // from the same ring until we get to a buildable event. Then start
                        // keeping count so we know when to switch to the next ring.
                        if (outputRingCount > 1 && !isUserOrControl) {
                            outputRingIndex = setNextEventAndRing();
//System.out.println("      DataChannel Et out, " + name + ": for next ev " + nextEvent +
//                           " SWITCH TO ring " + outputRingIndex);
                        }

                        // Implement a timeout for low rates.
                        // Also switch to new ET event for user & control banks
                        if ((emu.getTime() - startTime > TIMEOUT) || isUserOrControl) {
//                            if (isUserOrControl) {
//System.out.println("\n\n      DataChannel Et out, " + name + ": control ev = " + pBankControlType);
//                            }
//                                if (emu.getTime() - startTime > timeout) {
//                                    System.out.println("TIME FLUSH ******************");
//                                }
                            continue top;
                        }
                    }
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
             * @param sequence  ring buffer sequence to use
             * @param barrier   ring buffer barrier to use
             */
            EvPutter(Sequence sequence, SequenceBarrier barrier) {
                this.barrier = barrier;
                this.sequence = sequence;
            }


            /** {@inheritDoc} */
            public void run() {

                EtEvent[] events = new EtEvent[ringSize];
                EtContainer container, endContainer = null;

                int  eventCount, eventsToPut;
                long availableSequence = -1L;
                long nextSequence = sequence.get() + 1L;

                try {

                    while (true) {
                        if (gotResetCmd) {
                            return;
                        }

                        // Do we wait for next ring slot or do we already have something from last time?
//System.out.println("      DataChannel Et out: PUTTER try getting " + nextSequence);
                        if (availableSequence < nextSequence) {
                            // Wait for next available ring slot
                            availableSequence = barrier.waitFor(nextSequence);
                        }

                        //-------------------------------------------------------
                        // Get all available ET events from ring & put in 1 call
                        //-------------------------------------------------------

                        // # of events available right now.
                        // Warning: this may include extra, unused new ET events
                        // which come after END event. Don't put those into ET sys.
                        eventsToPut = eventCount = (int) (availableSequence - nextSequence + 1);
//System.out.println("      DataChannel Et out: PUTTER count = " + eventCount);

                        for (int i=0; i < eventCount; i++) {
                            container = rb.get(nextSequence++);
                            events[i] = container.event;

                            // Don't go past the END event
                            if (container.isEnd) {
                                endContainer = container;
                                eventsToPut = i+1;
//System.out.println("      DataChannel Et out: PUTTER end seq = " + (nextSequence - 1L) +
//                   ", available = " + availableSequence + ", eventCount = " + eventCount + ", eventsToPut = " + eventsToPut);
                                break;
                            }
                        }

                        // Put events back into ET system
                        etSystem.putEvents(attachment, events, 0, eventsToPut);

                        // Checks the last event we're putting to see if it's the END event
                        if (endContainer != null) {
                            // Empty the ring of additional unused events
                            // and dump them back into ET sys.
                            int index=0;
                            for (long l = nextSequence; l <= rb.getCursor(); l++) {
                                events[index++] = rb.get(l).event;
                            }

                            if (index > 0) {
//System.out.println("      DataChannel Et out: PUTTER dumping " + index + " ET events");
                                etSystem.dumpEvents(attachment, events, 0, index);
                            }

//System.out.println("      DataChannel Et out: " + name + " PUTTER releasing up to seq = " + rb.getCursor());
                            // Release slots in internal ring
                            sequence.set(rb.getCursor());

                            // Run callback saying we got & have processed END event
                            if (endCallback != null) endCallback.endWait();

//System.out.println("      DataChannel Et out: " + name + " got END event, quitting PUTTER thread, ET len = " + endEvent.getLength());
                            return;
                        }

                        // Tell ring we're done with these slots
                        // and getter thread can use them now.
//System.out.println("      DataChannel Et out: " + name + " PUTTER releasing up to seq = " + availableSequence);
                        sequence.set(availableSequence);
                    }
                }
                catch (AlertException e) {
                    channelState = CODAState.ERROR;
                    emu.setErrorState("DataChannel Et out: ring buffer error");
                }
                catch (InterruptedException e) {
                    // Quit thread
//System.out.println("      DataChannel Et out: " + name + " interrupted thread");
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
                }
                catch (EtException e) {
                    channelState = CODAState.ERROR;
System.out.println("      DataChannel Et out: " + name + " internal error handling Et");
                    emu.setErrorState("DataChannel Et out: internal error handling Et");
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
         * This class is a thread designed to get new ET events from the ET system.
         * It runs simultaneously with the thread that fills these events
         * with evio data and the thread that puts them back.
         */
        private class EvGetter extends Thread {

            /**
             * {@inheritDoc}<p>
             * Get the ET events.
             */
            public void run() {

                int evCount=0;
                long sequence;
                EtEvent[] events=null;
                EtContainer container;
                boolean gotError = false;
                String errorString = null;

                try {
                    while (true) {
                        if (stopGetterThread) {
                            return;
                        }
//System.out.println("      DataChannel Et out: GETTER get new events of size " + etSystem.getEventSize() );

                        events = etSystem.newEvents(attachment, Mode.SLEEP, false, 0,
                                                    chunk, (int)etSystem.getEventSize(), group);
//System.out.println("      DataChannel Et out: GETTER got " + events.length + " new events");
                        evCount = events.length;

                        // Place ET events, one-by-one, into ring buffer
                        for (EtEvent event : events) {
                            if (stopGetterThread) {
                                return;
                            }

                            // Will block here if no space in ring.
                            // But it should unblock when ET events
                            // are put back by the Putter thread.
//System.out.println("      DataChannel Et out: GETTER try getting slot in ring");
                            sequence  = rb.next(); // This just spins on parkNanos
                            container = rb.get(sequence);
                            container.event = event;
                            container.isEnd = false;
                            container.itemCount = 0;
                            container.bitInfo.clear();
                            rb.publish(sequence);
                            evCount--;
                            if (stopGetterThread) {
                                // DO SOMETHING;
                            }
                        }
                    }
                }
                catch (EtWakeUpException e) {
                    // Told to wake up because we're ending or resetting.
                }
                catch (IOException e) {
                    gotError = true;
                    errorString = "DataChannel Et out: network communication error with Et";
                }
                catch (EtException e) {
                    gotError = true;
                    errorString = "DataChannel Et out: internal error handling Et";
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
                finally {
                    // Dump any left over events
                    if (evCount > 0) {
                        try {
//System.out.println("      DataChannel Et out: GETTER will dump " + evCount+ " ET events");
                            etSystem.dumpEvents(attachment, events, events.length - evCount, evCount);
                        }
                        catch (Exception e1) {
                            if (!gotError) {
                                gotError = true;
                                errorString = "DataChannel Et out: " + e1.getMessage();
                            }
                        }
                    }
                }

                // ET system problem - run will come to an end
                if (gotError) {
                    channelState = CODAState.ERROR;
System.out.println("      DataChannel Et out: " + name + ", " + errorString);
                    emu.setErrorState(errorString);
                }
//System.out.println("      DataChannel Et out: GETTER is Quitting");
            }
        }

    }


}
