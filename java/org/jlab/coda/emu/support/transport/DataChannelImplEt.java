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


import org.jlab.coda.emu.Emu;
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


/**
 * This class implement a data channel which gets data from
 * or sends data to an ET system.
 *
 * @author timmer
 * (Dec 2, 2009)
 */
public class DataChannelImplEt extends DataChannelAdapter {

    /** Data transport subclass object for Et. */
    private DataTransportImplEt dataTransportImplEt;

    /** Use the evio block header's block number as a record id. */
    private int recordId;

    /** Do we pause the dataThread? */
    private volatile boolean pause;

    /** Read END event from input queue. */
    private volatile boolean haveInputEndEvent;

    /** Read END event from output queue. */
    private volatile boolean haveOutputEndEvent;

    /** Got END command from Run Control. */
    private volatile boolean gotEndCmd;

    /** Got RESET command from Run Control. */
    private volatile boolean gotResetCmd;

    // OUTPUT

    /** Number of writing threads to ask for when copying data from banks to ET events. */
    private int writeThreadCount;

    /** Thread used to output data. */
    private DataOutputHelper dataOutputThread;

    /** Place to store a bank off the queue for the next event out. */
    private RingItem firstBankFromQueue;

    /** Is the EMU using this ET output channel as a simulated ROC ? */
    private boolean isROC;

    /** Is the EMU using this ET output channel as an event builder? */
    private boolean isEB;

    /** Is the EMU using this ET output channel as the last level event builder? */
    private boolean isFinalEB;

    /** Is the EMU using this ET output channel as the first level event builder? */
    private boolean isFirstEB;

    // INPUT

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
    private SystemCreate etSysLocal = null;

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
    private int etWaitTime = 500000;


    //-------------------------------------------
    // Disruptor (RingBuffer)  Stuff
    //-------------------------------------------
    private long nextRingItem;       // input

    /** Ring buffer holding ByteBuffers when using EvioCompactEvent reader for incoming events. */
    protected ByteBufferSupply bbSupply;   // input

    private int rbIndex;



    /**
     * This method copies a ByteBuffer as efficiently as possible.
     * @param srcBuf  buffer to copy.
     * @param destBuf buffer to copy into.
     * @param len     number of bytes to copy
     * @return destination buffer.
     */
    private static final ByteBuffer copyBuffer(ByteBuffer srcBuf, ByteBuffer destBuf, int len) {
        int origPos = srcBuf.position();
        int origLim = srcBuf.limit();

        if (srcBuf.hasArray() && destBuf.hasArray()) {
            System.arraycopy(srcBuf.array(), 0, destBuf.array(), 0, len);
        }
        else {
            srcBuf.limit(len).position(0);
            destBuf.clear();
            destBuf.put(srcBuf);
            srcBuf.limit(origLim).position(origPos);
        }
        destBuf.order(srcBuf.order());

        return (ByteBuffer)destBuf.limit(len).position(0);
    }



    /**
     * Constructor to create a new DataChannelImplEt instance. Used only by
     * {@link DataTransportImplEt#createChannel(String, Map, boolean, Emu, EmuModule)}
     * which is only used during PRESTART in {@link Emu}.
     *
     * @param name          the name of this channel
     * @param transport     the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     * @param emu           emu this channel belongs to
     * @param module        module this channel belongs to
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplEt(String name, DataTransportImplEt transport,
                         Map<String, String> attributeMap, boolean input, Emu emu,
                         EmuModule module)
        throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module);

        dataTransportImplEt = transport;

        if (input) {
logger.info("      DataChannel Et : creating input channel " + name);
        }
        else {
logger.info("      DataChannel Et : creating output channel " + name);
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

        // set TCP_NODELAY option on
        boolean noDelay = false;
        attribString = attributeMap.get("noDelay");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("true") ||
                attribString.equalsIgnoreCase("on")   ||
                attribString.equalsIgnoreCase("yes"))   {
                noDelay = true;
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

            etSystem = new EtSystem(openConfig);
        }
        catch (EtException e) {
            throw new DataTransportException("", e);
        }

        // How may Et buffer filling threads for each data output thread?
        writeThreadCount = 2;
        attribString = attributeMap.get("wthreads");
        if (attribString != null) {
            try {
                writeThreadCount = Integer.parseInt(attribString);
                if (writeThreadCount <  1) writeThreadCount = 1;
                if (writeThreadCount > 10) writeThreadCount = 10;
            }
            catch (NumberFormatException e) {}
        }
//logger.info("      DataChannel Et : write threads = " + writeThreadCount);

        // How may buffers do we grab at a time?
        chunk = 10;
        attribString = attributeMap.get("chunk");
        if (attribString != null) {
            try {
                chunk = Integer.parseInt(attribString);
                if (chunk < 1) chunk = 1;
            }
            catch (NumberFormatException e) {}
        }
logger.info("      DataChannel Et : chunk = " + chunk);

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
//logger.info("      DataChannel Et : group = " + group);

        // Set station name. Use any defined in config file else use
        // "station"+id for input and "GRAND_CENTRAL" for output.
        stationName = attributeMap.get("stationName");
//logger.info("      DataChannel Et : station name = " + stationName);


        // Set station position. Use any defined in config file else use default (1)
        attribString = attributeMap.get("position");
        if (attribString != null) {
            try {
                stationPosition = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {  }
        }
//logger.info("      DataChannel Et : position = " + stationPosition);

        // if INPUT channel
        if (input) {

            try {
                // Rocs need feedback of minimum evio-events / et-buffer from
                // the DCs and PEBs.
                CODAClass emuClass = emu.getCodaClass();
                isFirstEB = (emuClass == CODAClass.PEB || emuClass == CODAClass.DC);

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
                    stationName = "station"+id;
                }


                // RingBuffer supply of buffers to hold all ET event bytes
                if (ringItemType == ModuleIoType.PayloadBuffer) {
                    // ET system parameters
                    int etEventSize = transport.getSystemConfig().getEventSize();

                    // Create reusable supply of ByteBuffer objects.
                    // Put a limit on the amount of memory (40MB). That may be
                    // the easiest way to figure out how many buffers to use.
                    // Number of bufs must be a power of 2.
                    int numEtBufs = 40000000 / etEventSize;
                    numEtBufs = numEtBufs < 8 ? 8 : numEtBufs;
                    // Make power of 2
                    if (Integer.bitCount(numEtBufs) != 1) {
                        int newVal = numEtBufs/2;
                        numEtBufs = 1;
                        while (newVal > 0) {
                            numEtBufs *= 2;
                            newVal /= 2;
                        }
//System.out.println("Figure out # of buffers in supply -> " + numEtBufs);
                    }
                    bbSupply = new ByteBufferSupply(numEtBufs, etEventSize);
                }
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
System.out.println("\nSetting control[0] = " + id + "\n");
                // Is this the last level event builder (not a DC)?
                // In this case, we want the first control word to indicate
                // what type of event is being sent.
                //
                // Control events will be received and dealt with by the FCS
                // (Farm Control Supervisor).
                isFinalEB = (emuClass == CODAClass.PEB || emuClass == CODAClass.SEB);
                // The value of control[0] will be set in the DataOutputHelper
            }
        }

        // Connect to ET system
        openEtSystem();
        // Start up threads to help with I/O
        startHelper();

        // State after prestart transition -
        // during which this constructor is called
        state = CODAState.PAUSED;
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
                e.printStackTrace();
                throw new DataTransportException("cannot create/attach to station " + stationName, e);
            }
        }
        // Otherwise open in usual manner, use sockets
        else {
            try {
                System.out.println("      DataChannel Et: try to open" + dataTransportImplEt.getOpenConfig().getEtName() );
                etSystem.open();
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
                emu.getCmsgPortal().rcGuiErrorMessage(errString);

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
                e.printStackTrace();
                throw new DataTransportException("cannot create/attach to station " + stationName, e);
            }
        }
    }


    private void closeEtSystem() throws DataTransportException {
        if (etSysLocal != null) {
            try {
//System.out.println("closeEtSystem: detach from station " + attachmentLocal.getStation().getName());
                etSysLocal.detach(attachmentLocal);
            }
            catch (Exception e) {
                // Might be detached already or cannot communicate with ET
            }

            try {
                if (!stationName.equals("GRAND_CENTRAL")) {
//System.out.println("closeEtSystem: remove station " + stationName);
                    etSysLocal.removeStation(stationLocal.getStationId());
                }
            }
            catch (Exception e) {
                // Station may not exist, may still have attachments, or
                // cannot communicate with ET
            }
        }
        else {
            try {
//System.out.println("closeEtSystem: detach from station " + attachment.getStation().getName());
                etSystem.detach(attachment);
            }
            catch (Exception e) {
                // Might be detached already or cannot communicate with ET
            }

            try {
                if (!stationName.equals("GRAND_CENTRAL")) {
//System.out.println("closeEtSystem: remove station " + stationName);
                    etSystem.removeStation(station);
                }
            }
            catch (Exception e) {
                // Station may not exist, may still have attachments, or
                // cannot communicate with ET
            }

            System.out.println("closeEtSystem: close ET connection");
            etSystem.close();
        }
    }


    /** {@inheritDoc} */
    public void go() {
        pause = false;
        state = CODAState.ACTIVE;
    }

    /** {@inheritDoc} */
    public void pause() {
        pause = true;
        state = CODAState.PAUSED;
    }

    /** {@inheritDoc}. Formerly this code was the close() method. */
    public void end() {
        logger.warn("      DataChannel Et end() : " + name + " - end threads & close ET system");

        gotEndCmd = true;
        gotResetCmd = false;

        // Do NOT interrupt threads which are communicating with the ET server.
        // This will mess up future communications !!!

        // How long do we wait for each input or output thread
        // to end before we just terminate them?
        // The total time for an emu to wait for the END transition
        // is emu.endingTimeLimit. Dividing that by the number of
        // in/output threads is probably a good guess.
        long waitTime;

        // Don't close ET system until helper threads are done
        try {
            waitTime = emu.getEndingTimeLimit();
//System.out.println("      DataChannelImplEt.end : waiting for helper threads to end ...");
            if (dataInputThread != null) {
                dataInputThread.join(waitTime);
                // kill it if not already dead since we waited as long as possible
                dataInputThread.interrupt();
            }

            if (dataOutputThread != null) {
                waitTime = emu.getEndingTimeLimit();
//System.out.println("        try joining output thread #" + i + " for " + (waitTime/1000) + " sec");
                dataOutputThread.join(waitTime);
                // kill everything since we waited as long as possible
                dataOutputThread.interrupt();
                dataOutputThread.shutdown();
//System.out.println("        out thread done");
            }
//System.out.println("      helper thds done");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        // At this point all threads should be done
        try {
            closeEtSystem();
        }
        catch (DataTransportException e) {
            e.printStackTrace();
        }

//        queue.clear();
        state = CODAState.DOWNLOADED;
System.out.println("      end() is done");
    }


    /**
     * {@inheritDoc}
     * Reset this channel by interrupting the data sending threads and closing ET system.
     */
    public void reset() {
logger.debug("      DataChannel Et reset() : " + name + " channel");

        gotEndCmd   = false;
        gotResetCmd = true;

        // Don't close ET system until helper threads are done
        if (dataInputThread != null) {
//System.out.println("        interrupt input thread #" + i + " ...");
                dataInputThread.interrupt();
                // Make sure the thread is done, otherwise you risk
                // killing the ET system while a getEvents() call is
                // still in progress (with et-14.0 this is OK).
                // Give it 25% more time than the wait.
                try {dataInputThread.join(400);}  // 625
                catch (InterruptedException e) {}
//System.out.println("        input thread done");
        }

        if (dataOutputThread != null) {
//System.out.println("        interrupt output thread #" + i + " ...");
            dataOutputThread.interrupt();
            dataOutputThread.shutdown();
            // Make sure all threads are done, otherwise you risk
            // killing the ET system while a new/put/dumpEvents() call
            // is still in progress (with et-14.0 this is OK).
            // Give it 25% more time than the wait.
            try {dataOutputThread.join(1000);}
            catch (InterruptedException e) {}
//System.out.println("        output thread done");
        }

        // At this point all threads should be done
        try {
            closeEtSystem();
        }
        catch (DataTransportException e) {
            e.printStackTrace();
        }

//        queue.clear();
        errorMsg.set(null);
        state = CODAState.CONFIGURED;
logger.debug("      DataChannel Et reset() : " + name + " - done");
    }



    /**
     * For input channel, start the DataInputHelper thread which takes ET events,
     * parses each, puts the events back into the ET system, and puts the parsed
     * evio banks onto the queue.<p>
     * For output channel, start the DataOutputHelper thread which takes a bank from
     * the queue, puts it into a new ET event and puts that into the ET system.
     */
    private void startHelper() {
        if (input) {
            dataInputThread = new DataInputHelper(emu.getThreadGroup(),
                                                  name() + " data in");
            dataInputThread.start();
            dataInputThread.waitUntilStarted();
        }
        else {
            dataOutputThread = new DataOutputHelper(emu.getThreadGroup(),
                                                               name() + " data out");
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

        /** The minimum amount of milliseconds between updates to lastMvalue. */
        private long timeBetweenMupdates = 500;


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
            if (ringItemType == ModuleIoType.PayloadBank) {
                runBanks();
            }
            else if  (ringItemType == ModuleIoType.PayloadBuffer) {
                runBuffers();
            }
        }


        private void runBanks() {

            // Tell the world I've started
            latch.countDown();

            try {
                int evioVersion, sourceId, recordId, evCount;
                BlockHeaderV4 header4;
                EventType eventType, bankType;
                ControlType controlType;
                EvioReader reader = null;
                ByteBuffer buf;
                EvioEvent event;
                PayloadBank payloadBank;
                long t1, t2;
                boolean delay = false;

                t1 = t2 = System.currentTimeMillis();

                while ( etSystem.alive() ) {

                    if (delay) {
                        Thread.sleep(5);
                        delay = false;
                    }

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
logger.warn("      DataChannel Et in helper: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // Get events while checking periodically to see if we must go away.
                    // Do some work to get accurate error msgs back to run control.
                    try {
//System.out.println("      DataChannel Et in helper: " + name + " getEvents() ...");
                        events = etSystem.getEvents(attachment, Mode.TIMED,
                                                    Modify.NOTHING, etWaitTime, chunk);
                    }
                    catch (IOException e) {
                        errorMsg.compareAndSet(null, "Network communication error with Et");
                        throw e;
                    }
                    catch (EtException e) {
                        errorMsg.compareAndSet(null, "Internal error handling Et");
                        throw e;
                    }
                    catch (EtDeadException e) {
                        errorMsg.compareAndSet(null, "Et system dead");
                        throw e;
                    }
                    catch (EtClosedException e) {
                        errorMsg.compareAndSet(null, "Et connection closed");
                        throw e;
                    }
                    catch (EtWakeUpException e) {
                        // Told to wake up because we're ending or resetting
                        if (haveInputEndEvent) {
System.out.println("      DataChannel Et in helper: wake up " + name + ", other thd found END, quit");
                        }
                        else if (gotResetCmd) {
System.out.println("      DataChannel Et in helper: " + name + " got RESET cmd, quit");
                        }
//System.out.println("      DataChannel Et in helper: wake up exception");
                        return;
                    }
                    catch (EtTimeoutException e) {
                        if (haveInputEndEvent) {
System.out.println("      DataChannel Et in helper: timeout " + name + ", other thd found END, quit");
                            return;
                        }
                        else if (gotResetCmd) {
System.out.println("      DataChannel Et in helper: " + name + " got RESET cmd, quit");
                            return;
                        }
//System.out.println("      DataChannel Et in helper: timeout exception");

                        // Want to delay before calling getEvents again
                        // but don't do it here in a synchronized block.
                        delay = true;
                        continue;
                    }

                    for (EtEvent ev : events) {
                        buf = ev.getDataBuffer();
                        try {
                            if (reader == null) {
                                reader = new EvioReader(buf);
                            }
                            else {
                                reader.setBuffer(buf);
                            }
                        }
                        catch (IOException e) {
                            errorMsg.compareAndSet(null, "ET data is NOT evio v4 format");
                            throw e;
                        }
                        // Speed things up since no EvioListeners are used - doesn't do much
                        reader.getParser().setNotificationActive(false);

                        // First block header in ET buffer
                        BlockHeaderV4 blockHeader = (BlockHeaderV4)reader.getFirstBlockHeader();
                        evioVersion = blockHeader.getVersion();
                        if (evioVersion < 4) {
                            errorMsg.compareAndSet(null, "ET data is NOT evio v4 format");
                            throw new EvioException("Evio data needs to be written in version 4+ format");
                        }
                        header4     = blockHeader;
                        // eventType may be null if no type info exists in block header.
                        // But it should always be there if reading from ROC or DC.
                        eventType   = EventType.getEventType(header4.getEventType());
                        controlType = null;

                        evCount = reader.getEventCount();

                        // Send the # of (buildable) evio events / ET event for ROC feedback,
                        // but only if this is the DC or PEB.
                        t2 = System.currentTimeMillis();
                        if (isFirstEB && eventType.isBuildable() && (t2-t1 > timeBetweenMupdates)) {
                            emu.getCmsgPortal().sendMHandlerMessage(evCount, "M");
                            t1 = t2;
                        }

                        // If ROC raw type, this is the source's CODA id
                        sourceId = header4.getReserved1();
                        // If DC, CODA id is first ET event control word
                        if (eventType == EventType.PARTIAL_PHYSICS) {
                            sourceId = ev.getControl()[0];
                        }

                        // The recordId associated with each bank is taken from the first
                        // evio block header in a single ET data buffer. For a physics or
                        // ROC raw type, it should start at zero and increase by one in the
                        // first evio block header of the next ET data buffer.
                        // There may be multiple banks from the same ET buffer and
                        // they will all have the same recordId.
                        //
                        // Thus, only the first block header # is significant. It is set sequentially
                        // by the evWriter object & incremented once per ET event with physics
                        // or ROC data (set to -1 for other data types). Copy it into each bank.
                        // Even though many banks will have the same number, it should only
                        // increment by one. This should work just fine as all evio events in
                        // a single ET event should always be there (not possible to skip any)
                        // since it is transferred all together.
                        //
                        // When the Preprocessing thread of the event builder gets a physics or ROC
                        // evio event, it checks to make sure this number is in sequence and
                        // prints a warning if it isn't.
                        recordId = header4.getNumber();

//logger.info("      DataChannel Et in helper: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId);

                        try {
//System.out.println("      DataChannel Et in helper: parse next event");
                            while ((event = reader.parseNextEvent()) != null) {
                                // Complication: from the ROC, we'll be receiving USER events
                                // mixed in with and labeled as ROC Raw events. Check for that
                                // and fix it.
                                bankType = eventType;
                                if (eventType == EventType.ROC_RAW) {
                                    if (Evio.isUserEvent(event)) {
                                        bankType = EventType.USER;
                                    }
                                }
                                else if (eventType == EventType.CONTROL) {
                                    // Find out exactly what type of control event it is
                                    // (May be null if there is an error).
                                    // TODO: It may NOT be enough just to check the tag
                                    controlType = ControlType.getControlType(event.getHeader().getTag());
                                    if (controlType == null) {
                                        errorMsg.compareAndSet(null, "Found unidentified control event");
                                        throw new EvioException("Found unidentified control event");
                                    }
                                }

//System.out.println("      DataChannel Et in helper: wait for next ring buf for writing");
                                nextRingItem = ringBufferIn.next();
//System.out.println("      DataChannel Et in helper: Got sequence " + nextRingItem);
                                payloadBank = (PayloadBank) ringBufferIn.get(nextRingItem);

                                payloadBank.setEvent(event);
                                payloadBank.setEventType(bankType);
                                payloadBank.setControlType(controlType);
                                payloadBank.setRecordId(recordId);
                                payloadBank.setSourceId(sourceId);
                                payloadBank.setSourceName(name);
                                payloadBank.setEventCount(1);
                                payloadBank.matchesId(sourceId == id);

                                ringBufferIn.publish(nextRingItem);
//System.out.println("      DataChannel Et in helper: published sequence " + nextRingItem);

                                // Handle end event ...
                                if (controlType == ControlType.END) {
                                    // There should be no more events coming down the pike so
                                    // go ahead write out existing events and then shut this
                                    // thread down.
                                    logger.info("      DataChannel Et in helper: " + name + " found END event");
                                    haveInputEndEvent = true;
                                    // run callback saying we got end event
                                    if (endCallback != null) endCallback.endWait();
                                    break;
                                }
                            }
                        }
                        catch (IOException e) {
                            errorMsg.compareAndSet(null, "ET data is NOT evio v4 format");
                            throw e;
                        }

                        if (haveInputEndEvent) {
                            break;
                        }
                    }

                    // Put all events back in ET system - even those unused.
                    // We should be OK since only the PRESTART event can follow
                    // the END event. And we'll only get a prestart after we
                    // put the ET events back, shut down this thread, and report
                    // back to RC that we're ended.

                    // Do some work to get accurate error msgs back to run control.
//System.out.println("      DataChannel Et in helper: 4 " + name + " putEvents() ...");
                    try {
                        etSystem.putEvents(attachment, events);
                    }
                    catch (IOException e) {
                        errorMsg.compareAndSet(null, "Network communication error with Et");
                        throw e;
                    }
                    catch (EtException e) {
                        errorMsg.compareAndSet(null, "Internal error handling Et");
                        throw e;
                    }
                    catch (EtDeadException e) {
                        errorMsg.compareAndSet(null, "Et system dead");
                        throw e;
                    }
                    catch (EtClosedException e) {
                        errorMsg.compareAndSet(null, "Et connection closed");
                        throw e;
                    }

                    if (haveInputEndEvent) {
                        logger.info("      DataChannel Et in helper: have END, " + name + " quit thd");
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Et in helper: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                e.printStackTrace();
                logger.warn("      DataChannel Et in helper: " + name + " exit thd: " + e.getMessage());
            }
        }



        private void runBuffers() {

            // Tell the world I've started
            latch.countDown();

            try {
                int evioVersion, sourceId, recordId;
                BlockHeaderV4 header4;
                EventType eventType, bankType;
                ControlType controlType;
                ByteBufferItem bbItem;
                ByteBuffer buf;
                EvioCompactReader compactReader = null;
                PayloadBuffer payloadBuffer;
                long t1, t2;
                boolean delay = false;
                boolean useDirectEt = (etSysLocal != null);
                boolean etAlive = true;

                if (!useDirectEt) {
                    etAlive = etSystem.alive();
                }

                t1 = t2 = System.currentTimeMillis();

                while ( etAlive ) {

                    if (delay) {
                        Thread.sleep(5);
                        delay = false;
                    }

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
                            logger.warn("      DataChannel Et in helper: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // Get events while checking periodically to see if we must go away.
                    // Do some work to get accurate error msgs back to run control.
                    try {
//System.out.println("      DataChannel Et in helper: 4 " + name + " getEvents() ...");
                        if (useDirectEt) {
                            eventsDirect = etSysLocal.getEvents(attachmentLocal, Mode.TIMED.getValue(),
                                                                etWaitTime, chunk);
                            events = eventsDirect;
                        }
                        else {
                            events = etSystem.getEvents(attachment, Mode.TIMED,
                                                        Modify.NOTHING, etWaitTime, chunk);
                        }
                    }
                    catch (IOException e) {
                        errorMsg.compareAndSet(null, "Network communication error with Et");
                        throw e;
                    }
                    catch (EtException e) {
                        errorMsg.compareAndSet(null, "Internal error handling Et");
                        throw e;
                    }
                    catch (EtDeadException e) {
                        errorMsg.compareAndSet(null, "Et system dead");
                        throw e;
                    }
                    catch (EtClosedException e) {
                        errorMsg.compareAndSet(null, "Et connection closed");
                        throw e;
                    }
                    catch (EtWakeUpException e) {
                        // Told to wake up because we're ending or resetting
                        if (haveInputEndEvent) {
System.out.println("      DataChannel Et in helper: wake up " + name + ", other thd found END, quit");
                        }
                        else if (gotResetCmd) {
System.out.println("      DataChannel Et in helper: " + name + " got RESET cmd, quitting");
                        }
                        return;
                    }
                    catch (EtTimeoutException e) {
//System.out.println("      DataChannel Et in helper: timeout in " + name);
                        if (haveInputEndEvent) {
System.out.println("      DataChannel Et in helper: timeout " + name + ", other thd found END, quit");
                            return;
                        }
                        else if (gotResetCmd) {
System.out.println("      DataChannel Et in helper: " + name + " got RESET cmd, quitting");
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
                        bbItem.setUsers(events.length);

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
                            if (compactReader == null) {
                                compactReader = new EvioCompactReader(buf);
                            }
                            else {
                                compactReader.setBuffer(buf);
                            }
                        }
                        catch (EvioException e) {
                            errorMsg.compareAndSet(null, "ET data is NOT evio v4 format");
                            throw e;
                        }

                        // First block header in ET buffer
                        header4 = compactReader.getFirstBlockHeader();
                        evioVersion = header4.getVersion();
                        if (evioVersion < 4) {
                            errorMsg.compareAndSet(null, "ET data is NOT evio v4 format");
                            throw new EvioException("Evio data needs to be written in version 4+ format");
                        }

                        eventType   = EventType.getEventType(header4.getEventType());
                        controlType = null;
// TODO: this only works from ROC !!!
                        sourceId    = header4.getReserved1();
                        if (eventType == EventType.PARTIAL_PHYSICS) {
                            sourceId = ev.getControl()[0];
                        }
                        recordId = header4.getNumber();

                        // Send the # of (buildable) evio events / ET event for ROC feedback.
                        // But only if this is the DC or PEB.
                        int eventCount = compactReader.getEventCount();
                        t2 = System.currentTimeMillis();
                        if (isFirstEB && eventType.isBuildable() && (t2-t1 > timeBetweenMupdates)) {
                            emu.getCmsgPortal().sendMHandlerMessage(eventCount, "M");
                            t1 = t2;
                        }
//logger.info("      DataChannel Et in helper: isFirstEb = " + isFirstEB + ", eventCount = " + eventCount +
//", last val = " + lastMvalue + ", isBuildable = " + eventType.isBuildable());
                        EvioNode node;

//logger.info("      DataChannel Et in helper: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId + ", event cnt = " + eventCount);

                        for (int i=1; i < eventCount+1; i++) {
                            node = compactReader.getScannedEvent(i);

                            // Complication: from the ROC, we'll be receiving USER events
                            // mixed in with and labeled as ROC Raw events. Check for that
                            // and fix it.
                            bankType = eventType;
                            if (eventType == EventType.ROC_RAW) {
                                if (Evio.isUserEvent(node)) {
                                    bankType = EventType.USER;
                                }
                            }
                            else if (eventType == EventType.CONTROL) {
                                // Find out exactly what type of control event it is
                                // (May be null if there is an error).
                                // TODO: It may NOT be enough just to check the tag
                                controlType = ControlType.getControlType(node.getTag());
                                if (controlType == null) {
                                    errorMsg.compareAndSet(null, "Found unidentified control event");
                                    throw new EvioException("Found unidentified control event");
                                }
                            }

//System.out.println("      DataChannel Et in helper: wait for next ring buf for writing");
                            nextRingItem = ringBufferIn.next();
//System.out.println("      DataChannel Et in helper: Got sequence " + nextRingItem);
                            payloadBuffer = (PayloadBuffer) ringBufferIn.get(nextRingItem);
                            payloadBuffer.setBuffer(node.getStructureBuffer(false));
                            payloadBuffer.setEventType(bankType);
                            payloadBuffer.setControlType(controlType);
                            payloadBuffer.setRecordId(recordId);
                            payloadBuffer.setSourceId(sourceId);
                            payloadBuffer.setSourceName(name);
                            payloadBuffer.setEventCount(1);
                            payloadBuffer.setNode(node);
                            payloadBuffer.setReusableByteBuffer(bbSupply, bbItem);
                            payloadBuffer.matchesId(sourceId == id);

                            ringBufferIn.publish(nextRingItem);
//System.out.println("      DataChannel Et in helper: published sequence " + nextRingItem);

                            // Handle end event ...
                            if (controlType == ControlType.END) {
                                // There should be no more events coming down the pike so
                                // go ahead write out existing events and then shut this
                                // thread down.
                                logger.info("      DataChannel Et in helper: " + name + " found END event");
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
                        }
                    }
                    catch (IOException e) {
                        errorMsg.compareAndSet(null, "Network communication error with Et");
                        throw e;
                    }
                    catch (EtException e) {
                        errorMsg.compareAndSet(null, "Internal error handling Et");
                        throw e;
                    }
                    catch (EtDeadException e) {
                        errorMsg.compareAndSet(null, "Et system dead");
                        throw e;
                    }
                    catch (EtClosedException e) {
                        errorMsg.compareAndSet(null, "Et connection closed");
                        throw e;
                    }

                    if (haveInputEndEvent) {
                        logger.info("      DataChannel Et in helper: have END, " + name + " quit thd");
                        return;
                    }

                    if (!useDirectEt) {
                        etAlive = etSystem.alive();
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Et in helper: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                e.printStackTrace();
                logger.warn("      DataChannel Et in helper: " + name + " exit thd: " + e.getMessage());
            }
        }

    }



    /**
     * Class used to take Evio banks from ring buffers, write them into ET events
     * and put them into an ET system.
     *
     * This version is same speed as the "latch" version. Using MyPhaser & having the last
     * one to reach the phaser write the ET events, seems to slow things way down (3x).
     */
    private class DataOutputHelperPhaser1 extends Thread {

        /** Used to sync things before putting new ET events. */
        private CountDownLatch latch;

        /** Used to sync things before putting new ET events. */
        private Phaser phaser;

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Thread pool for writing Evio banks into new ET events. */
        private final ExecutorService writeThreadPool;

        /** Thread pool for getting new ET events. */
        private final ExecutorService getThreadPool;

        /** Runnable object for getting new ET events - to be run in getThreadPool. */
        private final EvGetter getter;

        /** Syncing for getting new ET events from ET system. */
        private final CyclicBarrier getBarrier;

        /** Let a single waiter know that the main thread has been started. */
        private final CountDownLatch startLatch = new CountDownLatch(1);


         /** Constructor. */
         DataOutputHelperPhaser1(ThreadGroup group, String name) {
            super(group, name);

            // Thread pool with "writeThreadCount" number of threads & queue
            writeThreadPool = Executors.newFixedThreadPool(chunk);

            // Stuff for getting new ET events in parallel
            getBarrier = new CyclicBarrier(2);
            getter = new EvGetter(getBarrier);

            // Thread pool with 1 thread & queue
            getThreadPool = Executors.newSingleThreadExecutor();
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {}
        }


        /** Stop all this object's threads. */
        private void shutdown() {
            // Cancel queued jobs and call interrupt on executing threads
            getThreadPool.shutdown();
            writeThreadPool.shutdown();

            // If any EvGetter thread is stuck on etSystem.newEvents(), unstuck it
            try {
System.out.println("      DataChannel Et out helper: wake up attachment #" + attachment.getId());
                etSystem.wakeUpAttachment(attachment);
                // It may take 0.2 sec to detach
                Thread.sleep(250);
            }
            catch (Exception e) {
            }

            // May be blocked on getBarrier.await(), unblock it
            getBarrier.reset();

            // Only wait for threads to terminate if shutting
            // down gracefully for an END command.
            if (gotEndCmd) {
                try { writeThreadPool.awaitTermination(100L, TimeUnit.MILLISECONDS); }
                catch (InterruptedException e) {}
            }
        }



//        class MyPhaser extends Phaser {
//
//            private EtEvent[] events;
//            private int eventsToWrite;
//            private int totalEventCount;
//            private int ringBufferIndex;
//
//
//            MyPhaser(int parties) {
//                super(parties);
//            }
//
//
//            void setParameters(EtEvent[] events, int eventsToWrite,
//                               int totalEventCount, int ringBufferIndex) {
//                this.events = events;
//                this.eventsToWrite = eventsToWrite;
//                this.totalEventCount = totalEventCount;
//                this.ringBufferIndex = ringBufferIndex;
//            }
//
//
//            protected boolean onAdvance(int phase, int registeredParties) {
//
//                try {
////System.out.println("      DataChannel Et: write " + eventsToWrite + " events");
//                    // Put events back in ET system
//                    etSystem.putEvents(attachment, events, 0, eventsToWrite);
//
//                    // Dump any left over new ET events.
//                    if (eventsToWrite < totalEventCount) {
////System.out.println("Dumping " +  (totalEventCount - eventsToWrite) + " unused new events");
//                        etSystem.dumpEvents(attachment, events, eventsToWrite, (totalEventCount - eventsToWrite));
//                    }
//                }
//                catch (IOException e) {
//                    errorMsg.compareAndSet(null, "Network communication error with Et");
//                    return true;
//                }
//                catch (EtException e) {
//                    errorMsg.compareAndSet(null, "Internal error handling Et");
//                    return true;
//                }
//                catch (EtDeadException e) {
//                    errorMsg.compareAndSet(null, "Et system dead");
//                    return true;
//                }
//                catch (EtClosedException e) {
//                    errorMsg.compareAndSet(null, "Et connection closed");
//                    return true;
//                }
//
//                releaseOutputRingItem(ringBufferIndex);
//
//                //return phase >= 1 || registeredParties == 0;
//                return phase < 0 || registeredParties == 0;
//            }
//        }



        /** {@inheritDoc} */
        @Override
        public void run() {

            // Tell the world I've started
            startLatch.countDown();

            try {
                EventType previousType, pBankType;
                ControlType pBankControlType;
                ArrayList<RingItem> bankList;
                RingItem ringItem;
                int nextEventIndex, thisEventIndex, pBankSize, listTotalSizeMax;
                EvWriter[] writers = new EvWriter[chunk];

                EtEvent[] events;
				int etSize, eventCount, events2Write, eventArrayLen;
                int[] recordIds = new int[chunk];
                etSize = (int) etSystem.getEventSize();

                // RocSimulation generates "ringChunk" sequential events at once,
                // so, a single ring will have ringChunk sequential events together.
                // Take this into account when reading from multiple rings.
                // We must get the output order right.
                int ringChunkCounter = outputRingChunk;

                // Create an array of lists of RingItem objects by 2-step
                // initialization to avoid "generic array creation" error.
                // Create one list for every possible ET event.
                ArrayList<RingItem>[] bankListArray = new ArrayList[chunk];
                for (int i=0; i < chunk; i++) {
                    bankListArray[i] = new ArrayList<RingItem>();
                }

                phaser = new Phaser(1);

                // Get some new ET events
                getThreadPool.execute(getter);

                while ( etSystem.alive() ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) Thread.sleep(5);
                        continue;
                    }

                    // Get new ET events in "chunk" quantities at a time,
                    // then have a thread simultaneously get more.
                    // If things are working properly, we can always get
                    // new events, which means we should never block here.
                    getBarrier.await();
                    events = getter.getEvents();

                    if (events == null || events.length < 1) {
                        // If I've been told to RESET ...
                        if (gotResetCmd) {
                            shutdown();
System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 1");
                            return;
                        }
                        continue;
                    }

                    // Number of events obtained in a newEvents() call will
                    // always be <= chunk. Convenience variable.
                    eventArrayLen = events.length;

                    // Execute thread to get more new events while we're
                    // filling and putting the ones we have.
                    getThreadPool.execute(getter);

                    // First, clear all the lists of banks we need -
                    // one list for each ET event.
                    for (int j=0; j < eventArrayLen; j++) {
                        bankListArray[j].clear();
                    }

                    // Init variables
                    eventCount = 0;
                    events2Write = 0;
                    nextEventIndex = thisEventIndex = 0;
                    listTotalSizeMax = 32;   // first (or last) block header
                    previousType = null;
                    bankList = bankListArray[nextEventIndex];

                    // Grab a bank to put into an ET event buffer,
                    // checking occasionally to see if we got an
                    // RESET command or someone found an END event.
                    do {
//System.out.println("rbIndex = " + rbIndex);
                        // Get bank off of Q, unless we already did so in a previous loop
                        if (firstBankFromQueue != null) {
                            ringItem = firstBankFromQueue;
                            firstBankFromQueue = null;
                        }
                        else {
// TODO: How do we keep things from blocking here??? --- Interrupt thread!
                            //logger.debug("      DataChannel Et out helper: get next buffer from ring");
                            ringItem = getNextOutputRingItem(rbIndex);
                        }

                        eventCount++;

                        pBankType = ringItem.getEventType();
                        pBankSize = ringItem.getTotalBytes();
                        pBankControlType = ringItem.getControlType();

 //Utilities.printBuffer(ringItem.getBuffer(), 0, 10, "event");

                        // Assume worst case of one block header / bank
                        listTotalSizeMax += pBankSize + 64;

//                        if (listTotalSizeMax >= etSize) {
//                            System.out.println("listTotalSizeMax = " + listTotalSizeMax +
//                            ", etSize = " + etSize);
//                        }

                        // This the first time through the while loop
                        if (previousType == null) {
                            // Add bank to the list since there's always room for one
                            bankList.add(ringItem);

                            // First time through loop nextEventIndex = thisEventIndex,
                            // at least until it gets incremented below.
                            //
                            // Set recordId depending on what type this bank is
                            if (pBankType.isAnyPhysics() || pBankType.isROCRaw()) {
                                recordIds[thisEventIndex] = recordId++;
                            }
                            else {
                                recordIds[thisEventIndex] = -1;
                            }

                            // Index of next list
                            nextEventIndex++;
                        }
                        // Is this bank a diff type as previous bank?
                        // Will it not fit into the et buffer?
                        // In both these cases start using a new list.
                        // If we're in single event output mode, we only want
                        // 1 evio event per each et-buf/cmsg-msg so use a new list.
                        else if (singleEventOut ||
                                 (previousType != pBankType) ||
                                 (listTotalSizeMax >= etSize)  )  {

                            // If we've already used up all the events,
                            // write things out first. Be sure to store what we just
                            // pulled off the Q to be the next bank!
                            if (nextEventIndex >= eventArrayLen) {
//System.out.println("Used up " + nextEventIndex + " events, store bank for next round");
                                firstBankFromQueue = ringItem;
                                break;
                            }

                            // Start over with new list
                            listTotalSizeMax = pBankSize + 64;

                            // Get new list
                            bankList = bankListArray[nextEventIndex];
                            // Add bank to new list
                            bankList.add(ringItem);

                            // Set recordId depending on what type this bank is
                            if (pBankType.isAnyPhysics() || pBankType.isROCRaw()) {
                                recordIds[nextEventIndex] = recordId++;
                            }
                            else {
                                recordIds[nextEventIndex] = -1;
                            }

                            // Index of this & next lists
                            thisEventIndex++;
                            nextEventIndex++;
                        }
                        // It's OK to add this bank to the existing list.
                        else {
                            // Add bank to list since there's room and it's the right type
                            bankList.add(ringItem);
                        }

                        // Set this for next round
                        previousType = pBankType;
                        ringItem.setAttachment(Boolean.FALSE);

                        gotoNextRingItem(rbIndex);

                        // If control event, quit loop and write what we have
                        if (pBankControlType != null) {
                            System.out.println("SEND CONTROL RIGHT THROUGH: " + pBankControlType);

                            // Look for END event and mark it in attachment
                            if (pBankControlType == ControlType.END) {
                                ringItem.setAttachment(Boolean.TRUE);
                                haveOutputEndEvent = true;
                                System.out.println("      DataChannel Et out helper: " + name + " I got END event, quitting 2");
                                // run callback saying we got end event
                                if (endCallback != null) endCallback.endWait();
                            }

                            break;
                        }

                        // Be careful not to use up all the events in the output
                        // ring buffer before writing (& freeing up) some.
                        if (eventCount >= outputRingItemCount /2) {
                            break;
                        }

                    } while (!gotResetCmd && (thisEventIndex < eventArrayLen));

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 2");
                        shutdown();
                        return;
                    }

//logger.info("      DataChannel Et DataOutputHelper : nextEvIndx = " + nextEventIndex +
//                    ", evArrayLen = " + eventArrayLen);

//                    latch = new CountDownLatch(nextEventIndex);
                    phaser.bulkRegister(nextEventIndex);
//System.out.println("Set bulk, phase # = " + phase + ", count = " + (nextEventIndex));

                    // For each ET event that can be filled with something ...
                    for (int i=0; i < nextEventIndex; i++) {
                        // Get one of the list of banks to put into this ET event
                        bankList = bankListArray[i];

                        if (bankList.size() < 1) {
                            continue;
                        }

                        // Check to see if not enough room in ET event to hold bank.
                        // In this case, list will only contain 1 (big) bank.
                        if (bankList.size() == 1) {
                            // Max # of bytes to write this bank into buffer
                            int bankWrittenSize = bankList.get(0).getTotalBytes() + 64;
                            if (bankWrittenSize > etSize) {
logger.warn("      DataChannel Et DataOutputHelper : " + name + " ET event too small to contain built event");
                                // This new event is not large enough, so dump it and replace it
                                // with a larger one. Performance will be terrible but it'll work.
                                try {
                                    etSystem.dumpEvents(attachment, new EtEvent[]{events[i]});
//System.out.println("      DataChannel Et out helper: 4 " + name + " newEvents() ...");
                                    EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, false,
                                                                        0, 1, bankWrittenSize, group);
                                    events[i] = evts[0];
                                }
                                catch (IOException e) {
                                    errorMsg.compareAndSet(null, "Network communication error with Et");
                                    throw e;
                                }
                                catch (EtException e) {
                                    errorMsg.compareAndSet(null, "Internal error handling Et");
                                    throw e;
                                }
                                catch (EtDeadException e) {
                                    errorMsg.compareAndSet(null, "Et system dead");
                                    throw e;
                                }
                                catch (EtClosedException e) {
                                    errorMsg.compareAndSet(null, "Et connection closed");
                                    throw e;
                                }
                                catch (EtWakeUpException e) {
                                    // Told to wake up because we're ending or resetting
                                    if (haveOutputEndEvent) {
System.out.println("      DataChannel Et out helper: " + name + " have END event, quitting");
                                    }
                                    else if (gotResetCmd) {
System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting");
                                    }
                                    return;
                                }
                            }
                        }

                        // Set byte order of ET event
                        events[i].setByteOrder(bankList.get(0).getByteOrder());

                        // CODA owns the first ET event control int which contains source id.
                        // Set that control word only if this is an EB.
                        // If a PEB or SEB, set it to event type.
                        // If a DC or ROC,  set this to coda id.
                        if (isFinalEB) {
                            pBankType = bankList.get(0).getEventType();
                            if (pBankType != null) {
                                control[0] = pBankType.getValue();
                                events[i].setControl(control);
                            }
                        }
                        else if (isEB || isROC) {
                            events[i].setControl(control);
                        }

                        // Write banks' data into ET buffer in separate thread.
                        // Do not recreate writer object if not necessary.
                        if (writers[i] == null) {
                            writers[i] = new EvWriter(bankList, events[i], recordIds[i]);
                        }
                        else {
                            writers[i].setupWriter(bankList, events[i], recordIds[i]);
                        }
                        writeThreadPool.execute(writers[i]);

                        // Keep track of how many ET events we want to write
                        events2Write++;
                    }

                    // Wait for all events to finish processing
//                    latch.await();
                    //System.out.println("main thd arrived");
//                    phaser.setParameters(events, eventArrayLen, events2Write, rbIndex);

                    phaser.arriveAndAwaitAdvance();
                    //System.out.println("past main phaser, phase = " + p);

                    try {
//System.out.println("      DataChannel Et: write " + events2Write + " events");
                        // Put events back in ET system
                        etSystem.putEvents(attachment, events, 0, events2Write);

                        // Dump any left over new ET events.
                        if (events2Write < eventArrayLen) {
//System.out.println("Dumping " + (eventArrayLen - events2Write) + " unused new events");
                            etSystem.dumpEvents(attachment, events, events2Write, (eventArrayLen - events2Write));
                        }
                    }
                    catch (IOException e) {
                        errorMsg.compareAndSet(null, "Network communication error with Et");
                        throw e;
                    }
                    catch (EtException e) {
                        errorMsg.compareAndSet(null, "Internal error handling Et");
                        throw e;
                    }
                    catch (EtDeadException e) {
                        errorMsg.compareAndSet(null, "Et system dead");
                        throw e;
                    }
                    catch (EtClosedException e) {
                        errorMsg.compareAndSet(null, "Et connection closed");
                        throw e;
                    }

                    // FREE UP STUFF

                    //logger.debug("      DataChannel Emu out helper: release ring item");
                    releaseOutputRingItem(rbIndex);

                    if (--ringChunkCounter < 1) {
                        rbIndex = ++rbIndex % outputRingCount;
                        ringChunkCounter = outputRingChunk;
                    }

                    if (haveOutputEndEvent) {
System.out.println("      DataChannel Et out helper: " + name + " some thd got END event, quitting 4");
                        shutdown();
                        return;
                    }
                }

            } catch (InterruptedException e) {
logger.warn("      DataChannel Et out helper: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
logger.warn("      DataChannel Et out helper : exit thd: " + e.getMessage());
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                e.printStackTrace();
            }

        }



        /**
         * This class is designed to write an evio bank's
         * contents into an ET buffer by way of a thread pool.
         */
        private class EvWriter implements Runnable {

            /** List of evio banks to write. */
            private List<RingItem> bankList;

            /** ET event in which to write banks. */
            private EtEvent etEvent;

            /** ET event's data buffer. */
            private ByteBuffer etBuffer;

            /** Object for writing banks into ET data buffer. */
            private EventWriter evWriter;

            private int waiterCount;


            /**
             * Encode the event type into the bit info word
             * which will be in each evio block header.
             *
             * @param bSet bit set which will become part of the bit info word
             * @param type event type to be encoded
             */
            private void setEventType(BitSet bSet, int type) {
                // check args
                if (type < 0) type = 0;
                else if (type > 15) type = 15;

                if (bSet == null || bSet.size() < 6) {
                    return;
                }
                // do the encoding
                for (int i=2; i < 6; i++) {
                    bSet.set(i, ((type >>> i - 2) & 0x1) > 0);
                }
            }


            /**
             * Constructor.
             *
             * @param bankList list of banks to be written into a single ET event
             * @param event ET event in which to place the banks
             * @param myRecordId value of starting block header's block number
             */
            EvWriter(List<RingItem> bankList, EtEvent event,
                     int myRecordId) {
                setupWriter(bankList, event, myRecordId);
            }


            /**
             * Create and/or setup the object to write evio events into et buffer.
             *
             * @param bankList list of banks to be written into a single ET event
             * @param event ET event in which to place the banks
             * @param myRecordId value of starting block header's block number
             */
            void setupWriter(List<RingItem> bankList, EtEvent event,
                             int myRecordId) {

                this.etEvent  = event;
                this.bankList = bankList;
                this.waiterCount = waiterCount;

                try {
                    // Make the block size bigger than
                    // the Roc's 2MB ET buffer size so no additional block headers must
                    // be written. It should contain less than 100 ROC Raw records,
                    // but we'll allow 200 such banks per block header.

                    // ET event's data buffer
                    etBuffer = event.getDataBuffer();
                    etBuffer.clear();
                    etBuffer.order(byteOrder);

                    // Encode the event type into bits
                    BitSet bitInfo = new BitSet(24);
                    setEventType(bitInfo, bankList.get(0).getEventType().getValue());

                    // Create object to write evio banks into ET buffer
                    if (evWriter == null) {
                        evWriter = new EventWriter(etBuffer, 550000, 200, null, bitInfo, emu.getCodaid());
                    }
                    else {
                        evWriter.setBuffer(etBuffer, bitInfo);
                    }
                    evWriter.setStartingBlockNumber(myRecordId);
                }
                catch (EvioException e) {/* never happen */}
            }


            /**
             * {@inheritDoc}<p>
             * Write bank into et event buffer.
             */
            public void run() {
                try {
                    // Write banks into ET buffer
                    if (ringItemType == ModuleIoType.PayloadBank) {
                        for (RingItem ri : bankList) {
                            evWriter.writeEvent(ri.getEvent());
                            ri.releaseByteBuffer();
                        }
                    }
                    else {
                        for (RingItem ri : bankList) {
                            evWriter.writeEvent(ri.getBuffer());
                            ri.releaseByteBuffer();
                        }
                    }

                    evWriter.close();
                    // Be sure to set the length to bytes of data actually written
                    etEvent.setLength(etBuffer.position());
                    // Tell the DataOutputHelper thread that we're done
//                    latch.countDown();
                    //System.out.println("writing thd arrived");
                    //int p = phaser.arriveAndAwaitAdvance();
                    phaser.arriveAndDeregister();
                    //System.out.println("past writing phaser, phase # " + p);
                }
                catch (Exception e) {
                    // Doubt this would ever happen
                    e.printStackTrace();
                }
            }
        }


        /**
         * This class is designed to get new ET buffers/events
         * simultaneously by way of a thread pool. The design is
         * for an array of events to be available for use while
         * this thread is getting another.
         */
        private class EvGetter implements Runnable {

            /** Array of new events obtained from the ET system. */
            private EtEvent[] events;
            /** Object used to synchronize the getting of new ET events. */
            private final CyclicBarrier barrier;

            /**
             * Constructor.
             * @param barrier object used to synchronize the getting of new ET events
             */
            EvGetter(CyclicBarrier barrier) {
                this.barrier = barrier;
            }

            /**
             * Get the array of new ET events obtained in newEvents() call.
             * @return the array of new ET events obtained in newEvents() call
             */
            EtEvent[] getEvents() {
                return events;
            }

            /**
             * {@inheritDoc}<p>
             * Get the ET events.
             */
            public void run() {
                boolean gotError = false;

                try {
                    events = null;
                    events = etSystem.newEvents(attachment, Mode.SLEEP, false, 0,
                                                chunk, (int)etSystem.getEventSize(), group);
//System.out.println("I got " + events.length + " new events");
                    barrier.await();
                }
                catch (EtWakeUpException e) {
                    // Told to wake up because we're ending or resetting
                }
                catch (BrokenBarrierException e) {
                    // May happen when ending or resetting
                }
                catch (InterruptedException e) {
                    // Told to quit when in barrier.await()
                }
                catch (IOException e) {
                    gotError = true;
                    errorMsg.compareAndSet(null, "Network communication error with Et");
                }
                catch (EtException e) {
                    gotError = true;
                    errorMsg.compareAndSet(null, "Internal error handling Et");
                }
                catch (EtDeadException e) {
                    gotError = true;
                    errorMsg.compareAndSet(null, "Et system dead");
                }
                catch (EtClosedException e) {
                    gotError = true;
                    errorMsg.compareAndSet(null, "Et connection closed");
                }
                catch (Exception e) {
                    gotError = true;
                    errorMsg.compareAndSet(null, e.getMessage());
                }

                // ET system problem - run will come to an end
                if (gotError) {
                    // set state
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();
                }
            }
        }


    }





    /**
      * Class used to take Evio banks from ring buffers, write them into ET events
      * and put them into an ET system.
      */
     private class DataOutputHelper extends Thread {

         /** Used to sync things before putting new ET events. */
         private CountDownLatch latch;

         /** Help in pausing DAQ. */
         private int pauseCounter;

         /** Thread pool for writing Evio banks into new ET events. */
         private final ExecutorService writeThreadPool;

         /** Thread pool for getting new ET events. */
         private final ExecutorService getThreadPool;

         /** Runnable object for getting new ET events - to be run in getThreadPool. */
         private final EvGetter getter;

         /** Syncing for getting new ET events from ET system. */
         private final CyclicBarrier getBarrier;

         /** Let a single waiter know that the main thread has been started. */
         private final CountDownLatch startLatch = new CountDownLatch(1);


          /** Constructor. */
         DataOutputHelper(ThreadGroup group, String name) {
             super(group, name);

             // Thread pool with "writeThreadCount" number of threads & queue
             writeThreadPool = Executors.newFixedThreadPool(writeThreadCount);

             // Stuff for getting new ET events in parallel
             getBarrier = new CyclicBarrier(2);
             getter = new EvGetter(getBarrier);

             // Thread pool with 1 thread & queue
             getThreadPool = Executors.newSingleThreadExecutor();
         }


         /** A single waiter can call this method which returns when thread was started. */
         private void waitUntilStarted() {
             try {
                 startLatch.await();
             }
             catch (InterruptedException e) {}
         }


         /** Stop all this object's threads. */
         private void shutdown() {
             // Cancel queued jobs and call interrupt on executing threads
             getThreadPool.shutdown();
             writeThreadPool.shutdown();

             // If any EvGetter thread is stuck on etSystem.newEvents(), unstuck it
             try {
 System.out.println("      DataChannel Et out helper: wake up attachment #" + attachment.getId());
                 etSystem.wakeUpAttachment(attachment);
                 // It may take 0.2 sec to detach
                 Thread.sleep(250);
             }
             catch (Exception e) {
             }

             // May be blocked on getBarrier.await(), unblock it
             getBarrier.reset();

             // Only wait for threads to terminate if shutting
             // down gracefully for an END command.
             if (gotEndCmd) {
                 try { writeThreadPool.awaitTermination(100L, TimeUnit.MILLISECONDS); }
                 catch (InterruptedException e) {}
             }
         }



         /** {@inheritDoc} */
         @Override
         public void run() {

             // Tell the world I've started
             startLatch.countDown();

             try {
                 EventType previousType, pBankType;
                 ControlType pBankControlType;
                 ArrayList<RingItem> bankList;
                 RingItem ringItem;
                 int nextEventIndex, thisEventIndex, pBankSize, listTotalSizeMax;
                 EvWriter[] writers = new EvWriter[chunk];

                 EtEvent[] events;
 				int etSize, eventCount, events2Write, eventArrayLen;
                 int[] recordIds = new int[chunk];
                 etSize = (int) etSystem.getEventSize();

                 // RocSimulation generates "ringChunk" sequential events at once,
                 // so, a single ring will have ringChunk sequential events together.
                 // Take this into account when reading from multiple rings.
                 // We must get the output order right.
                 int ringChunkCounter = outputRingChunk;

                 // Create an array of lists of RingItem objects by 2-step
                 // initialization to avoid "generic array creation" error.
                 // Create one list for every possible ET event.
                 ArrayList<RingItem>[] bankListArray = new ArrayList[chunk];
                 for (int i=0; i < chunk; i++) {
                     bankListArray[i] = new ArrayList<RingItem>();
                 }

                 // Get some new ET events
                 getThreadPool.execute(getter);

                 while ( etSystem.alive() ) {

                     if (pause) {
                         if (pauseCounter++ % 400 == 0) Thread.sleep(5);
                         continue;
                     }

                     // Get new ET events in "chunk" quantities at a time,
                     // then have a thread simultaneously get more.
                     // If things are working properly, we can always get
                     // new events, which means we should never block here.
                     getBarrier.await();
                     events = getter.getEvents();

                     if (events == null || events.length < 1) {
                         // If I've been told to RESET ...
                         if (gotResetCmd) {
                             shutdown();
 System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 1");
                             return;
                         }
                         continue;
                     }

                     // Number of events obtained in a newEvents() call will
                     // always be <= chunk. Convenience variable.
                     eventArrayLen = events.length;

                     // Execute thread to get more new events while we're
                     // filling and putting the ones we have.
                     getThreadPool.execute(getter);

                     // First, clear all the lists of banks we need -
                     // one list for each ET event.
                     for (int j=0; j < eventArrayLen; j++) {
                         bankListArray[j].clear();
                     }

                     // Init variables
                     eventCount = 0;
                     events2Write = 0;
                     nextEventIndex = thisEventIndex = 0;
                     listTotalSizeMax = 32;   // first (or last) block header
                     previousType = null;
                     bankList = bankListArray[nextEventIndex];

                     // Grab a bank to put into an ET event buffer,
                     // checking occasionally to see if we got an
                     // RESET command or someone found an END event.
                     do {
 //System.out.println("rbIndex = " + rbIndex);
                         // Get bank off of Q, unless we already did so in a previous loop
                         if (firstBankFromQueue != null) {
                             ringItem = firstBankFromQueue;
                             firstBankFromQueue = null;
                         }
                         else {
 // TODO: How do we keep things from blocking here??? --- Interrupt thread!
                             //logger.debug("      DataChannel Et out helper: get next buffer from ring");
                             ringItem = getNextOutputRingItem(rbIndex);
                         }

                         eventCount++;

                         pBankType = ringItem.getEventType();
                         pBankSize = ringItem.getTotalBytes();
                         pBankControlType = ringItem.getControlType();

  //Utilities.printBuffer(ringItem.getBuffer(), 0, 10, "event");

                         // Assume worst case of one block header / bank
                         listTotalSizeMax += pBankSize + 64;

 //                        if (listTotalSizeMax >= etSize) {
 //                            System.out.println("listTotalSizeMax = " + listTotalSizeMax +
 //                            ", etSize = " + etSize);
 //                        }

                         // This the first time through the while loop
                         if (previousType == null) {
                             // Add bank to the list since there's always room for one
                             bankList.add(ringItem);

                             // First time through loop nextEventIndex = thisEventIndex,
                             // at least until it gets incremented below.
                             //
                             // Set recordId depending on what type this bank is
                             if (pBankType.isAnyPhysics() || pBankType.isROCRaw()) {
                                 recordIds[thisEventIndex] = recordId++;
                             }
                             else {
                                 recordIds[thisEventIndex] = -1;
                             }

                             // Index of next list
                             nextEventIndex++;
                         }
                         // Is this bank a diff type as previous bank?
                         // Will it not fit into the et buffer?
                         // In both these cases start using a new list.
                         // If we're in single event output mode, we only want
                         // 1 evio event per each et-buf/cmsg-msg so use a new list.
                         else if (singleEventOut ||
                                  (previousType != pBankType) ||
                                  (listTotalSizeMax >= etSize)  )  {

                             // If we've already used up all the events,
                             // write things out first. Be sure to store what we just
                             // pulled off the Q to be the next bank!
                             if (nextEventIndex >= eventArrayLen) {
 //System.out.println("Used up " + nextEventIndex + " events, store bank for next round");
                                 firstBankFromQueue = ringItem;
                                 break;
                             }

                             // Start over with new list
                             listTotalSizeMax = pBankSize + 64;

                             // Get new list
                             bankList = bankListArray[nextEventIndex];
                             // Add bank to new list
                             bankList.add(ringItem);

                             // Set recordId depending on what type this bank is
                             if (pBankType.isAnyPhysics() || pBankType.isROCRaw()) {
                                 recordIds[nextEventIndex] = recordId++;
                             }
                             else {
                                 recordIds[nextEventIndex] = -1;
                             }

                             // Index of this & next lists
                             thisEventIndex++;
                             nextEventIndex++;
                         }
                         // It's OK to add this bank to the existing list.
                         else {
                             // Add bank to list since there's room and it's the right type
                             bankList.add(ringItem);
                         }

                         // Set this for next round
                         previousType = pBankType;
                         ringItem.setAttachment(Boolean.FALSE);

                         gotoNextRingItem(rbIndex);

                         // If control event, quit loop and write what we have
                         if (pBankControlType != null) {
                             System.out.println("SEND CONTROL RIGHT THROUGH: " + pBankControlType);

                             // Look for END event and mark it in attachment
                             if (pBankControlType == ControlType.END) {
                                 ringItem.setAttachment(Boolean.TRUE);
                                 haveOutputEndEvent = true;
                                 System.out.println("      DataChannel Et out helper: " + name + " I got END event, quitting 2");
                                 // run callback saying we got end event
                                 if (endCallback != null) endCallback.endWait();
                             }

                             break;
                         }

                         // Be careful not to use up all the events in the output
                         // ring buffer before writing (& freeing up) some.
                         if (eventCount >= outputRingItemCount /2) {
                             break;
                         }

                     } while (!gotResetCmd && (thisEventIndex < eventArrayLen));

                     // If I've been told to RESET ...
                     if (gotResetCmd) {
                         System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 2");
                         shutdown();
                         return;
                     }

 //logger.info("      DataChannel Et DataOutputHelper : nextEvIndx = " + nextEventIndex +
 //                    ", evArrayLen = " + eventArrayLen);

                     latch = new CountDownLatch(nextEventIndex);

                     // For each ET event that can be filled with something ...
                     for (int i=0; i < nextEventIndex; i++) {
                         // Get one of the list of banks to put into this ET event
                         bankList = bankListArray[i];

                         if (bankList.size() < 1) {
                             continue;
                         }

                         // Check to see if not enough room in ET event to hold bank.
                         // In this case, list will only contain 1 (big) bank.
                         if (bankList.size() == 1) {
                             // Max # of bytes to write this bank into buffer
                             int bankWrittenSize = bankList.get(0).getTotalBytes() + 64;
                             if (bankWrittenSize > etSize) {
 logger.warn("      DataChannel Et DataOutputHelper : " + name + " ET event too small to contain built event");
                                 // This new event is not large enough, so dump it and replace it
                                 // with a larger one. Performance will be terrible but it'll work.
                                 try {
                                     etSystem.dumpEvents(attachment, new EtEvent[]{events[i]});
 //System.out.println("      DataChannel Et out helper: 4 " + name + " newEvents() ...");
                                     EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, false,
                                                                         0, 1, bankWrittenSize, group);
                                     events[i] = evts[0];
                                 }
                                 catch (IOException e) {
                                     errorMsg.compareAndSet(null, "Network communication error with Et");
                                     throw e;
                                 }
                                 catch (EtException e) {
                                     errorMsg.compareAndSet(null, "Internal error handling Et");
                                     throw e;
                                 }
                                 catch (EtDeadException e) {
                                     errorMsg.compareAndSet(null, "Et system dead");
                                     throw e;
                                 }
                                 catch (EtClosedException e) {
                                     errorMsg.compareAndSet(null, "Et connection closed");
                                     throw e;
                                 }
                                 catch (EtWakeUpException e) {
                                     // Told to wake up because we're ending or resetting
                                     if (haveOutputEndEvent) {
 System.out.println("      DataChannel Et out helper: " + name + " have END event, quitting");
                                     }
                                     else if (gotResetCmd) {
 System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting");
                                     }
                                     return;
                                 }
                             }
                         }

                         // Set byte order of ET event
                         events[i].setByteOrder(bankList.get(0).getByteOrder());

                         // CODA owns the first ET event control int which contains source id.
                         // Set that control word only if this is an EB.
                         // If a PEB or SEB, set it to event type.
                         // If a DC or ROC,  set this to coda id.
                         if (isFinalEB) {
                             pBankType = bankList.get(0).getEventType();
                             if (pBankType != null) {
                                 control[0] = pBankType.getValue();
                                 events[i].setControl(control);
                             }
                         }
                         else if (isEB || isROC) {
                             events[i].setControl(control);
                         }

                         // Write banks' data into ET buffer in separate thread.
                         // Do not recreate writer object if not necessary.
                         if (writers[i] == null) {
                             writers[i] = new EvWriter(bankList, events[i], recordIds[i]);
                         }
                         else {
                             writers[i].setupWriter(bankList, events[i], recordIds[i]);
                         }
                         writeThreadPool.execute(writers[i]);

                         // Keep track of how many ET events we want to write
                         events2Write++;
                     }

                     // Wait for all events to finish processing
                     latch.await();

                     try {
 //System.out.println("      DataChannel Et: write " + events2Write + " events");
                         // Put events back in ET system
                         etSystem.putEvents(attachment, events, 0, events2Write);

                         // Dump any left over new ET events.
                         if (events2Write < eventArrayLen) {
 //System.out.println("Dumping " + (eventArrayLen - events2Write) + " unused new events");
                             etSystem.dumpEvents(attachment, events, events2Write, (eventArrayLen - events2Write));
                         }
                     }
                     catch (IOException e) {
                         errorMsg.compareAndSet(null, "Network communication error with Et");
                         throw e;
                     }
                     catch (EtException e) {
                         errorMsg.compareAndSet(null, "Internal error handling Et");
                         throw e;
                     }
                     catch (EtDeadException e) {
                         errorMsg.compareAndSet(null, "Et system dead");
                         throw e;
                     }
                     catch (EtClosedException e) {
                         errorMsg.compareAndSet(null, "Et connection closed");
                         throw e;
                     }

                     // FREE UP STUFF

                     //logger.debug("      DataChannel Emu out helper: release ring item");
                     releaseOutputRingItem(rbIndex);

                     if (--ringChunkCounter < 1) {
                         rbIndex = ++rbIndex % outputRingCount;
                         ringChunkCounter = outputRingChunk;
                     }

                     if (haveOutputEndEvent) {
 System.out.println("      DataChannel Et out helper: " + name + " some thd got END event, quitting 4");
                         shutdown();
                         return;
                     }
                 }

             } catch (InterruptedException e) {
 logger.warn("      DataChannel Et out helper: " + name + "  interrupted thd, exiting");
             } catch (Exception e) {
 logger.warn("      DataChannel Et out helper : exit thd: " + e.getMessage());
                 // If we haven't yet set the cause of error, do so now & inform run control
                 errorMsg.compareAndSet(null, e.getMessage());

                 // set state
                 state = CODAState.ERROR;
                 emu.sendStatusMessage();

                 e.printStackTrace();
             }

         }



         /**
          * This class is designed to write an evio bank's
          * contents into an ET buffer by way of a thread pool.
          */
         private class EvWriter implements Runnable {

             /** List of evio banks to write. */
             private List<RingItem> bankList;

             /** ET event in which to write banks. */
             private EtEvent etEvent;

             /** ET event's data buffer. */
             private ByteBuffer etBuffer;

             /** Object for writing banks into ET data buffer. */
             private EventWriter evWriter;


             /**
              * Encode the event type into the bit info word
              * which will be in each evio block header.
              *
              * @param bSet bit set which will become part of the bit info word
              * @param type event type to be encoded
              */
             private void setEventType(BitSet bSet, int type) {
                 // check args
                 if (type < 0) type = 0;
                 else if (type > 15) type = 15;

                 if (bSet == null || bSet.size() < 6) {
                     return;
                 }
                 // do the encoding
                 for (int i=2; i < 6; i++) {
                     bSet.set(i, ((type >>> i - 2) & 0x1) > 0);
                 }
             }


             /**
              * Constructor.
              *
              * @param bankList list of banks to be written into a single ET event
              * @param event ET event in which to place the banks
              * @param myRecordId value of starting block header's block number
              */
             EvWriter(List<RingItem> bankList, EtEvent event, int myRecordId) {
                 setupWriter(bankList, event, myRecordId);
             }


             /**
              * Create and/or setup the object to write evio events into et buffer.
              *
              * @param bankList list of banks to be written into a single ET event
              * @param event ET event in which to place the banks
              * @param myRecordId value of starting block header's block number
              */
             void setupWriter(List<RingItem> bankList, EtEvent event, int myRecordId) {

                 this.etEvent  = event;
                 this.bankList = bankList;

                 try {
                     // Make the block size bigger than
                     // the Roc's 2MB ET buffer size so no additional block headers must
                     // be written. It should contain less than 100 ROC Raw records,
                     // but we'll allow 200 such banks per block header.

                     // ET event's data buffer
                     etBuffer = event.getDataBuffer();
                     etBuffer.clear();
                     etBuffer.order(byteOrder);

                     // Encode the event type into bits
                     BitSet bitInfo = new BitSet(24);
                     setEventType(bitInfo, bankList.get(0).getEventType().getValue());

                     // Create object to write evio banks into ET buffer
                     if (evWriter == null) {
                         evWriter = new EventWriter(etBuffer, 550000, 200, null, bitInfo, emu.getCodaid());
                     }
                     else {
                         evWriter.setBuffer(etBuffer, bitInfo);
                     }
                     evWriter.setStartingBlockNumber(myRecordId);
                 }
                 catch (EvioException e) {/* never happen */}
             }


             /**
              * {@inheritDoc}<p>
              * Write bank into et event buffer.
              */
             public void run() {
                 try {
                     // Write banks into ET buffer
                     if (ringItemType == ModuleIoType.PayloadBank) {
                         for (RingItem ri : bankList) {
                             evWriter.writeEvent(ri.getEvent());
                             ri.releaseByteBuffer();
                         }
                     }
                     else {
                         for (RingItem ri : bankList) {
                             evWriter.writeEvent(ri.getBuffer());
                             ri.releaseByteBuffer();
                         }
                     }

                     evWriter.close();
                     // Be sure to set the length to bytes of data actually written
                     etEvent.setLength(etBuffer.position());
                     // Tell the DataOutputHelper thread that we're done
                     latch.countDown();
                 }
                 catch (Exception e) {
                     // Doubt this would ever happen
                     e.printStackTrace();
                 }
             }
         }


         /**
          * This class is designed to get new ET buffers/events
          * simultaneously by way of a thread pool. The design is
          * for an array of events to be available for use while
          * this thread is getting another.
          */
         private class EvGetter implements Runnable {

             /** Array of new events obtained from the ET system. */
             private EtEvent[] events;
             /** Object used to synchronize the getting of new ET events. */
             private final CyclicBarrier barrier;

             /**
              * Constructor.
              * @param barrier object used to synchronize the getting of new ET events
              */
             EvGetter(CyclicBarrier barrier) {
                 this.barrier = barrier;
             }

             /**
              * Get the array of new ET events obtained in newEvents() call.
              * @return the array of new ET events obtained in newEvents() call
              */
             EtEvent[] getEvents() {
                 return events;
             }

             /**
              * {@inheritDoc}<p>
              * Get the ET events.
              */
             public void run() {
                 boolean gotError = false;

                 try {
                     events = null;
                     events = etSystem.newEvents(attachment, Mode.SLEEP, false, 0,
                                                 chunk, (int)etSystem.getEventSize(), group);
 //System.out.println("I got " + events.length + " new events");
                     barrier.await();
                 }
                 catch (EtWakeUpException e) {
                     // Told to wake up because we're ending or resetting
                 }
                 catch (BrokenBarrierException e) {
                     // May happen when ending or resetting
                 }
                 catch (InterruptedException e) {
                     // Told to quit when in barrier.await()
                 }
                 catch (IOException e) {
                     gotError = true;
                     errorMsg.compareAndSet(null, "Network communication error with Et");
                 }
                 catch (EtException e) {
                     gotError = true;
                     errorMsg.compareAndSet(null, "Internal error handling Et");
                 }
                 catch (EtDeadException e) {
                     gotError = true;
                     errorMsg.compareAndSet(null, "Et system dead");
                 }
                 catch (EtClosedException e) {
                     gotError = true;
                     errorMsg.compareAndSet(null, "Et connection closed");
                 }
                 catch (Exception e) {
                     gotError = true;
                     errorMsg.compareAndSet(null, e.getMessage());
                 }

                 // ET system problem - run will come to an end
                 if (gotError) {
                     // set state
                     state = CODAState.ERROR;
                     emu.sendStatusMessage();
                 }
             }
         }


     }







    // This works well but gets no better performance than the original
    // design of this class. And it uses an extra thread.
    // Then I changed the new event supply so it is also a ring buffer.
    // Oddly, this slowed things down a little. Probably because it gets
    // events so quickly that it only gets them in chunks of 2 or 3 instead
    // of 5. After all that effort, no improvement in performance.

    /**
      * Class used to take Evio banks from ring buffers, write them into ET events
      * and put them into an ET system.
      */
     private class DataOutputHelperDis extends Thread {

         /** Used to sync things before putting new ET events. */
         private CountDownLatch latch;

         /** Help in pausing DAQ. */
         private int pauseCounter;

         /** Thread pool for writing Evio banks into new ET events. */
         private final ExecutorService writeThreadPool;

         /** Thread pool for getting new ET events. */
         private final ExecutorService getThreadPool;

         /** Runnable object for getting new ET events - to be run in getThreadPool. */
         private final EvGetter getter;

        /** Let a single waiter know that the main thread has been started. */
        private final CountDownLatch startLatch = new CountDownLatch(3);

        private final Thread writingThread;

        private final EtEventsSupply eventSupply;
        private final EtEventsSupply newEventSupply;


          /** Constructor. */
         DataOutputHelperDis(ThreadGroup group, String name) {
             super(group, name);

             // Thread pool with "writeThreadCount" number of threads & queue
             writeThreadPool = Executors.newFixedThreadPool(writeThreadCount);

             // Stuff for getting new ET events in parallel
             getter = new EvGetter();
             getter.start();

             // Thread pool with 1 thread & queue
             getThreadPool = Executors.newSingleThreadExecutor();

             eventSupply = new EtEventsSupply(32);
             newEventSupply = new EtEventsSupply(4);

             writingThread = new EtWriter();
             writingThread.start();
             System.out.println("STARTED Et Writing Thread @@@@@  !!!!!!!!!!!");
         }


         /** A single waiter can call this method which returns when thread was started. */
         private void waitUntilStarted() {
             try {
                 startLatch.await();
             }
             catch (InterruptedException e) {}
         }


         /** Stop all this object's threads. */
         private void shutdown() {
             // Cancel queued jobs and call interrupt on executing threads
             getThreadPool.shutdown();
             writeThreadPool.shutdown();
             writingThread.interrupt();
             getter.interrupt();

             // If any EvGetter thread is stuck on etSystem.newEvents(), unstuck it
             try {
 System.out.println("      DataChannel Et out helper: wake up attachment #" + attachment.getId());
                 etSystem.wakeUpAttachment(attachment);
                 // It may take 0.2 sec to detach
                 Thread.sleep(250);
             }
             catch (Exception e) {
             }

             // Only wait for threads to terminate if shutting
             // down gracefully for an END command.
             if (gotEndCmd) {
                 try { writeThreadPool.awaitTermination(100L, TimeUnit.MILLISECONDS); }
                 catch (InterruptedException e) {}
             }
         }



         /** {@inheritDoc} */
         @Override
         public void run() {

             // Tell the world I've started
             startLatch.countDown();

             try {
                 EventType previousType, pBankType;
                 ControlType pBankControlType;
                 ArrayList<RingItem> bankList;
                 RingItem ringItem;
                 int nextEventIndex, thisEventIndex, pBankSize, listTotalSizeMax;
                 EvWriter[] writers = new EvWriter[chunk];

                 EtEventsItem newEventItem, eventItem;
                 EtEvent[] events;
 				 int etSize, eventCount, events2Write, eventArrayLen;
                 int[] recordIds = new int[chunk];
                 etSize = (int) etSystem.getEventSize();

                 // RocSimulation generates "ringChunk" sequential events at once,
                 // so, a single ring will have ringChunk sequential events together.
                 // Take this into account when reading from multiple rings.
                 // We must get the output order right.
                 int ringChunkCounter = outputRingChunk;

                 // Create an array of lists of RingItem objects by 2-step
                 // initialization to avoid "generic array creation" error.
                 // Create one list for every possible ET event.
                 ArrayList<RingItem>[] bankListArray = new ArrayList[chunk];
                 for (int i=0; i < chunk; i++) {
                     bankListArray[i] = new ArrayList<RingItem>();
                 }

//                 // Get some new ET events
//                 getThreadPool.execute(getter);

                 while ( etSystem.alive() ) {

                     if (pause) {
                         if (pauseCounter++ % 400 == 0) Thread.sleep(5);
                         continue;
                     }

                     // Get new ET events in "chunk" quantities at a time,
                     // then have a thread simultaneously get more.
                     // If things are working properly, we can always get
                     // new events, which means we should never block here.
                     newEventItem = newEventSupply.consumerGet();
                     events = newEventItem.getArray();

                     if (events == null || events.length < 1) {
                         // If I've been told to RESET ...
                         if (gotResetCmd) {
                             shutdown();
 System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 1");
                             return;
                         }
                         continue;
                     }

                     // Number of events obtained in a newEvents() call will
                     // always be <= chunk. Convenience variable.
                     eventArrayLen = events.length;

//                     // Execute thread to get more new events while we're
//                     // filling and putting the ones we have.
//                     getThreadPool.execute(getter);

                     // First, clear all the lists of banks we need -
                     // one list for each ET event.
                     for (int j=0; j < eventArrayLen; j++) {
                         bankListArray[j].clear();
                     }

                     // Init variables
                     eventCount = 0;
                     events2Write = 0;
                     nextEventIndex = thisEventIndex = 0;
                     listTotalSizeMax = 32;   // first (or last) block header
                     previousType = null;
                     bankList = bankListArray[nextEventIndex];

                     // Grab a bank to put into an ET event buffer,
                     // checking occasionally to see if we got an
                     // RESET command or someone found an END event.
                     do {
 //System.out.println("rbIndex = " + rbIndex);
                         // Get bank off of Q, unless we already did so in a previous loop
                         if (firstBankFromQueue != null) {
                             ringItem = firstBankFromQueue;
                             firstBankFromQueue = null;
                         }
                         else {
 // TODO: How do we keep things from blocking here??? --- Interrupt thread!
                             //logger.debug("      DataChannel Et out helper: get next buffer from ring");
                             ringItem = getNextOutputRingItem(rbIndex);
                         }

                         eventCount++;

                         pBankType = ringItem.getEventType();
                         pBankSize = ringItem.getTotalBytes();
                         pBankControlType = ringItem.getControlType();

  //Utilities.printBuffer(ringItem.getBuffer(), 0, 10, "event");

                         // Assume worst case of one block header / bank
                         listTotalSizeMax += pBankSize + 64;

 //                        if (listTotalSizeMax >= etSize) {
 //                            System.out.println("listTotalSizeMax = " + listTotalSizeMax +
 //                            ", etSize = " + etSize);
 //                        }

                         // This the first time through the while loop
                         if (previousType == null) {
                             // Add bank to the list since there's always room for one
                             bankList.add(ringItem);

                             // First time through loop nextEventIndex = thisEventIndex,
                             // at least until it gets incremented below.
                             //
                             // Set recordId depending on what type this bank is
                             if (pBankType.isAnyPhysics() || pBankType.isROCRaw()) {
                                 recordIds[thisEventIndex] = recordId++;
                             }
                             else {
                                 recordIds[thisEventIndex] = -1;
                             }

                             // Index of next list
                             nextEventIndex++;
                         }
                         // Is this bank a diff type as previous bank?
                         // Will it not fit into the et buffer?
                         // In both these cases start using a new list.
                         // If we're in single event output mode, we only want
                         // 1 evio event per each et-buf/cmsg-msg so use a new list.
                         else if (singleEventOut ||
                                  (previousType != pBankType) ||
                                  (listTotalSizeMax >= etSize)  )  {

                             // If we've already used up all the events,
                             // write things out first. Be sure to store what we just
                             // pulled off the Q to be the next bank!
                             if (nextEventIndex >= eventArrayLen) {
 //System.out.println("Used up " + nextEventIndex + " events, store bank for next round");
                                 firstBankFromQueue = ringItem;
                                 break;
                             }

                             // Start over with new list
                             listTotalSizeMax = pBankSize + 64;

                             // Get new list
                             bankList = bankListArray[nextEventIndex];
                             // Add bank to new list
                             bankList.add(ringItem);

                             // Set recordId depending on what type this bank is
                             if (pBankType.isAnyPhysics() || pBankType.isROCRaw()) {
                                 recordIds[nextEventIndex] = recordId++;
                             }
                             else {
                                 recordIds[nextEventIndex] = -1;
                             }

                             // Index of this & next lists
                             thisEventIndex++;
                             nextEventIndex++;
                         }
                         // It's OK to add this bank to the existing list.
                         else {
                             // Add bank to list since there's room and it's the right type
                             bankList.add(ringItem);
                         }

                         // Set this for next round
                         previousType = pBankType;
                         ringItem.setAttachment(Boolean.FALSE);

                         gotoNextRingItem(rbIndex);

                         // If control event, quit loop and write what we have
                         if (pBankControlType != null) {
                             System.out.println("SEND CONTROL RIGHT THROUGH: " + pBankControlType);

                             // Look for END event and mark it in attachment
                             if (pBankControlType == ControlType.END) {
                                 ringItem.setAttachment(Boolean.TRUE);
                                 haveOutputEndEvent = true;
                                 System.out.println("      DataChannel Et out helper: " + name + " I got END event, quitting 2");
                                 // run callback saying we got end event
                                 if (endCallback != null) endCallback.endWait();
                             }

                             break;
                         }

                         // Be careful not to use up all the events in the output
                         // ring buffer before writing (& freeing up) some.
                         if (eventCount >= outputRingItemCount /2) {
                             break;
                         }

                     } while (!gotResetCmd && (thisEventIndex < eventArrayLen));

                     // If I've been told to RESET ...
                     if (gotResetCmd) {
                         System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 2");
                         shutdown();
                         return;
                     }

 //logger.info("      DataChannel Et DataOutputHelper : nextEvIndx = " + nextEventIndex +
 //                    ", evArrayLen = " + eventArrayLen);

                     latch = new CountDownLatch(nextEventIndex);

                     // For each ET event that can be filled with something ...
                     for (int i=0; i < nextEventIndex; i++) {
                         // Get one of the list of banks to put into this ET event
                         bankList = bankListArray[i];

                         if (bankList.size() < 1) {
                             continue;
                         }

                         // Check to see if not enough room in ET event to hold bank.
                         // In this case, list will only contain 1 (big) bank.
                         if (bankList.size() == 1) {
                             // Max # of bytes to write this bank into buffer
                             int bankWrittenSize = bankList.get(0).getTotalBytes() + 64;
                             if (bankWrittenSize > etSize) {
 logger.warn("      DataChannel Et DataOutputHelper : " + name + " ET event too small to contain built event");
                                 // This new event is not large enough, so dump it and replace it
                                 // with a larger one. Performance will be terrible but it'll work.
                                 try {
                                     etSystem.dumpEvents(attachment, new EtEvent[]{events[i]});
 //System.out.println("      DataChannel Et out helper: 4 " + name + " newEvents() ...");
                                     EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, false,
                                                                         0, 1, bankWrittenSize, group);
                                     events[i] = evts[0];
                                 }
                                 catch (IOException e) {
                                     errorMsg.compareAndSet(null, "Network communication error with Et");
                                     throw e;
                                 }
                                 catch (EtException e) {
                                     errorMsg.compareAndSet(null, "Internal error handling Et");
                                     throw e;
                                 }
                                 catch (EtDeadException e) {
                                     errorMsg.compareAndSet(null, "Et system dead");
                                     throw e;
                                 }
                                 catch (EtClosedException e) {
                                     errorMsg.compareAndSet(null, "Et connection closed");
                                     throw e;
                                 }
                                 catch (EtWakeUpException e) {
                                     // Told to wake up because we're ending or resetting
                                     if (haveOutputEndEvent) {
 System.out.println("      DataChannel Et out helper: " + name + " have END event, quitting");
                                     }
                                     else if (gotResetCmd) {
 System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting");
                                     }
                                     return;
                                 }
                             }
                         }

                         // Set byte order of ET event
                         events[i].setByteOrder(bankList.get(0).getByteOrder());

                         // CODA owns the first ET event control int which contains source id.
                         // Set that control word only if this is an EB.
                         // If a PEB or SEB, set it to event type.
                         // If a DC or ROC,  set this to coda id.
                         if (isFinalEB) {
                             pBankType = bankList.get(0).getEventType();
                             if (pBankType != null) {
                                 control[0] = pBankType.getValue();
                                 events[i].setControl(control);
                             }
                         }
                         else if (isEB || isROC) {
                             events[i].setControl(control);
                         }

                         // Write banks' data into ET buffer in separate thread.
                         // Do not recreate writer object if not necessary.
                         if (writers[i] == null) {
                             writers[i] = new EvWriter(bankList, events[i], recordIds[i]);
                         }
                         else {
                             writers[i].setupWriter(bankList, events[i], recordIds[i]);
                         }
                         writeThreadPool.execute(writers[i]);

                         // Keep track of how many ET events we want to write
                         events2Write++;
                     }

                     // Wait for all events to finish processing
                     latch.await();

                     // Put stuff in a ring buffer to be written out by another thread
                     eventItem = eventSupply.get();
//logger.debug("      DataChannel Emu out helper: place event into output ring, ev1 = " + events[0] +
//", # out = " + events2Write);
                     eventItem.setParameters(events, events2Write, eventArrayLen);
                     eventSupply.publish(eventItem);
                     newEventSupply.consumerRelease(newEventItem);

                     // FREE UP STUFF

//logger.debug("      DataChannel Emu out helper: release ring item");
                     releaseOutputRingItem(rbIndex);

                     if (--ringChunkCounter < 1) {
                         rbIndex = ++rbIndex % outputRingCount;
                         ringChunkCounter = outputRingChunk;
                     }

                     if (haveOutputEndEvent) {
 System.out.println("      DataChannel Et out helper: " + name + " some thd got END event, quitting 4");
                         shutdown();
                         return;
                     }
                 }

             } catch (InterruptedException e) {
 logger.warn("      DataChannel Et out helper: " + name + "  interrupted thd, exiting");
             } catch (Exception e) {
 logger.warn("      DataChannel Et out helper : exit thd: " + e.getMessage());
                 // If we haven't yet set the cause of error, do so now & inform run control
                 errorMsg.compareAndSet(null, e.getMessage());

                 // set state
                 state = CODAState.ERROR;
                 emu.sendStatusMessage();

                 e.printStackTrace();
             }

         }


        private final class EtWriter extends Thread {


            /**
             * {@inheritDoc}<p>
             * Write bank into et event buffer.
             */
            public void run() {
                int eventsToWrite, totalEvents;
                EtEvent[] events;
                EtEventsItem item;

                // Tell the world I've started
                startLatch.countDown();
//System.out.println("      DataChannel Et: Et Writing Thread: PAst latch @@@@@  !!!!!!!!!!!");

                while (true) {
                    try {
                        // Get array of events to write
                        item = eventSupply.consumerGet();
                        events = item.getArray();
                        totalEvents = item.getTotalEvents();
                        eventsToWrite = item.getEventsToWrite();
//System.out.println("      DataChannel Et: write " + eventsToWrite + ",ev1 = " + events[0]);

                        // Put events back in ET system
                        etSystem.putEvents(attachment, events, 0, eventsToWrite);

                        // Dump any left over new events
                        if (eventsToWrite < totalEvents) {
//System.out.println("      DataChannel Et: dumping " + (totalEvents - eventsToWrite) + " unused new events");
                            etSystem.dumpEvents(attachment, events, eventsToWrite, (totalEvents - eventsToWrite));
                        }

                        // Release array to be used again
                        eventSupply.consumerRelease(item);
                    }
                    catch (InterruptedException e) {
                        // just return if interrupted
                        break;
                    }
                    catch (IOException e) {
                        errorMsg.compareAndSet(null, "Network communication error with Et");
                        break;
                    }
                    catch (EtException e) {
                        errorMsg.compareAndSet(null, "Internal error handling Et");
                        break;
                    }
                    catch (EtDeadException e) {
                        errorMsg.compareAndSet(null, "Et system dead");
                        break;
                    }
                    catch (EtClosedException e) {
                        errorMsg.compareAndSet(null, "Et connection closed");
                        break;
                    }
                }
            }
        }



         /**
          * This class is designed to write an evio bank's
          * contents into an ET buffer by way of a thread pool.
          */
         private final class EvWriter implements Runnable {

             /** List of evio banks to write. */
             private List<RingItem> bankList;

             /** ET event in which to write banks. */
             private EtEvent etEvent;

             /** ET event's data buffer. */
             private ByteBuffer etBuffer;

             /** Object for writing banks into ET data buffer. */
             private EventWriter evWriter;


             /**
              * Constructor.
              *
              * @param bankList list of banks to be written into a single ET event
              * @param event ET event in which to place the banks
              * @param myRecordId value of starting block header's block number
              */
             EvWriter(List<RingItem> bankList, EtEvent event, int myRecordId) {
                 setupWriter(bankList, event, myRecordId);
             }


             /**
              * Create and/or setup the object to write evio events into et buffer.
              *
              * @param bankList list of banks to be written into a single ET event
              * @param event ET event in which to place the banks
              * @param myRecordId value of starting block header's block number
              */
             void setupWriter(List<RingItem> bankList, EtEvent event, int myRecordId) {

                 this.etEvent  = event;
                 this.bankList = bankList;

                 try {
                     // Make the block size bigger than
                     // the Roc's 2MB ET buffer size so no additional block headers must
                     // be written. It should contain less than 100 ROC Raw records,
                     // but we'll allow 200 such banks per block header.

                     // ET event's data buffer
                     etBuffer = event.getDataBuffer();
                     etBuffer.clear();
                     etBuffer.order(byteOrder);

                     // Encode the event type into bits
                     BitSet bitInfo = new BitSet(24);
                     EmuUtilities.setEventType(bitInfo, bankList.get(0).getEventType());

                     // Create object to write evio banks into ET buffer
                     if (evWriter == null) {
                         evWriter = new EventWriter(etBuffer, 550000, 200, null, bitInfo, emu.getCodaid());
                     }
                     else {
                         evWriter.setBuffer(etBuffer, bitInfo);
                     }
                     evWriter.setStartingBlockNumber(myRecordId);
                 }
                 catch (EvioException e) {/* never happen */}
             }


             /**
              * {@inheritDoc}<p>
              * Write bank into et event buffer.
              */
             public void run() {
                 try {
                     // Write banks into ET buffer
                     if (ringItemType == ModuleIoType.PayloadBank) {
                         for (RingItem ri : bankList) {
                             evWriter.writeEvent(ri.getEvent());
                             ri.releaseByteBuffer();
                         }
                     }
                     else {
                         for (RingItem ri : bankList) {
                             evWriter.writeEvent(ri.getBuffer());
                             ri.releaseByteBuffer();
                         }
                     }

                     evWriter.close();
                     // Be sure to set the length to bytes of data actually written
                     etEvent.setLength(etBuffer.position());
                     // Tell the DataOutputHelper thread that we're done
                     latch.countDown();
                 }
                 catch (Exception e) {
                     // Doubt this would ever happen
                     e.printStackTrace();
                 }
             }
         }


         /**
          * This class is designed to get new ET buffers/events
          * simultaneously by way of a thread pool. The design is
          * for an array of events to be available for use while
          * this thread is getting another.
          */
         private class EvGetter extends Thread {

             /**
              * {@inheritDoc}<p>
              * Get the ET events.
              */
             public void run() {
                 EtEvent[] events;
                 boolean gotError = false;
                 EtEventsItem item;

                 // Tell the world I've started
                 startLatch.countDown();

                 while (true) {
                     try {
                         events = etSystem.newEvents(attachment, Mode.SLEEP, false, 0,
                                                     chunk, (int)etSystem.getEventSize(), group);
                         item = newEventSupply.get();
                         item.setArray(events);
                         newEventSupply.publish(item);

//System.out.println("I got " + events.length + " new events");
                     }
                     catch (EtWakeUpException e) {
                         // Told to wake up because we're ending or resetting
                         break;
                     }
                     catch (IOException e) {
                         gotError = true;
                         errorMsg.compareAndSet(null, "Network communication error with Et");
                         break;
                     }
                     catch (EtException e) {
                         gotError = true;
                         errorMsg.compareAndSet(null, "Internal error handling Et");
                         break;
                     }
                     catch (EtDeadException e) {
                         gotError = true;
                         errorMsg.compareAndSet(null, "Et system dead");
                         break;
                     }
                     catch (EtClosedException e) {
                         gotError = true;
                         errorMsg.compareAndSet(null, "Et connection closed");
                         break;
                     }
                     catch (Exception e) {
                         gotError = true;
                         errorMsg.compareAndSet(null, e.getMessage());
                         break;
                     }
                 }

                 // ET system problem - run will come to an end
                 if (gotError) {
                     // set state
                     state = CODAState.ERROR;
                     emu.sendStatusMessage();
                 }
             }
         }


     }



}
