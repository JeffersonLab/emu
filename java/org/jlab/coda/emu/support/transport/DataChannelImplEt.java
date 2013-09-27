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
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.et.enums.Modify;
import org.jlab.coda.et.exception.*;
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

    /** Number of data output helper threads each of which has a pool of writeThreadCount. */
    private int outputThreadCount;

    /** Array of threads used to output data. */
    private DataOutputHelper[] dataOutputThreads;

    /** Order number of next array of new ET events (containing
     *  bank lists) to be put back into ET system. */
    private int outputOrder;

    /** Order of array of bank lists (to be put
     *  into array of ET events) taken off Q. */
    private int inputOrder;

    /** Synchronize getting banks off Q for multiple DataOutputHelpers. */
    private Object lockIn  = new Object();

    /** Synchronize putting new ET events into ET system for multiple DataOutputHelpers. */
    private Object lockOut = new Object();

    /** Place to store a bank off the queue for the next event out. */
    private PayloadBank firstBankFromQueue;

    /** Place to store a buffer off the queue for the next event out. */
    private PayloadBuffer firstBufferFromQueue;

    // INPUT

    /** Number of data input helper threads. */
    private int inputThreadCount;

    /** Array of threads used to input data. */
    private DataInputHelper[] dataInputThreads;

    /** Order number of next list of evio banks to be put onto Q. */
    private int outputOrderIn;

    /** Order of array of ET events read from the ET system. */
    private int inputOrderIn;

    /** Synchronize getting ET events for multiple DataInputHelpers. */
    private Object lockIn2  = new Object();

    /** Synchronize putting evio banks onto Q for multiple DataInputHelpers. */
    private Object lockOut2 = new Object();

    //-------------------------------------------
    // ET Stuff
    //-------------------------------------------

    /** Number of events to ask for in an array. */
    private int chunk;

    /** Number of group from which new ET events are taken. */
    private int group;

    /** ET system connected to. */
    private EtSystem etSystem;

    /** ET station attached to. */
    private EtStation station;

    /** Name of ET station attached to. */
    private String stationName;

    /** Position of ET station attached to. */
    private int stationPosition = 1;

    /** Attachment to ET station. */
    private EtAttachment attachment;

    /** Configuration of ET station being created and attached to. */
    private EtStationConfig stationConfig;

    /** Time in microseconds to wait for the ET system to deliver requested events
     *  before throwing an EtTimeoutException. */
    private int etWaitTime = 500000;



    /**
     * Constructor to create a new DataChannelImplEt instance. Used only by
     * {@link DataTransportImplEt#createChannel(String, Map, boolean, Emu, QueueItemType)}
     * which is only used during PRESTART in {@link Emu}.
     *
     * @param name          the name of this channel
     * @param transport     the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     * @param emu           emu this channel belongs to
     * @param queueItemType type of object to expect in queue item
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplEt(String name, DataTransportImplEt transport,
                         Map<String, String> attributeMap, boolean input, Emu emu,
                         QueueItemType queueItemType)
        throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, queueItemType);

        dataTransportImplEt = transport;

logger.info("      DataChannel Et : creating channel " + name);

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
                if (writeThreadCount > 20) writeThreadCount = 20;
            }
            catch (NumberFormatException e) {}
        }
//logger.info("      DataChannel Et : write threads = " + writeThreadCount);

        // How may data reading threads?
        inputThreadCount = 3;
        attribString = attributeMap.get("ithreads");
        if (attribString != null) {
            try {
                inputThreadCount = Integer.parseInt(attribString);
                if (inputThreadCount <  1) inputThreadCount = 1;
                if (inputThreadCount > 10) inputThreadCount = 10;
            }
            catch (NumberFormatException e) {}
        }
//logger.info("      DataChannel Et : input threads = " + inputThreadCount);

        // How may data writing threads?
        outputThreadCount = 1;
        attribString = attributeMap.get("othreads");
        if (attribString != null) {
            try {
                outputThreadCount = Integer.parseInt(attribString);
                if (outputThreadCount <  1) outputThreadCount = 1;
                if (outputThreadCount > 10) outputThreadCount = 10;
            }
            catch (NumberFormatException e) {}
        }
//logger.info("      DataChannel Et : output threads = " + outputThreadCount);

        // How may buffers do we grab at a time?
        chunk = 100;
        attribString = attributeMap.get("chunk");
        if (attribString != null) {
            try {
                chunk = Integer.parseInt(attribString);
                if (chunk < 1) chunk = 1;
            }
            catch (NumberFormatException e) {}
        }
//logger.info("      DataChannel Et : chunk = " + chunk);

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
                // configuration of a new station
                stationConfig = new EtStationConfig();
                try {
                    stationConfig.setUserMode(EtConstants.stationUserSingle);
                }
                catch (EtException e) { /* never happen */}

                String filter = attributeMap.get("idFilter");
                if (filter != null && filter.equalsIgnoreCase("on")) {
                    // Create filter for station so only events from a particular ROC
                    // (id as defined in config file) make it in.
                    // Station filter is the built-in selection function.
                    int[] selects = new int[EtConstants.stationSelectInts];
                    Arrays.fill(selects, -1);
                    selects[0] = id;
                    stationConfig.setSelect(selects);
                    stationConfig.setSelectMode(EtConstants.stationSelectMatch);
                }

                // create station if it does not already exist
                if (stationName == null) {
                    stationName = "station"+id;
                }
            }
            catch (Exception e) {/* never happen */}
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
        try {
//System.out.println("      DataChannel Et: try to open" + dataTransportImplEt.getOpenConfig().getEtName() );
            etSystem.open();
        }
        catch (Exception e) {
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
            throw new DataTransportException("cannot create/attach to station " + stationName, e);
        }
    }


    private void closeEtSystem() throws DataTransportException {
        try {
System.out.println("closeEtSystem: detach from station " + attachment.getStation().getName());
            etSystem.detach(attachment);
        }
        catch (Exception e) {
            // Might be detached already or cannot communicate with ET
        }

        try {
            if (!stationName.equals("GRAND_CENTRAL")) {
System.out.println("closeEtSystem: remove station " + stationName);
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
            waitTime = emu.getEndingTimeLimit() / inputThreadCount;
//System.out.println("      DataChannelImplEt.end : waiting for helper threads to end ...");
            if (dataInputThreads != null) {
                for (int i=0; i < inputThreadCount; i++) {
//System.out.println("        try joining input thread #" + i + " ...");
                    dataInputThreads[i].join(waitTime);
                    // kill it if not already dead since we waited as long as possible
                    dataInputThreads[i].interrupt();
//System.out.println("        in thread done");
                }
            }

            if (dataOutputThreads != null) {
                waitTime = emu.getEndingTimeLimit() / outputThreadCount;
                for (int i=0; i < outputThreadCount; i++) {
//System.out.println("        try joining output thread #" + i + " for " + (waitTime/1000) + " sec");
                    dataOutputThreads[i].join(waitTime);
                    // kill everything since we waited as long as possible
                    dataOutputThreads[i].interrupt();
                    dataOutputThreads[i].shutdown();
//System.out.println("        out thread done");
                }
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

        queue.clear();
        state = CODAState.DOWNLOADED;
System.out.println("      end() is done");
    }


    /**
     * {@inheritDoc}
     * Reset this channel by interrupting the data sending threads and closing ET system.
     */
    public void reset() {
logger.debug("      DataChannel Et reset() : " + name + " channel, in threads = " + inputThreadCount);

        gotEndCmd   = false;
        gotResetCmd = true;

        // Don't close ET system until helper threads are done
        if (dataInputThreads != null) {
            for (int i=0; i < inputThreadCount; i++) {
//System.out.println("        interrupt input thread #" + i + " ...");
                dataInputThreads[i].interrupt();
                // Make sure the thread is done, otherwise you risk
                // killing the ET system while a getEvents() call is
                // still in progress (with et-14.0 this is OK).
                // Give it 25% more time than the wait.
                try {dataInputThreads[i].join(400);}  // 625
                catch (InterruptedException e) {}
//System.out.println("        input thread done");
            }
        }

        if (dataOutputThreads != null) {
            for (int i=0; i < outputThreadCount; i++) {
//System.out.println("        interrupt output thread #" + i + " ...");
                dataOutputThreads[i].interrupt();
                dataOutputThreads[i].shutdown();
                // Make sure all threads are done, otherwise you risk
                // killing the ET system while a new/put/dumpEvents() call
                // is still in progress (with et-14.0 this is OK).
                // Give it 25% more time than the wait.
                try {dataOutputThreads[i].join(1000);}
                catch (InterruptedException e) {}
//System.out.println("        output thread done");
            }
        }

        // At this point all threads should be done
        try {
            closeEtSystem();
        }
        catch (DataTransportException e) {
            e.printStackTrace();
        }

        queue.clear();
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
             dataInputThreads = new DataInputHelper[inputThreadCount];

             for (int i=0; i < inputThreadCount; i++) {
                 DataInputHelper helper = new DataInputHelper(emu.getThreadGroup(),
                                                  name() + " data in" + i);
                 dataInputThreads[i] = helper;
                 dataInputThreads[i].start();
                 helper.waitUntilStarted();
             }
         }
         else {
             dataOutputThreads = new DataOutputHelper[outputThreadCount];

             for (int i=0; i < outputThreadCount; i++) {
                 DataOutputHelper helper = new DataOutputHelper(emu.getThreadGroup(),
                                                                name() + " data out" + i);
                 dataOutputThreads[i] = helper;
                 dataOutputThreads[i].start();
                 helper.waitUntilStarted();
             }
         }
     }



    /**
     * Class used to get ET events, parse them into Evio banks,
     * and put them onto a Q.
     */
    private class DataInputHelper extends Thread {

        /** Array of ET events to be gotten from ET system. */
        private EtEvent[] events;

        /** Variable to print messages when paused. */
        private int pauseCounter = 0;

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch latch = new CountDownLatch(1);


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

        /**
         * This method is used to put a list of PayloadBank objects
         * onto a queue. It allows coordination between multiple DataInputHelper
         * threads so that event order is preserved.
         *
         * @param banks a list of payload banks to put on the queue
         * @param order the record Id of the bank taken from the ET event
         * @throws InterruptedException if put or wait interrupted
         */
        private void writeEvents(List<PayloadBank> banks, int order)
                throws InterruptedException {

            synchronized (lockOut2) {
                // Is the bank we grabbed next to be output? If not, wait.
                while (order != outputOrderIn) {
                    lockOut2.wait();
                }

                // put banks in our Q, for module
                for (PayloadBank bank : banks) {
                    queue.put(new QueueItem(bank));
                }

                // next one to be put on output channel
                outputOrderIn = ++outputOrderIn % Integer.MAX_VALUE;
                lockOut2.notifyAll();
            }
        }


        /**
         * This method is used to put a list of PayloadBuffer (ByteBuffer) objects
         * onto a queue. It allows coordination between multiple DataInputHelper
         * threads so that event order is preserved.
         *
         * @param buffers a list of payload buffers to put on the queue
         * @param order the record Id of the buffer taken from the ET event
         * @throws InterruptedException if put or wait interrupted
         */
        private void writeEventsBB(List<PayloadBuffer> buffers, int order)
                throws InterruptedException {

            synchronized (lockOut2) {
                // Is the bank we grabbed next to be output? If not, wait.
                while (order != outputOrderIn) {
                    lockOut2.wait();
                }

                // put banks in our Q, for module
                for (PayloadBuffer buf : buffers) {
                    queue.put(new QueueItem(buf));
                }

                // next one to be put on output channel
                outputOrderIn = ++outputOrderIn % Integer.MAX_VALUE;
                lockOut2.notifyAll();
            }
        }


        /** {@inheritDoc} */
        @Override
        public void run() {
            if (queueItemType == QueueItemType.PayloadBank) {
                runBanks();
            }
            else if  (queueItemType == QueueItemType.PayloadBuffer) {
                runBuffers();
            }
        }


        private void runBanks() {

            // Tell the world I've started
            latch.countDown();

            try {

                EvioBank bank;
                PayloadBank payloadBank;
                LinkedList<PayloadBank> payloadBanks = new LinkedList<PayloadBank>();
                int myInputOrder, evioVersion, sourceId, recordId;
                BlockHeaderV4 header4;
                EventType eventType, bankType;
                ControlType controlType;

                EvioReader reader = null;
                ByteBuffer buf;

                while ( etSystem.alive() ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
logger.warn("      DataChannel Et in helper: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    synchronized (lockIn2) {

                        // Get events while checking periodically to see if we must go away.
                        // Do some work to get accurate error msgs back to run control.
                        try {
//System.out.println("      DataChannel Et in helper: 4 " + name + " getEvents() ...");
                            events = etSystem.getEvents(attachment, Mode.TIMED,
                                                        Modify.NOTHING, etWaitTime, chunk);
                            // Keep track of the order in which events are grabbed
                            // in order to preserve event order with multiple threads.
                            myInputOrder = inputOrderIn;
                            inputOrderIn = (inputOrderIn + events.length) % Integer.MAX_VALUE;
//System.out.println("\n      DataChannel Et in helper: Got " + events.length +
// " events from ET, inputOrder = " + myInputOrder);
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
System.out.println("      DataChannel Et in helper: " + name + " have END event, quitting");
                            }
                            else if (gotResetCmd) {
System.out.println("      DataChannel Et in helper: " + name + " got RESET cmd, quitting");
                            }
                            return;
                        }
                        catch (EtTimeoutException e) {
                            if (haveInputEndEvent) {
System.out.println("      DataChannel Et in helper: " + name + " have END event, quitting");
                                return;
                            }
                            else if (gotResetCmd) {
System.out.println("      DataChannel Et in helper: " + name + " got RESET cmd, quitting");
                                return;
                            }

                            Thread.sleep(5);
                            continue;
                        }
                    }

                    for (EtEvent ev : events) {
                        buf = ev.getDataBuffer();
                        try {
                            reader = new EvioReader(buf);
                        }
                        catch (IOException e) {
                            errorMsg.compareAndSet(null, "ET data is NOT evio v4 format");
                            throw e;
                        }
                        // Speed things up since no EvioListeners are used - doesn't do much
                        reader.getParser().setNotificationActive(false);

                        // First block header in ET buffer
                        IBlockHeader blockHeader = reader.getCurrentBlockHeader();
                        evioVersion = blockHeader.getVersion();
                        if (evioVersion < 4) {
                            errorMsg.compareAndSet(null, "ET data is NOT evio v4 format");
                            throw new EvioException("Evio data needs to be written in version 4+ format");
                        }
                        header4     = (BlockHeaderV4)blockHeader;
                        eventType   = EventType.getEventType(header4.getEventType());
                        controlType = null;
                        sourceId    = header4.getReserved1();

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
                        // When the QFiller thread of the event builder gets a physics or ROC
                        // evio event, it checks to make sure this number is in sequence and
                        // prints a warning if it isn't.
                        recordId = header4.getNumber();

                        payloadBanks.clear();

//logger.info("      DataChannel Et in helper: " + name + " block header, data type " + type +
//            ", src id = " + sourceId + ", recd id = " + recordId);

                        try {
                            while ((bank = reader.parseNextEvent()) != null) {
                                // Complication: from the ROC, we'll be receiving USER events
                                // mixed in with and labeled as ROC Raw events. Check for that
                                // and fix it.
                                bankType = eventType;
                                if (eventType == EventType.ROC_RAW) {
                                    if (Evio.isUserEvent(bank)) {
                                        bankType = EventType.USER;
                                    }
                                }
                                else if (eventType == EventType.CONTROL) {
                                    // Find out exactly what type of control event it is
                                    // (May be null if there is an error).
                                    // TODO: It may NOT be enough just to check the tag
                                    controlType = ControlType.getControlType(bank.getHeader().getTag());
                                    if (controlType == null) {
                                        errorMsg.compareAndSet(null, "Found unidentified control event");
                                        throw new EvioException("Found unidentified control event");
                                    }
                                }

                                // Not a real copy, just points to stuff in bank
                                payloadBank = new PayloadBank(bank);
                                // Add vital info from block header.
                                payloadBank.setEventType(bankType);
                                payloadBank.setControlType(controlType);
                                payloadBank.setRecordId(recordId);
                                payloadBank.setSourceId(sourceId);

                                // add bank to list for later writing
                                payloadBanks.add(payloadBank);

                                // Handle end event ...
                                if (controlType == ControlType.END) {
                                    // There should be no more events coming down the pike so
                                    // go ahead write out existing events and then shut this
                                    // thread down.
                                    logger.info("      DataChannel Et in helper: found END event");
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

                        // Write any existing banks
                        writeEvents(payloadBanks, myInputOrder++);

                        if (haveInputEndEvent) {
                            break;
                        }
                    }

                    // Put all events back in ET system - even those unused.
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
                PayloadBuffer payloadBuffer;
                LinkedList<PayloadBuffer> payloadBuffers = new LinkedList<PayloadBuffer>();
                int myInputOrder, evioVersion, sourceId, recordId;
                BlockHeaderV4 header4;
                EventType eventType, bankType;
                ControlType controlType;

                ByteBuffer buf;
                EvioReader reader;
                EvioCompactReader compactReader;

                while ( etSystem.alive() ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
                            logger.warn("      DataChannel Et in helper: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    synchronized (lockIn2) {

                        // Get events while checking periodically to see if we must go away.
                        // Do some work to get accurate error msgs back to run control.
                        try {
//System.out.println("      DataChannel Et in helper: 4 " + name + " getEvents() ...");
                            events = etSystem.getEvents(attachment, Mode.TIMED,
                                                        Modify.NOTHING, etWaitTime, chunk);
                            // Keep track of the order in which events are grabbed
                            // in order to preserve event order with multiple threads.
                            myInputOrder = inputOrderIn;
                            inputOrderIn = (inputOrderIn + events.length) % Integer.MAX_VALUE;
//System.out.println("\n      DataChannel Et in helper: Got " + events.length +
// " events from ET, inputOrder = " + myInputOrder);
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
                                System.out.println("      DataChannel Et in helper: " + name + " have END event, quitting");
                            }
                            else if (gotResetCmd) {
                                System.out.println("      DataChannel Et in helper: " + name + " got RESET cmd, quitting");
                            }
                            return;
                        }
                        catch (EtTimeoutException e) {
                            if (haveInputEndEvent) {
                                System.out.println("      DataChannel Et in helper: " + name + " have END event, quitting");
                                return;
                            }
                            else if (gotResetCmd) {
                                System.out.println("      DataChannel Et in helper: " + name + " got RESET cmd, quitting");
                                return;
                            }

                            Thread.sleep(5);
                            continue;
                        }
                    }

                    for (EtEvent ev : events) {
                        buf = ev.getDataBuffer();
                        try {
                            reader = new EvioReader(buf);
                            compactReader = new EvioCompactReader(buf);
                        }
                        catch (IOException e) {
                            errorMsg.compareAndSet(null, "ET data is NOT evio v4 format");
                            throw e;
                        }

                        // Speed things up since no EvioListeners are used - doesn't do much
                        reader.getParser().setNotificationActive(false);

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
                        // When the QFiller thread of the event builder gets a physics or ROC
                        // evio event, it checks to make sure this number is in sequence and
                        // prints a warning if it isn't.
                        recordId = header4.getNumber();

                        payloadBuffers.clear();

                        int eventCount = compactReader.getEventCount();
                        EvioNode node;

//logger.info("      DataChannel Et in helper: " + name + " block header, data type " + type +
//            ", src id = " + sourceId + ", recd id = " + recordId);

                        for (int i=0; i < eventCount; i++) {
                            node = compactReader.getEvent(i);

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

                            // Not a real copy, just points to stuff in buf
                            payloadBuffer = new PayloadBuffer(compactReader.getEventBuffer(i));
                            // Add vital info from block header.
                            payloadBuffer.setEventType(bankType);
                            payloadBuffer.setControlType(controlType);
                            payloadBuffer.setRecordId(recordId);
                            payloadBuffer.setSourceId(sourceId);

                            // add buffer to list for later writing
                            payloadBuffers.add(payloadBuffer);

                            // Handle end event ...
                            if (controlType == ControlType.END) {
                                // There should be no more events coming down the pike so
                                // go ahead write out existing events and then shut this
                                // thread down.
                                logger.info("      DataChannel Et in helper: found END event");
                                haveInputEndEvent = true;
                                // run callback saying we got end event
                                if (endCallback != null) endCallback.endWait();
                                break;
                            }
                        }

                        // Write any existing banks
                        writeEventsBB(payloadBuffers, myInputOrder++);

                        if (haveInputEndEvent) {
                            break;
                        }
                    }

                    // Put all events back in ET system - even those unused.
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



    }


    /**
     * Class used to take Evio banks from Q, write them into ET events
     * and put them into an ET system.
     */
    private class DataOutputHelper extends Thread {

        /** Used to sync things before putting new ET events. */
        private CountDownLatch latch;

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Thread pool for writing Evio banks into new ET events. */
        private ExecutorService writeThreadPool;

        /** Thread pool for getting new ET events. */
        private ExecutorService getThreadPool;

        /** Runnable object for getting new ET events - to be run in getThreadPool. */
        private EvGetter getter;

        /** Syncing for getting new ET events from ET system. */
        private CyclicBarrier getBarrier;

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch startLatch = new CountDownLatch(1);



         /** Constructor. */
        DataOutputHelper(ThreadGroup group, String name) {
            super(group, name);

            // Thread pool with "writeThreadCount" number of threads & queue.
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


        /**
         * This method is used to put an array of ET events into an ET system.
         * It allows coordination between multiple DataOutputHelper threads so that
         * event order is preserved.
         *
         * @param events the ET events to put back into ET system
         * @param inputOrder the order in which evio events were grabbed off Q
         * @param offset index into events array
         * @param events2Write number of events to write
         *
         * @throws InterruptedException if put or wait interrupted
         * @throws IOException ET communication error
         * @throws EtException will not happen
         * @throws EtDeadException ET system is dead
         * @throws EtClosedException ET connection was closed
         */
        private void writeEvents(EtEvent[] events, int inputOrder,
                                 int offset, int events2Write)
                throws InterruptedException, IOException, EtException,
                        EtDeadException, EtClosedException {

            if (dataOutputThreads.length > 1) {
                synchronized (lockOut) {
                    // Is the bank we grabbed next to be output? If not, wait.
                    while (inputOrder != outputOrder) {
                        lockOut.wait();
                    }

                    // put events back in ET system
//System.out.println("multithreaded put: array len = " + events.length + ", put " + events2Write +
//                     " # of events into ET");
//System.out.println("      DataChannel Et out helper: 4 " + name + " putEvents() ...");
                    etSystem.putEvents(attachment, events, offset, events2Write);

                    // next one to be put on output channel
                    outputOrder = ++outputOrder % Integer.MAX_VALUE;
                    lockOut.notifyAll();
                }
            }
            else {
//System.out.println("singlethreaded put: array len = " + events.length + ", put " + events2Write +
// " # of events into ET");
//System.out.println("      DataChannel Et out helper: 4 " + name + " putEvents() ...");
                etSystem.putEvents(attachment, events, offset, events2Write);
            }
        }


        /** {@inheritDoc} */
        @Override
        public void run() {
            runBanks();
        }


        public void runBanks() {

            // Tell the world I've started
            startLatch.countDown();

            try {
                EtEvent[] events;
                EventType previousType, pBankType;
                ControlType pBankControlType;
                QueueItem qItem;
                PayloadBank pBank;
                LinkedList<PayloadBank> bankList;
                boolean gotNothingYet;
                int nextEventIndex, thisEventIndex, pBankSize, listTotalSizeMax, etSize;
                int events2Write, eventArrayLen, myInputOrder=0;
                int[] recordIds = new int[chunk];

                // Create an array of lists of PayloadBank objects by 2-step
                // initialization to avoid "generic array creation" error.
                // Create one list for every possible ET event.
                LinkedList<PayloadBank>[] bankListArray = new LinkedList[chunk];
                for (int i=0; i < chunk; i++) {
                    bankListArray[i] = new LinkedList<PayloadBank>();
                }

                etSize = (int) etSystem.getEventSize();

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
                    events2Write = 0;
                    nextEventIndex = thisEventIndex = 0;
                    listTotalSizeMax = 32;   // first (or last) block header
                    previousType = null;
                    gotNothingYet = true;
                    bankList = bankListArray[nextEventIndex];

                    // If more than 1 output thread, need to sync things
                    if (dataOutputThreads.length > 1) {

                        synchronized (lockIn) {

                            // Because "haveOutputEndEvent" is set true only in this
                            // synchronized code, we can check for it upon entering.
                            // If found already, we can quit.
                            if (haveOutputEndEvent) {
System.out.println("      DataChannel Et out helper: " + name + " some thd got END event, quitting 1");
                                shutdown();
                                return;
                            }

                            // Grab a bank to put into an ET event buffer,
                            // checking occasionally to see if we got an
                            // RESET command or someone found an END event.
                            do {
                                // Get bank off of Q, unless we already did so in a previous loop
                                if (firstBankFromQueue != null) {
                                    pBank = firstBankFromQueue;
                                    firstBankFromQueue = null;
                                }
                                else {
                                    qItem = queue.poll(100L, TimeUnit.MILLISECONDS);
                                    // If wait longer than 100ms, and there are things to write,
                                    // send them to the ET system.
                                    if (qItem == null) {
                                        if (gotNothingYet) {
                                            continue;
                                        }
                                        break;
                                    }
                                    pBank = qItem.getPayloadBank();
                                }

                                gotNothingYet = false;
                                pBankType = pBank.getEventType();
                                pBankSize = pBank.getTotalBytes();
                                pBankControlType = pBank.getControlType();

                                // Assume worst case of one block header / bank
                                listTotalSizeMax += pBankSize + 32;

                                // This the first time through the while loop
                                if (previousType == null) {
                                    // Add bank to the list since there's always room for one
                                    bankList.add(pBank);

                                    // First time thru loop nextEventIndex = thisEventIndex,
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
                                else if ((previousType != pBankType) ||
                                        (listTotalSizeMax >= etSize)) {

//                                    if (listTotalSizeMax >= etSize) {
//                                        System.out.println("LIST IS TOO BIG, start another");
//                                    }

                                    // If we've already used up all the events,
                                    // write things out first. Be sure to store what we just
                                    // pulled off the Q to be the next bank!
                                    if (nextEventIndex >= eventArrayLen) {
//System.out.println("Used up " + nextEventIndex + " events, store bank for next round");
                                        firstBankFromQueue = pBank;
                                        break;
                                    }

                                    // Get new list
                                    bankList = bankListArray[nextEventIndex];
                                    // Add bank to new list
                                    bankList.add(pBank);

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
                                    bankList.add(pBank);
                                }

                                // Look for END event and mark it in attachment
                                //if (Evio.isEndEvent(pBank)) {
  // TODO: make sure this is OK
                                if (pBankControlType == ControlType.END) {
                                    pBank.setAttachment(Boolean.TRUE);
                                    haveOutputEndEvent = true;
System.out.println("      DataChannel Et out helper: " + name + " I got END event, quitting 2");
                                    // run callback saying we got end event
                                    if (endCallback != null) endCallback.endWait();
                                    break;
                                }

                                // Set this for next round
                                previousType = pBankType;
                                pBank.setAttachment(Boolean.FALSE);

                            } while (!gotResetCmd && (thisEventIndex < eventArrayLen));

                            // If I've been told to RESET ...
                            if (gotResetCmd) {
System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 2");
                                shutdown();
                                return;
                            }

                            myInputOrder = inputOrder;
                            inputOrder = ++inputOrder % Integer.MAX_VALUE;
                        }
                    }
                    else {
                        do {
                            if (firstBankFromQueue != null) {
                                pBank = firstBankFromQueue;
                                firstBankFromQueue = null;
                            }
                            else {
                                qItem = queue.poll(100L, TimeUnit.MILLISECONDS);
                                if (qItem == null) {
                                    if (gotNothingYet) {
                                        continue;
                                    }
                                    break;
                                }
                                pBank = qItem.getPayloadBank();
                            }

                            gotNothingYet = false;
                            pBankType = pBank.getEventType();
                            pBankSize = pBank.getTotalBytes();
                            listTotalSizeMax += pBankSize + 32;

                            if (previousType == null) {
                                bankList.add(pBank);

                                if (pBankType.isAnyPhysics() || pBankType.isROCRaw()) {
                                    recordIds[thisEventIndex] = recordId++;
                                }
                                else {
                                    recordIds[thisEventIndex] = -1;
                                }

                                nextEventIndex++;
                            }
                            else if ((previousType != pBankType) ||
                                     (listTotalSizeMax >= etSize)) {

                                if (nextEventIndex >= eventArrayLen) {
                                    firstBankFromQueue = pBank;
                                    break;
                                }

                                bankList = bankListArray[nextEventIndex];
                                bankList.add(pBank);

                                if (pBankType.isAnyPhysics() || pBankType.isROCRaw()) {
                                    recordIds[nextEventIndex] = recordId++;
                                }
                                else {
                                    recordIds[nextEventIndex] = -1;
                                }

                                thisEventIndex++;
                                nextEventIndex++;
                            }
                            else {
                                bankList.add(pBank);
                            }

                            if (Evio.isEndEvent(pBank)) {
                                pBank.setAttachment(Boolean.TRUE);
                                haveOutputEndEvent = true;
System.out.println("      DataChannel Et out helper: " + name + " I got END event, quitting 3");
                                if (endCallback != null) endCallback.endWait();
                                break;
                            }

                            previousType = pBankType;
                            pBank.setAttachment(Boolean.FALSE);

                        } while (!gotResetCmd && (thisEventIndex < eventArrayLen));

                        if (gotResetCmd) {
System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 3");
                            shutdown();
                            return;
                        }
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
                            int bankWrittenSize = bankList.getFirst().getTotalBytes() + 64;
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
                                    if (haveInputEndEvent) {
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
                        events[i].setByteOrder(bankList.getFirst().getByteOrder());

                        // CODA owns first select int which contains source id
                        int[] selects = events[i].getControl();
                        selects[0] = id; // id in ROC output channel
                        events[i].setControl(selects);

                        // Write banks' data into ET buffer in separate thread
                        EvWriter writer = new EvWriter(bankList, events[i], recordIds[i]);
                        writeThreadPool.execute(writer);

                        // Keep track of how many ET events we want to write
                        events2Write++;

                        // Handle END event ...
                        for (PayloadBank bank : bankList) {
                            if (bank.getAttachment() == Boolean.TRUE) {
                                // There should be no more events coming down the pike so
                                // go ahead write out events and then shut this thread down.
                                break;
                            }
                        }
                    }

                    // Wait for all events to finish processing
                    latch.await();

                    try {
//System.out.println("      DataChannel Et: write " + events2Write + " events");
                        // Put events back in ET system
                        writeEvents(events, myInputOrder, 0, events2Write);

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





        public void runBuffers() {

            // Tell the world I've started
            startLatch.countDown();

            try {
                EtEvent[] events;
                ControlType pBufferControlType;
                EventType previousType, pBufferType;
                QueueItem qItem;
                PayloadBuffer pBuffer;
                LinkedList<PayloadBuffer> bufferList;
                boolean gotNothingYet;
                int nextEventIndex, thisEventIndex, pBufferSize, listTotalSizeMax, etSize;
                int events2Write, eventArrayLen, myInputOrder=0;
                int[] recordIds = new int[chunk];

                // Create an array of lists of PayloadBuffer objects by 2-step
                // initialization to avoid "generic array creation" error.
                // Create one list for every possible ET event.
                LinkedList<PayloadBuffer>[]bufferListArray = new LinkedList[chunk];
                for (int i=0; i < chunk; i++) {
                    bufferListArray[i] = new LinkedList<PayloadBuffer>();
                }

                etSize = (int) etSystem.getEventSize();

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
                        bufferListArray[j].clear();
                    }

                    // Init variables
                    events2Write = 0;
                    nextEventIndex = thisEventIndex = 0;
                    listTotalSizeMax = 32;   // first (or last) block header
                    previousType = null;
                    gotNothingYet = true;
                    bufferList = bufferListArray[nextEventIndex];

                    // If more than 1 output thread, need to sync things
                    if (dataOutputThreads.length > 1) {

                        synchronized (lockIn) {

                            // Because "haveOutputEndEvent" is set true only in this
                            // synchronized code, we can check for it upon entering.
                            // If found already, we can quit.
                            if (haveOutputEndEvent) {
System.out.println("      DataChannel Et out helper: " + name + " some thd got END event, quitting 1");
                                shutdown();
                                return;
                            }

                            // Grab a buffer to put into an ET event buffer,
                            // checking occasionally to see if we got an
                            // RESET command or someone found an END event.
                            do {
                                // Get buffer off of Q, unless we already did so in a previous loop
                                if (firstBufferFromQueue != null) {
                                    pBuffer = firstBufferFromQueue;
                                    firstBufferFromQueue = null;
                                }
                                else {
                                    qItem = queue.poll(100L, TimeUnit.MILLISECONDS);
                                    // If wait longer than 100ms, and there are things to write,
                                    // send them to the ET system.
                                    if (qItem == null) {
                                        if (gotNothingYet) {
                                            continue;
                                        }
                                        break;
                                    }
                                    pBuffer = qItem.getBuffer();
                                }

                                gotNothingYet = false;
                                pBufferType = pBuffer.getEventType();
                                pBufferSize = pBuffer.getTotalBytes();
                                pBufferControlType = pBuffer.getControlType();

                                // Assume worst case of one block header / bank
                                listTotalSizeMax += pBufferSize + 32;

                                // This the first time through the while loop
                                if (previousType == null) {
                                    // Add bank to the list since there's always room for one
                                    bufferList.add(pBuffer);

                                    // First time thru loop nextEventIndex = thisEventIndex,
                                    // at least until it gets incremented below.
                                    //
                                    // Set recordId depending on what type this bank is
                                    if (pBufferType.isAnyPhysics() || pBufferType.isROCRaw()) {
                                        recordIds[thisEventIndex] = recordId++;
                                    }
                                    else {
                                        recordIds[thisEventIndex] = -1;
                                    }

                                    // Index of next list
                                    nextEventIndex++;
                                }

                                // Is this buffer a diff type as previous buffer?
                                // Will it not fit into the ET event?
                                // In both these cases start using a new list.
                                else if ((previousType != pBufferType) ||
                                        (listTotalSizeMax >= etSize)) {

//                                    if (listTotalSizeMax >= etSize) {
//                                        System.out.println("LIST IS TOO BIG, start another");
//                                    }

                                    // If we've already used up all the events,
                                    // write things out first. Be sure to store what we just
                                    // pulled off the Q to be the next buffer!
                                    if (nextEventIndex >= eventArrayLen) {
//System.out.println("Used up " + nextEventIndex + " events, store buffer for next round");
                                        firstBufferFromQueue = pBuffer;
                                        break;
                                    }

                                    // Get new list
                                    bufferList = bufferListArray[nextEventIndex];
                                    // Add buffer to new list
                                    bufferList.add(pBuffer);

                                    // Set recordId depending on what type this buffer is
                                    if (pBufferType.isAnyPhysics() || pBufferType.isROCRaw()) {
                                        recordIds[nextEventIndex] = recordId++;
                                    }
                                    else {
                                        recordIds[nextEventIndex] = -1;
                                    }

                                    // Index of this & next lists
                                    thisEventIndex++;
                                    nextEventIndex++;
                                }
                                // It's OK to add this buffer to the existing list.
                                else {
                                    // Add buf to list since there's room and it's the right type
                                    bufferList.add(pBuffer);
                                }

                                // Look for END event and mark it in attachment
                                //if (Evio.isEndEvent(pBuffer)) {
  // TODO: make sure this is OK
                                if (pBufferControlType == ControlType.END) {
                                        pBuffer.setAttachment(Boolean.TRUE);
                                    haveOutputEndEvent = true;
System.out.println("      DataChannel Et out helper: " + name + " I got END event, quitting 2");
                                    // run callback saying we got end event
                                    if (endCallback != null) endCallback.endWait();
                                    break;
                                }

                                // Set this for next round
                                previousType = pBufferType;
                                pBuffer.setAttachment(Boolean.FALSE);

                            } while (!gotResetCmd && (thisEventIndex < eventArrayLen));

                            // If I've been told to RESET ...
                            if (gotResetCmd) {
System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 2");
                                shutdown();
                                return;
                            }

                            myInputOrder = inputOrder;
                            inputOrder = ++inputOrder % Integer.MAX_VALUE;
                        }
                    }
                    else {
                        do {
                            if (firstBufferFromQueue != null) {
                                pBuffer = firstBufferFromQueue;
                                firstBufferFromQueue = null;
                            }
                            else {
                                qItem = queue.poll(100L, TimeUnit.MILLISECONDS);
                                if (qItem == null) {
                                    if (gotNothingYet) {
                                        continue;
                                    }
                                    break;
                                }
                                pBuffer = qItem.getBuffer();
                            }

                            gotNothingYet = false;
                            pBufferType = pBuffer.getEventType();
                            pBufferSize = pBuffer.getTotalBytes();
                            pBufferControlType = pBuffer.getControlType();
                            listTotalSizeMax += pBufferSize + 32;

                            if (previousType == null) {
                                bufferList.add(pBuffer);

                                if (pBufferType.isAnyPhysics() || pBufferType.isROCRaw()) {
                                    recordIds[thisEventIndex] = recordId++;
                                }
                                else {
                                    recordIds[thisEventIndex] = -1;
                                }

                                nextEventIndex++;
                            }
                            else if ((previousType != pBufferType) ||
                                    (listTotalSizeMax >= etSize)) {

                                if (nextEventIndex >= eventArrayLen) {
                                    firstBufferFromQueue = pBuffer;
                                    break;
                                }

                                bufferList = bufferListArray[nextEventIndex];
                                bufferList.add(pBuffer);

                                if (pBufferType.isAnyPhysics() || pBufferType.isROCRaw()) {
                                    recordIds[nextEventIndex] = recordId++;
                                }
                                else {
                                    recordIds[nextEventIndex] = -1;
                                }

                                thisEventIndex++;
                                nextEventIndex++;
                            }
                            else {
                                bufferList.add(pBuffer);
                            }

                            if (pBufferControlType == ControlType.END) {
                                pBuffer.setAttachment(Boolean.TRUE);
                                haveOutputEndEvent = true;
System.out.println("      DataChannel Et out helper: " + name + " I got END event, quitting 3");
                                if (endCallback != null) endCallback.endWait();
                                break;
                            }

                            previousType = pBufferType;
                            pBuffer.setAttachment(Boolean.FALSE);

                        } while (!gotResetCmd && (thisEventIndex < eventArrayLen));

                        if (gotResetCmd) {
System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 3");
                            shutdown();
                            return;
                        }
                    }

//logger.info("      DataChannel Et DataOutputHelper : nextEvIndx = " + nextEventIndex +
//                    ", evArrayLen = " + eventArrayLen);

                    latch = new CountDownLatch(nextEventIndex);

                    // For each ET event that can be filled with something ...
                    for (int i=0; i < nextEventIndex; i++) {
                        // Get one of the list of banks to put into this ET event
                        bufferList = bufferListArray[i];

                        if (bufferList.size() < 1) {
                            continue;
                        }

                        // Check to see if not enough room in ET event to hold bank.
                        // In this case, list will only contain 1 (big) bank.
                        if (bufferList.size() == 1) {
                            // Max # of bytes to write this bank into buffer
                            int bankWrittenSize = bufferList.getFirst().getTotalBytes() + 64;
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
                                    if (haveInputEndEvent) {
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
                        events[i].setByteOrder(bufferList.getFirst().getByteOrder());

                        // CODA owns first select int which contains source id
                        int[] selects = events[i].getControl();
                        selects[0] = id; // id in ROC output channel
                        events[i].setControl(selects);

                        // Write banks' data into ET buffer in separate thread
                        EvWriter writer = new EvWriter(bufferList, events[i], recordIds[i], 0);
                        writeThreadPool.execute(writer);

                        // Keep track of how many ET events we want to write
                        events2Write++;

                        // Handle END event ...
                        for (PayloadBuffer buf : bufferList) {
                            if (buf.getAttachment() == Boolean.TRUE) {
                                // There should be no more events coming down the pike so
                                // go ahead write out events and then shut this thread down.
                                break;
                            }
                        }
                    }

                    // Wait for all events to finish processing
                    latch.await();

                    try {
//System.out.println("      DataChannel Et: write " + events2Write + " events");
                        // Put events back in ET system
                        writeEvents(events, myInputOrder, 0, events2Write);

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
            private LinkedList<PayloadBank> bankList;

            /** List of evio banks in ByteBuffer format to write. */
            private LinkedList<PayloadBuffer> bufferList;

            /** ET event in which to write banks. */
            private EtEvent event;

            /** ET event's data buffer. */
            private ByteBuffer etBuffer;

            /** Object for writing banks into ET data buffer. */
            private EventWriter evWriter;

            /** Object for writing bank buffers into ET data buffer. */
            private EvioCompactEventWriter evCompactWriter;

            /**
             * Encode the event type into the bit info word
             * which will be in each evio block header.
             *
             * @param bSet bit set which will become part of the bit info word
             * @param type event type to be encoded
             */
            void setEventType(BitSet bSet, int type) {
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
             * @param bankList list of banks to be written into a single ET event
             * @param event ET event in which to place the banks
             * @param myRecordId value of starting block header's block number
             */
            EvWriter(LinkedList<PayloadBank> bankList, EtEvent event, int myRecordId) {
                this.event = event;
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
                    setEventType(bitInfo, bankList.getFirst().getEventType().getValue());

                    // Create object to write evio banks into ET buffer
                    evWriter = new EventWriter(etBuffer, 550000, 200, null, bitInfo, emu.getCodaid());
                    evWriter.setStartingBlockNumber(myRecordId);
                }
                catch (EvioException e) {/* never happen */}
            }

            /**
             * Constructor.
             * @param bufferList list of buffers to be written into a single ET event
             * @param event ET event in which to place the banks
             * @param myRecordId value of starting block header's block number
             * @param junk used only to distinguish constructors
             */
            EvWriter(LinkedList<PayloadBuffer> bufferList, EtEvent event, int myRecordId, int junk) {
                this.event = event;
                this.bufferList = bufferList;

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
                    setEventType(bitInfo, bankList.getFirst().getEventType().getValue());

                    // Create object to write evio banks into ET buffer
                    evCompactWriter = new EvioCompactEventWriter(etBuffer, 550000, 200, null);
                    evCompactWriter.setStartingBlockNumber(myRecordId);
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
                    if (bankList != null) {
                        for (PayloadBank bank : bankList) {
                            evWriter.writeEvent(bank);
                        }
                    }
                    else {
                        for (PayloadBuffer pBuf : bufferList) {
                            evCompactWriter.writeEvent(pBuf.getBuffer());
                        }
                    }

                    evWriter.close();
                    // Be sure to set the length to bytes of data actually written
                    event.setLength(etBuffer.position());
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



}