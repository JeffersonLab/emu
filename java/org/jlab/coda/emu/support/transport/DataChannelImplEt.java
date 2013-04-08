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


import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.et.enums.Modify;
import org.jlab.coda.et.exception.*;
import org.jlab.coda.jevio.*;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class implement a data channel which gets data from
 * or sends data to an ET system.
 *
 * @author timmer
 * Dec 2, 2009
 */
public class DataChannelImplEt implements DataChannel {


    /** EMU object that created this channel. */
    private Emu emu;

    /** Logger associated with this EMU. */
    private Logger logger;

    /** Transport object that created this channel. */
    private final DataTransportImplEt dataTransport;

    /** Channel name */
    private final String name;

    /** Channel id (corresponds to sourceId of ROCs for CODA event building). */
    private int id;

    /** Number of writing threads to ask for in copying data from banks to ET events. */
    private int writeThreadCount;

    /** Number of data output helper threads each of which has a pool of writeThreadCount. */
    private int outputThreadCount;

    /** Number of data input helper threads. */
    private int inputThreadCount;

    /** Field queue - filled buffer queue */
    private final BlockingQueue<QueueItem> queue;

    /** Array of threads used to input data. */
    private DataInputHelper[] dataInputThreads;

    /** Array of threads used to output data. */
    private DataOutputHelper[] dataOutputThreads;

    /** Byte order of output data (input data's order is specified in msg). */
    private ByteOrder byteOrder;

    // OUTPUT

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

    // INPUT

    /** Order number of next list of evio banks to be put onto Q. */
    private int outputOrderIn;
    /** Order of array of ET events read from the ET system. */
    private int inputOrderIn;
    /** Synchronize getting ET events for multiple DataInputHelpers. */
    private Object lockIn2  = new Object();
    /** Synchronize putting evio banks onto Q for multiple DataInputHelpers. */
    private Object lockOut2 = new Object();

    // ----

    /** Use the evio block header's block number as a record id. */
    private int recordId;

    /** Do we pause the dataThread? */
    private volatile boolean pause;

    /** Is this channel an input (true) or output (false) channel? */
    private boolean input;

    /** Read END event from input queue. */
    private volatile boolean haveInputEndEvent;

    /** Read END event from output queue. */
    private volatile boolean haveOutputEndEvent;

    /** Got END command from Run Control. */
    private volatile boolean gotEndCmd;

    /** Got RESET command from Run Control. */
    private volatile boolean gotResetCmd;


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
     * Constructor to create a new DataChannelImplEt instance.
     * Used only by {@link DataTransportImplEt#createChannel(String, java.util.Map, boolean, org.jlab.coda.emu.Emu)}
     * which is only used during PRESTART in {@link org.jlab.coda.emu.EmuModuleFactory}.
     *
     * @param name        the name of this channel
     * @param transport   the DataTransport object that this channel belongs to
     * @param attrib      the hashmap of config file attributes for this channel
     * @param input       true if this is an input data channel, otherwise false
     * @param emu         emu this channel belongs to
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplEt(String name, DataTransportImplEt transport,
                      Map<String, String> attrib, boolean input,
                      Emu emu)
            throws DataTransportException {

        this.emu = emu;
        this.name = name;
        this.input = input;
        this.dataTransport = transport;
        logger = emu.getLogger();

logger.info("      DataChannel Et : creating channel " + name);

        // set queue capacity
        int capacity = 100;    // 100 buffers * 100 events/buf * 220 bytes/Roc/ev =  2.2Mb/Roc
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
        }
        queue = new LinkedBlockingQueue<QueueItem>(capacity);


        // Set id number. Use any defined in config file else use default (0)
        id = 0;
        String attribString = attrib.get("id");
        if (attribString != null) {
            try {
                id = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {  }
        }

        // size of TCP send buffer (0 means use operating system default)
        int tcpSendBuf = 0;
        attribString = attrib.get("sendBuf");
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
        attribString = attrib.get("recvBuf");
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
        attribString = attrib.get("noDelay");
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
            EtSystemOpenConfig openConfig = new EtSystemOpenConfig(dataTransport.getOpenConfig());
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
        attribString = attrib.get("wthreads");
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
        attribString = attrib.get("ithreads");
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
        attribString = attrib.get("othreads");
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
        attribString = attrib.get("chunk");
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
        attribString = attrib.get("group");
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
        stationName = attrib.get("stationName");
//logger.info("      DataChannel Et : station name = " + stationName);


        // Set station position. Use any defined in config file else use default (1)
        attribString = attrib.get("position");
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

                String filter = attrib.get("idFilter");
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

            // set endianness of data
            byteOrder = ByteOrder.BIG_ENDIAN;
            try {
                String order = attrib.get("endian");
                if (order != null && order.equalsIgnoreCase("little")) {
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                }
            } catch (Exception e) {
                logger.info("      DataChannel Et : no output data endianness specified, default to big.");
            }

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
                    catch (EtException e) { /* never happen */}
                 }
                catch (Exception e) {/* never happen */}
            }
       }

        // start up thread to help with input or output
        openEtSystem();
        startHelper();
    }


    /** {@inheritDoc} */
    public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    public int getID() {
        return id;
    }

    /** {@inheritDoc} */
    public boolean isInput() {
        return input;
    }

    /** {@inheritDoc} */
    public DataTransportImplEt getDataTransport() {
        return dataTransport;
    }

    /**
     * Get the ET sytem object.                                                                         , e
     * @return the ET system object.
     */
    public void openEtSystem() throws DataTransportException {
        try {
//System.out.println("      DataChannel Et: try to open" + dataTransport.getOpenConfig().getEtName() );
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

    /** {@inheritDoc} */
    public QueueItem receive() throws InterruptedException {
        return queue.take();
    }

    /** {@inheritDoc} */
    public void send(QueueItem item) {
        try {
            queue.put(item);   // blocks if capacity reached
            //queue.add(item);   // throws exception if capacity reached
            //queue.offer(item); // returns false if capacity reached
        }
        catch (InterruptedException e) {/* ignore */}
    }

    /** {@inheritDoc} */
    public void close() {
        logger.warn("      DataChannel Et close() : " + name + " - closing this channel (close ET system)");

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
//System.out.println("      DataChannelImplEt.close : waiting for helper threads to end ...");
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
            etSystem.detach(attachment);
            if (!stationName.equals("GRAND_CENTRAL")) {
                etSystem.removeStation(station);
            }
            etSystem.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        queue.clear();
//System.out.println("      close() is done");
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
                // still in progress which may give you a seg fault
                // in the JNI code. Give it 25% more time than the wait.
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
                // is still in progress which may give you a seg fault
                // in the JNI code. Give it 25% more time than the wait.
                try {dataOutputThreads[i].join(1000);}
                catch (InterruptedException e) {}
//System.out.println("        output thread done");
            }
        }

        // At this point all threads should be done
        try {
            etSystem.detach(attachment);
System.out.println("        " + attachment.getId() + "in, detached from ET");
            if (!stationName.equals("GRAND_CENTRAL")) {
//System.out.println("        remove " + station.getName() + " station");
                etSystem.removeStation(station);
            }
            etSystem.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        queue.clear();
logger.debug("      DataChannel Et reset() : " + name + " - done");
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
            catch (InterruptedException e) {
            }
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


        /** {@inheritDoc} */
        @Override
        public void run() {

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

                EvioReader reader;
                ByteBuffer buf;

                while ( etSystem.alive() ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
logger.warn("      DataChannel Et in helper: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // Get events while checking periodically to see if we must go away
                    synchronized (lockIn2) {
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
                            // Speed things up since no EvioListeners are used - doesn't do much
                            reader.getParser().setNotificationActive(false);
                            // First block header in ET buffer
                            IBlockHeader blockHeader = reader.getCurrentBlockHeader();
                            evioVersion = blockHeader.getVersion();
                            if (evioVersion < 4) {
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
                                    break;
                                }
                            }

                            // Write any existing banks
                            writeEvents(payloadBanks, myInputOrder++);

                            if (haveInputEndEvent) {
                                break;
                            }
                        }
                        catch (EvioException e) {
                            // if ET event data NOT in evio format, skip over it
                            e.printStackTrace();
                            logger.error("        DataChannel Et in helper: " + name +
                                         " ET event data is NOT evio format, skip");
                        }
                    }

                    // put all events back in ET system - even those unused
//System.out.println("      DataChannel Et in helper: 4 " + name + " putEvents() ...");
                    etSystem.putEvents(attachment, events);

                    if (haveInputEndEvent) {
logger.info("      DataChannel Et in helper: have END, " + name + " quit thd");
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Et in helper: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
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
            catch (InterruptedException e) {
            }
        }


        /** Stop all this object's threads. */
        private void shutdown() {
            // Cancel queued jobs and call interrupt on executing threads
            getThreadPool.shutdown();
            writeThreadPool.shutdown();

            // If any EvGetter thread is stuck on etSystem.newEvents(), unstuck it
            try {
                etSystem.wakeUpAttachment(attachment);
                // It may take 0.2 sec to detach
                Thread.sleep(250);
            }
            catch (InterruptedException e) {
            }
            catch (IOException e) {
            }
            catch (EtException e) {
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


        /** Shutdown all the thread pools. */
        private void shutdownOrig() {
            getThreadPool.shutdown();
            writeThreadPool.shutdown();

            // Make sure these threads are finished so we can close the ET system
            try {
                boolean success = getThreadPool.awaitTermination(100L, TimeUnit.MILLISECONDS);
                if (!success) {
                    // May be blocked on getBarrier.await()
                    getBarrier.reset();
                }
            }
            catch (InterruptedException e) {}

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

            // Tell the world I've started
            startLatch.countDown();

            try {
                EtEvent[] events;
                EventType previousType, pBanktype;
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
                                shutdown();
System.out.println("      DataChannel Et out helper: " + name + " some thd got END event, quitting 1");
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
                                pBanktype = pBank.getEventType();
                                pBankSize = pBank.getTotalBytes();
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
                                    if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
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
                                else if ((previousType != pBanktype) ||
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
                                    if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
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
                                if (Evio.isEndEvent(pBank)) {
                                    pBank.setAttachment(Boolean.TRUE);
                                    haveOutputEndEvent = true;
System.out.println("      DataChannel Et out helper: " + name + " I got END event, quitting 2");
                                    break;
                                }

                                // Set this for next round
                                previousType = pBanktype;
                                pBank.setAttachment(Boolean.FALSE);

                            } while (!gotResetCmd && (thisEventIndex < eventArrayLen));

                            // If I've been told to RESET ...
                            if (gotResetCmd) {
                                shutdown();
System.out.println("      DataChannel Et out helper: " + name + " got RESET cmd, quitting 2");
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
                            pBanktype = pBank.getEventType();
                            pBankSize = pBank.getTotalBytes();
                            listTotalSizeMax += pBankSize + 32;

                            if (previousType == null) {
                                bankList.add(pBank);

                                if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
                                    recordIds[thisEventIndex] = recordId++;
                                }
                                else {
                                    recordIds[thisEventIndex] = -1;
                                }

                                nextEventIndex++;
                            }
                            else if ((previousType != pBanktype) ||
                                     (listTotalSizeMax >= etSize)) {

                                if (nextEventIndex >= eventArrayLen) {
                                    firstBankFromQueue = pBank;
                                    break;
                                }

                                bankList = bankListArray[nextEventIndex];
                                bankList.add(pBank);

                                if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
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
                                break;
                            }

                            previousType = pBanktype;
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
                                etSystem.dumpEvents(attachment, new EtEvent[]{events[i]});
//System.out.println("      DataChannel Et out helper: 4 " + name + " newEvents() ...");
                                EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, false,
                                                                    0, 1, bankWrittenSize, group);
                                events[i] = evts[0];
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

                    // Put events back in ET system
//System.out.println("      DataChannel Et: write " + events2Write + " events");
                    writeEvents(events, myInputOrder, 0, events2Write);

                    // Dump any left over new ET events.
                    if (events2Write < eventArrayLen) {
//System.out.println("Dumping " + (eventArrayLen - events2Write) + " unused new events");
                        etSystem.dumpEvents(attachment, events, events2Write, (eventArrayLen - events2Write));
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
            /** ET event in which to write banks. */
            private EtEvent     event;
            /** ET event's data buffer. */
            private ByteBuffer  buffer;
            /** Object for writing banks into ET data buffer. */
            private EventWriter evWriter;

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
                    buffer = event.getDataBuffer();
                    buffer.clear();
                    buffer.order(byteOrder);

                    // Encode the event type into bits
                    BitSet bitInfo = new BitSet(24);
                    setEventType(bitInfo, bankList.getFirst().getEventType().getValue());

                    // Create object to write evio banks into ET buffer
                    evWriter = new EventWriter(buffer, 550000, 200, null, bitInfo, emu.getCodaid());
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
                    for (PayloadBank bank : bankList) {
                        evWriter.writeEvent(bank);
                    }
                    evWriter.close();
                    // Be sure to set the length to bytes of data actually written
                    event.setLength(buffer.position());
                    // Tell the DataOutputHelper thread that we're done
                    latch.countDown();
                }
                catch (EvioException e) {
                    e.printStackTrace();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                catch (EtException e) {
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
                try {
                    events = null;
//System.out.println("      DataChannel Et out helper: 4 " + name + " newEvents() ...");
                    events = etSystem.newEvents(attachment, Mode.SLEEP, false, 0,
                                                chunk, (int)etSystem.getEventSize(), group);
//System.out.println("I got " + events.length + " new events");
                    barrier.await();
                }
                catch (BrokenBarrierException e) {
                    // may happen when ending or resetting
                }
                catch (InterruptedException e) {
                    // told to quit when in barrier.await()
                }
                catch (Exception e) {
                    // ET system problem - run will come to an end
System.out.println(e.getMessage());
                }
            }
        }


    }


    /**
     * For input channel, start the DataInputHelper thread which takes ET events,
     * parses each, puts the events back into the ET system, and puts the parsed
     * evio banks onto the queue.<p>
     * For output channel, start the DataOutputHelper thread which takes a bank from
     * the queue, puts it into a new ET event and puts that into the ET system.
     */
    public void startHelper() {
        if (input) {
            dataInputThreads = new DataInputHelper[inputThreadCount];

            for (int i=0; i < inputThreadCount; i++) {
                DataInputHelper helper = new DataInputHelper(emu.getThreadGroup(),
                                                 getName() + " data in" + i);
                dataInputThreads[i] = helper;
                dataInputThreads[i].start();
                helper.waitUntilStarted();
            }
        }
        else {
            dataOutputThreads = new DataOutputHelper[outputThreadCount];

            for (int i=0; i < outputThreadCount; i++) {
                DataOutputHelper helper = new DataOutputHelper(emu.getThreadGroup(),
                                                               getName() + " data out" + i);
                dataOutputThreads[i] = helper;
                dataOutputThreads[i].start();
                helper.waitUntilStarted();
            }
        }
    }

    /** Pause the DataInputHelper or DataOutputHelper thread. */
    public void pauseHelper() {
        pause = true;
    }

    /** Resume running the DataInputHelper or DataOutputHelper thread. */
    public void resumeHelper() {
        pause = false;
    }

    /** {@inheritDoc} */
    public BlockingQueue<QueueItem> getQueue() {
        return queue;
    }


}