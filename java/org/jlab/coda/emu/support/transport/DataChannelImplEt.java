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


import org.jlab.coda.emu.support.data.EventType;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.emu.support.data.PayloadBank;
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

    /** Number of output threads each of which has a pool of writeThreadCount. */
    private int outputThreadCount;
    private int inputThreadCount;

    /** Field queue - filled buffer queue */
    private final BlockingQueue<EvioBank> queue;

    /** Field dataThread */
    private Thread[] dataInputThreads;
    private Thread[] dataOutputThreads;

    /** Byte order of output data (input data's order is specified in msg). */
    private ByteOrder byteOrder;

    /** Map of config file attributes. */
    private Map<String, String> attributeMap;

    private int outputOrder;
    private int inputOrder;
    private Object lockIn  = new Object();
    private Object lockOut = new Object();
    private DataOutputHelper[] outputHelpers;

    private Object lockIn2  = new Object();
    private Object lockOut2 = new Object();
    private int outputOrderIn;
    private int inputOrderIn;
    private DataInputHelper[] inputHelpers;

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

//    /** Enforce evio block header numbers to be sequential? */
//    boolean blockNumberChecking;

    /** Group sequential events on the output queue into a single ET buffer. */
    boolean autoGroupOutput;


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
    private int etWaitTime = 1000000;



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
        this.attributeMap = attrib;
        this.dataTransport = transport;
        logger = emu.getLogger();

logger.info("      DataChannel Et : creating channel " + name);

        // set queue capacity
        int capacity = 100;    // 100 buffers * 100 events/buf * 220 bytes/Roc/ev =  2.2Mb/Roc
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
        }
        queue = new LinkedBlockingQueue<EvioBank>(capacity);


        // Set id number. Use any defined in config file else use default (0)
        id = 0;
        String attribString = attributeMap.get("id");
        if (attribString != null) {
            try {
                id = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {  }
        }
//logger.info("      DataChannel Et : id = " + id);


//        // set option whether or not to enforce evio
//        // block header numbers to be sequential
//        attribString = attrib.get("blockNumCheck");
//        if (attribString != null) {
//            if (attribString.equalsIgnoreCase("true") ||
//                attribString.equalsIgnoreCase("on")   ||
//                attribString.equalsIgnoreCase("yes"))   {
//                blockNumberChecking = true;
//            }
//        }

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

        // How may data writing threads at a time?
        writeThreadCount = 1;
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

        // How may groups of data writing threads at a time?
        inputThreadCount = 1;
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

        // How may groups of data writing threads at a time?
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

            // set endianness of data
            byteOrder = ByteOrder.BIG_ENDIAN;
            try {
                String order = attributeMap.get("endian");
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
        // TODO: race condition, should make sure threads are started before returning
    }

    public String getName() {
        return name;
    }

    public int getID() {
        return id;
    }

    public boolean isInput() {
        return input;
    }

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
//logger.info("      DataChannel Et: created or found station = " + stationName);

            // attach to station
            attachment = etSystem.attach(station);
//logger.info("      DataChannel Et: attached to station " + stationName);
        }
        catch (Exception e) {
            throw new DataTransportException("cannot open ET system", e);
        }
    }

    public EvioBank receive() throws InterruptedException {
        return queue.take();
    }

    public void send(EvioBank bank) {
        try {
            queue.put(bank);   // blocks if capacity reached
            //queue.add(bank);   // throws exception if capacity reached
            //queue.offer(bank); // returns false if capacity reached
        }
        catch (InterruptedException e) {/* ignore */}
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
        logger.warn("      DataChannel Et close() : " + name + " - closing this channel (close ET system)");

        gotEndCmd = true;
        gotResetCmd = false;

        // Do NOT interrupt threads which are communicating with the ET server.
        // This will mess up all future communications !!!

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
//System.out.println("        try joining output thread #" + i + " ...");
                    dataOutputThreads[i].join(waitTime);
                    // kill it if not already dead since we waited as long as possible
                    dataOutputThreads[i].interrupt();
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
    }


    /**
     * {@inheritDoc}
     * Reset this channel by interrupting the data sending threads and closing ET system.
     */
    public void reset() {
        logger.warn("      DataChannel Et reset() : " + name + " - closing this channel (close ET system)");

        gotEndCmd   = false;
        gotResetCmd = true;

        // Don't close ET system until helper threads are done
        if (dataInputThreads != null) {
            for (int i=0; i < inputThreadCount; i++) {
//System.out.println("        interrupt input thread #" + i + " ...");
                dataInputThreads[i].interrupt();
//System.out.println("        in thread done");
            }
        }

        if (dataOutputThreads != null) {
            for (int i=0; i < outputThreadCount; i++) {
//System.out.println("        interrupt output thread #" + i + " ...");
                dataOutputThreads[i].interrupt();
//System.out.println("        out thread done");
            }
        }
//System.out.println("      helper thds interrupted");

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
    }



    /**
     * Class used to get ET events, parse them into Evio banks,
     * and put them onto a Q.
     */
    private class DataInputHelper implements Runnable {

        private EtEvent[] events;
        private int pauseCounter = 0;


        /** Constructor. */
        DataInputHelper () {
        }


        /**
         * This method is used to put an Evio bank (Data Transport Record)
         * onto a queue. It allows coordination between multiple DataInputHelper
         * threads so that event order is preserved.
         *
         * @param bank the Evio bank to put on the queue
         * @param order the record Id of the DTR bank taken from the ET event
         * @throws InterruptedException if put or wait interrupted
         */
        private void writeEvents(EvioBank bank, int order)
                throws InterruptedException {

            synchronized (lockOut2) {
                // Is the bank we grabbed next to be output? If not, wait.
                while (order != outputOrderIn) {
                    lockOut2.wait();
                }

                // put events back in ET system
                queue.put(bank);

                // next one to be put on output channel
                outputOrderIn = ++outputOrderIn % Integer.MAX_VALUE;
                lockOut2.notifyAll();
            }
        }


        /**
         * This method is used to put a list of PayloadBank objects
         * onto a queue. It allows coordination between multiple DataInputHelper
         * threads so that event order is preserved.
         *
         * @param banks a list of payload banks to put on the queue
         * @param order the record Id of the DTR bank taken from the ET event
         * @throws InterruptedException if put or wait interrupted
         */
        private void writeEvents(List<PayloadBank> banks, int order)
                throws InterruptedException {

            synchronized (lockOut2) {
                // Is the bank we grabbed next to be output? If not, wait.
                while (order != outputOrderIn) {
                    lockOut2.wait();
                }

                // put events back in ET system
                for (PayloadBank bank : banks) {
                    queue.put(bank);
                }

                // next one to be put on output channel
                outputOrderIn = ++outputOrderIn % Integer.MAX_VALUE;
                lockOut2.notifyAll();
            }
        }


        /** Method run ... */
        public void run() {

            try {

                EvioBank bank;
                PayloadBank payloadBank;
                LinkedList<PayloadBank> payloadBanks = new LinkedList<PayloadBank>();
                int myInputOrder, evioVersion, payloadCount, sourceId;
                BlockHeaderV4 header4;
                EventType type;

                EvioReader reader;
                ByteBuffer buf;

                while ( etSystem.alive() ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
logger.warn("      DataChannel Et : " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // Get events while checking periodically to see if we must go away
                    synchronized (lockIn2) {
                        try {
                            events = etSystem.getEvents(attachment, Mode.TIMED,
                                                        Modify.NOTHING, etWaitTime, chunk);
                            // Keep track of the order in which events are grabbed
                            // in order to preserve event order with multiple threads.
                            myInputOrder = inputOrderIn;
                            inputOrderIn = (inputOrderIn + events.length) % Integer.MAX_VALUE;
                        }
                        catch (EtTimeoutException e) {
                            if (haveInputEndEvent) {
System.out.println("      DataChannel Et : " + name + " have END, quitting");
                                return;
                            }
                            else if (gotResetCmd) {
System.out.println("      DataChannel Et : " + name + " got RESET, quitting");
                                return;
                            }
                            Thread.sleep(5);
                            continue;
                        }
                    }


                    for (EtEvent ev : events) {
                        buf = ev.getDataBuffer();

//                        if (ev.needToSwap()) {
////System.out.println("      DataChannel Et : " + name + " SETTING byte order to LITTLE endian");
//                            buf.order(ByteOrder.LITTLE_ENDIAN);
//                        }

                        try {
                            reader = new EvioReader(buf);
//                            // Have reader throw an exception if evio
//                            // block numbers are not sequential.
//                            if (blockNumberChecking) {
//logger.info("      DataChannel Et : " + name + " have evio check block # sequence");
//                                reader.checkBlockNumberSequence(true);
//                            }
                            // Speed things up since no EvioListeners are used - doesn't do much
                            reader.getParser().setNotificationActive(false);
                            IBlockHeader blockHeader = reader.getCurrentBlockHeader();
                            evioVersion = blockHeader.getVersion();
                            if (evioVersion < 4) {
                                throw new EvioException("Evio data needs to be written in version 4+ format");
                            }
                            header4      = (BlockHeaderV4)blockHeader;
                            type         = EventType.getEventType(header4.getEventType());
//logger.info("      DataChannel Et : " + name + " got block header, data type " + type +
//                ", source id = " + header4.getReserved1() + ", payld count = " + header4.getEventCount());
//System.out.println("Header = " + header4.toString());
                            sourceId     = header4.getReserved1();
                            payloadCount = header4.getEventCount();

                            payloadBanks.clear();

                            while ((bank = reader.parseNextEvent()) != null) {
                                if (payloadCount < 1) {
                                    throw new EvioException("Evio header inconsistency");
                                }

                                // Not a real copy, just points to stuff in bank
                                payloadBank = new PayloadBank(bank);
                                // Add vital info from block header.
                                payloadBank.setRecordId(blockHeader.getNumber());
                                payloadBank.setType(type);
                                payloadBank.setSourceId(sourceId);

                                // add bank to list for later writing
                                payloadBanks.add(payloadBank);

                                // Handle end event ...
                                if (Evio.isEndEvent(bank)) {
                                    // There should be no more events coming down the pike so
                                    // go ahead write out existing events and then shut this
                                    // thread down.
logger.info("      DataChannel Et : found END event");
                                    haveInputEndEvent = true;
                                    break;
                                }

                                payloadCount--;
                            }

                            // Write any existing banks
                            writeEvents(payloadBanks, myInputOrder);

                            if (haveInputEndEvent) break;
                        }
                        catch (EvioException e) {
                            // if ET event data NOT in evio format, skip over it
                            logger.error("        DataChannel Et : " + name +
                                         " ET event data is NOT (latest) evio format, skip");
                        }
                    }

                    // put all events back in ET system - even those unused
                    etSystem.putEvents(attachment, events);

                    if (haveInputEndEvent) {
logger.info("      DataChannel Et : have END, " + name + " quit input helping thread");
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Et : " + name + "  interrupted, exiting");
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("      DataChannel Et : " + name + " exit " + e.getMessage());
            }
        }



                /** Method run ... */
        public void runOrig() {

            try {

                EvioBank bank;
                int myInputOrder;
                EvioReader parser;
                ByteBuffer buf;
//                EvioBank fakeEv = new EvioEvent(1,DataType.BANK,1);

                while ( etSystem.alive() ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
logger.warn("      DataChannel Et : " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // Get events while checking periodically to see if we must go away
                    synchronized (lockIn2) {
                        try {
                            events = etSystem.getEvents(attachment, Mode.TIMED,
                                                        Modify.NOTHING, etWaitTime, chunk);
                            // Keep track of the order in which events are grabbed
                            // in order to preserve event order with multiple threads.
                            myInputOrder = inputOrderIn;
                            inputOrderIn = (inputOrderIn + events.length) % Integer.MAX_VALUE;
                        }
                        catch (EtTimeoutException e) {
                            if (haveInputEndEvent) {
//System.out.println("      DataChannel Et : " + name + " have END, quitting");
                                return;
                            }
                            else if (gotResetCmd) {
//System.out.println("      DataChannel Et : " + name + " got RESET, quitting");
                                return;
                            }
                            Thread.sleep(5);
                            continue;
                        }
                    }

                    int index = 0;

                    for (EtEvent ev : events) {
                        buf = ev.getDataBuffer();

                        if (ev.needToSwap()) {
//System.out.println("      DataChannel Et : " + name + " SETTING byte order to LITTLE endian");
                            buf.order(ByteOrder.LITTLE_ENDIAN);
                        }

                        try {
                            parser = new EvioReader(buf);
                            // Speed things up since no EvioListeners are used - doesn't do much
                            parser.getParser().setNotificationActive(false);
                            bank = parser.parseNextEvent();
//                            bank = fakeEv;

                            // Put evio bank (DTR) on Q if it parses
                            writeEvents(bank, myInputOrder + index++);

                            // Handle end event ...
                            if (Evio.dtrHasEndEvent(bank)) {
                                // There should be no more events coming down the pike so
                                // go ahead write out existing events and then shut this
                                // thread down.
//logger.info("      DataChannel Et : found END event");
                                haveInputEndEvent = true;
                                break;
                            }
                        }
                        catch (EvioException e) {
                            // if ET event data NOT in evio format, skip over it
                            logger.error("        DataChannel Et : " + name +
                                         " ET event data is NOT evio format, skip");
                        }
                    }

                    // put all events back in ET system - even those unused
                    etSystem.putEvents(attachment, events);

                    if (haveInputEndEvent) {
//logger.info("      DataChannel Et : have END, " + name + " quit input helping thread");
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Et : " + name + "  interrupted, exiting");
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("      DataChannel Et : " + name + " exit " + e.getMessage());
            }
        }

    }


    /**
     * Class used to take Evio events from Q, write them into ET events
     * and put them into an ET system.
     */
    private class DataOutputHelper implements Runnable {

        /** Used to sync things before putting new ET events. */
        CountDownLatch latch;

        /** Help in pausing DAQ. */
        int pauseCounter;

        /** Thread pool for writing Evio events into new ET events. */
        ExecutorService writeThreadPool;

        /** Thread pool for getting new ET events. */
        ExecutorService getThreadPool;

        /** Runnable object for getting new ET events - to be run in getThreadPool. */
        EvGetter getter;

        /** Syncing for putting ET events into ET system. */
        CyclicBarrier getBarrier;


        /** Constructor. */
        DataOutputHelper() {
            // Thread pool with "writeThreadCount" number of threads & queue.
            writeThreadPool = Executors.newFixedThreadPool(writeThreadCount);

            // Stuff for getting new ET events in parallel
            getBarrier = new CyclicBarrier(2);
            getter = new EvGetter(getBarrier);

            // Thread pool with 1 thread & queue
            getThreadPool = Executors.newSingleThreadExecutor();
        }


        /** Shutdown all the thread pools. */
        private void shutdown() {
            getThreadPool.shutdown();
            writeThreadPool.shutdown();

            // Make sure these threads are finished so we can close the ET system
            try {
                boolean success = getThreadPool.awaitTermination(100L, TimeUnit.MILLISECONDS);
                if (!success) {
                    // may be blocked on getBarrier.await()
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
         * It allows coordination between 2 DataOutputHelper threads so that
         * event order is preserved.
         *
         * @param events the ET events to put back into ET system
         * @param inputOrder the order in which evio events were grabbed off Q
         * @param offset index into events array
         * @param events2Write number of events to write
         * @throws InterruptedException if put or wait interrupted
         * @throws IOException ET communication error
         * @throws EtException will not happen
         * @throws EtDeadException ET system is dead
         */
        private void writeEvents(EtEvent[] events, int inputOrder,
                                 int offset, int events2Write)
                throws InterruptedException, IOException, EtException, EtDeadException {

            if (dataOutputThreads.length > 1) {
                synchronized (lockOut) {
                    // Is the bank we grabbed next to be output? If not, wait.
                    while (inputOrder != outputOrder) {
                        lockOut.wait();
                    }

                    // put events back in ET system
//System.out.println("multithreaded put: array len = " + events.length + ", put " + events2Write +
//                     " # of events into ET");
                    etSystem.putEvents(attachment, events, offset, events2Write);

                    // next one to be put on output channel
                    outputOrder = ++outputOrder % Integer.MAX_VALUE;
                    lockOut.notifyAll();
                }
            }
            else {
//System.out.println("singlethreaded put: array len = " + events.length + ", put " + events2Write +
// " # of events into ET");
                etSystem.putEvents(attachment, events, offset, events2Write);
            }
        }


        /** Method run ... */
        public void run() {

            try {
                EtEvent[] events;
                EventType previousType, pBanktype;
                PayloadBank pBank;
                LinkedList<PayloadBank> bankList;
                boolean gotNothingYet;
                int etEventsIndex, pBankSize, banksTotalSize, etSize;
                int events2Write, eventArrayLen, myInputOrder=0;

                // Create an array of list of PayloadBank objects by 2-step
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
                    etEventsIndex = 0;
                    banksTotalSize = 0;
                    previousType = null;
                    gotNothingYet = true;
                    bankList = bankListArray[etEventsIndex++];

                    // If more than 1 output thread, need to sync things
                    if (dataOutputThreads.length > 1) {

                        synchronized (lockIn) {

                            // Because "haveOutputEndEvent" is set true only in this
                            // synchronized code, we can check for it upon entering.
                            // If found already, we can quit.
                            if (haveOutputEndEvent) {
                                shutdown();
                                return;
                            }

                            // Grab a bank to put into an ET event buffer,
                            // checking occasionally to see if we got an
                            // RESET command or someone found an END event.
                            do {
                                // Get bank off of Q.
                                pBank = (PayloadBank) queue.poll(100L, TimeUnit.MILLISECONDS);
                                // If wait longer than 100ms, and there are thing to write,
                                // send them to the ET system.
                                if (pBank == null) {
                                    if (gotNothingYet) {
                                        continue;
                                    }
                                    break;
                                }

                                gotNothingYet = false;
                                pBanktype = pBank.getType();
                                pBankSize = pBank.getTotalBytes();
                                // assume worst case of one block header / banks
                                banksTotalSize += pBankSize + 32;

                                // Is this bank a diff type as previous bank?
                                // Or if it's the same type, will it not fit in
                                // the ET event? In any case start using a new list.
                                if (previousType != null &&
                                   (previousType != pBanktype || etSize < banksTotalSize)) {

                                    bankList = bankListArray[etEventsIndex++];
                                    // Add bank to new list
                                    bankList.add(pBank);
                                    // 64 -> take possible ending header into account
                                    banksTotalSize = pBankSize + 64;
                                }
                                // This the first time through the while loop
                                // or it's OK to add this bank to the list.
                                else {
                                    // Add bank to the list since there's
                                    // room and it's the right type.
                                    bankList.add(pBank);
                                }

                                // Look for END event and mark it in attachment
                                if (Evio.isEndEvent(pBank)) {
                                    pBank.setAttachment(Boolean.TRUE);
                                    haveOutputEndEvent = true;
                                    break;
                                }

                                // Set this for next round
                                previousType = pBanktype;
                                pBank.setAttachment(Boolean.FALSE);

                            } while (!gotResetCmd && (etEventsIndex < eventArrayLen));

                            // If I've been told to RESET ...
                            if (gotResetCmd) {
                                shutdown();
                                return;
                            }
                            // If I have END event in hand ...
                            else if (haveOutputEndEvent) {
                                break;
                            }

                            myInputOrder = inputOrder;
                            inputOrder = ++inputOrder % Integer.MAX_VALUE;
                        }
                    }
                    else {
                        do {
                            pBank = (PayloadBank) queue.poll(100L, TimeUnit.MILLISECONDS);
                            if (pBank == null) {
                                if (gotNothingYet) {
                                    continue;
                                }
                                break;
                            }

                            gotNothingYet = false;
                            pBanktype = pBank.getType();
                            pBankSize = pBank.getTotalBytes();
                            banksTotalSize += pBankSize + 32;

                            if (previousType != null &&
                               (previousType != pBanktype || etSize < banksTotalSize)) {

                                bankList = bankListArray[etEventsIndex++];
                                bankList.add(pBank);
                                banksTotalSize = pBankSize + 64;
                            }
                            else {
                                bankList.add(pBank);
                            }

                            if (Evio.isEndEvent(pBank)) {
                                pBank.setAttachment(Boolean.TRUE);
                                haveOutputEndEvent = true;
                                break;
                            }

                            previousType = pBanktype;
                            pBank.setAttachment(Boolean.FALSE);

                        } while (!gotResetCmd && (etEventsIndex < eventArrayLen));

                        if (gotResetCmd) {
                            shutdown();
                            return;
                        }
                        else if (haveOutputEndEvent) {
                            break;
                        }
                    }

                    latch = new CountDownLatch(etEventsIndex);

                    for (int i=0; i < etEventsIndex; i++) {
                        // Get list of banks to put into this ET event
                        bankList = bankListArray[i];

                        if (bankList.size() < 1) {
                            continue;
                        }

                        // Check to see if not enough room in ET event to hold bank.
                        // In this case, list will only contain 1 (big) bank.
                        if (bankList.size() == 1) {
                            // Minimum # of bytes to write this bank into buffer
                            int bankWrittenSize = bankList.getFirst().getTotalBytes() + 64;
                            if (bankWrittenSize > etSize) {
logger.warn("      DataChannel Et DataOutputHelper : " + name + " ET event too small to contain built event");
                                // This new event is not large enough, so dump it and replace it
                                // with a larger one. Performance will be terrible but it'll work.
                                etSystem.dumpEvents(attachment, new EtEvent[]{events[i]});
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
                        EvWriterNew writer = new EvWriterNew(bankList, events[i]);
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
System.out.println("Ending");
                        shutdown();
                        return;
                    }
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("      DataChannel Et DataOutputHelper : exit " + e.getMessage());
            }

        }

        /**
         * This class is designed to write an evio bank's
         * contents into an ET buffer by way of a thread pool.
         */
        private class EvWriterNew implements Runnable {

            private LinkedList<PayloadBank> bankList;
            private EtEvent     event;
            private ByteBuffer  buffer;
            private EventWriter evWriter;

            void setEventType(BitSet bSet, int type) {
                if (type < 0) type = 0;
                else if (type > 15) type = 15;

                if (bSet.size() < 6) {
                    return;
                }

                for (int i=2; i < 6; i++) {
                    bSet.set(i, ((type >>> i - 2) & 0x1) > 0);
                }
            }

            /** Constructor. */
            EvWriterNew(LinkedList<PayloadBank> bankList, EtEvent event) {
                this.event = event;
                this.bankList = bankList;

                try {
                    // Make the block size bigger than
                    // the Roc's 2MB ET buffer size so no additional block headers must
                    // be written. It should contain less than 100 ROC Raw records,
                    // but we'll allow 200 such banks per block header.
                    buffer = event.getDataBuffer();
                    buffer.clear();
                    buffer.order(byteOrder);

                    // encode the event type into bits
                    BitSet bitInfo = new BitSet(24);
//System.out.println("      EvWriterNew Et: setting block header type to " +
//                                               bankList.getFirst().getType().getValue());
                    setEventType(bitInfo, bankList.getFirst().getType().getValue());
//System.out.println("      EvWriterNew Et: bit info = " + bitInfo + ", source id = " + emu.getCodaid());


                    evWriter = new EventWriter(buffer, 550000, 200, null, bitInfo, emu.getCodaid());
                }
                catch (EvioException e) {
                /* never happen */
                    e.printStackTrace();
                }
            }


            // Write bank into et event buffer.
            public void run() {
                try {
                    for (PayloadBank bank : bankList) {
//System.out.println("      EvWriterNew Et: writing bank of type " +
//                         bank.getType() + ", and total bytes " + bank.getTotalBytes());
                        evWriter.writeEvent(bank);
                    }
                    evWriter.close();
                    event.setLength(buffer.position());
System.out.println("LC " + latch.getCount());
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


        /** Method run ... */
         public void runOld() {

             try {
                 EtEvent[] events;
                 PayloadBank[] banks = new PayloadBank[chunk];
                 int bankSize, bankCount, events2Write, eventArrayLen, putLimit, index, myInputOrder=0;

                 // Get some new ET events
                 getThreadPool.execute(getter);

                 while ( etSystem.alive() ) {

                     if (pause) {
                         if (pauseCounter++ % 400 == 0) Thread.sleep(5);
                         continue;
                     }

                     // Get new ET events, then have thread simultaneously get more.
                     // If things are working properly, we can always get new events,
                     // which means we should never block here.
                     getBarrier.await();
                     events = getter.getEvents();
                     eventArrayLen = events.length; // convenience variable
                     // Execute thread to get more new events
                     getThreadPool.execute(getter);

                     // Write events in chunks of this size.
                     events2Write = 0;
                     bankCount = 0;

                     // First, grab all the banks we need ... in order!
                     for (int j=0; j < eventArrayLen; j++) {
                         banks[j] = null;
                     }

                     // If more than 1 output thread, need to sync things
                     if (dataOutputThreads.length > 1) {
                         synchronized (lockIn) {

                             for (int j=0; j < eventArrayLen; j++) {
                                 // Because "haveOutputEndEvent" is set true only in this
                                 // synchronized code, we can check for it upon entering.
                                 // If found already, we can quit.
                                 if (haveOutputEndEvent) {
                                     shutdown();
                                     return;
                                 }

                                 // Grab a bank to put into an ET event buffer
                                 // (one bank into one buffer),
                                 // checking occasionally to see if we got an
                                 // RESET command or someone found an END event.
                                 while ((banks[j] == null) && !gotResetCmd) {
                                     // Get bank off of Q
                                     banks[j] = (PayloadBank) queue.poll(100L, TimeUnit.MILLISECONDS);

                                     // Look for END event and mark it in attachment
                                     if (Evio.isEndEvent(banks[j])) {
                                         bankCount++;
                                         banks[j].setAttachment(Boolean.TRUE);
                                         haveOutputEndEvent = true;
                                         break;
                                     }
                                     else if (banks[j] != null) {
                                         bankCount++;
                                         banks[j].setAttachment(Boolean.FALSE);
                                     }
                                 }

                                 // If I've been told to RESET ...
                                 if (gotResetCmd) {
                                     shutdown();
                                     return;
                                 }
                                 // If I have END event in hand ...
                                 else if (haveOutputEndEvent) {
                                     break;
                                 }
                             }

                             myInputOrder = inputOrder;
                             inputOrder = ++inputOrder % Integer.MAX_VALUE;
                         }
                     }
                     else {
                         for (int j=0; j < eventArrayLen; j++) {
                             while ((banks[j] == null) && !gotResetCmd) {
                                 banks[j] = (PayloadBank) queue.poll(100L, TimeUnit.MILLISECONDS);

                                 if (Evio.isEndEvent(banks[j])) {
                                     bankCount++;
                                     banks[j].setAttachment(Boolean.TRUE);
                                     haveOutputEndEvent = true;
                                     break;
                                 }
                                 else if (banks[j] != null) {
                                     bankCount++;
                                     banks[j].setAttachment(Boolean.FALSE);
                                 }
                             }

                             if (gotResetCmd) {
                                 shutdown();
                                 return;
                             }
                             else if (haveOutputEndEvent) {
                                 break;
                             }
                         }
                     }

                     latch = new CountDownLatch(bankCount);
                     // How many threads do we use to write this batch?
                     putLimit = writeThreadCount > bankCount ? bankCount : writeThreadCount;

                     outerLoop:
                     for (int i=0; i < bankCount; ) {

                         putLimit = putLimit > (bankCount - i) ? (bankCount - i) : putLimit;

                         for (int j=0; j < putLimit; j++) {
                             index = j+i;

                             bankSize = banks[index].getTotalBytes();

                             // if not enough room in et event to hold bank ...
                             if (events[index].getDataBuffer().capacity() < bankSize) {
 logger.warn("      DataChannel Et DataOutputHelper : " + name + " et event too small to contain built event");
                                 // This new event is not large enough, so dump it and replace it
                                 // with a larger one. Performance will be terrible but it'll work.
                                 etSystem.dumpEvents(attachment, new EtEvent[]{events[index]});
                                 EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, false, 0, 1, bankSize, group);
                                 events[index] = evts[0];
                             }

                             // write bank's data into ET buffer in separate thread
                             EvWriter writer = new EvWriter(banks[index], events[index]);
                             writeThreadPool.execute(writer);

                             events[index].setByteOrder(banks[index].getByteOrder());

                             // CODA owns first select int
                             int[] selects = events[index].getControl();
                             selects[0] = id; // id in ROC output channel
                             events[index].setControl(selects);

                             // Keep track of how many events we want to write
                             events2Write++;

                             // Handle end event ...
                             if (banks[index].getAttachment() == Boolean.TRUE) {
                                 // There should be no more events coming down the pike so
                                 // go ahead write out events and then shut this thread down.
                                 break outerLoop;
                             }
                         }
                         i += putLimit;
                     }

                     // Wait for all events to finish processing
                     latch.await();

                     // Put events back in ET system
                     writeEvents(events, myInputOrder, 0, events2Write);

                     // Dump any left over new ET events.
                     if (events2Write < eventArrayLen) {
                         etSystem.dumpEvents(attachment, events, events2Write, (eventArrayLen - events2Write));
                     }

                     if (haveOutputEndEvent) {
                         shutdown();
                         return;
                     }
                 }

             } catch (InterruptedException e) {
             } catch (Exception e) {
                 e.printStackTrace();
                 logger.warn("      DataChannel Et DataOutputHelper : exit " + e.getMessage());
             }

         }


//        /** Method run ... */
//        public void runOrig() {
//
//            try {
//                EtEvent[] events;
//                EvioBank[] banks = new EvioBank[chunk];
//                int bankSize, bankCount, events2Write, eventArrayLen, putLimit, index, myInputOrder=0;
//
//                // Get some new ET events
//                getThreadPool.execute(getter);
//
//                while ( etSystem.alive() ) {
//
//                    if (pause) {
//                        if (pauseCounter++ % 400 == 0) Thread.sleep(5);
//                        continue;
//                    }
//
//                    // Get new ET events, then have thread simultaneously get more.
//                    // If things are working properly, we can always get new events,
//                    // which means we should never block here.
//                    getBarrier.await();
//                    events = getter.getEvents();
//                    eventArrayLen = events.length; // convenience variable
//                    // Execute thread to get more new events
//                    getThreadPool.execute(getter);
//
//                    // Write events in chunks of this size.
//                    events2Write = 0;
//                    bankCount = 0;
//
//                    // First, grab all the banks we need ... in order!
//                    for (int j=0; j < eventArrayLen; j++) {
//                        banks[j] = null;
//                    }
//                    // If more than 1 output thread, need to sync things
//                    if (dataOutputThreads.length > 1) {
//                        synchronized (lockIn) {
//
//                            for (int j=0; j < eventArrayLen; j++) {
//                                // Because "haveOutputEndEvent" is set true only in this
//                                // synchronized code, we can check for it upon entering.
//                                // If found already, we can quit.
//                                if (haveOutputEndEvent) {
//                                    shutdown();
//                                    return;
//                                }
//
//                                // Grab a bank to put into an ET event buffer,
//                                // checking occasionally to see if we got an
//                                // RESET command or someone found an END event.
//                                while ((banks[j] == null) && !gotResetCmd) {
//                                    // Get bank off of Q
//                                    banks[j] = queue.poll(100L, TimeUnit.MILLISECONDS);
//
//                                    // Look for END event and mark it in attachment
//                                    if (Evio.isEndEvent(banks[j])) {
//                                        bankCount++;
//                                        banks[j].setAttachment(Boolean.TRUE);
//                                        haveOutputEndEvent = true;
//                                        break;
//                                    }
//                                    else if (banks[j] != null) {
//                                        bankCount++;
//                                        banks[j].setAttachment(Boolean.FALSE);
//                                    }
//                                }
//
//                                // If I've been told to RESET ...
//                                if (gotResetCmd) {
//                                    shutdown();
//                                    return;
//                                }
//                                // If I have END event in hand ...
//                                else if (haveOutputEndEvent) {
//                                    break;
//                                }
//                            }
//
//                            myInputOrder = inputOrder;
//                            inputOrder = ++inputOrder % Integer.MAX_VALUE;
//                        }
//                    }
//                    else {
//                        for (int j=0; j < eventArrayLen; j++) {
//                            while ((banks[j] == null) && !gotResetCmd) {
//                                banks[j] = queue.poll(100L, TimeUnit.MILLISECONDS);
//
//                                if (Evio.isEndEvent(banks[j])) {
//                                    bankCount++;
//                                    banks[j].setAttachment(Boolean.TRUE);
//                                    haveOutputEndEvent = true;
//                                    break;
//                                }
//                                else if (banks[j] != null) {
//                                    bankCount++;
//                                    banks[j].setAttachment(Boolean.FALSE);
//                                }
//                            }
//
//                            if (gotResetCmd) {
//                                shutdown();
//                                return;
//                            }
//                            else if (haveOutputEndEvent) {
//                                break;
//                            }
//                        }
//                    }
//
//                    latch = new CountDownLatch(bankCount);
//                    putLimit = writeThreadCount > bankCount ? bankCount : writeThreadCount;
//
//                    outerLoop:
//                    for (int i=0; i < bankCount; ) {
//
//                        putLimit = putLimit > (bankCount - i) ? (bankCount - i) : putLimit;
//
//                        for (int j=0; j < putLimit; j++) {
//                            index = j+i;
//
//                            bankSize = banks[index].getTotalBytes();
//
//                            // if not enough room in et event to hold bank ...
//                            if (events[index].getDataBuffer().capacity() < bankSize) {
//logger.warn("      DataChannel Et DataOutputHelper : " + name + " et event too small to contain built event");
//                                // This new event is not large enough, so dump it and replace it
//                                // with a larger one. Performance will be terrible but it'll work.
//                                etSystem.dumpEvents(attachment, new EtEvent[]{events[index]});
//                                EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, false, 0, 1, bankSize, group);
//                                events[index] = evts[0];
//                            }
//
//                            // write bank's data into ET buffer in separate thread
//                            EvWriter writer = new EvWriter(banks, events, index);
//                            writeThreadPool.execute(writer);
//
//                            events[index].setByteOrder(banks[index].getByteOrder());
//
//                            // CODA owns first select int
//                            int[] selects = events[index].getControl();
//                            selects[0] = id; // id in ROC output channel
//                            events[index].setControl(selects);
//
//                            // Keep track of how many events we want to write
//                            events2Write++;
//
//                            // Handle end event ...
//                            if (banks[index].getAttachment() == Boolean.TRUE) {
//                                // There should be no more events coming down the pike so
//                                // go ahead write out events and then shut this thread down.
//                                break outerLoop;
//                            }
//                        }
//                        i += putLimit;
//                    }
//
//                    // Wait for all events to finish processing
//                    latch.await();
//
//                    // Put events back in ET system
//                    writeEvents(events, myInputOrder, 0, events2Write);
//
//                    // Dump any left over new ET events.
//                    if (events2Write < eventArrayLen) {
//                        etSystem.dumpEvents(attachment, events, events2Write, (eventArrayLen - events2Write));
//                    }
//
//                    if (haveOutputEndEvent) {
//                        shutdown();
//                        return;
//                    }
//                }
//
//            } catch (InterruptedException e) {
//            } catch (Exception e) {
//                e.printStackTrace();
//                logger.warn("      DataChannel Et DataOutputHelper : exit " + e.getMessage());
//            }
//
//        }


         /**
         * This class is designed to write an evio bank's
         * contents into an ET buffer by way of a thread pool.
         */
        private class EvWriter implements Runnable {

            private PayloadBank bank;
            private EtEvent     event;
            private ByteBuffer  buffer;
            private EventWriter evWriter;


            /** Constructor. */
            EvWriter(PayloadBank bank, EtEvent event) {
                this.bank  = bank;
                this.event = event;

                try {
                    // Make the block size bigger than
                    // the Roc's 2MB ET buffer size so no additional block headers must
                    // be written. It should contain less than 100 ROC Raw records,
                    // but we'll allow 200 such banks per block header.
                    buffer = event.getDataBuffer();
                    buffer.clear();
                    buffer.order(byteOrder);

                    // encode the event type into bits
                    BitSet bitInfo = new BitSet(24);
                    BlockHeaderV4.setEventType(bitInfo, bank.getType().getValue());

                    evWriter = new EventWriter(buffer, 550000, 200, null, bitInfo, emu.getCodaid());
                }
                catch (EvioException e) {/* never happen */}
            }


            // Write bank into et event buffer.
            public void run() {
                try {
                    evWriter.writeEvent(bank);
                    evWriter.close();
                    event.setLength(buffer.position());
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
         * simultaneously by way of a thread pool.
         */
        private class EvGetter implements Runnable {

            private EtEvent[] events;
            private final CyclicBarrier barrier;

            /** Constructor. */
            EvGetter(CyclicBarrier barrier) {
                this.barrier = barrier;
            }

            EtEvent[] getEvents() {
                return events;
            }

            // get et events
            public void run() {
                try {
                    events = null;
                    events = etSystem.newEvents(attachment, Mode.SLEEP, false, 0,
                                                chunk, (int)etSystem.getEventSize(), group);
System.out.println("ET channel, EvGetter: await barrier");
                    barrier.await();
System.out.println("ET channel, EvGetter: await past");
                }
                catch (BrokenBarrierException e) {
                    // may happen when ending or resetting
                    e.printStackTrace();
                }
                catch (InterruptedException e) {
                    // told to quit
                    e.printStackTrace();
                }
                catch (Exception e) {
                    // ET system problem - run will come to an end
                    e.printStackTrace();
                }
            }
        }


    }


    /**
     * For input channel, start the DataInputHelper thread which takes ET events,
     * parses each, puts the events back into the ET system, and puts the parsed
     * evio banks onto the queue.<p>
     * For output channel, start the DataOutputHelper thread which takes a bank from
     * the queue, puts it in a message, and sends it.
     */
    public void startHelper() {
        if (input) {
            dataInputThreads = new Thread[inputThreadCount];
            inputHelpers = new DataInputHelper[inputThreadCount];
//System.out.println("  # inputThreadCount = " + inputThreadCount);

            for (int i=0; i < inputThreadCount; i++) {
                inputHelpers[i] = new DataInputHelper();
                dataInputThreads[i] = new Thread(emu.getThreadGroup(), inputHelpers[i], getName() + " data in" + i);
//System.out.println("STARTING INPUT THD");
                dataInputThreads[i].start();
            }
        }
        else {
            dataOutputThreads = new Thread[outputThreadCount];
            outputHelpers = new DataOutputHelper[outputThreadCount];

            for (int i=0; i < outputThreadCount; i++) {
                outputHelpers[i] = new DataOutputHelper();
                dataOutputThreads[i] = new Thread(emu.getThreadGroup(), outputHelpers[i], getName() + " data out" + i);
                dataOutputThreads[i].start();
            }
        }
    }

    /**
     * Pause the DataInputHelper or DataOutputHelper thread.
     */
    public void pauseHelper() {
        pause = true;
    }

    /**
     * Resume running the DataInputHelper or DataOutputHelper thread.
     */
    public void resumeHelper() {
        pause = false;
    }

    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }



}