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
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.et.enums.Modify;
import org.jlab.coda.et.exception.*;
import org.jlab.coda.jevio.EventWriter;
import org.jlab.coda.jevio.EvioBank;
import org.jlab.coda.jevio.EvioReader;
import org.jlab.coda.jevio.EvioException;


import java.util.concurrent.BlockingQueue;
import java.util.Map;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author timmer
 * Dec 2, 2009
 */
public class DataChannelImplEt implements DataChannel {

    /** Field transport */
    private final DataTransportImplEt dataTransport;

    /** Field name */
    private final String name;

    /** ID of this channel (corresponds to sourceId of ROCs for CODA event building). */
    private int id;

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

    /** Attachment to ET staton. */
    private EtAttachment attachment;

    /** Configuration of ET station being created and attached to. */
    private EtStationConfig stationConfig;

    /** Arrays of events obtained from ET system. */
    private EtEvent[] events1, events2;

    /** Time in microseconds to wait for the ET system to deliver requested events
     *  before throwing an EtTimeoutException. */
    private int etWaitTime = 2000000;

    /** Field queue - filled buffer queue */
    private final BlockingQueue<EvioBank> queue;

    /** Field dataThread */
    private Thread dataInputThread;
    private Thread dataOutputThread;

    volatile private boolean interruptInputThread;
    volatile private boolean interruptOutputThread;

    /** Do we pause the dataThread? */
    volatile private boolean pause;

    /** Object for parsing evio data contained in incoming messages. */
    private EvioReader parser;

    /** Byte order of output data (input data's order is specified in msg). */
    ByteOrder byteOrder;

    /** Map of config file attributes. */
    Map<String, String> attributeMap;

    /** Is this channel an input (true) or output (false) channel? */
    boolean input;

    private Logger logger;

    private Emu emu;



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
logger.info("      DataChannelImplEt.const : creating channel " + name);

        // set queue capacity
        int capacity = 100;    // 100 buffers * 100 events/buf * 150 bytes/Roc/ev =  1.5Mb/Roc
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
            logger.info("      DataChannelImplEt.const : " +  e.getMessage() + ", default to " + capacity + " records.");
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
//logger.info("      DataChannelImplEt.const : id = " + id);


        // create ET system object & info
        try {
            etSystem = new EtSystem(dataTransport.getOpenConfig());
        }
        catch (EtException e) {
            throw new DataTransportException("", e);
        }

        // How may buffers do we grab at a time?
        chunk = 100;
        attribString = attributeMap.get("chunk");
        if (attribString != null) {
            try {
                chunk = Integer.parseInt(attribString);
                if (chunk < 1) chunk = 100;
            }
            catch (NumberFormatException e) {}
        }
//logger.info("      DataChannelImplEt.const : chunk = " + chunk);

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
//logger.info("      DataChannelImplEt.const : group = " + group);

        // Set station name. Use any defined in config file else use
        // "station"+id for input and "GRAND_CENTRAL" for output.
        stationName = attributeMap.get("stationName");
        logger.info("      DataChannelImplEt.const : station name = " + stationName);


        // Set station position. Use any defined in config file else use default (1)
        attribString = attributeMap.get("position");
        if (attribString != null) {
            try {
                stationPosition = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {  }
        }
//logger.info("      DataChannelImplEt.const : position = " + stationPosition);

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
                logger.info("      DataChannelImplEt.const : no output data endianness specified, default to big.");
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
//        logger.info("      DataChannelImplEt.const : constructor END");
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
System.out.println("Try to open" + dataTransport.getOpenConfig().getEtName() );
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
logger.info("      DataChannelImplEt.const : created or found station = " + stationName);

            // attach to station
            attachment = etSystem.attach(station);
logger.info("      DataChannelImplEt.const : attached to station " + stationName);
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
        logger.warn("      DataChannelImplEt.close : " + name + " - closing this channel (close ET system)");


        // TODO: BUG BUG, cannot interrupt threads which are communicating with the ET server !!!
        // This will mess up all future communications !!!

        if (dataInputThread  != null) interruptInputThread = true;
        if (dataOutputThread != null) interruptOutputThread = true;
//        if (newEventThread   != null) newEventThread.interrupt();

        // don't close ET system until helper threads are done
        try {
System.out.print("      DataChannelImplEt.close : waiting for helper threads to end ... ");
            if (dataInputThread  != null) dataInputThread.join();
            if (dataOutputThread != null) dataOutputThread.join();
System.out.print(" done");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

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
     * Close this channel by closing ET system and interrupting the data sending thread.
     */
    public void reset() {
        logger.warn("      DataChannelImplEt.reset : " + name + " - closing this channel (close ET system)");

        if (dataInputThread  != null)  dataInputThread.interrupt();
        if (dataOutputThread != null) dataOutputThread.interrupt();

        // don't close ET system until helper threads are done
        try {
System.out.print("      DataChannelImplEt.reset : waiting for helper threads to end ... ");
            if (dataInputThread  != null) dataInputThread.join();
            if (dataOutputThread != null) dataOutputThread.join();
System.out.print(" done");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

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
     * <pre>
     * Class <b>DataInputHelper</b>
     * </pre>
     * Handles sending data.
     */
    private class DataInputHelper implements Runnable {
        int printCounter = 0;

        /** Method run ... */
        public void run() {
            EvioBank bank;
            int arrayLength;
            EventType type = EventType.GO;

//logger.info("      DataChannelImplEt.DataInputHelper : " + name + " - STARTED");

            try {
                while ( etSystem.alive() ) {

                    if (pause) {
                        if (printCounter++ % 400 == 0)
logger.warn("      DataChannelImplEt.DataInputHelper : " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

//                    // be careful to kill this thread NOT while talking to the ET system
//                    if (interruptInputThread) {
//                        logger.warn("      DataChannelImplEt.DataInputHelper : " + name + " cleanly interrupted, exiting");
//                        return;
//                    }

                    // read in event in chunks
                    try {
                        events1 = etSystem.getEvents(attachment, Mode.TIMED, Modify.NOTHING, etWaitTime, chunk);
                    }
                    catch (EtTimeoutException e) {
                        if (Thread.currentThread().isInterrupted() || interruptInputThread) {
                            return;
                        }
//logger.warn("      DataChannelImplEt.DataInputHelper : " + name + " read TIMEOUT");
                        continue;
                    }

                    // parse events
                    ByteBuffer buf;
                    arrayLength = 0;

                    for (EtEvent ev : events1) {
                        buf = ev.getDataBuffer();

                        if (ev.needToSwap()) {
//System.out.println("      DataChannelImplEt.DataInputHelper : " + name + " SETTING byte order to LITTLE endian");
                            buf.order(ByteOrder.LITTLE_ENDIAN);
                        }

                        try {
                            parser = new EvioReader(buf);
                            // Speed things up since no EvioListeners are used - doesn't do much
                            parser.getParser().setNotificationActive(false);
                            bank = parser.parseNextEvent();

                            // What type of bank is this?
                            type = Evio.getEventType(bank);

                            // put evio bank on queue if it parses
                            queue.put(bank);

                            arrayLength++;

                            // Handle end event ...
                            if (type == EventType.END) {
                                // There should be no more events coming down the pike so
                                // go ahead write out existing events and then shut this
                                // thread down.
System.out.println("      DataChannelImplEt.DataInputHelper : got END event");
                                break;
                            }

int size = queue.size();
if (size > 400 && size % 100 == 0) System.out.println("et chan IN Q: " + size);
                        }
                        catch (EvioException e) {
                            // if ET event data NOT in evio format, skip over it
                            logger.error("      DataChannelImplEt.DataInputHelper : " + name +
                                         " ET event data is NOT evio format, skip");
                        }
                    }

                    // put events back in ET system
                    etSystem.putEvents(attachment, events1, 0, arrayLength);

                    if (type == EventType.END) {
System.out.println("      DataChannelImplEt.DataInputHelper : " + name + " quit input helping thread");
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannelImplEt.DataInputHelper : " + name + "  interrupted, exiting");
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("      DataChannelImplEt.DataInputHelper : " + name + " exit " + e.getMessage());
            }
        }

    }



    private class DataOutputHelper implements Runnable {
        int printCounter = 0;

        /** Method run ... */
        public void run() {

            try {
                int bankSize, arrayLength;
                EventType type = EventType.GO;
                EvioBank bank;
                ByteBuffer buffer = ByteBuffer.allocate(4*8);
                EventWriter evWriter = null;
                try {
                    // Won't use buffer, just need it to avoid NullPointerException
                    // and get the ball rolling
                    evWriter = new EventWriter(buffer, 128000, 10, null, null);
                    evWriter.close();
                }
                catch (EvioException e) {e.printStackTrace();/* never happen */}

                while ( etSystem.alive() ) {

                    if (pause) {
                        if (printCounter++ % 400 == 0)
logger.warn("      DataChannelImplEt.DataOutputHelper : " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // Read in new event in chunks
                    arrayLength = 0;
                    events1 = etSystem.newEvents(attachment, Mode.SLEEP, 0, chunk,
                                                (int)etSystem.getEventSize(), group);

                    for (int i=0; i < events1.length; i++) {
                        // Grab a bank and put it into an ET event buffer
                        bank = queue.take();  // blocks

                        // What type of bank is this?
                        type = Evio.getEventType(bank);
int size = queue.size();
if (size > 400 && size % 100 == 0) System.out.println("et chan OUT Q: " + size);
                        bankSize = bank.getTotalBytes();
                        buffer   = events1[i].getDataBuffer();

                        // if not enough room in et event to hold bank ...
                        if (buffer.capacity() < bankSize) {
logger.warn("      DataChannelImplEt.DataOutputHelper : " + name + " et event too small to contain built event");
logger.warn("                                         : et ev buf = " + buffer.capacity() + ", bank size = " + bankSize);
logger.warn("                                         : header length = " + bank.getHeader().getLength());
                            // This new event is not large enough, so dump it and replace it
                            // with a larger one. Performance will be terrible but it'll work.
                            etSystem.dumpEvents(attachment, new EtEvent[]{events1[i]});
                            EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, 0, 1, bankSize, group);
                            events1[i] = evts[0];
                            buffer = events1[i].getDataBuffer();
                        }

                        // write bank into et event buffer
                        buffer.clear();
                        evWriter.setBuffer(buffer);
                        evWriter.writeEvent(bank);
                        evWriter.close();

                        events1[i].setByteOrder(bank.getByteOrder());
                        events1[i].setLength(buffer.position());

                        // CODA owns first select int
                        int[] selects = events1[i].getControl();
                        selects[0] = id; // id in ROC output channel
                        events1[i].setControl(selects);

                        // Keep track of how many events we write
                        arrayLength = i + 1;

                        // Handle end event ...
                        if (type == EventType.END) {
                            // There should be no more events coming down the pike so
                            // go ahead write out events and then shut this thread down.
System.out.println("      DataChannelImplEt.DataOutputHelper : got END event");
                            break;
                        }
                    }

                    // put events back in ET system
                    etSystem.putEvents(attachment, events1, 0, arrayLength);

                    if (type == EventType.END) {
System.out.println("      DataChannelImplEt.DataOutputHelper : quit output helping thread");
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannelImplEt.DataOutputHelper : interrupted, exiting");
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("      DataChannelImplEt.DataOutputHelper : exit " + e.getMessage());
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
            dataInputThread = new Thread(emu.getThreadGroup(), new DataInputHelper(), getName() + " data in");
            dataInputThread.start();
        }
        else {
            dataOutputThread = new Thread(emu.getThreadGroup(), new DataOutputHelper(), getName() + " data out");
            dataOutputThread.start();
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