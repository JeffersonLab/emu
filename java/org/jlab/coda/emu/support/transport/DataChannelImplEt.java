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

import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.et.enums.Modify;
import org.jlab.coda.et.exception.*;
import org.jlab.coda.jevio.EvioBank;
import org.jlab.coda.jevio.EventParser;
import org.jlab.coda.jevio.EvioReader;
import org.jlab.coda.jevio.EvioException;
import org.jlab.coda.jevio.EvioReader;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.Map;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

    /** Size of events in bytes we ask the ET system for. */
    private int evSize;

    /** Number of events to ask for in an array. */
    private int chunk;

    /** ET system connected to. */
    private EtSystem etSystem;

    /** ET station attached to. */
    private EtStation station;

    /** Name of ET station attached to. */
    private String stationName;

    /** Attachment to ET staton. */
    private EtAttachment attachment;

    /** Configuration of ET station being created and attached to. */
    private EtStationConfig stationConfig;

    /** Array of events obtained from ET system. */
    private EtEvent[] events;

    /** Time in microseconds to wait for the ET system to deliver requested events
     *  before throwing an EtTimeoutException. */
    private int etWaitTime = 2000000;

    /** Field queue - filled buffer queue */
    private final BlockingQueue<EvioBank> queue;

    /** Field dataThread */
    private Thread dataThread;

    /** Do we pause the dataThread? */
    private boolean pause;

    /** Object for parsing evio data contained in incoming messages. */
    private EvioReader parser;

    /** Byte order of output data (input data's order is specified in msg). */
    ByteOrder byteOrder;

    /** Map of config file attributes. */
    Map<String, String> attributeMap;

    /** Is this channel an input (true) or output (false) channel? */
    boolean input;



    /**
     * Constructor to create a new DataChannelImplEt instance.
     * Used only by {@link DataTransportImplEt#createChannel} which is
     * only used during PRESTART in {@link org.jlab.coda.emu.EmuModuleFactory}.
     *
     * @param name       the name of this channel
     * @param transport  the DataTransport object that this channel belongs to
     * @param attrib     the hashmap of config file attributes for this channel
     * @param input      true if this is an input data channel, otherwise false
     *
     * @throws org.jlab.coda.emu.support.transport.DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplEt(String name, DataTransportImplEt transport,
                      Map<String, String> attrib, boolean input)
            throws DataTransportException {

        this.name = name;
        this.input = input;
        this.attributeMap  = attrib;
        this.dataTransport = transport;
Logger.info("      DataChannelImplEt.const : creating channel " + name);

        // set queue capacity
        int capacity = 100;    // 100 buffers * 100 events/buf * 150 bytes/Roc/ev =  1.5Mb/Roc
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
            Logger.info("      DataChannelImplEt.const : " +  e.getMessage() + ", default to " + capacity + " records.");
        }
        queue = new ArrayBlockingQueue<EvioBank>(capacity);


        // Set id number. Use any defined in config file else use default (0)
        id = 0;
        String idVal = attributeMap.get("id");
        if (idVal != null) {
            try {
                id = Integer.parseInt(idVal);
            }
            catch (NumberFormatException e) {  }
        }
Logger.info("      DataChannelImplEt.const : id = " + id);


        // create ET system object & info
        etSystem = new EtSystem(dataTransport.getOpenConfig());

        // How may buffers do we grab at a time?
        chunk = 100;
        String attribString = attributeMap.get("chunk");
        if (attribString != null) {
            try {
                chunk = Integer.parseInt(attribString);
                if (chunk < 1) chunk = 100;
            }
            catch (NumberFormatException e) {}
        }
Logger.info("      DataChannelImplEt.const : chunk = " + chunk);


        // For output events, how big are they going to be (in bytes)?
        evSize = 20000;
        attribString = attributeMap.get("size");
        if (attribString != null) {
            try {
                evSize = Integer.parseInt(attribString);
                if (evSize < 1) evSize = 20000;
            }
            catch (NumberFormatException e) {}
        }
Logger.info("      DataChannelImplEt.const : ev size = " + evSize);


        // if INPUT channel
        if (input) {

            try {
                // configuration of a new station
                stationConfig = new EtStationConfig();
                try {
                    stationConfig.setUserMode(EtConstants.stationUserSingle);
                }
                catch (EtException e) { /* never happen */}

                // Create filter for station so only events from a particular ROC
                // (id as defined in config file) make it in.
                // Station filter is the built-in selection function.
                int[] selects = new int[EtConstants.stationSelectInts];
                Arrays.fill(selects, -1);
                selects[0] = id;
                stationConfig.setSelect(selects);
                stationConfig.setSelectMode(EtConstants.stationSelectMatch);

                // create station if it does not already exist
                stationName = "station"+id;
                            }
            catch (Exception e) {
                e.printStackTrace();
                throw new DataTransportException("cannot create/attach to ET station", e);
            }
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
                Logger.info("      DataChannelImplEt.const : no output data endianness specifed, default to big.");
            }
       }

        // start up thread to help with input or output
        openEtSystem();
        startHelper();
        Logger.info("      DataChannelImplEt.const : constructor END");
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

    /**
     * Get the ET sytem object.
     * @return the ET system object.
     */
    public void openEtSystem() throws DataTransportException {
        try {
            etSystem.open();

            if (input) {
                try {
                    station = etSystem.createStation(stationConfig, stationName);
                }
                catch (EtExistsException e) {
                    station = etSystem.stationNameToObject(stationName);
                }
Logger.info("      DataChannelImplEt.const : created or found station = " + stationName);

                // attach to station
                attachment = etSystem.attach(station);
Logger.info("      DataChannelImplEt.const : attached to station " + stationName);
            }
            else {
                // get GRAND_CENTRAL station object
                station = etSystem.stationNameToObject("GRAND_CENTRAL");

                // attach to grandcentral station
                attachment = etSystem.attach(station);
Logger.info("      DataChannelImplEt.const : attached to grandCentral");
            }
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
     * Close this channel by closing ET system and ending the data sending thread.
     */
    public void close() {
        Logger.warn("      DataChannelImplEt.close : " + name + " - closing this channel (close ET system)");
        if (dataThread != null) dataThread.interrupt();
        try {
            etSystem.detach(attachment);
            etSystem.removeStation(station);
            etSystem.close();
        }
        catch (Exception e) {/* ignore */}
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
//Logger.info("      DataChannelImplEt.DataInputHelper : " + name + " - STARTED");

            try {
                while ( etSystem.alive() ) {

                    if (pause) {
                        if (printCounter++ % 400 == 0)
Logger.warn("      DataChannelImplEt.DataInputHelper : " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // read in event in chunks
                    try {
                        events = etSystem.getEvents(attachment, Mode.TIMED, Modify.NOTHING, etWaitTime, chunk);
                    }
                    catch (EtTimeoutException e) {
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }
//Logger.warn("      DataChannelImplEt.DataInputHelper : " + name + " read TIMEOUT");
                        continue;
                    }

                    // parse events
                    ByteBuffer buf;
                    for (EtEvent ev : events) {
                        buf = ev.getDataBuffer();

                        if (ev.needToSwap()) {
System.out.println("\n\n      DataChannelImplEt.DataInputHelper : " + name + " SETTING byte order to LITTLE endian\n");
                            buf.order(ByteOrder.LITTLE_ENDIAN);
                        }

                        try {
                            parser = new EvioReader(buf);
                            bank = parser.parseNextEvent();
                            // put evio bank on queue if it parses
                            queue.put(bank);
                        }
                        catch (EvioException e) {
                            // if ET event data NOT in evio format, skip over it
                            Logger.error("      DataChannelImplEt.DataInputHelper : " + name +
                                         " ET event data is NOT evio format, skip");
                        }
                    }

                    // put events back in ET system
                    etSystem.putEvents(attachment, events);
                }

            } catch (InterruptedException e) {
                Logger.warn("      DataChannelImplEt.DataInputHelper : " + name + "  interrupted, exiting");
            } catch (Exception e) {
                e.printStackTrace();
                Logger.warn("      DataChannelImplEt.DataInputHelper : " + name + " exit " + e.getMessage());
            }
        }

    }


    private class DataOutputHelper implements Runnable {
        int printCounter = 0;
    
        /** Method run ... */
        public void run() {

            try {
                int bankSize;
                EvioBank bank;
                ByteBuffer buffer;

                while ( etSystem.alive() ) {

                    if (pause) {
                        if (printCounter++ % 400 == 0)
Logger.warn("      DataChannelImplEt.DataOutputHelper : " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // read in new event in chunks
                    events = etSystem.newEvents(attachment, Mode.SLEEP, 0, chunk, evSize);

                    for (int i=0; i < events.length; i++) {
                        // grab a bank and put it into an ET event buffer
                        bank     = queue.take();  // blocks
                        bankSize = bank.getTotalBytes();
                        buffer   = events[i].getDataBuffer();

                        // if not enough room in et event to hold bank ...
                        if (buffer.capacity() < bankSize) {
Logger.warn("      DataChannelImplEt.DataOutputHelper : " + name + " et event too small to contain built event");
                            // This new event is not large enough, so dump it and replace it
                            // with a larger one. Performance will be terrible but it'll work.
                            etSystem.dumpEvents(attachment, new EtEvent[] {events[i]});
                            EtEvent[] evts = etSystem.newEvents(attachment, Mode.SLEEP, 0, 1, bankSize);
                            events[i] = evts[0];
                        }

                        // write bank into et event
                        buffer.clear();
                        bank.write(buffer);
                        events[i].setByteOrder(bank.getByteOrder());
                        events[i].setLength(buffer.position());
                    }

                    // put events back in ET system
                    etSystem.putEvents(attachment, events);
                }

            } catch (InterruptedException e) {
                Logger.warn("      DataChannelImplEt.DataOutputHelper : interrupted, exiting");
            } catch (Exception e) {
                e.printStackTrace();
                Logger.warn("      DataChannelImplEt.DataOutputHelper : exit " + e.getMessage());
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
            dataThread = new Thread(Emu.THREAD_GROUP, new DataInputHelper(), getName() + " data in");
        }
        else {
            dataThread = new Thread(Emu.THREAD_GROUP, new DataOutputHelper(), getName() + " data out");
        }
        dataThread.start();
    }

    /**
     * Pause the DataInputHelper or DataOutputHelper thread.
     */
    public void pauseHelper() {
        if (dataThread == null) return;
        pause = true;
    }

    /**
     * Resume running the DataInputHelper or DataOutputHelper thread.
     */
    public void resumeHelper() {
        if (dataThread == null) return;
        pause = false;
    }

    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }

}