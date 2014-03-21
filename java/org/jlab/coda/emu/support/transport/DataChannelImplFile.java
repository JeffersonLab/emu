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

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Implementation of a DataChannel reading/writing from/to a file in EVIO format.
 *
 * @author heyes
 * @author timmer
 * (Nov 10, 2008)
 */
public class DataChannelImplFile extends DataChannelAdapter {

    /** Thread used to input or output data. */
    private Thread dataThread;

    /** The default size in bytes at which a new file is created. */
    private long split;

    /** If splitting files, the number file being written currently to. */
    private int splitCount;

    /** For input or output files, the directory. */
    private String directory;

    /** Name of file currently being written to. */
    private String fileName;

    /** Dictionary to be include in file. */
    private String dictionaryXML;

    /** Evio data file. */
    private EvioReader evioFileReader;

    /** Evio data file. */
    private EvioCompactReader compactFileReader;

    /** Object to write evio file. */
    private EventWriter evioFileWriter;

    //----------------------------------------
    // Input file parameters
    //----------------------------------------

    /** First evio block header read from a version 4 file. */
    private BlockHeaderV4 firstBlockHeader;

    /** EventType taken from first block header of file. */
    private EventType eventType;

    /** Source CODA id taken from first block header of file. */
    private int sourceId;

    /** Record id taken from first block header of file. */
    private int recordId;

    /** Number of evio events (banks) in file. */
    private int eventCount;


    /**
     * Constructor DataChannelImplFifo creates a new DataChannelImplFifo instance.
     *
     * @param name          name of file channel
     * @param transport     DataTransport object that created this channel
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input
     * @param emu           emu this channel belongs to
     * @param module        module this channel belongs to
     * @throws DataTransportException if unable to create fifo buffer.
     */
    DataChannelImplFile(String name, DataTransportImplFile transport,
                        Map<String, String> attributeMap, boolean input, Emu emu,
                        EmuModule module)
            throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module);

        // Set option whether or not to enforce evio block header
        // numbers to be sequential (throw an exception if not).
        boolean blockNumberChecking = false;
        String attribString = attributeMap.get("blockNumCheck");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("true") ||
                attribString.equalsIgnoreCase("on")   ||
                attribString.equalsIgnoreCase("yes"))   {
                blockNumberChecking = true;
            }
        }

        int runNumber  = emu.getRunNumber();
        String runType = emu.getRunType();

        // Directory given in config file?
        try {
            directory = attributeMap.get("dir");
//logger.info("      DataChannel File: config file directory = " + directory);
        }
        catch (Exception e) {}

        // Filename given in config file?
        try {
            fileName = attributeMap.get("fileName");
//logger.info("      DataChannel File: config file name = " + fileName);
        }
        catch (Exception e) {}

        // Dictionary given in config file?
        try {
            String dictionaryFile = attributeMap.get("dictionary");
            if (dictionaryFile != null) {
                // Load the contents of the file into a String
                File dFile = new File(dictionaryFile);
                if (dFile.exists() && dFile.isFile()) {
                    FileInputStream fileInputStream = new FileInputStream(dFile);
                    int fileSize = (int)fileInputStream.getChannel().size();
                    byte[] buf = new byte[fileSize];
                    DataInputStream dataStream = new DataInputStream(fileInputStream);
                    dataStream.read(buf);
                    dictionaryXML = new String(buf, 0, fileSize, "US-ASCII");
//
// This works but does a lot of XML parsing - good way to check format
//                EvioXMLDictionary dictionary = new EvioXMLDictionary(dFile);
//                dictionaryXML = dictionary.toXML();
                }
                else {
logger.info("      DataChannel File: dictionary file cannot be read");
                }

//logger.info("      DataChannel File: config dictionary = " + dictionaryFile);
            }
        }
        catch (Exception e) {}

        // Split parameter given in config file?
        try {
            String splitStr = attributeMap.get("split");
            if (splitStr != null) {
                try {
                    split = Long.parseLong(splitStr);
                    // Ignore negative values
                    if (split < 0L) split = 0L;
                }
                catch (NumberFormatException e) {
                    split = 0L;
                }
//logger.info("      DataChannel File: split = " + split);
            }
        }
        catch (Exception e) {}

        if (fileName == null) {
            if (input) {
                fileName = "codaInputFile.dat";
            }
            else {
                if (split > 0L) {
                    // First specifier   (%d)  replaced with run #,
                    // second specifier (%05d) replaced with split #
                    fileName = "codaOutputFile_%d.dat%05d";
                }
                else {
                    fileName = "codaOutputFile_%d.dat";
                }
            }
        }

        try {
            if (input) {
logger.info("      DataChannel File: try opening input file of " + fileName);

                if (queueItemType == QueueItemType.PayloadBank) {
                    evioFileReader = new EvioReader(fileName, blockNumberChecking);

                    // Only deal with evio version 4 files for simplicity
                    if (evioFileReader.getEvioVersion() < 4) {
                        throw new IOException("Evio version " +
                                evioFileReader.getEvioVersion() + " files not supported");
                    }

                    // Speed things up since no EvioListeners are used - doesn't do much
                    evioFileReader.getParser().setNotificationActive(false);

                    // Get the first block header
                    firstBlockHeader = (BlockHeaderV4)evioFileReader.getFirstBlockHeader();
                }
                else if  (queueItemType == QueueItemType.PayloadBuffer) {
                    // This will throw an exception if evio version < 4
                    compactFileReader = new EvioCompactReader(fileName);

                    // Get the first block header
                    firstBlockHeader = compactFileReader.getFirstBlockHeader();

                    // Get the # of events in file
                    eventCount = compactFileReader.getEventCount();
                }

                eventType = EventType.getEventType(firstBlockHeader.getEventType());
                sourceId  = firstBlockHeader.getReserved1();
                recordId  = firstBlockHeader.getNumber();

                DataInputHelper helper = new DataInputHelper();
                dataThread = new Thread(emu.getThreadGroup(), helper, name() + " data input");
                dataThread.start();
                helper.waitUntilStarted();

            } else {
logger.info("      DataChannel File: try opening output base file of " + fileName);

                // Make overwriting the file OK if there is NO splitting of the file.
                // If there is no file splitting, overwriting the file will occur when
                // the file name is static or if the run # is repeated.
                boolean overWriteOK = true;
                if (split > 0L) overWriteOK = false;

                evioFileWriter = new EventWriter(fileName, directory, runType,
                                                 runNumber, split, byteOrder,
                                                 dictionaryXML, overWriteOK);

                // Tell emu what that output name is for stat reporting.
                // Get the name from the file writer object so that the
                // final filename is used with all string substitutions made.
                // This must be done each time the file is split.
                emu.setOutputDestination(evioFileWriter.getCurrentFilename());

                // Keep track of how many files we create
                if (split > 0L) splitCount = evioFileWriter.getSplitCount();

                DataOutputHelper helper = new DataOutputHelper();
                dataThread = new Thread(emu.getThreadGroup(), helper, name() + " data out");
                dataThread.start();
                helper.waitUntilStarted();
            }
        }
        catch (Exception e) {
            if (input) {
                throw new DataTransportException("DataChannelImplFile : Cannot open data file " + e.getMessage(), e);
            }
            else {
                throw new DataTransportException("DataChannelImplFile : Cannot create data file" + e.getMessage(), e);
            }
        }
    }


    /** {@inheritDoc} */
    public void reset() {
        if (dataThread != null) dataThread.interrupt();

        try {
            if (evioFileReader != null) evioFileReader.close();
        } catch (Exception e) {}

        try {
            if (compactFileReader != null) compactFileReader.close();
        } catch (Exception e) {}

        try {
            if (evioFileWriter != null) evioFileWriter.close();
        } catch (Exception e) {}

        queue.clear();
        errorMsg.set(null);
        state = CODAState.CONFIGURED;
    }


    /**
     * Class <b>DataInputHelper</b>
     * This class reads data from the file and queues it on the fifo.
     * Don't know if this will ever be useful. Might as well generate
     * an END event when file is fully read.
     */
    private class DataInputHelper implements Runnable {

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch latch = new CountDownLatch(1);

        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
            }
        }

        /** {@inheritDoc} */
        public void run() {
            if (queueItemType == QueueItemType.PayloadBank) {
                runBanks();
            }
            else if  (queueItemType == QueueItemType.PayloadBuffer) {
                runBuffers();
            }
        }

        /** {@inheritDoc} */
        public void runBanks() {

            int counter = 0;
            EventType  bankType;
            ControlType controlType;
            PayloadBank payloadBank;

            // I've started
            latch.countDown();

            try {
                while (!dataThread.isInterrupted()) {
                    EvioEvent event;
                    try {
                        event = evioFileReader.parseNextEvent();
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "File data is NOT evio v4 format");
                        throw e;
                    }

                    if (event == null) {
                        break;
                    }

                    // From first block header in file
                    controlType = null;
                    bankType = eventType;

                    // Unlikely that a file has roc raw data, but accommodate it anyway
                    if (eventType == EventType.ROC_RAW) {
                        if (Evio.isUserEvent(event)) {
                            bankType = EventType.USER;
                        }
                    }
                    else if (eventType == EventType.CONTROL) {
                        // Find out exactly what type of control event it is
                        // (May be null if there is an error).
                        controlType = ControlType.getControlType(event.getHeader().getTag());
                        if (controlType == null) {
                            errorMsg.compareAndSet(null, "Found unidentified control event");
                            throw new EvioException("Found unidentified control event");
                        }
                    }

                    // Not a real copy, just points to stuff in bank
                    payloadBank = new PayloadBank(event, bankType,
                                                  controlType, recordId,
                                                  sourceId, name);

                    queue.put(payloadBank);  // will block
                    counter++;
                }

                // Put in END event
                EvioEvent controlEvent = Evio.createControlEvent(ControlType.END, 0, 0, counter, 0);
                PayloadBank bank = new PayloadBank(controlEvent);
                bank.setEventType(EventType.CONTROL);
                bank.setControlType(ControlType.END);
                queue.put(bank);  // will block
                if (endCallback != null) endCallback.endWait();

            }
            catch (InterruptedException e) {
                // time to quit
            }
            catch (Exception e) {
//logger.warn("      DataChannel File (" + name + "): close file");
//logger.warn("      DataChannel File (" + name + "): exit " + e.getMessage());
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();
            }
        }


        /** {@inheritDoc} */
        public void runBuffers() {

            int counter = 0;
            EventType bankType;
            ControlType controlType;
            PayloadBuffer payloadBuffer;

            // I've started
            latch.countDown();

            try {
                // From first block header in file
                controlType = null;
                EvioNode node;

                for (int i=0; i < eventCount; i++) {
                    if (dataThread.isInterrupted()) break;

                    node = compactFileReader.getEvent(i);
                    bankType = eventType;

                    // Unlikely that a file has roc raw data, but accommodate it anyway
                    if (eventType == EventType.ROC_RAW) {
                        if (Evio.isUserEvent(node)) {
                            bankType = EventType.USER;
                        }
                    }
                    else if (eventType == EventType.CONTROL) {
                        // Find out exactly what type of control event it is
                        // (May be null if there is an error).
                        controlType = ControlType.getControlType(node.getTag());
                        if (controlType == null) {
                            errorMsg.compareAndSet(null, "Found unidentified control event");
                            throw new EvioException("Found unidentified control event");
                        }
                    }

                    // Not a real copy, just points to stuff in bank
                    payloadBuffer = new PayloadBuffer(compactFileReader.getEventBuffer(i),
                                                      bankType, controlType, recordId,
                                                      sourceId, name, node);

                    queue.put(payloadBuffer);  // will block
                    counter++;
                }

                // Put in END event
                EvioEvent controlEvent = Evio.createControlEvent(ControlType.END, 0, 0, counter, 0);
                PayloadBank bank = new PayloadBank(controlEvent);
                bank.setEventType(EventType.CONTROL);
                bank.setControlType(ControlType.END);
                queue.put(bank);  // will block
                if (endCallback != null) endCallback.endWait();

            }
            catch (InterruptedException e) {
                // time to quit
            }
            catch (Exception e) {
//logger.warn("      DataChannel File (" + name + "): close file");
//logger.warn("      DataChannel File (" + name + "): exit " + e.getMessage());
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();
            }
        }


    }



    /**
     * Class <b>DataOutputHelper </b>
     * Handles writing evio events (banks) to a file.
     * A lot of the work is done in jevio such as splitting files.
     */
    private class DataOutputHelper implements Runnable {

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch latch = new CountDownLatch(1);

        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {}
        }


        /** {@inheritDoc} */
        public void run() {

            // I've started
            latch.countDown();

            QueueItem qItem;
            PayloadBank bank  = null;
            PayloadBuffer buf = null;
            EventType eventType;
            ControlType controlType;
            boolean gotPrestart = false, gotEnd = false;

            try {

                while (!dataThread.isInterrupted()) {

                    qItem = queue.take(); // will block

                    if (queueItemType == QueueItemType.PayloadBank) {
                        bank = (PayloadBank)qItem;
                        eventType = bank.getEventType();
                        controlType = bank.getControlType();
                    }
                    else {
                        buf = (PayloadBuffer)qItem;
                        eventType = buf.getEventType();
                        controlType = buf.getControlType();
                    }

                    if (eventType == EventType.CONTROL) {
                        if (controlType == ControlType.END) {
                            gotEnd = true;
                            if (endCallback != null) endCallback.endWait();
                            logger.info("      DataChannel File (" + name + "): got END, close file " + fileName);
                        }
                        else if (controlType == ControlType.PRESTART) {
                            gotPrestart = true;
                            logger.info("      DataChannel File (" + name + "): got PRESTART");
                        }
                    }
                    else {
//logger.info("      DataChannel File (" + name + "): got bank of type " + eventType);
                    }

                    // Don't start writing to file until we get PRESTART
                    if (!gotPrestart) {
logger.warn("      DataChannel File (" + name + "): got event but NO PRESTART, get another off Q");
                        continue;
                    }

                    try {
                        // evioFileWriter will automatically split the file
                        if (queueItemType == QueueItemType.PayloadBank) {
                            evioFileWriter.writeEvent(bank.getEvent());
//                            if (controlType == ControlType.PRESTART) {
//                                evioFileWriter.flushToFile();
//                            }
                        }
                        else {
                            evioFileWriter.writeEvent(buf.getBuffer());
                        }
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "Cannot write to file");
                        throw e;
                    }

                    // If splitting the output, the file name may change.
                    // Inform the authorities about this.
                    if (split > 0L && evioFileWriter.getSplitCount() > splitCount) {
                        emu.setOutputDestination(evioFileWriter.getCurrentFilename());
                        splitCount = evioFileWriter.getSplitCount();
                    }

                    if (gotEnd) {
                        try {
                            evioFileWriter.close();
                        }
                        catch (Exception e) {
                            errorMsg.compareAndSet(null, "Cannot write to file");
                            throw e;
                        }
                        return;
                    }
                }

                logger.info("      DataChannel File (" + name + "): close file " + fileName);

            }
            catch (InterruptedException e) {
                // time to quit
            }
            catch (Exception e) {
//logger.warn("      DataChannel File (" + name + "): exit, " + e.getMessage());
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();
            }

            try { evioFileWriter.close(); }
            catch (Exception e) {}
        }
    }

}