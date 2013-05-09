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
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of a DataChannel reading/writing from/to a file in EVIO format.
 *
 * @author heyes
 * @author timmer
 * @date Nov 10, 2008
 */
public class DataChannelImplFile extends DataChannelAdapter {

    /** Thread used to input or output data. */
    private Thread dataThread;

    /** The default size in bytes at which a new file is created. */
    private long split;

    /** Number on end of split data file. */
    private int fileCount;

    /** For output files, the name prefix. */
    private String outputFilePrefix;

    /** For input or output files, the directory. */
    private String directory;

    /** Name of file currently being written to. */
    private String fileName;

    /** Evio data file. */
    private EvioReader evioFileReader;

    /** Object to write evio file. */
    private EventWriter evioFileWriter;



    /**
     * Constructor DataChannelImplFifo creates a new DataChannelImplFifo instance.
     *
     * @param name          name of file channel
     * @param transport     DataTransport object that created this channel
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input
     * @param emu           emu this channel belongs to
     *
     * @throws DataTransportException if unable to create fifo buffer.
     */
    DataChannelImplFile(String name, DataTransportImplFile transport,
                      Map<String, String> attributeMap, boolean input,
                      Emu emu) throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu);

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

        // Default output file name =   <session>_<run#>.dat<seq#>
        String session = emu.getSession();
        int runNumber  = emu.getRunNumber();
        outputFilePrefix = session + "_" + runNumber + ".dat";
        String outputFileName = outputFilePrefix + "000000"; // fileCount = 000000
        String defaultInputFileName = "codaDataFile.evio";


        // Directory given in config file?
        try {
            directory = attributeMap.get("dir");
//logger.info("      DataChannel File: config file dir = " + directory);
        }
        catch (Exception e) {}

        // Filename given in config file?
        try {
            fileName = attributeMap.get("fileName");

            // Scan for %d which must be replaced by the run number
            fileName = fileName.replace("%d", "" + runNumber);

            // Scan for environmental variables of the form $(xxx)
            // and substitute the values for them (blank string if not found)
            if (fileName.contains("$(")) {
                Pattern pattern = Pattern.compile("\\$\\((.*?)\\)");
                Matcher matcher = pattern.matcher(fileName);
                StringBuffer result = new StringBuffer(100);

                while (matcher.find()) {
                    String envVar = matcher.group(1);
                    String envVal = System.getenv(envVar);
                    if (envVal == null) envVal = "";
//System.out.println("replacing " + envVar + " with " + envVal);
                    matcher.appendReplacement(result, envVal);
                }
                matcher.appendTail(result);
//System.out.println("Resulting string = " + result);
                fileName = result.toString();
            }
            outputFilePrefix = fileName;
//logger.info("      DataChannel File: config file name = " + fileName);
        }
        catch (Exception e) {}

        // Filename given in config file?
        String prefix = null;
        try {
            prefix = attributeMap.get("prefix");
//logger.info("      DataChannel File: config file prefix = " + prefix);
        }
        catch (Exception e) {}

        // Split parameter given in config file?
        try {
            String splitStr = attributeMap.get("split");
            if (splitStr != null) {
                split = Long.parseLong(splitStr);
                // Ignore negative values
                if (split < 0L) split = 0L;

                // If were splitting files with a given name, add fileCount on end of name
                if (split > 0L && fileName != null) {
                    fileName += "000000";
                }
//logger.info("      DataChannel File: split = " + split);
            }
        }
        catch (Exception e) {}

        if (fileName == null) {
            if (prefix != null) {
                outputFileName = outputFilePrefix = prefix + "_" + runNumber + ".dat";
                if (split > 0L) {
                    outputFileName += "000000";
                }
            }

            if (input) {
                fileName = defaultInputFileName;
            }
            else {
                fileName = outputFileName;
            }
        }

        if (directory != null) {
            fileName = directory + "/" + fileName;
        }

        try {
            if (input) {
logger.info("      DataChannel File: try opening input file of " + fileName);
                evioFileReader = new EvioReader(fileName, blockNumberChecking);
                // Speed things up since no EvioListeners are used - doesn't do much
                evioFileReader.getParser().setNotificationActive(false);
                DataInputHelper helper = new DataInputHelper();
                dataThread = new Thread(emu.getThreadGroup(), helper, name() + " data input");
                dataThread.start();
                helper.waitUntilStarted();

            } else {
logger.info("      DataChannel File: try opening output file of " + fileName);
                // Tell emu what that output name is for stat reporting
                emu.setOutputDestination(fileName);

                evioFileWriter = new EventWriter(fileName, false, byteOrder);
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
        } catch (Exception e) {
            //ignore
        }

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

            int sourceId, recordId, counter = 0;
            IBlockHeader blockHeader;
            BlockHeaderV4 header4;
            EventType eventType, bankType;
            ControlType controlType;
            PayloadBank payloadBank;

            // I've started
            latch.countDown();

            try {
                while (!dataThread.isInterrupted()) {
                    EvioBank bank;
                    try {
                        bank = evioFileReader.parseNextEvent();
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "File data is NOT evio v4 format");
                        throw e;
                    }

                    if (bank == null) {
                        break;
                    }

                    // First block header in ET buffer
                    blockHeader = evioFileReader.getCurrentBlockHeader();
                    header4     = (BlockHeaderV4)blockHeader;
                    eventType   = EventType.getEventType(header4.getEventType());
                    controlType = null;
                    sourceId    = header4.getReserved1();
                    recordId    = header4.getNumber();

                    bankType = eventType;
                    // Unlikely that a file has roc raw data, but accommodate it anyway
                    if (eventType == EventType.ROC_RAW) {
                        if (Evio.isUserEvent(bank)) {
                            bankType = EventType.USER;
                        }
                    }
                    else if (eventType == EventType.CONTROL) {
                        // Find out exactly what type of control event it is
                        // (May be null if there is an error).
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
// TODO: put control type into constructor and avoid using CPU time, like below
                    queue.put(new QueueItem(payloadBank));  // will block
                    counter++;
                }

                // Put in END event
                EvioEvent controlEvent = Evio.createControlEvent(ControlType.END, 0, 0, counter, 0);
                PayloadBank bank = new PayloadBank(controlEvent);
                bank.setEventType(EventType.CONTROL);
                bank.setControlType(ControlType.END);
                queue.put(new QueueItem(bank, ControlType.END));  // will block
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
     * Handles sending data.
     */
    private class DataOutputHelper implements Runnable {

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

            // I've started
            latch.countDown();

            QueueItem qItem;
            PayloadBank bank;
            int bankBytes;
            long numBytesWritten = 0L;
            boolean gotPrestart = false, gotEnd = false;

            try {

                while (!dataThread.isInterrupted()) {

                    qItem = queue.take(); // will block
                    bank = qItem.getPayloadBank();

                    if (bank.getEventType() == EventType.CONTROL) {
                        ControlType cType = bank.getControlType();
                        if (cType == ControlType.END) {
                            gotEnd = true;
                            if (endCallback != null) endCallback.endWait();
                            logger.info("      DataChannel File (" + name + "): got END, close file " + fileName);
                        }
                        else if (cType == ControlType.PRESTART) {
                            gotPrestart = true;
                            logger.info("      DataChannel File (" + name + "): got PRESTART");
                        }
                    }
                    else {
//logger.info("      DataChannel File (" + name + "): got bank of type " + bank.getEventType());
                    }

                    // Don't start writing to file until we get PRESTART
                    if (!gotPrestart) {
logger.warn("      DataChannel File (" + name + "): got event but NO PRESTART, get another off Q");
                        continue;
                    }

                    bankBytes = bank.getTotalBytes();

                    // If we're splitting the output file and writing the next bank
                    // would put it over the split size limit ...
                    if (split > 0L && (numBytesWritten + bankBytes > split)) {
                        try {
                            evioFileWriter.close();
                        }
                        catch (Exception e) {
                            errorMsg.compareAndSet(null, "Cannot write to file");
                            throw e;
                        }

                        numBytesWritten = 0L;
                        fileName = String.format("%s%06d", outputFilePrefix, (++fileCount));
                        if (directory != null) {
                            fileName = directory + "/" + fileName;
                        }
//logger.info("      DataChannel File (" + name + "): split, new file = " + fileName);
                        try {
                            evioFileWriter = new EventWriter(fileName);
                        }
                        catch (EvioException e) {
                            errorMsg.compareAndSet(null, "Cannot creat file " + fileName);
                            throw e;
                        }
                    }

//logger.info("      DataChannel File (" + name + "): try writing into file" + fileName);
                    try {
                        evioFileWriter.writeEvent(bank);
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "Cannot write to file");
                        throw e;
                    }

                    numBytesWritten += bankBytes;

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