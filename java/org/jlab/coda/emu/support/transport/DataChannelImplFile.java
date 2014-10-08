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

    /** Thread used to output data. */
    private DataOutputHelper dataOutputThread;

    /** Name of file being written -to / read-from. */
    private String fileName;

    //----------------------------------------
    // Output file parameters
    //----------------------------------------

    /** The default size in bytes at which a new file is created. */
    private long split;

    /** If splitting files, the number file being written currently to. */
    private int splitCount;

    /** For output files, the directory. */
    private String directory;

    /** Dictionary to be include in file. */
    private String dictionaryXML;

    /** Evio file writer. */
    private EventWriter evioFileWriter;

    //----------------------------------------
    // Input file parameters
    //----------------------------------------

    /** Evio file reader. */
    private EvioReader evioFileReader;

    /** Evio file reader which does NOT deserialize into objects. */
    private EvioCompactReader compactFileReader;

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
     * @param outputIndex   order in which module's events will be sent to this
     *                      output channel (0 for first output channel, 1 for next, etc.).
     * @throws DataTransportException if unable to create fifo buffer.
     */
    DataChannelImplFile(String name, DataTransportImplFile transport,
                        Map<String, String> attributeMap, boolean input, Emu emu,
                        EmuModule module, int outputIndex)
            throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module, outputIndex);

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

                if (ringItemType == ModuleIoType.PayloadBank) {
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
                else if  (ringItemType == ModuleIoType.PayloadBuffer) {
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
                // Make overwriting the file OK if there is NO splitting of the file.
                // If there is no file splitting, overwriting the file will occur when
                // the file name is static or if the run # is repeated.
                boolean overWriteOK = true;
                if (split > 0L) overWriteOK = false;

                evioFileWriter = new EventWriter(fileName, directory, runType,
                                                 runNumber, split, byteOrder,
                                                 dictionaryXML, overWriteOK);
logger.info("      DataChannel File: create EventWriter of order = " + byteOrder);
logger.info("      DataChannel File: try writing to file " + evioFileWriter.getCurrentFilePath());

                // Tell emu what that output name is for stat reporting.
                // Get the name from the file writer object so that the
                // final filename is used with all string substitutions made.
                // This must be done each time the file is split.
                emu.setOutputDestination(evioFileWriter.getCurrentFilePath());

                // Keep track of how many files we create
                if (split > 0L) splitCount = evioFileWriter.getSplitCount();

                dataOutputThread = new DataOutputHelper((emu.getThreadGroup()),  name() + " data out");
                dataOutputThread.start();
                dataOutputThread.waitUntilStarted();
            }
        }
        catch (Exception e) {
            if (input) {
                throw new DataTransportException("      DataChannel File: Cannot open data file " + e.getMessage(), e);
            }
            else {
                throw new DataTransportException("      DataChannel File: Cannot create data file" + e.getMessage(), e);
            }
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
        logger.warn("      DataChannel File: end() " + name);

        gotEndCmd = true;
        gotResetCmd = false;

        state = CODAState.DOWNLOADED;
    }


    /**
     * {@inheritDoc}
     */
    public void reset() {
logger.debug("      DataChannel File: reset() " + name + " channel");

        gotEndCmd   = false;
        gotResetCmd = true;

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

        errorMsg.set(null);
        state = CODAState.CONFIGURED;
logger.debug("      DataChannel File: reset() " + name + " - done");
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

//        super.processEnd(eventIndex, ringIndex);

        eventIndexEnd = eventIndex;
        ringIndexEnd  = ringIndex;

        if (input || !dataOutputThread.isAlive()) {
logger.debug("      DataChannel File out " + outputIndex + ": processEnd(), thread already done");
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
logger.debug("      DataChannel File out " + outputIndex + ": processEnd(), thread done after waiting");
            return;
        }

        // Probably stuck trying to get item from ring buffer,
        // so interrupt it and get it to read the END event from
        // the correct ring.
logger.debug("      DataChannel File out " + outputIndex + ": processEnd(), interrupt thread in state " +
                     dataOutputThread.threadState);
        dataOutputThread.interrupt();
    }


    /**
     * Class <b>DataInputHelper</b>
     * This class reads data from the file and puts it on the ring.
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
            if (ringItemType == ModuleIoType.PayloadBank) {
                runBanks();
            }
            else if  (ringItemType == ModuleIoType.PayloadBuffer) {
                runBuffers();
            }
        }

        /** {@inheritDoc} */
        public void runBanks() {

            int counter = 0;
            long nextRingItem;
            EventType bankType;
            ControlType controlType;
            RingItem ringItem;

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

                    nextRingItem = ringBufferIn.next();
                    ringItem = ringBufferIn.get(nextRingItem);

                    ringItem.setEvent(event);
                    ringItem.setEventType(bankType);
                    ringItem.setControlType(controlType);
                    ringItem.setRecordId(recordId);
                    ringItem.setSourceId(sourceId);
                    ringItem.setSourceName(name);
                    ringItem.setEventCount(1);
                    ringItem.matchesId(sourceId == id);

                    ringBufferIn.publish(nextRingItem);

                    counter++;
                }

                // Put in END event
                nextRingItem = ringBufferIn.next();
                ringItem = ringBufferIn.get(nextRingItem);

                ringItem.setEvent(Evio.createControlEvent(ControlType.END, 0, 0, counter, 0));
                ringItem.setEventType(EventType.CONTROL);
                ringItem.setControlType(ControlType.END);
                ringItem.setSourceName(name);
                ringItem.setEventCount(1);

                ringBufferIn.publish(nextRingItem);

                if (endCallback != null) endCallback.endWait();

            }
            catch (Exception e) {
//logger.warn("      DataChannel File in: (" + name + ") close file");
//logger.warn("      DataChannel File in: (" + name + ") exit " + e.getMessage());
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
            long nextRingItem;
            EventType bankType;
            ControlType controlType;
            RingItem ringItem;

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
//                    payloadBuffer = new PayloadBuffer(compactFileReader.getEventBuffer(i),
//                                                      bankType, controlType, recordId,
//                                                      sourceId, name, node);

                    nextRingItem = ringBufferIn.next();
                    ringItem = ringBufferIn.get(nextRingItem);

                    ringItem.setNode(node);
                    ringItem.setBuffer(node.getStructureBuffer(false));
                    ringItem.setEventType(bankType);
                    ringItem.setControlType(controlType);
                    ringItem.setRecordId(recordId);
                    ringItem.setSourceId(sourceId);
                    ringItem.setSourceName(name);
                    ringItem.setEventCount(1);
                    ringItem.matchesId(sourceId == id);

                    ringBufferIn.publish(nextRingItem);

                    counter++;
                }

                // Put in END event
                nextRingItem = ringBufferIn.next();
                ringItem = ringBufferIn.get(nextRingItem);

                ringItem.setEvent(Evio.createControlEvent(ControlType.END, 0, 0, counter, 0));
                ringItem.setEventType(EventType.CONTROL);
                ringItem.setControlType(ControlType.END);
                ringItem.setSourceName(name);
                ringItem.setEventCount(1);

                ringBufferIn.publish(nextRingItem);

                if (endCallback != null) endCallback.endWait();

            }
            catch (Exception e) {
//logger.warn("      DataChannel File in: (" + name + ") close file");
//logger.warn("      DataChannel File in: (" + name + ") exit " + e.getMessage());
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
    private class DataOutputHelper extends Thread {

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch latch = new CountDownLatch(1);

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** What state is this thread in? */
        private volatile ThreadState threadState;


        DataOutputHelper(ThreadGroup group, String name) {
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
         * Write event to file.
         *
         * @param ri          item to write to disk
         * @param forceToDisk if true, force event to hard disk
         * @throws IOException
         * @throws EvioException
         */
        private final void writeEvioData(RingItem ri, boolean forceToDisk) throws IOException, EvioException {

            if (ringItemType == ModuleIoType.PayloadBank) {
                evioFileWriter.writeEvent(ri.getEvent());
            }
            else if  (ringItemType == ModuleIoType.PayloadBuffer) {
//logger.info("      DataChannel File out: write buffer with order = " + ri.getBuffer().order());
                evioFileWriter.writeEvent(ri.getBuffer(), forceToDisk);
                ri.releaseByteBuffer();
            }
        }



        /** {@inheritDoc} */
        public void run() {

            threadState = ThreadState.RUNNING;

            // Tell the world I've started
            latch.countDown();

            try {
                RingItem ringItem;
                EventType pBankType;
                ControlType pBankControlType;

                // First event will be "prestart", by convention in ring 0
                ringItem = getNextOutputRingItem(0);
                // Force "prestart" event to hard disk
                writeEvioData(ringItem, true);
logger.debug("      DataChannel File out  " + outputIndex + ": wrote prestart");
                releaseCurrentAndGoToNextOutputRingItem(0);

                // Second event will be "go", by convention in ring 0
                ringItem = getNextOutputRingItem(0);
                // Do not force "go" event to hard disk as data will
                // soon follow and things will be written out shortly.
                writeEvioData(ringItem, false);
logger.debug("      DataChannel File out " + outputIndex + ": wrote go");
                releaseCurrentAndGoToNextOutputRingItem(0);

                while ( true ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) {
                            try {Thread.sleep(5);}
                            catch (InterruptedException e1) {}
                        }
                        continue;
                    }

                    try {
//logger.debug("      DataChannel File out " + outputIndex + ": try getting next buffer from ring");
                        ringItem = getNextOutputRingItem(ringIndex);
//logger.debug("      DataChannel File out " + outputIndex + ": got next buffer");
//Utilities.printBuffer(ringItem.getBuffer(), 0, 6, name+": ev" + nextEvent + ", ring " + ringIndex);
                    }
                    catch (InterruptedException e) {
                        threadState = ThreadState.INTERRUPTED;
                        // If we're here we were blocked trying to read the next
                        // (END) event from the wrong ring. We've had 1/4 second
                        // to read everything else so let's try reading END from
                        // given ring.
System.out.println("      DataChannel File out " + outputIndex + ": try again, read END from ringIndex " + ringIndexEnd +
" not " + ringIndex);
                        ringItem = getNextOutputRingItem(ringIndexEnd);
                    }

                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    if (pBankControlType == ControlType.END) {
logger.debug("      DataChannel File out " + outputIndex + ": got  ev " + nextEvent +
                     ", ring " + ringIndex + " (END!)");
                    }
//                    else {
//logger.debug("      DataChannel File out " + outputIndex + ": got  ev " + nextEvent +
//                     ", ring " + ringIndex);
//                    }

                    try {
                        writeEvioData(ringItem, false);
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "Cannot write to file");
                        throw e;
                    }

//logger.debug("      DataChannel File out: release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);

                    // Do not go to the next ring if we got a control or user event.
                    // All prestart, go, & users go to the first ring. Just keep reading
                    // until we get to a built event. Then start keeping count so
                    // we know when to switch to the next ring.
                    if (outputRingCount > 1 && pBankControlType == null &&
                            !pBankType.isUser()) {
                        setNextEventAndRing();
//System.out.println("      DataChannel File out, " + name + ": for next ev " + nextEvent + " SWITCH TO ring = " + ringIndex);
                    }


                    // If splitting the output, the file name may change.
                    // Inform the authorities about this.
                    if (split > 0L && evioFileWriter.getSplitCount() > splitCount) {
                        emu.setOutputDestination(evioFileWriter.getCurrentFilename());
                        splitCount = evioFileWriter.getSplitCount();
                    }

                    if (pBankControlType == ControlType.END) {
//System.out.println("      DataChannel File out, " + outputIndex + ": got END event");
                        try {
                            evioFileWriter.close();
                        }
                        catch (Exception e) {
                            errorMsg.compareAndSet(null, "Cannot write to file");
                            throw e;
                        }
                        // run callback saying we got end event
                        if (endCallback != null) endCallback.endWait();
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
System.out.println("      DataChannel File out, " + outputIndex + ": got RESET/END cmd, quitting 1");
                        threadState = ThreadState.DONE;
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel File out, " + outputIndex + ": interrupted thd, exiting");
                e.printStackTrace();
            } catch (Exception e) {
                logger.warn("      DataChannel File out, " + outputIndex + " : exit thd: " + e.getMessage());
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());

                // set state
                state = CODAState.ERROR;
                emu.sendStatusMessage();

                e.printStackTrace();
            }

            threadState = ThreadState.DONE;
        }

    }

}