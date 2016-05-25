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
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
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

    /** Store locally whether this channel's module is an ER or not.
      * If so, don't parse incoming data so deeply - only top bank header. */
    private boolean isER;

    /** Evio file reader which does NOT deserialize into objects. */
    private EvioCompactReader compactFileReader;

    /** EventType taken from first block header of file. */
    private EventType eventType;

    /** Source CODA id taken from first block header of file. */
    private int sourceId;

    /** Record id taken from first block header of file. */
    private int recordId;

    /** Number of evio events (banks) in file. */
    private int eventCount;

    /** Does this file have a "first event" ? */
    private boolean hasFirstEvent;



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
                isER = (emu.getCodaClass() == CODAClass.ER);

                // This will throw an exception if evio version < 4
                compactFileReader = new EvioCompactReader(fileName);

                // Get the first block header
                // First evio block header read from a version 4 file
                BlockHeaderV4 firstBlockHeader = compactFileReader.getFirstBlockHeader();
                hasFirstEvent = firstBlockHeader.hasFirstEvent();

                // Get the # of events in file
                eventCount = compactFileReader.getEventCount();

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
logger.info("      DataChannel File: file = " + evioFileWriter.getCurrentFilePath());

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
            channelState = CODAState.ERROR;
            if (input) {
                emu.setErrorState("DataChannel File in: Cannot open file, " + e.getMessage());
                throw new DataTransportException("      DataChannel File: Cannot open data file " + e.getMessage(), e);
            }
            else {
                emu.setErrorState("DataChannel File out: Cannot create file, " + e.getMessage());
                throw new DataTransportException("      DataChannel File: Cannot create data file " + e.getMessage(), e);
            }
        }
    }


    /** {@inheritDoc} */
    public TransportType getTransportType() {
        return TransportType.FILE;
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
        logger.warn("      DataChannel File: end() " + name);

        gotEndCmd = true;
        gotResetCmd = false;

        channelState = CODAState.DOWNLOADED;
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
            if (compactFileReader != null) compactFileReader.close();
        } catch (Exception e) {}

        try {
            if (evioFileWriter != null) {
                // Insert an END event "by hand" if actively taking data when RESET hit
                if (channelState == CODAState.PAUSED || channelState == CODAState.ACTIVE) {
                    Object[] stats = module.getStatistics();
                    long eventsWritten = 0;
                    if (stats != null) {
                        eventsWritten = (Long) stats[0];
                    }
                    // This END, has error condition set in 2nd data word
                    PayloadBuffer endBuf = Evio.createControlBuffer(ControlType.END, emu.getRunNumber(),
                                                                    emu.getRunTypeId(), (int) eventsWritten,
                                                                    0, byteOrder, true);
                    evioFileWriter.writeEvent(endBuf.getBuffer());
                }

                // Then close to save everything to disk.
                evioFileWriter.close();
            }
        } catch (Exception e) {}

        channelState = CODAState.CONFIGURED;
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
//logger.debug("      DataChannel File out " + outputIndex + ": processEnd(), thread already done");
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
//logger.debug("      DataChannel File out " + outputIndex + ": processEnd(), thread done after waiting");
            return;
        }

        // Probably stuck trying to get item from ring buffer,
        // so interrupt it and get it to read the END event from
        // the correct ring.
//logger.debug("      DataChannel File out " + outputIndex + ": processEnd(), interrupt thread in state " +
//                     dataOutputThread.threadState);
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

            int counter = 0;
            long nextRingItem;
            EventType bankType;
            ControlType controlType;
            RingItem ringItem;
            boolean isUser = false;

            // I've started
            latch.countDown();

            try {
                // From first block header in file
                controlType = null;
                EvioNode node;

                for (int i=0; i < eventCount; i++) {
                    if (dataThread.isInterrupted()) break;

                    if (isER) {
                        // Don't need to parse all bank headers, just top level.
                        node = compactFileReader.getEvent(i);
                    }
                    else {
                        node = compactFileReader.getScannedEvent(i);
                    }

                    bankType = eventType;

                    // Unlikely that a file has roc raw data, but accommodate it anyway
                    if (eventType == EventType.ROC_RAW) {
                        if (Evio.isUserEvent(node)) {
                            bankType = EventType.USER;
                            isUser = true;
                        }
                    }
                    else if (eventType == EventType.CONTROL) {
                        // Find out exactly what type of control event it is
                        // (May be null if there is an error).
                        controlType = ControlType.getControlType(node.getTag());
                        if (controlType == null) {
                            channelState = CODAState.ERROR;
                            emu.setErrorState("DataChannel File in: found unidentified control event");
                            return;
                        }
                    }
                    else if (eventType == EventType.USER) {
                        isUser = true;
                    }

                    nextRingItem = ringBufferIn.next();
                    ringItem = ringBufferIn.get(nextRingItem);

                    if (bankType.isBuildable()) {
                        ringItem.setAll(null, null, node, bankType, controlType,
                                        isUser, hasFirstEvent, id, recordId, sourceId,
                                        node.getNum(), name, null, null);
                    }
                    else {
                        ringItem.setAll(null, null, node, bankType, controlType,
                                       isUser, hasFirstEvent, id, recordId, sourceId,
                                       1, name, null, null);
                    }

                    // In a file, only the first event can be a "first" or "beginning-of-run" event
                    isUser = hasFirstEvent = false;

                    ringBufferIn.publish(nextRingItem);

                    counter++;
                }

                // Put in END event
                nextRingItem = ringBufferIn.next();
                ringItem = ringBufferIn.get(nextRingItem);

                ringItem.setAll(Evio.createControlEvent(ControlType.END, 0, 0, counter, 0, false),
                                null, null, EventType.CONTROL, ControlType.END, false,
                                false, id, recordId, sourceId, 1, name, null, null);

                ringBufferIn.publish(nextRingItem);

                if (endCallback != null) endCallback.endWait();

            }
            catch (Exception e) {
//logger.warn("      DataChannel File in: (" + name + ") close file");
//logger.warn("      DataChannel File in: (" + name + ") exit " + e.getMessage());
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel File in: " + e.getMessage());
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
        private final void writeEvioData(RingItem ri, boolean forceToDisk)
                throws IOException, EvioException {

            if (ri.getBuffer() != null) {
//logger.info("      DataChannel File out: write buffer with order = " + ri.getBuffer().order());
                evioFileWriter.writeEvent(ri.getBuffer(), forceToDisk);
            }
            else {
//logger.info("      DataChannel File out: write buffer with order = " + ri.getNode().getBufferNode().getBuffer().order());
                // Last boolean arg means do (not) duplicate node's buffer when writing.
                // Setting this to false led to problems since the input channel is using
                // the buffer at the same time.
                evioFileWriter.writeEvent(ri.getNode(), forceToDisk, true);
            }

            ri.releaseByteBuffer();
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
                boolean gotPrestart=false;

                // The 1st event may be a user event or a prestart.
                // After the prestart, the next event may be "go", "end", or a user event.
                // The non-END control events are placed on ring 0 of all output channels.
                // The END event is placed in the ring in which the next data event would
                // have gone. The user events are placed on ring 0 of only the first output
                // channel.

                // Keep reading user & control events (all of which will appear in ring 0)
                // until the 2nd control event (go or end) is read.
                while (true) {
                    // Read next event
                    ringItem  = getNextOutputRingItem(0);
                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    // If control event ...
                    if (pBankType == EventType.CONTROL) {
                        // if prestart ..
                        if (pBankControlType == ControlType.PRESTART) {
                            if (gotPrestart) {
                                throw new EmuException("got 2 prestart events");
                            }
//System.out.println("      DataChannel File out " + outputIndex + ": found & write prestart event");
                            gotPrestart = true;
                            // Force prestart to hard disk
                            writeEvioData(ringItem, true);
                        }
                        else {
                            if (!gotPrestart) {
                                throw new EmuException("prestart, not " + pBankControlType +
                                                       ", must be first control event");
                            }

                            if (pBankControlType != ControlType.GO &&
                                pBankControlType != ControlType.END)  {
                                throw new EmuException("second control event must be go or end");
                            }

                            // Do NOT force to hard disk as it may be go and will slow things down
//System.out.println("      DataChannel File out " + outputIndex + ": found & write " + pBankControlType + " event");
                            writeEvioData(ringItem, false);

                            // Go to the next event
                            gotoNextRingItem(0);

                            // Done looking for the 2 control events
                            break;
                        }
                    }
                    // If user event ...
                    else if (pBankType == EventType.USER) {
//System.out.println("      DataChannel File out " + outputIndex + ": found user event");
                        if (ringItem.isFirstEvent()) {
//System.out.println("      DataChannel File out " + outputIndex + ": writing first event");
                            evioFileWriter.setFirstEvent(ringItem.getNode());
                            // The writer will handle the first event from here
                            ringItem.releaseByteBuffer();
                        }
                        else {
                            // force to hard disk.
//System.out.println("      DataChannel File out " + outputIndex + ": writing user event");
                            writeEvioData(ringItem, true);
                        }
                    }
                    // Only user and control events should come first, so error
                    else {
                        throw new EmuException(pBankType + " type of events must come after go event");
                    }

                    // Keep reading events till we hit go/end
                    gotoNextRingItem(0);
                }

                // Release ring items gotten so far
                releaseOutputRingItem(0);
//System.out.println("      DataChannel File out " + outputIndex + ": releasing initial control & user events");

                // END may come right after PRESTART
                if (pBankControlType == ControlType.END) {
logger.debug("      DataChannel File out " + outputIndex + ": wrote end");
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

logger.debug("      DataChannel File out " + outputIndex + ": wrote go");

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
                        // If we're here we were blocked trying to read the next event.
                        // If there are multiple event building threads in the module,
                        // then the END event may show up in an unexpected ring.
                        // The reason for this is that one thread writes to only one ring.
                        // But since only 1 thread gets the END event, it must write it
                        // into that ring in all output channels whether that ring was
                        // the next place to put a data event or not. Thus it may end up
                        // in a ring which was not the one to be read next.
                        // We've had 1/4 second to read everything else so let's try
                        // reading END from this now-known "unexpected" ring.
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

                    try {
                        // If this a user and "first event", let the writer know
                        if (ringItem.isFirstEvent()) {
                            evioFileWriter.setFirstEvent(ringItem.getNode());
                            // The writer will handle the first event from here,
                            // go to the next event now.
                            ringItem.releaseByteBuffer();
                        }
                        else {
//logger.debug("      DataChannel File out " + outputIndex + ": write!");
                            writeEvioData(ringItem, false);
                        }
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "Cannot write to file");
                        throw e;
                    }

//logger.debug("      DataChannel File out: release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);

                    // Do not go to the next ring if we got a user event.
                    // Just keep reading until we get to a built event.
                    // Then start keeping count so we know when to switch to the next ring.
                    //
                    // Prestart & go went to the first ring and have already been
                    // dealt with. End event will stop this thread so don't worry about
                    // not switching rings.
                    if (outputRingCount > 1 && !pBankType.isUser()) {
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
            } catch (Exception e) {
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel File out: " + e.getMessage());
logger.warn("      DataChannel File out, " + outputIndex + " : exit thd: " + e.getMessage());
            }

            threadState = ThreadState.DONE;
        }


        /** {@inheritDoc} */
        public void runOrig() {

            threadState = ThreadState.RUNNING;

            // Tell the world I've started
            latch.countDown();

            try {
                RingItem ringItem;
                EventType pBankType;
                ControlType pBankControlType;

                // The 1st event will be "prestart", by convention in ring 0
                ringItem = getNextOutputRingItem(0);
                // Force "prestart" event to hard disk
                writeEvioData(ringItem, true);
                releaseCurrentAndGoToNextOutputRingItem(0);

                // The 2nd event may be "go", "end", or a user event.
                // The non-END control events are placed on ring 0 of all output channels.
                // The END event is placed in the ring in which the next data event would
                // have gone.
                // The user events are placed on ring 0 of only the first output channel.
                // Keep reading user events in ring 0 until a control (go/end) is read.
                while (true) {
                    // Read next event
                    ringItem  = getNextOutputRingItem(0);
                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();
                    // If user event, force to hard disk.
                    if (pBankType == EventType.USER) {
                        // If this user event is also a "first event", let the writer know
                        if (ringItem.isFirstEvent()) {
                            evioFileWriter.setFirstEvent(ringItem.getNode());
                            // The writer will handle the first event from here,
                            // go to the next event now.
                            ringItem.releaseByteBuffer();
                        }
                        else {
                            writeEvioData(ringItem, true);
                        }
                    }
                    // If possibly GO, do not force to hard disk as that slows things down.
                    else {
                        writeEvioData(ringItem, false);
                    }
                    releaseCurrentAndGoToNextOutputRingItem(0);
                    // Keep reading user events
                    if (pBankType != EventType.USER) break;
                }

                // END may come right after PRESTART
                if (pBankControlType == ControlType.END) {
System.out.println("      DataChannel File out, " + outputIndex + ": wrote end");
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

logger.debug("      DataChannel File out " + outputIndex + ": wrote go");

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
                        // If we're here we were blocked trying to read the next event.
                        // If there are multiple event building threads in the module,
                        // then the END event may show up in an unexpected ring.
                        // The reason for this is that one thread writes to only one ring.
                        // But since only 1 thread gets the END event, it must write it
                        // into that ring in all output channels whether that ring was
                        // the next place to put a data event or not. Thus it may end up
                        // in a ring which was not the one to be read next.
                        // We've had 1/4 second to read everything else so let's try
                        // reading END from this now-known "unexpected" ring.
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

                    try {
                        // If this a user and "first event", let the writer know
                        if (ringItem.isFirstEvent()) {
                            evioFileWriter.setFirstEvent(ringItem.getNode());
                            // The writer will handle the first event from here,
                            // go to the next event now.
                            ringItem.releaseByteBuffer();
                        }
                        else {
//logger.debug("      DataChannel File out " + outputIndex + ": write!");
                            writeEvioData(ringItem, false);
                        }
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "Cannot write to file");
                        throw e;
                    }

//logger.debug("      DataChannel File out: release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);

                    // Do not go to the next ring if we got a user event.
                    // Just keep reading until we get to a built event.
                    // Then start keeping count so we know when to switch to the next ring.
                    //
                    // Prestart & go went to the first ring and have already been
                    // dealt with. End event will stop this thread so don't worry about
                    // not switching rings.
                    if (outputRingCount > 1 && !pBankType.isUser()) {
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
            } catch (Exception e) {
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel File out: " + e.getMessage());
logger.warn("      DataChannel File out, " + outputIndex + " : exit thd: " + e.getMessage());
            }

            threadState = ThreadState.DONE;
        }

    }

//
//
//    /**
//     * Class <b>DataOutputHelper </b>
//     * Handles writing evio events (banks) to a file.
//     * A lot of the work is done in jevio such as splitting files.
//     */
//    private class DataOutputHelper extends Thread {
//
//        /** Let a single waiter know that the main thread has been started. */
//        private CountDownLatch latch = new CountDownLatch(1);
//
//        /** Help in pausing DAQ. */
//        private int pauseCounter;
//
//        /** What state is this thread in? */
//        private volatile ThreadState threadState;
//
//
//        DataOutputHelper(ThreadGroup group, String name) {
//            super(group, name);
//        }
//
//
//        /** A single waiter can call this method which returns when thread was started. */
//        private void waitUntilStarted() {
//            try {
//                latch.await();
//            }
//            catch (InterruptedException e) {}
//        }
//
//
//        /**
//         * Write event to file.
//         *
//         * @param ri          item to write to disk
//         * @param forceToDisk if true, force event to hard disk
//         * @throws IOException
//         * @throws EvioException
//         */
//        private final void writeEvioData(RingItem ri, boolean forceToDisk)
//                throws IOException, EvioException {
//
//            if (ri.getBuffer() != null) {
////logger.info("      DataChannel File out: write buffer with order = " + ri.getBuffer().order());
//                evioFileWriter.writeEvent(ri.getBuffer(), forceToDisk);
//            }
//            else {
////logger.info("      DataChannel File out: write buffer with order = " + ri.getNode().getBufferNode().getBuffer().order());
//                // Last boolean arg means do (not) duplicate node's buffer when writing.
//                // Setting this to false led to problems since the input channel is using
//                // the buffer at the same time.
//                evioFileWriter.writeEvent(ri.getNode(), forceToDisk, true);
//            }
//
//            ri.releaseByteBuffer();
//        }
//
//
//        /** {@inheritDoc} */
//        public void run() {
//
//            threadState = ThreadState.RUNNING;
//
//            // Tell the world I've started
//            latch.countDown();
//
//            try {
//                RingItem ringItem;
//                EventType pBankType;
//                ControlType pBankControlType;
//                ArrayList<RingItem> userList = new ArrayList<RingItem>(20);
//                boolean gotPrestart=false;
//
//                // The 1st event may be a user event or a prestart. If user events come
//                // before prestart, store them, wait for prestart, write prestart, then
//                // finally write the users. This ensures that the prestart event is always
//                // first in the file.
//
//                // After the prestart, the next event may be "go", "end", or a user event.
//                // The non-END control events are placed on ring 0 of all output channels.
//                // The END event is placed in the ring in which the next data event would
//                // have gone. The user events are placed on ring 0 of only the first output
//                // channel.
//
//                // Keep reading user & control events (all of which will appear in ring 0)
//                // until the 2nd control event (go or end) is read.
//                while (true) {
//                    // Read next event
//                    ringItem  = getNextOutputRingItem(0);
//                    pBankType = ringItem.getEventType();
//                    pBankControlType = ringItem.getControlType();
//
//                    // If control event ...
//                    if (pBankType == EventType.CONTROL) {
//                        // if prestart ..
//                        if (pBankControlType == ControlType.PRESTART) {
//                            if (gotPrestart) {
//                                throw new EmuException("got 2 prestart events");
//                            }
////System.out.println("      DataChannel File out " + outputIndex + ": found & write prestart event");
//                            gotPrestart = true;
//                            // Force prestart to hard disk
//                            writeEvioData(ringItem, true);
//                        }
//                        else {
//                            if (!gotPrestart) {
//                                throw new EmuException("prestart, not " + pBankControlType +
//                                                       ", must be first control event");
//                            }
//
//                            if (pBankControlType != ControlType.GO &&
//                                pBankControlType != ControlType.END)  {
//                                throw new EmuException("second control event must be go or end");
//                            }
//
//                            // Prestart has been written, but not go/end,
//                            // so write any stored user events now
//                            for (RingItem ri : userList) {
//                                // If this user event is also a "first event", let the writer know
//                                if (ri.isFirstEvent()) {
////System.out.println("      DataChannel File out " + outputIndex + ": writing stored first event");
//                                    evioFileWriter.setFirstEvent(ri.getNode());
//                                    // The writer will handle the first event from here
//                                    ri.releaseByteBuffer();
//                                }
//                                else {
//                                    // force to hard disk.
////System.out.println("      DataChannel File out " + outputIndex + ": writing stored user event");
//                                    writeEvioData(ri, true);
//                                }
//                            }
//                            userList.clear();
//
//                            // Do NOT force to hard disk as it may be go and will slow things down
////System.out.println("      DataChannel File out " + outputIndex + ": found & write " + pBankControlType + " event");
//                            writeEvioData(ringItem, false);
//
//                            // Go to the next event
//                            gotoNextRingItem(0);
//
//                            // Done looking for the 2 control events
//                            break;
//                        }
//                    }
//                    // If user event ...
//                    else if (pBankType == EventType.USER) {
////System.out.println("      DataChannel File out " + outputIndex + ": found & store user event");
//                        userList.add(ringItem);
//                    }
//                    // Only user and control events should come first, so error
//                    else {
//                        throw new EmuException(pBankType + " type of events must come after go event");
//                    }
//
//                    // Keep reading events till we hit go/end
//                    gotoNextRingItem(0);
//                }
//
//                // Release ring items gotten so far
//                releaseOutputRingItem(0);
////System.out.println("      DataChannel File out " + outputIndex + ": releasing initial control & user events");
//
//                // END may come right after PRESTART
//                if (pBankControlType == ControlType.END) {
//logger.debug("      DataChannel File out " + outputIndex + ": wrote end");
//                    try {
//                        evioFileWriter.close();
//                    }
//                    catch (Exception e) {
//                        errorMsg.compareAndSet(null, "Cannot write to file");
//                        throw e;
//                    }
//                    // run callback saying we got end event
//                    if (endCallback != null) endCallback.endWait();
//                    threadState = ThreadState.DONE;
//                    return;
//                }
//
//logger.debug("      DataChannel File out " + outputIndex + ": wrote go");
//
//                while ( true ) {
//
//                    if (pause) {
//                        if (pauseCounter++ % 400 == 0) {
//                            try {Thread.sleep(5);}
//                            catch (InterruptedException e1) {}
//                        }
//                        continue;
//                    }
//
//                    try {
////logger.debug("      DataChannel File out " + outputIndex + ": try getting next buffer from ring");
//                        ringItem = getNextOutputRingItem(ringIndex);
////logger.debug("      DataChannel File out " + outputIndex + ": got next buffer");
////Utilities.printBuffer(ringItem.getBuffer(), 0, 6, name+": ev" + nextEvent + ", ring " + ringIndex);
//                    }
//                    catch (InterruptedException e) {
//                        threadState = ThreadState.INTERRUPTED;
//                        // If we're here we were blocked trying to read the next event.
//                        // If there are multiple event building threads in the module,
//                        // then the END event may show up in an unexpected ring.
//                        // The reason for this is that one thread writes to only one ring.
//                        // But since only 1 thread gets the END event, it must write it
//                        // into that ring in all output channels whether that ring was
//                        // the next place to put a data event or not. Thus it may end up
//                        // in a ring which was not the one to be read next.
//                        // We've had 1/4 second to read everything else so let's try
//                        // reading END from this now-known "unexpected" ring.
//System.out.println("      DataChannel File out " + outputIndex + ": try again, read END from ringIndex " + ringIndexEnd +
//" not " + ringIndex);
//                        ringItem = getNextOutputRingItem(ringIndexEnd);
//                    }
//
//                    pBankType = ringItem.getEventType();
//                    pBankControlType = ringItem.getControlType();
//
//                    if (pBankControlType == ControlType.END) {
//logger.debug("      DataChannel File out " + outputIndex + ": got  ev " + nextEvent +
//                     ", ring " + ringIndex + " (END!)");
//                    }
//
//                    try {
//                        // If this a user and "first event", let the writer know
//                        if (ringItem.isFirstEvent()) {
//                            evioFileWriter.setFirstEvent(ringItem.getNode());
//                            // The writer will handle the first event from here,
//                            // go to the next event now.
//                            ringItem.releaseByteBuffer();
//                        }
//                        else {
////logger.debug("      DataChannel File out " + outputIndex + ": write!");
//                            writeEvioData(ringItem, false);
//                        }
//                    }
//                    catch (Exception e) {
//                        errorMsg.compareAndSet(null, "Cannot write to file");
//                        throw e;
//                    }
//
////logger.debug("      DataChannel File out: release ring item");
//                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);
//
//                    // Do not go to the next ring if we got a user event.
//                    // Just keep reading until we get to a built event.
//                    // Then start keeping count so we know when to switch to the next ring.
//                    //
//                    // Prestart & go went to the first ring and have already been
//                    // dealt with. End event will stop this thread so don't worry about
//                    // not switching rings.
//                    if (outputRingCount > 1 && !pBankType.isUser()) {
//                        setNextEventAndRing();
////System.out.println("      DataChannel File out, " + name + ": for next ev " + nextEvent + " SWITCH TO ring = " + ringIndex);
//                    }
//
//                    // If splitting the output, the file name may change.
//                    // Inform the authorities about this.
//                    if (split > 0L && evioFileWriter.getSplitCount() > splitCount) {
//                        emu.setOutputDestination(evioFileWriter.getCurrentFilename());
//                        splitCount = evioFileWriter.getSplitCount();
//                    }
//
//                    if (pBankControlType == ControlType.END) {
////System.out.println("      DataChannel File out, " + outputIndex + ": got END event");
//                        try {
//                            evioFileWriter.close();
//                        }
//                        catch (Exception e) {
//                            errorMsg.compareAndSet(null, "Cannot write to file");
//                            throw e;
//                        }
//                        // run callback saying we got end event
//                        if (endCallback != null) endCallback.endWait();
//                        threadState = ThreadState.DONE;
//                        return;
//                    }
//
//                    // If I've been told to RESET ...
//                    if (gotResetCmd) {
//System.out.println("      DataChannel File out, " + outputIndex + ": got RESET/END cmd, quitting 1");
//                        threadState = ThreadState.DONE;
//                        return;
//                    }
//                }
//
//            } catch (InterruptedException e) {
//logger.warn("      DataChannel File out, " + outputIndex + ": interrupted thd, exiting");
//            } catch (Exception e) {
//                channelState = CODAState.ERROR;
//                emu.setErrorState("DataChannel File out: " + e.getMessage());
//logger.warn("      DataChannel File out, " + outputIndex + " : exit thd: " + e.getMessage());
//            }
//
//            threadState = ThreadState.DONE;
//        }
//
//
//        /** {@inheritDoc} */
//        public void runOrig() {
//
//            threadState = ThreadState.RUNNING;
//
//            // Tell the world I've started
//            latch.countDown();
//
//            try {
//                RingItem ringItem;
//                EventType pBankType;
//                ControlType pBankControlType;
//
//                // The 1st event will be "prestart", by convention in ring 0
//                ringItem = getNextOutputRingItem(0);
//                // Force "prestart" event to hard disk
//                writeEvioData(ringItem, true);
//                releaseCurrentAndGoToNextOutputRingItem(0);
//
//                // The 2nd event may be "go", "end", or a user event.
//                // The non-END control events are placed on ring 0 of all output channels.
//                // The END event is placed in the ring in which the next data event would
//                // have gone.
//                // The user events are placed on ring 0 of only the first output channel.
//                // Keep reading user events in ring 0 until a control (go/end) is read.
//                while (true) {
//                    // Read next event
//                    ringItem  = getNextOutputRingItem(0);
//                    pBankType = ringItem.getEventType();
//                    pBankControlType = ringItem.getControlType();
//                    // If user event, force to hard disk.
//                    if (pBankType == EventType.USER) {
//                        // If this user event is also a "first event", let the writer know
//                        if (ringItem.isFirstEvent()) {
//                            evioFileWriter.setFirstEvent(ringItem.getNode());
//                            // The writer will handle the first event from here,
//                            // go to the next event now.
//                            ringItem.releaseByteBuffer();
//                        }
//                        else {
//                            writeEvioData(ringItem, true);
//                        }
//                    }
//                    // If possibly GO, do not force to hard disk as that slows things down.
//                    else {
//                        writeEvioData(ringItem, false);
//                    }
//                    releaseCurrentAndGoToNextOutputRingItem(0);
//                    // Keep reading user events
//                    if (pBankType != EventType.USER) break;
//                }
//
//                // END may come right after PRESTART
//                if (pBankControlType == ControlType.END) {
//System.out.println("      DataChannel File out, " + outputIndex + ": wrote end");
//                    try {
//                        evioFileWriter.close();
//                    }
//                    catch (Exception e) {
//                        errorMsg.compareAndSet(null, "Cannot write to file");
//                        throw e;
//                    }
//                    // run callback saying we got end event
//                    if (endCallback != null) endCallback.endWait();
//                    threadState = ThreadState.DONE;
//                    return;
//                }
//
//logger.debug("      DataChannel File out " + outputIndex + ": wrote go");
//
//                while ( true ) {
//
//                    if (pause) {
//                        if (pauseCounter++ % 400 == 0) {
//                            try {Thread.sleep(5);}
//                            catch (InterruptedException e1) {}
//                        }
//                        continue;
//                    }
//
//                    try {
////logger.debug("      DataChannel File out " + outputIndex + ": try getting next buffer from ring");
//                        ringItem = getNextOutputRingItem(ringIndex);
////logger.debug("      DataChannel File out " + outputIndex + ": got next buffer");
////Utilities.printBuffer(ringItem.getBuffer(), 0, 6, name+": ev" + nextEvent + ", ring " + ringIndex);
//                    }
//                    catch (InterruptedException e) {
//                        threadState = ThreadState.INTERRUPTED;
//                        // If we're here we were blocked trying to read the next event.
//                        // If there are multiple event building threads in the module,
//                        // then the END event may show up in an unexpected ring.
//                        // The reason for this is that one thread writes to only one ring.
//                        // But since only 1 thread gets the END event, it must write it
//                        // into that ring in all output channels whether that ring was
//                        // the next place to put a data event or not. Thus it may end up
//                        // in a ring which was not the one to be read next.
//                        // We've had 1/4 second to read everything else so let's try
//                        // reading END from this now-known "unexpected" ring.
//System.out.println("      DataChannel File out " + outputIndex + ": try again, read END from ringIndex " + ringIndexEnd +
//" not " + ringIndex);
//                        ringItem = getNextOutputRingItem(ringIndexEnd);
//                    }
//
//                    pBankType = ringItem.getEventType();
//                    pBankControlType = ringItem.getControlType();
//
//                    if (pBankControlType == ControlType.END) {
//logger.debug("      DataChannel File out " + outputIndex + ": got  ev " + nextEvent +
//                     ", ring " + ringIndex + " (END!)");
//                    }
//
//                    try {
//                        // If this a user and "first event", let the writer know
//                        if (ringItem.isFirstEvent()) {
//                            evioFileWriter.setFirstEvent(ringItem.getNode());
//                            // The writer will handle the first event from here,
//                            // go to the next event now.
//                            ringItem.releaseByteBuffer();
//                        }
//                        else {
////logger.debug("      DataChannel File out " + outputIndex + ": write!");
//                            writeEvioData(ringItem, false);
//                        }
//                    }
//                    catch (Exception e) {
//                        errorMsg.compareAndSet(null, "Cannot write to file");
//                        throw e;
//                    }
//
////logger.debug("      DataChannel File out: release ring item");
//                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);
//
//                    // Do not go to the next ring if we got a user event.
//                    // Just keep reading until we get to a built event.
//                    // Then start keeping count so we know when to switch to the next ring.
//                    //
//                    // Prestart & go went to the first ring and have already been
//                    // dealt with. End event will stop this thread so don't worry about
//                    // not switching rings.
//                    if (outputRingCount > 1 && !pBankType.isUser()) {
//                        setNextEventAndRing();
////System.out.println("      DataChannel File out, " + name + ": for next ev " + nextEvent + " SWITCH TO ring = " + ringIndex);
//                    }
//
//                    // If splitting the output, the file name may change.
//                    // Inform the authorities about this.
//                    if (split > 0L && evioFileWriter.getSplitCount() > splitCount) {
//                        emu.setOutputDestination(evioFileWriter.getCurrentFilename());
//                        splitCount = evioFileWriter.getSplitCount();
//                    }
//
//                    if (pBankControlType == ControlType.END) {
////System.out.println("      DataChannel File out, " + outputIndex + ": got END event");
//                        try {
//                            evioFileWriter.close();
//                        }
//                        catch (Exception e) {
//                            errorMsg.compareAndSet(null, "Cannot write to file");
//                            throw e;
//                        }
//                        // run callback saying we got end event
//                        if (endCallback != null) endCallback.endWait();
//                        threadState = ThreadState.DONE;
//                        return;
//                    }
//
//                    // If I've been told to RESET ...
//                    if (gotResetCmd) {
//System.out.println("      DataChannel File out, " + outputIndex + ": got RESET/END cmd, quitting 1");
//                        threadState = ThreadState.DONE;
//                        return;
//                    }
//                }
//
//            } catch (InterruptedException e) {
//logger.warn("      DataChannel File out, " + outputIndex + ": interrupted thd, exiting");
//            } catch (Exception e) {
//                channelState = CODAState.ERROR;
//                emu.setErrorState("DataChannel File out: " + e.getMessage());
//logger.warn("      DataChannel File out, " + outputIndex + " : exit thd: " + e.getMessage());
//            }
//
//            threadState = ThreadState.DONE;
//        }
//
//    }


}