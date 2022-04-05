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
import org.jlab.coda.hipo.CompressionType;
import org.jlab.coda.jevio.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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

    /** Name of file being written-to / read-from. */
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
    private EventWriterUnsync evioFileWriter;

    /** Remember which element of the output destinations array belongs to this file. */
    private int outputNameIndex = 0;

    /** Type of compression in output file.
     * <ol start="0">
     *     <li>none</li>
     *     <li>lz4/li>
     *     <li>lz4 best</li>
     *     <li>gzip</li>
     * </ol> */
    private int compression;

    /** Number of threads with which to compress data when writing to a file. */
    private int compressionThreads;

    /**
     * When END is hit and the event pipeline is backed up due to a full disk,
     * then start dumping physics events in order for user and command events to
     * get through. This is the count of how many physics events were discarded.
     */
    private long dumpedEvents;

    /**
     * When END is hit and the event pipeline is backed up due to a full disk,
     * then start dumping physics events in order for user and command events to
     * get through. This is the count of how much memory in words was discarded.
     */
    private long dumpedWords;

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

    /** Use this object to set the sub stream ids if multiple output files per emu. */
    static private AtomicInteger subStreamIdCount = new AtomicInteger(0);

    
    /**
     * Constructor DataChannelImplFile creates a new DataChannelImplFile instance.
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

        //----------------------------------
        // Create file name
        //----------------------------------
        try {
            // Filename given in config file?
            fileName = attributeMap.get("fileName");
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
        //logger.info("      DataChannel File: config file name = " + fileName);
        //----------------------------------

        try {
            if (input) {
                isER = (emu.getCodaClass() == CODAClass.ER);

                // This will throw an exception if evio version > 4
                compactFileReader = new EvioCompactReader(fileName);

                // Get the first block header
                // First evio block header read from a version 4 file
                IBlockHeader firstBlockHeader = compactFileReader.getFirstBlockHeader();
                hasFirstEvent = firstBlockHeader.hasFirstEvent();

                // Get the # of events in file
                eventCount = compactFileReader.getEventCount();

                eventType = EventType.getEventType(firstBlockHeader.getEventType());
                sourceId  = firstBlockHeader.getSourceId();
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

                // Type of compression in file
                String comp = attributeMap.get("compression");
                if (comp != null) {
                    try {
                        compression = Integer.parseInt(comp);
                        // Ignore negative values
                        if (compression < 0) compression = 0;
                    }
                    catch (NumberFormatException e) {
                        compression = 0;
                    }
                }
                compression = 0;
                CompressionType compType = CompressionType.getCompressionType(compression);
                if (compType == null) {
                    compType = CompressionType.RECORD_UNCOMPRESSED;
                }
logger.info("      DataChannel File: compression = " + compType);

                // Number of compression thread
                compressionThreads = 1;
                comp = attributeMap.get("compressionThreads");
                if (comp != null) {
                    try {
                        compressionThreads = Integer.parseInt(comp);
                        // Ignore negative values
                        if (compressionThreads < 1) compressionThreads = 1;
                    }
                    catch (NumberFormatException e) {}
                }
logger.info("      DataChannel File: compressionThreads = " + compressionThreads);

                // Number of bytes specified for writer's internal buffer?
                int internalBufferSizeBytes = 64000000;  // 64MB by default

                String bufBytes = attributeMap.get("evioRamBuffer");
                if (bufBytes != null) {
                    try {
                        internalBufferSizeBytes = Integer.parseInt(bufBytes);
                        // Ignore small values
                        if (internalBufferSizeBytes < 64000000) internalBufferSizeBytes = 64000000;
                    }
                    catch (NumberFormatException e) {}
                }

                evioFileWriter = new EventWriterUnsync(fileName, directory, runType,
                                                       runNumber, split, 0, 100000,
                                                       byteOrder, dictionaryXML, overWriteOK,
                                                       false, null,
                                                       emu.getDataStreamId(),
                                                       subStreamIdCount.getAndIncrement(), // starting splitNumber
                                                       emu.getFileOutputCount(),  // splitIncrement
                                                       emu.getDataStreamCount(),  // stream count
                                                       compType, compressionThreads,
                                                       0, internalBufferSizeBytes);  // internal buffers of 64MB (0 -> 9MB)

                if (evioFileWriter.isDiskFull()) {
                    emu.sendRcWarningMessage("files cannot be written, disk almost full");
                }
logger.info("      DataChannel File: streamId = " + emu.getDataStreamId() + ", stream count = " +
            emu.getDataStreamCount() + ", filecount = " + emu.getFileOutputCount());

logger.info("      DataChannel File: file = " + evioFileWriter.getCurrentFilePath());

                // Tell emu what that output name is for stat reporting.
                // Get the name from the file writer object so that the
                // final filename is used with all string substitutions made.
                // This must be done each time the file is split.
                outputNameIndex = emu.addOutputDestination(evioFileWriter.getCurrentFilePath());

                // Keep track of how many files we create
                if (split > 0L) splitCount = evioFileWriter.getSplitCount();

                dataOutputThread = new DataOutputHelper((emu.getThreadGroup()),  name() + " data out");
                dataOutputThread.start();
                dataOutputThread.waitUntilStarted();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            channelState = CODAState.ERROR;
            if (input) {
System.out.println("      DataChannel File in: Cannot open file, " + e.getMessage());
                emu.setErrorState("DataChannel File in: Cannot open file, " + e.getMessage());
                throw new DataTransportException("      DataChannel File in: Cannot open data file " + e.getMessage(), e);
            }
            else {
System.out.println("      DataChannel File out: Cannot create file, " + e.getMessage());
                emu.setErrorState("DataChannel File out: Cannot create file, " + e.getMessage());
                throw new DataTransportException("      DataChannel File out: Cannot create data file " + e.getMessage(), e);
            }
        }
    }


    /** {@inheritDoc} */
    public TransportType getTransportType() {return TransportType.FILE;}


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


    /** {@inheritDoc} */
    public void end() {
        logger.info("      DataChannel File: end() " + name);

        gotEndCmd = true;
        gotResetCmd = false;

        // Reset the split count
        subStreamIdCount.set(0);

        if (dataThread != null) dataThread.interrupt();

        channelState = CODAState.DOWNLOADED;
    }


    /** {@inheritDoc} */
    public void reset() {
logger.info("      DataChannel File: reset " + name + " channel");

        gotEndCmd   = false;
        gotResetCmd = true;
        // Reset the split count
        subStreamIdCount.set(0);

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
// TODO: Take a look at this !!!
                    PayloadBuffer endBuf = Evio.createControlBuffer(ControlType.END, emu.getRunNumber(),
                                                                    emu.getRunTypeId(), (int) eventsWritten, 0,
                                                                    0, byteOrder, true, module.isStreamingData());
                    if (emu.isFileWritingOn()) {
                        evioFileWriter.writeEventToFile(null, endBuf.getBuffer(), true);
                    }
                }

                // Then close to save everything to disk.
                if (emu.isFileWritingOn()) {
logger.info("      DataChannel File: reset " + name + " - CALL writer close()");
                    evioFileWriter.close();
logger.info("      DataChannel File: reset " + name + " - DONE writer close()");
                }
            }
        } catch (Exception e) {}

        channelState = CODAState.CONFIGURED;
logger.info("      DataChannel File: reset " + name + " - done");
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

                    nextRingItem = ringBufferIn.nextIntr(1);
                    ringItem = ringBufferIn.get(nextRingItem);

                    if (bankType.isBuildable()) {
                        ringItem.setAll(null, null, node, bankType, controlType,
                                        isUser, hasFirstEvent, module.isStreamingData(), id, recordId, sourceId,
                                        node.getNum(), name, null, null);
                    }
                    else {
                        ringItem.setAll(null, null, node, bankType, controlType,
                                       isUser, hasFirstEvent, module.isStreamingData(), id, recordId, sourceId,
                                       1, name, null, null);
                    }

                    // In a file, only the first event can be a "first" or "beginning-of-run" event
                    isUser = hasFirstEvent = false;

                    ringBufferIn.publish(nextRingItem);

                    counter++;
                }

                // Put in END event
                nextRingItem = ringBufferIn.nextIntr(1);
                ringItem = ringBufferIn.get(nextRingItem);
                ringItem.setAll(Evio.createControlEvent(ControlType.END, 0, 0, counter, counter,
                                           0, false, module.isStreamingData()),
                                null, null, EventType.CONTROL, ControlType.END, false,
                                false, module.isStreamingData(), id, recordId, sourceId, 1, name, null, null);

                ringBufferIn.publish(nextRingItem);

                if (endCallback != null) endCallback.endWait();

            }
            catch (InterruptedException e) {
logger.warn("      DataChannel File in: (" + name + ") thd interrupted");
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
         * @throws InterruptedException
         */
        private final void writeEvioData(RingItem ri, boolean forceToDisk)
                throws IOException, EvioException, InterruptedException {

            if (emu.isFileWritingOn()) {

                boolean written;
                int repeatLoops = 0;
                boolean sentMsgToRC = false;

                if (ri.getBuffer() != null) {
//logger.info("      DataChannel File out: write buffer with order = " + ri.getBuffer().order());
                    written = evioFileWriter.writeEventToFile(null, ri.getBuffer(), forceToDisk);
                }
                else {
                    //logger.info("      DataChannel File out: write node with order = " + ri.getNode().getBuffer().order());
//logger.info("      DataChannel File out: write node");
                    // Last boolean arg means do (not) duplicate node's buffer when writing.
                    // Setting this to false led to problems since the input channel is using
                    // the buffer at the same time.
                    written = evioFileWriter.writeEventToFile(ri.getNode(), forceToDisk, true);
                }
//System.out.println("      DataChannel File out: written = " + written);

                while (!written) {

                    if (!sentMsgToRC && repeatLoops++ > 1) {
logger.info("      DataChannel File out: disc is full, waiting ...");
                        emu.sendRcWarningMessage("cannot write file, disc is full");
                        sentMsgToRC = true;
                    }

                    // If user has hit the "END" button ...
                    if (emu.theEndIsNigh()) {
//System.out.println("      DataChannel File out: END button hit .....");
                        // The user has hit the "END" button, but the END event (and therefore
                        // END command) cannot make it here since the disc is full.
                        // We're stuck in this loop and events have piled up.
                        // So, if we find that END has been hit through this "back door" method,
                        // we'll dump all the physics events and allow the user and
                        // END events to come through.

                        // At this point, write things regardless. Setting this flag will
                        // allow a new file to be created if necessary.
                        forceToDisk = true;

                        // If physics, dump it
                        if (ri.getEventType().isBuildable()) {
                            dumpedEvents++;
                            dumpedWords += ri.getTotalBytes()/4;
                            break;
                        }
                        else {
                            // Force these into the file. This write should be successful.
                            if (ri.getBuffer() != null) {
                                written = evioFileWriter.writeEventToFile(null, ri.getBuffer(), forceToDisk);
                            }
                            else {
                                written = evioFileWriter.writeEventToFile(ri.getNode(), forceToDisk, true);
                            }

                            break;
                        }
                    }
                    else {
//System.out.println("      DataChannel File out: sleep 1 sec, try write again .....");
                        // Wait 1 sec
                        Thread.sleep(1000);

                        // Try writing again
                        if (ri.getBuffer() != null) {
                            written = evioFileWriter.writeEventToFile(null, ri.getBuffer(), forceToDisk);
                        }
                        else {
                            written = evioFileWriter.writeEventToFile(ri.getNode(), forceToDisk, true);
                        }
                    }
                }

                // If msg was sent to RC saying disk if full AND we're here, then space got freed up
                if (sentMsgToRC && !emu.theEndIsNigh()) {
logger.info("      DataChannel File out: disc space is now available");
                    emu.sendRcInfoMessage("disc space is now available");
                }
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

                if (!emu.isFileWritingOn()) {
                    logger.rcConsole("      DataChannel File out " + outputIndex +
                                     ": File writing TURNED OFF", "NO FILE");
                }

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
                                throw new EmuException("got 2 PRESTART events");
                            }
                            gotPrestart = true;
                            // Force prestart to hard disk
System.out.println("      DataChannel File out " + outputIndex + ": try writing prestart event");
                            writeEvioData(ringItem, true);
System.out.println("      DataChannel File out " + outputIndex + ": wrote prestart event");
                        }
                        else {
                            if (!gotPrestart) {
                                throw new EmuException("PRESTART, not " + pBankControlType +
                                                       ", must be first control event");
                            }

                            if (pBankControlType != ControlType.GO &&
                                pBankControlType != ControlType.END)  {
                                throw new EmuException("second control event must be GO or END");
                            }

                            if (pBankControlType == ControlType.GO) {
                                // Do NOT force GO to hard disk as it will slow things down
                                writeEvioData(ringItem, false);
                            }
                            else {
                                writeEvioData(ringItem, true);
                            }

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
                            if (emu.isFileWritingOn()) {
                                try {
//System.out.println("      DataChannel File out " + outputIndex + ": try writing first event");
                                    // Buffer always gets first priority
                                    if (ringItem.getBuffer() != null) {
                                        evioFileWriter.setFirstEvent(ringItem.getBuffer());
                                    }
                                    else {
                                        evioFileWriter.setFirstEvent(ringItem.getNode());
                                    }
//System.out.println("      DataChannel File out " + outputIndex + ": wrote first event");
                                }
                                catch (EvioException e) {
                                    // Probably here due to bad evio format
                                    emu.sendRcWarningMessage(e.getMessage());
System.out.println("      DataChannel File out " + outputIndex + ": failed writing \"first\" user event -> " + e.getMessage());
Utilities.printBytes(ringItem.getNode().getStructureBuffer(true), 0, 80, "Bad user event bytes:");
System.out.println("\n      DataChannel File out " + outputIndex + ": IGNORING USER EVENT, go to next event");
                                }
                            }
                            // The writer will handle the first event from here
                            ringItem.releaseByteBuffer();
                        }
                        else {
                            // force to hard disk.
                            try {
//System.out.println("      DataChannel File out " + outputIndex + ": try writing user event");
                                writeEvioData(ringItem, true);
//System.out.println("      DataChannel File out " + outputIndex + ": wrote user event");
                            }
                            catch (EvioException e) {
                                // Probably here due to bad evio format
                                emu.sendRcWarningMessage(e.getMessage());
System.out.println("      DataChannel File out " + outputIndex + ": failed writing user event -> " + e.getMessage());
Utilities.printBytes(ringItem.getNode().getStructureBuffer(true), 0, 80, "Bad user event bytes:");
System.out.println("\n      DataChannel File out " + outputIndex + ": IGNORING USER EVENT, go to next event");
                            }
                        }
                    }
                    // Only user and control events should come first, so error
                    else {
                        throw new EmuException(pBankType + " type of events must come after GO event");
                    }

                    // Keep reading events till we hit go/end
                    gotoNextRingItem(0);
                }

                // Release ring items gotten so far
                releaseOutputRingItem(0);
//System.out.println("      DataChannel File out " + outputIndex + ": releasing initial control & user events");

                // END may come right after PRESTART
                if (pBankControlType == ControlType.END) {
logger.info("      DataChannel File out " + outputIndex + ": wrote END");
                    if (emu.isFileWritingOn()) {
                        try {
                            evioFileWriter.close();
                        }
                        catch (Exception e) {
                            errorMsg.compareAndSet(null, "Cannot write to file");
                            throw e;
                        }
                    }
                    // run callback saying we got end event
                    if (endCallback != null) endCallback.endWait();
                    threadState = ThreadState.DONE;
                    return;
                }

logger.info("      DataChannel File out " + outputIndex + ": wrote GO");

                while ( true ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) {
                            try {Thread.sleep(5);}
                            catch (InterruptedException e1) {}
                        }
                        continue;
                    }

                    try {
//System.out.println("      DataChannel File out " + outputIndex + ": try getting next buffer from ring");
                        ringItem = getNextOutputRingItem(ringIndex);
//System.out.println("      DataChannel File out " + outputIndex + ": got next buffer");
//Utilities.printBuffer(ringItem.getBuffer(), 0, 6, name+": ev" + nextEvent + ", ring " + ringIndex);
                    }
                    catch (InterruptedException e) {
                        return;
                    }

                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    try {
                        // If this a user and "first event", let the writer know
                        if (ringItem.isFirstEvent()) {
                            if (emu.isFileWritingOn()) {
                                if (ringItem.getBuffer() != null) {
                                    evioFileWriter.setFirstEvent(ringItem.getBuffer());
                                }
                                else {
                                    evioFileWriter.setFirstEvent(ringItem.getNode());
                                }
                            }
                            // The writer will handle the first event from here,
                            // go to the next event now.
                            ringItem.releaseByteBuffer();
                        }
                        // If not END event, don't force to disk
                        else if (pBankType != EventType.CONTROL) {
//System.out.println("      DataChannel File out " + outputIndex + ": write!");
                            writeEvioData(ringItem, false);
                        }
                        else {
                            writeEvioData(ringItem, true);
                        }
                    }
                    catch (Exception e) {
                        errorMsg.compareAndSet(null, "Cannot write to file");
                        throw e;
                    }

//System.out.println("      DataChannel File out: release ring item");
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
                        emu.setOutputDestination(outputNameIndex, evioFileWriter.getCurrentFilePath());
                        splitCount = evioFileWriter.getSplitCount();
                        // HallD wants a warning if splitCount > 3 digits (ie 999).
                        // But send it only once (hence the upper limit).
                        if ( (splitCount > 999) && (split <= (999 + emu.getDataStreamCount())) ) {
                            emu.sendRcWarningMessage("split number over 999");
                        }
                    }

                    if (pBankControlType == ControlType.END) {
logger.info("      DataChannel File out " + outputIndex + ": got ev " + nextEvent +
            ", ring " + ringIndex + " = END!");

                        if (dumpedEvents > 0) {
                            // Time to adjust statistics to account for any physics
                            // events discarded due to full disk partition.
                            module.adjustStatistics(-1 * dumpedEvents, -1 * dumpedWords);
                            emu.sendRcWarningMessage(dumpedEvents + " physics events discarded");
logger.info("      DataChannel File out " + outputIndex + ": discarded " + dumpedEvents +
            " events due to full disk");
                        }

                        if (emu.isFileWritingOn()) {
                            try {
                                evioFileWriter.close();
                            }
                            catch (Exception e) {
                                errorMsg.compareAndSet(null, "Cannot write to file");
                                throw e;
                            }
                        }
                        // run callback saying we got end event
                        if (endCallback != null) {endCallback.endWait();}
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
System.out.println("      DataChannel File out, " + outputIndex + ": got RESET cmd, quitting 1");
                        threadState = ThreadState.DONE;
                        return;
                    }
                }

            } catch (InterruptedException e) {
logger.warn("      DataChannel File out, " + outputIndex + ": interrupted thd, exiting");
            } catch (Exception e) {
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel File out: " + e.getMessage());
                e.printStackTrace();
System.out.println("      DataChannel File out, " + outputIndex + " : exit thd: " + e.getMessage());
            }

            threadState = ThreadState.DONE;
        }

    }  /* DataOutputHelper internal class */

}