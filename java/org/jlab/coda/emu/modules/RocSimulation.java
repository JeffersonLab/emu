/*
 * Copyright (c) 2011, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.modules;

import com.lmax.disruptor.*;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODAStateIF;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Phaser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class simulates a Roc. It is a module which can use multiple threads
 * to create events and send them to a single output channel.<p>
 * Multiple Rocs can be synchronized by running test.RocSynchronizer.
 * @author timmer
 * (2011)
 */
public class RocSimulation extends ModuleAdapter {

    /** Keep track of the number of records built in this ROC. Reset at prestart. */
    private volatile int rocRecordId;

    /** Threads used for generating events. */
    private EventGeneratingThread[] eventGeneratingThreads;

    /** Type of trigger sent from trigger supervisor. */
    private int triggerType;

    /** Number of events in each ROC raw record. */
    private int eventBlockSize;

    /** The id of the detector which produced the data in block banks of the ROC raw records. */
    private int detectorId;

    /** Size of a single generated Roc raw event in 32-bit words (including header). */
    private int eventWordSize;

    /** Size of a single generated event in WORDS (including header). */
    private int eventSize;

    /** Number of Roc raw events to produce before syncing with other Rocs. */
    private int syncCount;

    /** Number of computational loops to act as a delay. */
    private int loops;

    /** Number of ByteBuffers in each EventGeneratingThread. */
    private int bufSupplySize = 1024;

    /** Flag saying we got the END command. */
    private volatile boolean gotEndCommand;

    /** Flag saying we got the END command. */
    private volatile boolean gotResetCommand;

    /** Flag saying we got the GO command. */
    private volatile boolean gotGoCommand;

    /** Number suffix on ROC name. If none, 0. */
    private int rocNumber;

    //-------------------------------------------
    // For using real data
    //-------------------------------------------

    /** If true, use real data taken from HallD data file.
     * Useful when running file compression tests. */
    private boolean useRealData;

    /** Amount of real data bytes in hallDdata array. */
    static private int arrayBytes;

    /** Put extracted real data in here. */
    static private byte[] hallDdata;

    /** Current position in hallDdata array to start copying from. */
    private int hallDdataPosition;

    //----------------------------------------------------
    // Members used to synchronize all fake Rocs to each other which allows run to
    // end properly. I.e., they all produce the same number of buildable events.
    //----------------------------------------------------
    /** If true, no physics events are produced.
     *  This is equivalent to having no triggers. */
    private boolean noPhysics;

    /** Is this ROC to be synced with others? */
    private boolean synced;

    /** Set this ROC's sync bit set every syncBitCount events.
     *  Value of 0 means no sync bit. */
    private final int syncBitCount;

    /** Connection to platform's cMsg name server. */
    private cMsg cMsgServer;

    /** Message to send synchronizer saying that we finished our loops. */
    private final cMsgMessage message;

    /** Object to handle callback subscription. */
    private cMsgSubscriptionHandle cmsgSubHandle;

    /** Synchronization primitive for producing events. */
    private Phaser phaser;

    /** Synchronization primitive for END cmd. */
    private Phaser endPhaser;

    /** Flag used to stop event production. */
    private volatile boolean timeToEnd;


    /** Callback to be run when a message from synchronizer
     *  arrives, allowing all ROCs to sync up. We subscribe
     *  once to subject = "sync" and type = "ROC". */
    private class SyncCallback extends cMsgCallbackAdapter {
        public void callback(cMsgMessage msg, Object userObject) {
System.out.println("callback: got msg from synchronizer");
            int endIt = msg.getUserInt();
            if (endIt > 0) {
                // Signal to finish end() method and it will
                // also quit event-producing thread.
System.out.println("callback: ARRIVE -> END IT, WWWWWAITTTTT");
                timeToEnd = true;
                endPhaser.arriveAndDeregister();
                System.out.println("callback: ARRIVE -> END IT, deregistered");
            }

            // Signal for event-producing loop to continue
System.out.println("callback: phaser arrive, WWWWWWAITTT");
            phaser.arrive();
System.out.println("callback: phaser past");
        }
    }

    /** Instantiate a callback to use in subscription. */
    private SyncCallback callback = new SyncCallback();

    //----------------------------------------------------


    /**
     * Run as a stand-alone application to file real data files.
     * @param args args
     */
    public static void main(String[] args) {
        try {
            getRealDataFromDataFile();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Method to get real data from an existing file of data previously extracted from
     * a Hall D data file.
     *
     * The system is that there are 9 files, each containing 16 MB of unique real Hall D data.
     * The exact file loaded is determined by parsing the Rocs's name.
     * Since Rocs are generally called Roc1, Roc2, Roc34, etc. The number in the name is obtained
     * thru the parsing and the least significant digit determines which file to load.
     * Thus Roc1 loads data file 1, Roc34 loads data file 4. etc. There may be Rocs loading
     * the identical file, but not usually since we seldom run simulations with more than 9 rocs.
     *
     * @return true if hall D data found and available, else false.
     */
    private boolean getRealData() {

        // Parse roc name
        Pattern pattern = Pattern.compile("^.+([0-9]{1})$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(emu.name());

        // Try to get the last digit at end of name if any
        int num;
        String numStr;
        if (matcher.matches()) {
            numStr = matcher.group(1);
        }
        else {
            System.out.println("getRealData: cannot find a single digit at end of ROC's name (" + emu.name() + ")");
            return false;
        }

        try {
            num = Integer.parseInt(numStr);
        }
        catch (NumberFormatException e) {
            System.out.println("getRealData: cannot find a single digit at end of ROC's name (" + emu.name() + ")");
            return false;
        }

        rocNumber = num;

        // First check to see if we already have some data in a file
        String filename = System.getenv("CODA");
        if (filename != null) {
            filename += "/common/bin/hallDdata/hallDdata" + num + ".bin";
        }
        else {
            filename = "/Users/timmer/coda/coda3/common/bin/hallDdata/hallData" + num + ".bin";
        }

        try {
            RandomAccessFile file = new RandomAccessFile(filename, "rw");
            arrayBytes = (int) file.length();
            hallDdata = new byte[arrayBytes];
            file.read(hallDdata, 0, arrayBytes);
            file.close();
        }
        catch (Exception e) {
            // No file to be read, so try creating our own
            System.out.println("getRealData: cannot open data file " + filename);
            return false;
        }

System.out.println("getRealData: successfully read " + arrayBytes + " bytes from file " + filename);
        return true;
    }



    /**
     * Method to get real data from a Hall D data file and save it into
     * 9 files of 16 MB each. This method is here for info only.
     * It can be run from evio repository, ExtractHallDdata.java
     */
    static private void getRealDataFromDataFile() {

        // Number of files to create
        int fileCount = 9;

        // Amount of real data bytes to end up in each created file.
        arrayBytes = 16000000; // 16M

        // Put extracted real data for a single file in here
        hallDdata = new byte[arrayBytes];

        // File to read
        String fileName  = "/Volumes/USB30FD/hd_rawdata_042560_000.evio";
        File fileIn = new File(fileName);
        System.out.println("read ev file: " + fileName + " size: " + fileIn.length());

        try {
            // Read sequentially
            EvioReader fileReader = new EvioReader(fileName, false, true);
            EvioEvent event;

            // In this file, skip first 3 events, only go up to event #2415
            // since file was abnormally truncated as it was put on thumb drive.
            int j = 4;

            // Want 9 files, each with 16 MB of data, starting count = 1
            for (int k=1; k <= fileCount; k++) {

                // Reset for each file to write
                int bytesWritten = 0;
                boolean finishedFillingArray = false;

                // Output file name
                String destFileName = "/Users/timmer/coda/emu.GIT/hallDdata" + k + ".bin";

                for (; j < 2415; j++) {

                    // Top level bank or event is bank of banks.
                    // Under each data event, skip first bank which is trigger bank.
                    // The next banks are data banks each of which also contain banks.
                    // In most cases, the first sub banks is very small.
                    // Second sub bank contains lots of data. Grab this data and fill our container
                    // with it.

                    event = fileReader.parseEvent(j);
                    int eventKidCount = event.getChildCount();

                    // Start at one to skip trigger bank
                    for (int i = 1; i < eventKidCount; i++) {

                        BaseStructure bs = (BaseStructure) event.getChildAt(i);
                        int subEvKidCount = bs.getChildCount();

                        // Grab second bank under general data bank
                        if (subEvKidCount > 1) {

                            EvioBank bank = (EvioBank) (bs.getChildAt(1));
                            byte[] data = bank.getRawBytes();
                            int dataBytes = 4 * (bank.getHeader().getLength() - 1);

                            // Make sure we quit when array is full
                            int bytesToWrite = dataBytes;
                            if (bytesWritten + dataBytes > arrayBytes) {
                                bytesToWrite = arrayBytes - bytesWritten;
                                finishedFillingArray = true;
                            }

                            // Copy Hall D data into our array
                            System.arraycopy(data, 0, hallDdata, bytesWritten, bytesToWrite);
                            bytesWritten += bytesToWrite;

                            if (finishedFillingArray) break;
                        }
                    }

                    if (finishedFillingArray) break;
                }

                System.out.println("Write " + bytesWritten + " bytes to file: " + destFileName);

                RandomAccessFile file = new RandomAccessFile(destFileName, "rw");
                file.write(hallDdata, 0, bytesWritten);
                file.close();
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Constructor RocSimulation creates a simulated ROC instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     * @param emu Emu this module belongs to.
     */
    public RocSimulation(String name, Map<String, String> attributeMap, Emu emu) {

        super(name, attributeMap, emu);

        //outputOrder = ByteOrder.LITTLE_ENDIAN;
        outputOrder = ByteOrder.BIG_ENDIAN;

        // Set the sync bit every 5000th record
        syncBitCount = 5000;

        // Fill out message to send to synchronizer
        message = new cMsgMessage();
        message.setSubject("syncFromRoc");
        message.setType(emu.name());

        String s;

        // Value for trigger type from trigger supervisor
        triggerType = 15;
        try { triggerType = Integer.parseInt(attributeMap.get("triggerType")); }
        catch (NumberFormatException e) { /* defaults to 15 */ }
        if (triggerType <  0) triggerType = 0;
        else if (triggerType > 15) triggerType = 15;

        // Id of detector producing data
        detectorId = 111;
        try { detectorId = Integer.parseInt(attributeMap.get("detectorId")); }
        catch (NumberFormatException e) { /* defaults to 111 */ }
        if (detectorId < 0) detectorId = 0;

        // How many entangled events in one data block?
        eventBlockSize = 40;
        try { eventBlockSize = Integer.parseInt(attributeMap.get("blockSize")); }
        catch (NumberFormatException e) { /* defaults to 1 */ }
        if (eventBlockSize <   1) eventBlockSize = 1;
        else if (eventBlockSize > 255) eventBlockSize = 255;

        // How many WORDS in a single event?
        eventSize = 75;  // 300 bytes
        try { eventSize = Integer.parseInt(attributeMap.get("eventSize")); }
        catch (NumberFormatException e) { /* defaults to 40 */ }
        if (eventSize < 1) eventSize = 1;

        // How many loops to constitute a delay?
        loops = 0;
        try { loops = Integer.parseInt(attributeMap.get("loops")); }
        catch (NumberFormatException e) { /* defaults to 0 */ }
        if (loops < 1) loops = 0;

        // How many iterations (writes of an entangled block of evio events)
        // before syncing fake ROCs together?
        syncCount = 80000;
        try { syncCount = Integer.parseInt(attributeMap.get("syncCount")); }
        catch (NumberFormatException e) { /* defaults to 100k */ }
        if (syncCount < 10) syncCount = 10;

        // Is this ROC to be synced with others?
        synced = true;
        s = attributeMap.get("sync");
        if (s != null) {
            if (s.equalsIgnoreCase("false") ||
                s.equalsIgnoreCase("off")   ||
                s.equalsIgnoreCase("no"))   {
                synced = false;
            }
        }
System.out.println("  Roc mod: sync = " + synced);

        // Does this ROC produce physics events?
        // Set this to true if you want to test a zero-trigger setup.
        // Just for testing! No physics events are sent.
        // Use this with synced = false, and don't run a TS.
        noPhysics = false;
        s = attributeMap.get("noPhysics");
        if (s != null) {
            if (s.equalsIgnoreCase("true") ||
                s.equalsIgnoreCase("on")   ||
                s.equalsIgnoreCase("yes"))   {
                noPhysics = true;
                eventProducingThreads = 1;
            }
        }

        // Keep things simple for streaming
        if (isStreamingData()) {
            eventProducingThreads = 1;
        }

        // Event generating threads
        eventGeneratingThreads = new EventGeneratingThread[eventProducingThreads];

        // the module sets the type of CODA class it is.
        emu.setCodaClass(CODAClass.ROC);

        // Make this ROC use real data for file compression testing
        useRealData = true;
        if (useRealData) {
            // If this fails, returns false, we don't use real data.
            useRealData = getRealData();
        }
        useRealData = true;

System.out.println("  Roc mod: using real Hall D data = " + useRealData);
    }


    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {}

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getInputChannels() {return null;}

    /** {@inheritDoc} */
    public void clearChannels() {outputChannels.clear();}

    /** {@inheritDoc} */
    public int getInternalRingCount() {return bufSupplySize;}

    //---------------------------------------
    // Threads
    //---------------------------------------


    /** End all threads because a END cmd or END event came through.  */
    private void endThreads() {
        // The order in which these threads are shutdown does(should) not matter.
        // Transport objects should already have been shutdown followed by this module.
        if (RateCalculator != null) RateCalculator.interrupt();
        RateCalculator = null;

        for (EventGeneratingThread thd : eventGeneratingThreads) {
            if (thd != null) {
                try {
//System.out.println("  Roc mod: endThreads() event generating thd, try interrupting " + thd.getName());
                    thd.interrupt();
                    thd.join();
//System.out.println("  Roc mod: endThreads() event generating thd, joined " + thd.getName());
                }
                catch (InterruptedException e) {
                }
            }
        }
//System.out.println("  Roc mod: endThreads() DONE");
    }


    /** Kill all threads immediately because a RESET cmd came through.  */
    private void killThreads() {
//System.out.println("  Roc mod: start killThreads()");
        // The order in which these threads are shutdown does(should) not matter.
        // Transport objects should already have been shutdown followed by this module.
        if (RateCalculator != null) {
//System.out.println("  Roc mod: interrupt rate calc thread");
            RateCalculator.interrupt();
        }
        RateCalculator = null;

        for (EventGeneratingThread thd : eventGeneratingThreads) {
            if (thd != null) {
                // Kill this thread with deprecated stop method because it can easily
                // block on the uninterruptible rb.next() method call and RESET never
                // completes. First give it a chance to end gracefully.
                thd.endThread();
System.out.println("  Roc mod: interrupted event generating thread");
                try {
                    thd.join(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
System.out.println("  Roc mod: done killThreads()");
    }


    //---------------------------------------


    /**
     * This method is called by a running EventGeneratingThread.
     * It generates many ROC Raw events in it with simulated data,
     * and places them onto the ring buffer of an output channel.
     *
     * @param ringNum the id number of the output channel ring buffer
     * @param buf     the event to place on output channel ring buffer
     * @param item    item corresponding to the buffer allowing buffer to be reused
     * @param bbSupply supply of ByteBuffers.
     */
    void eventToOutputRing(int ringNum, ByteBuffer buf,
                                   ByteBufferItem item, ByteBufferSupply bbSupply) {

        if (outputChannelCount < 1) {
            bbSupply.release(item);
            return;
        }

        // TODO: assumes only one output channel ...
        RingBuffer rb = outputChannels.get(0).getRingBuffersOut()[ringNum];

//System.out.println("  Roc mod: wait for next ring buf for writing");
        long nextRingItem = rb.next();
//System.out.println("  Roc mod: GOT next ring buf");

//System.out.println("  Roc mod: get out sequence " + nextRingItem);
        RingItem ri = (RingItem) rb.get(nextRingItem);
//System.out.println("  Roc mod: GOT out sequence " + nextRingItem);
        ri.setBuffer(buf);
        if (streamingData) {
            ri.setEventType(EventType.ROC_RAW_STREAM);
        }
        else {
            ri.setEventType(EventType.ROC_RAW);
        }
        ri.setControlType(null);
        ri.setSourceName(null);
        ri.setReusableByteBuffer(bbSupply, item);

//System.out.println("  Roc mod: publish ring item #" + nextRingItem + " to ring " + ringNum);
        rb.publish(nextRingItem);
//System.out.println("  Roc mod: published " + nextRingItem);
    }


    private void sendMsgToSynchronizer(boolean gotEnd) throws cMsgException {
        if (gotEnd) {
            message.setUserInt(1);
//System.out.println("  Roc mod: sent \"got End cmd\"");
        }
        else {
            message.setUserInt(0);
//System.out.println("  Roc mod: send \"no End received\"");
        }
        cMsgServer.send(message);
    }


    private int getSingleEventBufferWords(int generatedDataWords) {

        int dataWordLength = 1 + generatedDataWords;

        // bank header + bank header +  eventBlockSize segments + data,
        // seg  = (1 seg header + 3 data)
        // data = bank header + int data

        //  totalEventWords = 2 + 2 + eventBlockSize*(1+3) + (2 + data.length);
        return (6 + 4*eventBlockSize + dataWordLength);
    }


    private ByteBuffer createSingleEventBuffer(int generatedDataWords, long eventNumber, long timestamp) {
        int writeIndex=0;
        int dataLen = 1 + generatedDataWords;

        // Note: buf limit is set to its capacity in allocate()
        ByteBuffer buf = ByteBuffer.allocate(4*eventWordSize);
        buf.order(outputOrder);

        // Event bank header
        // sync, error, isBigEndian, singleEventMode
        int rocTag = Evio.createCodaTag(false, false, true, false, id);

        // 2nd bank header word = tag << 16 | ((padding & 0x3) << 14) | ((type & 0x3f) << 8) | num
        int secondWord = rocTag << 16 |
                         (DataType.BANK.getValue() << 8) |
                         (eventBlockSize & 0xff);

        buf.putInt(writeIndex, (eventWordSize - 1)); writeIndex += 4;
        buf.putInt(writeIndex, secondWord); writeIndex += 4;


        // Trigger bank header
        secondWord = CODATag.RAW_TRIGGER_TS.getValue() << 16 |
                     (DataType.SEGMENT.getValue() << 8) |
                     (eventBlockSize & 0xff);

        buf.putInt(writeIndex, (eventWordSize - 2 - dataLen - 2 - 1)); writeIndex += 4;
        buf.putInt(writeIndex, secondWord); writeIndex += 4;

        // segment header word = tag << 24 | ((padding << 22) & 0x3) | ((type << 16) & 0x3f) | length
        int segWord = triggerType << 24 | (DataType.UINT32.getValue() << 16) | 3;

        // Add each segment
        for (int i = 0; i < eventBlockSize; i++) {
            buf.putInt(writeIndex, segWord); writeIndex += 4;

            // Generate 3 integers per event (no miscellaneous data)
            buf.putInt(writeIndex, (int) (eventNumber + i)); writeIndex += 4;
            buf.putInt(writeIndex, (int)  timestamp); writeIndex += 4;// low 32 bits
            buf.putInt(writeIndex, (int) (timestamp >>> 32 & 0xFFFF)); writeIndex += 4;// high 16 of 48 bits
            timestamp += 4;
        }


        int dataTag = Evio.createCodaTag(false, false, true, false, detectorId);
        secondWord = dataTag << 16 |
                     (DataType.UINT32.getValue() << 8) |
                     (eventBlockSize & 0xff);

        buf.putInt(writeIndex, dataLen + 1); writeIndex += 4;
        buf.putInt(writeIndex, secondWord); writeIndex += 4;


        // First put in starting event # (32 bits)
        buf.putInt(writeIndex, (int)eventNumber); writeIndex += 4;
        if (useRealData) {
            // buf has a backing array, so fill it with data the quick way
            int bytes = 4*generatedDataWords;
            if (bytes > arrayBytes) {
System.out.println("  Roc mod: NEED TO GENERATE MORE REAL DATA, have " + arrayBytes +
        " but need " + bytes);
            }
            System.arraycopy(hallDdata, hallDdataPosition, buf.array(), writeIndex, bytes);
            hallDdataPosition += bytes;
        }
        else {
            for (int i = 0; i < generatedDataWords; i++) {
                    buf.putInt(writeIndex, 1);
                    writeIndex += 4;
            }
        }

        // buf is ready to read
        return buf;
    }


    //TODO: 1st
    void  writeEventBuffer(ByteBuffer buf, ByteBuffer templateBuf,
                           long eventNumber, long timestamp,
                           boolean syncBit, boolean copy,
                           int generatedDataBytes) {

        // Since we're using recirculating buffers, we do NOT need to copy everything
        // into the buffer each time. Once each of the buffers in the BufferSupply object
        // have been copied into, we only need to change the few places that need updating
        // with event number and timestamp!
        if (copy) {
            // This will be the case if buf is direct
            if (!buf.hasArray()) {
                templateBuf.position(0);
                buf.put(templateBuf);
            }
            else {
                System.arraycopy(templateBuf.array(), 0, buf.array(), 0, templateBuf.limit());
            }
        }

        // Get buf ready to read for output channel
        buf.limit(templateBuf.limit()).position(0);

        // Set sync bit in event bank header
        // sync, error, isBigEndian, singleEventMode
        int rocTag = Evio.createCodaTag(syncBit, false, true, false, id);

        // 2nd bank header word = tag << 16 | ((padding & 0x3) << 14) | ((type & 0x3f) << 8) | num
        int secondWord = rocTag << 16 |
                (DataType.BANK.getValue() << 8) |
                (eventBlockSize & 0xff);

        buf.putInt(4, secondWord);

        // Skip over 2 bank headers
        int writeIndex = 16;

        // Write event number and timestamp into trigger bank segments
        for (int i = 0; i < eventBlockSize; i++) {
            // Skip segment header
            writeIndex += 4;

            // Generate 3 integers per event (no miscellaneous data)
            buf.putInt(writeIndex, (int) (eventNumber + i)); writeIndex += 4;
            buf.putInt(writeIndex, (int)  timestamp); writeIndex += 4;// low 32 bits
            buf.putInt(writeIndex, (int) (timestamp >>> 32 & 0xFFFF)); writeIndex += 4;// high 16 of 48 bits
            timestamp += 4;
        }

        // Move past data bank header
        writeIndex += 8;

        // Write event number into data bank
        buf.putInt(writeIndex, (int) eventNumber);

        // For testing compression, need to have real data that changes,
        // endianness does not matter.
        // Only copy data into each of the "bufSupplySize" number of events once.
        // Doing this for each event produced every time slows things down too much.
        // Each event has eventBlockSize * eventSize (40*75 = 3000) data words.
        // 4 * 3k bytes * 1024 events = 12.3MB. This works out nicely since we have
        // retrieved 16MB from a single Hall D data file.
        // However, each Roc has the same data which will lend itself to more compression.
        // So the best thing is for each ROC to have different data.
        if (copy && useRealData) {
            // Move to data input position
            writeIndex += 4;

            // Have we run out of data? If so, start over from beginning ...
            if (arrayBytes - hallDdataPosition < generatedDataBytes) {
                hallDdataPosition = 0;
            }

            if (buf.hasArray()) {
                System.arraycopy(hallDdata, hallDdataPosition, buf.array(), writeIndex, generatedDataBytes);
            }
            else {
                buf.position(writeIndex);
                buf.put(hallDdata, hallDdataPosition, generatedDataBytes);
                // Get buf ready to read for output channel
                buf.limit(templateBuf.limit()).position(0);
            }

            hallDdataPosition += generatedDataBytes;
        }
    }


    //////////////////////////////
    // For Streaming Data
    //////////////////////////////

    /** Store intermediate calculation here. */
    private int bytesPerDataBank = 0;

    /**
     * <p>Get the total bytes of a time slice buffer (and containing bank).
     * If not using real data, i.e. using composite data type in data banks,
     * the result is only an approximation.</p>
     *
     * @param generatedDataWords total number of desired data words to be generated for a single slice.
     * @param payloadCount number of payloads in this ROC.
     * @return total bytes of time slice buffer created by createSingleTimeSliceBuffer().
     */
    private int getSingleTimeSliceBufferWords(int generatedDataWords, int payloadCount) {
        // Make generatedDataWords a multiple of 4, round up
        generatedDataWords = 4*((generatedDataWords + 3) / 4);

        // This is only an estimate if not using realData since composite type is used
        // and exact calculation of its length is not worth hassling with.

        // bank header + bank header + 1st seg (4 words) + 2nd seg (1 + 2*((payloadCount + 1)/2)) +
        // 2*payloadCount (data banks' headers) + generatedDataWords
        // If using composite data we need to add roughly 4*5words. This depends on the format string
        // and other complications.

        int len = 9 + 2*((payloadCount + 1)/2) + 2*payloadCount + generatedDataWords;
        if (useRealData) {
            len += 20;
        }
        return len;
    }


    /**
     * Generate data from a streaming ROC.
     *
     * @param generatedDataWords desired amount of total words (not including headers)
     *                           for all data banks (each corresponding to one payload port).
     * @param frameNumber frame number
     * @param timestamp   time stamp
     * @return ByteBuffer with generated single ROC time slice bank inside containing bank.
     */
    private ByteBuffer createSingleTimeSliceBuffer(int generatedDataWords, long frameNumber, long timestamp) {

        try {
            // Make generatedDataWords a multiple of 4, round up
            generatedDataWords = 4*((generatedDataWords + 3) / 4);
            int totalLen = 14 + generatedDataWords + 1000; // total of 14 header words + 1K extra

            // Each of 4 data banks has 1/4 of total words so generateDataWords = # bytes for each bank ...
            // Store calculation here
            bytesPerDataBank = generatedDataWords;

            CompactEventBuilder builder = new CompactEventBuilder(4*totalLen, outputOrder, false);

            // ROC Time Slice Bank
            int rocId = 7;
            int totalStreams = 2;
            int streamMask = 3;
            int streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
            builder.openBank(rocId, streamStatus, DataType.BANK);

            // Stream Info Bank (SIB)
            builder.openBank(CODATag.STREAMING_SIB.getValue(), streamStatus, DataType.SEGMENT);

            // 1st SIB Segment -> TSS or Time Slice Segment
            builder.openSegment(0x31, DataType.UINT32);
            int[] intData = new int[3];
            intData[0] = (int)frameNumber;
            intData[1] = (int)timestamp;
            intData[2] = (int)((timestamp >>> 32) & 0xFFFF);
            builder.addIntData(intData);
            builder.closeStructure();

            // 2nd SIB Segment -> AIS or Aggregation Info Segment
            builder.openSegment(0x41, DataType.USHORT16);
            short[] shortData = new short[4];

            int payloadPort1 = 0;
            int laneId = 0;
            int bond = 0;
            int moduleId = 2;
            short payload1 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort1 & 0x1f));
            shortData[0] = payload1;

            int payloadPort2 = 1;
            laneId = 1;
            bond = 0;
            moduleId = 3;
            short payload2 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort2 & 0x1f));
            shortData[1] = payload2;

            int payloadPort3 = 2;
            laneId = 2;
            bond = 0;
            moduleId = 5;
            short payload3 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort3 & 0x1f));
            shortData[2] = payload3;

            int payloadPort4 = 3;
            laneId = 3;
            bond = 0;
            moduleId = 7;
            short payload4 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort4 & 0x1f));
            shortData[3] = payload4;

            builder.addShortData(shortData);
            builder.closeStructure();
            // Close SIB
            builder.closeStructure();

            // Add Data Bank, 1 for each payload (4)
            // TODO: Question: is this stream status different??
            // Assume this stream status is only for the payload in question

            if (useRealData) {
                byte[] iData = new byte[bytesPerDataBank];
                if (4*bytesPerDataBank > arrayBytes) {
                    System.out.println("  Roc mod: NEED TO GENERATE MORE REAL DATA, have " + arrayBytes +
                            " bytes but need " + (4*bytesPerDataBank));
                }
                System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                hallDdataPosition += bytesPerDataBank;

                // Fill banks with real data ...

                totalStreams = 1;
                streamMask = 1;
                streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(payloadPort1, streamStatus, DataType.UCHAR8);
                builder.addByteData(iData);
                builder.closeStructure();

                System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                hallDdataPosition += bytesPerDataBank;
                streamMask = 2;
                streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(payloadPort2, streamStatus, DataType.UCHAR8);
                builder.addByteData(iData);
                builder.closeStructure();

                System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                hallDdataPosition += bytesPerDataBank;
                streamMask = 4;
                streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(payloadPort3, streamStatus, DataType.UCHAR8);
                builder.addByteData(iData);
                builder.closeStructure();

                System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                hallDdataPosition += bytesPerDataBank;
                streamMask = 8;
                streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(payloadPort4, streamStatus, DataType.UCHAR8);
                builder.addByteData(iData);
                builder.closeStructure();
            }
            else {
                // Put the same composite data in each data bank.
                // Format to write a N signed 32-bit ints, 1 float, 1 double a total of N times
                // Because the we have 4 payloads, the number of generatedDataWords, conveniently,
                // becomes the number of bytes for each of the payloads. But, because of the complex
                // structure of composite data, we won't bother to include all the header info
                // contained in that data type in our simple calculations.

                String format = "N(NI,F,D)";

                // Let's pick the first N to be 2, the 2nd N becomes (within rounding error)
                int N = (generatedDataWords / 2 - 4 - 8) / 4;

                // Now create some data
                CompositeData.Data myData = new CompositeData.Data();
                myData.addN(2);

                myData.addN(N);
                int[] iData = new int[N];
                Arrays.fill(iData, 1);
                myData.addInt(iData);
                myData.addFloat(1.0F);
                myData.addDouble(Math.PI);

                myData.addN(N);
                Arrays.fill(iData, 2);
                myData.addInt(iData);
                myData.addFloat(2.0F);
                myData.addDouble(2. * Math.PI);

                // Create CompositeData object
                CompositeData cData = null;
                try {
                    cData = new CompositeData(format, 1, myData, 0, 0);
                }
                catch (EvioException e) {
                    e.printStackTrace();
                }
                CompositeData[] cArray = new CompositeData[]{cData};

                // Fill banks with generated composite data ...

                totalStreams = 1;
                streamMask = 1;
                streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(payloadPort1, streamStatus, DataType.COMPOSITE);
                builder.addCompositeData(cArray);
                builder.closeStructure();

                streamMask = 2;
                streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(payloadPort2, streamStatus, DataType.COMPOSITE);
                builder.addCompositeData(cArray);
                builder.closeStructure();

                streamMask = 4;
                streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(payloadPort3, streamStatus, DataType.COMPOSITE);
                builder.addCompositeData(cArray);
                builder.closeStructure();

                streamMask = 8;
                streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(payloadPort4, streamStatus, DataType.COMPOSITE);
                builder.addCompositeData(cArray);
                builder.closeStructure();
            }

            builder.closeAll();
            // buf is ready to read
            return builder.getBuffer();
        }
        catch (EvioException e) {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * Instead of rewriting the entire event buffer for each event
     * (with only slightly different data), only update the couple of places
     * in which data changes. Save time, memory and garbage collection time.
     * After the first round of writing the entire event, once for each buffer
     * in the ByteBufferSupply, just do updates.
     * The only 2 quantities that need updating are the frame number and time stamp.
     * Both of these are data in the Stream Info Bank.
     *
     * @param buf          buffer from supply.
     * @param templateBuf  buffer with time slice data
     * @param frameNumber  new frame number to place into buf.
     * @param timestamp    new time stamp to place into buf
     * @param copy         ss templateBuf to be copied into buf or not.
     * @param generatedDataBytes number of bytes generated as data for each payload data bank.
     */
    void  writeTimeSliceBuffer(ByteBuffer buf, ByteBuffer templateBuf,
                               long frameNumber, long timestamp,
                               boolean copy, int generatedDataBytes) {

        // Since we're using recirculating buffers, we do NOT need to copy everything
        // into the buffer each time. Once each of the buffers in the BufferSupply object
        // have been copied into, we only need to change the few places that need updating
        // with frame number and timestamp!
        if (copy) {
            // This will be the case if buf is direct
            if (!buf.hasArray()) {
                templateBuf.position(0);
                buf.put(templateBuf);
            }
            else {
                System.arraycopy(templateBuf.array(), 0, buf.array(), 0, templateBuf.limit());
            }
        }

        // Get buf ready to read for output channel
        buf.limit(templateBuf.limit()).position(0);
//System.out.println("  Roc mod: setting frame = " + frameNumber);
        buf.putInt(20, (int)frameNumber);
        buf.putInt(24, (int)timestamp);// low 32 bits
        buf.putInt(28, (int)(timestamp >>> 32 & 0xFFFF)); // high 32 bits

        // For testing compression, need to have real data that changes,
        // endianness does not matter.
        // Only copy data into each of the "bufSupplySize" number of events once.
        // Doing this for each event produced every time slows things down too much.
        // Each event has eventBlockSize * eventSize (40*75 = 3000) data words.
        // 4 * 3k bytes * 1024 events = 12.3MB. This works out nicely since we have
        // retrieved 16MB from a single Hall D data file.
        // However, each Roc has the same data which will lend itself to more compression.
        // So the best thing is for each ROC to have different data.

        // Move to data input position
        int writeIndex = 4*13;

        if (copy && useRealData) {
            // For each of 4 banks
            for (int i=0; i < 4; i++) {
                // Have we run out of data? If so, start over from beginning ...
                if (arrayBytes - hallDdataPosition < generatedDataBytes) {
                    hallDdataPosition = 0;
                }

                if (buf.hasArray()) {
                    System.arraycopy(hallDdata, hallDdataPosition, buf.array(), writeIndex, generatedDataBytes);
                }
                else {
                    buf.position(writeIndex);
                    buf.put(hallDdata, hallDdataPosition, generatedDataBytes);
                    // Get buf ready to read for output channel
                    buf.limit(templateBuf.limit()).position(0);
                }

                hallDdataPosition += generatedDataBytes;

                // Move to next data bank's data position
                writeIndex += 8 + generatedDataBytes;
            }
        }
    }

    //----------------------------
    // For 2 time slices in one main bank
    //----------------------------

    /**
     * Generate data from a streaming ROC.
     *
     * @param generatedDataWords desired amount of total words (not including headers),
     *                           for all data banks (each corresponding to one payload port),
     *                           in one time slice bank.
     * @param frameNumber frame number
     * @param timestamp   time stamp
     * @return ByteBuffer with generated single ROC time slice bank inside containing bank.
     */
    private ByteBuffer createDualTimeSliceBuffer(int generatedDataWords, long frameNumber, long timestamp) {

        try {
            // Make generatedDataWords a multiple of 4, round up
            generatedDataWords = 4*((generatedDataWords + 3) / 4);
            int totalLen = 13 + generatedDataWords + 10; // total of 13 header words + 10 extra

            // Since we're doing 2 slices
            totalLen *= 2;

            // Each of 4 data banks has 1/4 of total words so generateDataWords = # bytes for each bank ...
            // Store calculation here
            bytesPerDataBank = generatedDataWords;

            CompactEventBuilder builder = new CompactEventBuilder(4*totalLen, outputOrder, false);

                //-----------------------------------
                // ROC Time Slice Bank 1
                //-----------------------------------
                int rocId = 7;
                int totalStreams = 2;
                int streamMask = 3;
                int streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(rocId, streamStatus, DataType.BANK);

                    // Stream Info Bank (SIB)
                    builder.openBank(CODATag.STREAMING_SIB.getValue(), streamStatus, DataType.SEGMENT);

                        // 1st SIB Segment -> TSS or Time Slice Segment
                        builder.openSegment(0x31, DataType.UINT32);
                            int[] intData = new int[3];
                            intData[0] = (int)frameNumber;
                            intData[1] = (int)timestamp;
                            intData[2] = (int)((timestamp >>> 32) & 0xFFFF);
                            builder.addIntData(intData);
                        builder.closeStructure();

                        // 2nd SIB Segment -> AIS or Aggregation Info Segment
                        builder.openSegment(0x41, DataType.USHORT16);
                            short[] shortData = new short[4];

                            int payloadPort1 = 0;
                            int laneId = 0;
                            int bond = 0;
                            int moduleId = 2;
                            short payload1 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort1 & 0x1f));
                            shortData[0] = payload1;

                            int payloadPort2 = 1;
                            laneId = 1;
                            bond = 0;
                            moduleId = 3;
                            short payload2 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort2 & 0x1f));
                            shortData[1] = payload2;

                            int payloadPort3 = 2;
                            laneId = 2;
                            bond = 0;
                            moduleId = 5;
                            short payload3 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort3 & 0x1f));
                            shortData[2] = payload3;

                            int payloadPort4 = 3;
                            laneId = 3;
                            bond = 0;
                            moduleId = 7;
                            short payload4 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort4 & 0x1f));
                            shortData[3] = payload4;

                            builder.addShortData(shortData);
                        builder.closeStructure();

                    // Close SIB
                    builder.closeStructure();

                    // Add Data Bank, 1 for each payload (4)
                    // TODO: Question: is this stream status different??
                    // Assume this stream status is only for the payload in question

                    if (useRealData) {
                        byte[] iData = new byte[bytesPerDataBank];
                        if (4*bytesPerDataBank > arrayBytes) {
                            System.out.println("  Roc mod: NEED TO GENERATE MORE REAL DATA, have " + arrayBytes +
                                    " bytes but need " + (4*bytesPerDataBank));
                        }
                        System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                        hallDdataPosition += bytesPerDataBank;

                        // Fill banks with real data ...

                        totalStreams = 1;
                        streamMask = 1;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort1, streamStatus, DataType.UCHAR8);
                            builder.addByteData(iData);
                        builder.closeStructure();

                        System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                        hallDdataPosition += bytesPerDataBank;
                        streamMask = 2;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort2, streamStatus, DataType.UCHAR8);
                            builder.addByteData(iData);
                        builder.closeStructure();

                        System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                        hallDdataPosition += bytesPerDataBank;
                        streamMask = 4;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort3, streamStatus, DataType.UCHAR8);
                            builder.addByteData(iData);
                        builder.closeStructure();

                        System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                        hallDdataPosition += bytesPerDataBank;
                        streamMask = 8;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort4, streamStatus, DataType.UCHAR8);
                            builder.addByteData(iData);
                        builder.closeStructure();
                    }
                    else {
                        // Put the same composite data in each data bank.
                        // Format to write a N signed 32-bit ints, 1 float, 1 double a total of N times
                        // Because the we have 4 payloads, the number of generatedDataWords, conveniently,
                        // becomes the number of bytes for each of the payloads. But, because of the complex
                        // structure of composite data, we won't bother to include all the header info
                        // contained in that data type in our simple calculations.

                        String format = "N(NI,F,D)";

                        // Let's pick the first N to be 2, the 2nd N becomes (within rounding error)
                        int N = (generatedDataWords / 2 - 4 - 8) / 4;

                        // Now create some data
                        CompositeData.Data myData = new CompositeData.Data();
                        myData.addN(2);

                        myData.addN(N);
                        int[] iData = new int[N];
                        Arrays.fill(iData, 1);
                        myData.addInt(iData);
                        myData.addFloat(1.0F);
                        myData.addDouble(Math.PI);

                        myData.addN(N);
                        Arrays.fill(iData, 2);
                        myData.addInt(iData);
                        myData.addFloat(2.0F);
                        myData.addDouble(2. * Math.PI);

                        // Create CompositeData object
                        CompositeData cData = null;
                        try {
                            cData = new CompositeData(format, 1, myData, 0, 0);
                        }
                        catch (EvioException e) {
                            e.printStackTrace();
                        }
                        CompositeData[] cArray = new CompositeData[]{cData};

                        // Fill banks with generated composite data ...

                        totalStreams = 1;
                        streamMask = 1;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort1, streamStatus, DataType.COMPOSITE);
                            builder.addCompositeData(cArray);
                        builder.closeStructure();

                        streamMask = 2;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort2, streamStatus, DataType.COMPOSITE);
                            builder.addCompositeData(cArray);
                        builder.closeStructure();

                        streamMask = 4;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort3, streamStatus, DataType.COMPOSITE);
                            builder.addCompositeData(cArray);
                        builder.closeStructure();

                        streamMask = 8;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort4, streamStatus, DataType.COMPOSITE);
                            builder.addCompositeData(cArray);
                        builder.closeStructure();
                    }

                // End of 1st ROC Time Slice Bank
                builder.closeStructure();

                //-----------------------------------
                // ROC Time Slice Bank 2
                //-----------------------------------
                rocId = 7;
                totalStreams = 2;
                streamMask = 12;
                streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                builder.openBank(rocId, streamStatus, DataType.BANK);

                    // Stream Info Bank (SIB)
                    builder.openBank(CODATag.STREAMING_SIB.getValue(), streamStatus, DataType.SEGMENT);

                        // 1st SIB Segment -> TSS or Time Slice Segment
                        builder.openSegment(0x31, DataType.UINT32);
                            intData[0] = (int)frameNumber;
                            intData[1] = (int)timestamp;
                            intData[2] = (int)((timestamp >>> 32) & 0xFFFF);
                            builder.addIntData(intData);
                        builder.closeStructure();

                        // 2nd SIB Segment -> AIS or Aggregation Info Segment
                        builder.openSegment(0x41, DataType.USHORT16);

                            payloadPort1 = 0;
                            laneId = 0;
                            bond = 0;
                            moduleId = 2;
                            payload1 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort1 & 0x1f));
                            shortData[0] = payload1;

                            payloadPort2 = 1;
                            laneId = 1;
                            bond = 0;
                            moduleId = 3;
                            payload2 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort2 & 0x1f));
                            shortData[1] = payload2;

                            payloadPort3 = 2;
                            laneId = 2;
                            bond = 0;
                            moduleId = 5;
                            payload3 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort3 & 0x1f));
                            shortData[2] = payload3;

                            payloadPort4 = 3;
                            laneId = 3;
                            bond = 0;
                            moduleId = 7;
                            payload4 = (short) (((moduleId << 8) & 0xf) | ((bond << 7) & 0x1) | ((laneId << 5) & 0x3)| (payloadPort4 & 0x1f));
                            shortData[3] = payload4;

                            builder.addShortData(shortData);
                        builder.closeStructure();

                    // Close SIB
                    builder.closeStructure();

                    // Add Data Bank, 1 for each payload (4)
                    // TODO: Question: is this stream status different??
                    // Assume this stream status is only for the payload in question

                    if (useRealData) {
                        byte[] iData = new byte[bytesPerDataBank];
                        if (4*bytesPerDataBank > arrayBytes) {
                            System.out.println("  Roc mod: NEED TO GENERATE MORE REAL DATA, have " + arrayBytes +
                                    " bytes but need " + (4*bytesPerDataBank));
                        }
                        System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                        hallDdataPosition += bytesPerDataBank;

                        // Fill banks with real data ...

                        totalStreams = 1;
                        streamMask = 1;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort1, streamStatus, DataType.UCHAR8);
                            builder.addByteData(iData);
                        builder.closeStructure();

                        System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                        hallDdataPosition += bytesPerDataBank;
                        streamMask = 2;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort2, streamStatus, DataType.UCHAR8);
                            builder.addByteData(iData);
                        builder.closeStructure();

                        System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                        hallDdataPosition += bytesPerDataBank;
                        streamMask = 4;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort3, streamStatus, DataType.UCHAR8);
                            builder.addByteData(iData);
                        builder.closeStructure();

                        System.arraycopy(hallDdata, hallDdataPosition, iData, 0, bytesPerDataBank);
                        hallDdataPosition += bytesPerDataBank;
                        streamMask = 8;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort4, streamStatus, DataType.UCHAR8);
                            builder.addByteData(iData);
                        builder.closeStructure();
                    }
                    else {
                        // Put the same composite data in each data bank.
                        // Format to write a N signed 32-bit ints, 1 float, 1 double a total of N times
                        // Because the we have 4 payloads, the number of generatedDataWords, conveniently,
                        // becomes the number of bytes for each of the payloads. But, because of the complex
                        // structure of composite data, we won't bother to include all the header info
                        // contained in that data type in our simple calculations.

                        String format = "N(NI,F,D)";

                        // Let's pick the first N to be 2, the 2nd N becomes (within rounding error)
                        int N = (generatedDataWords / 2 - 4 - 8) / 4;

                        // Now create some data
                        CompositeData.Data myData = new CompositeData.Data();
                        myData.addN(2);

                        myData.addN(N);
                        int[] iData = new int[N];
                        Arrays.fill(iData, 1);
                        myData.addInt(iData);
                        myData.addFloat(1.0F);
                        myData.addDouble(Math.PI);

                        myData.addN(N);
                        Arrays.fill(iData, 2);
                        myData.addInt(iData);
                        myData.addFloat(2.0F);
                        myData.addDouble(2. * Math.PI);

                        // Create CompositeData object
                        CompositeData cData = null;
                        try {
                            cData = new CompositeData(format, 1, myData, 0, 0);
                        }
                        catch (EvioException e) {
                            e.printStackTrace();
                        }
                        CompositeData[] cArray = new CompositeData[]{cData};

                        // Fill banks with generated composite data ...

                        totalStreams = 1;
                        streamMask = 1;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort1, streamStatus, DataType.COMPOSITE);
                            builder.addCompositeData(cArray);
                        builder.closeStructure();

                        streamMask = 2;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort2, streamStatus, DataType.COMPOSITE);
                            builder.addCompositeData(cArray);
                        builder.closeStructure();

                        streamMask = 4;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort3, streamStatus, DataType.COMPOSITE);
                            builder.addCompositeData(cArray);
                        builder.closeStructure();

                        streamMask = 8;
                        streamStatus = ((totalStreams << 4) & 0x7) | (streamMask & 0xf);
                        builder.openBank(payloadPort4, streamStatus, DataType.COMPOSITE);
                           builder.addCompositeData(cArray);
                        builder.closeStructure();
                    }

                // End of 2nd ROC Time Slice Bank
                builder.closeStructure();

            //-----------------------------------

            // buf is ready to read
            return builder.getBuffer();
        }
        catch (EvioException e) {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * Instead of rewriting the entire event buffer for each event
     * (with only slightly different data), only update the couple of places
     * in which data changes. Save time, memory and garbage collection time.
     * After the first round of writing the entire event, once for each buffer
     * in the ByteBufferSupply, just do updates.
     * The only 2 quantities that need updating are the frame number and time stamp.
     * Both of these are data in the Stream Info Bank.
     *
     * @param buf          buffer from supply.
     * @param templateBuf  buffer with time slice data
     * @param frameNumber  new frame number to place into buf.
     * @param timestamp    new time stamp to place into buf
     * @param copy         ss templateBuf to be copied into buf or not.
     * @param generatedDataBytes number of bytes generated as data for each payload data bank.
     * @param singleTimeSliceBytes number of bytes for just 1 of the 2 time slice banks.
     *                             Find this by calling createSingleTimeSliceBuffer, getting
     *                             its length and subtracting 1 bank header (8 bytes).
     */
    void  writeDualTimeSliceBuffer(ByteBuffer buf, ByteBuffer templateBuf,
                                   long frameNumber, long timestamp,
                                   boolean copy, int generatedDataBytes,
                                   int singleTimeSliceBytes) {

        // Since we're using recirculating buffers, we do NOT need to copy everything
        // into the buffer each time. Once each of the buffers in the BufferSupply object
        // have been copied into, we only need to change the few places that need updating
        // with frame number and timestamp!
        if (copy) {
            // This will be the case if buf is direct
            if (!buf.hasArray()) {
                templateBuf.position(0);
                buf.put(templateBuf);
            }
            else {
                System.arraycopy(templateBuf.array(), 0, buf.array(), 0, templateBuf.limit());
            }
        }

        // Get buf ready to read for output channel
        buf.limit(templateBuf.limit()).position(0);

        // Change data in 1st TSB
        buf.putInt(20, (int)frameNumber);
        buf.putInt(24, (int)timestamp);// low 32 bits
        buf.putInt(28, (int)(timestamp >>> 32 & 0xFFFF)); // high 32 bits

        // Change data in 2nd TSB
        buf.putInt(20 + singleTimeSliceBytes, (int)frameNumber);
        buf.putInt(24 + singleTimeSliceBytes, (int)timestamp);
        buf.putInt(28 + singleTimeSliceBytes, (int)(timestamp >>> 32 & 0xFFFF));

        // For testing compression, need to have real data that changes,
        // endianness does not matter.
        // Only copy data into each of the "bufSupplySize" number of events once.
        // Doing this for each event produced every time slows things down too much.
        // Each event has eventBlockSize * eventSize (40*75 = 3000) data words.
        // 4 * 3k bytes * 1024 events = 12.3MB. This works out nicely since we have
        // retrieved 16MB from a single Hall D data file.
        // However, each Roc has the same data which will lend itself to more compression.
        // So the best thing is for each ROC to have different data.

        // Move to data input position
        int writeIndex1 = 4*13;
        int writeIndex2 = writeIndex1 + singleTimeSliceBytes;

        if (copy && useRealData) {
            // For each of 4 banks
            for (int i=0; i < 4; i++) {
                // Have we run out of data? If so, start over from beginning ...
                if (arrayBytes - hallDdataPosition < generatedDataBytes) {
                    hallDdataPosition = 0;
                }

                if (buf.hasArray()) {
                    System.arraycopy(hallDdata, hallDdataPosition, buf.array(), writeIndex1, generatedDataBytes);
                }
                else {
                    buf.position(writeIndex1);
                    buf.put(hallDdata, hallDdataPosition, generatedDataBytes);
                    // Get buf ready to read for output channel
                    buf.limit(templateBuf.limit()).position(0);
                }

                hallDdataPosition += generatedDataBytes;

                // Move to next data bank's data position
                writeIndex1 += 8 + generatedDataBytes;
            }

            // For each of 4 banks
            for (int i=0; i < 4; i++) {
                // Have we run out of data? If so, start over from beginning ...
                if (arrayBytes - hallDdataPosition < generatedDataBytes) {
                    hallDdataPosition = 0;
                }

                if (buf.hasArray()) {
                    System.arraycopy(hallDdata, hallDdataPosition, buf.array(), writeIndex2, generatedDataBytes);
                }
                else {
                    buf.position(writeIndex2);
                    buf.put(hallDdata, hallDdataPosition, generatedDataBytes);
                    // Get buf ready to read for output channel
                    buf.limit(templateBuf.limit()).position(0);
                }

                hallDdataPosition += generatedDataBytes;

                // Move to next data bank's data position
                writeIndex2 += 8 + generatedDataBytes;
            }
        }

    }



    //////////////////////////////
    //////////////////////////////


    /**
     * <p>This thread generates events with junk data in it (all zeros except first word which
     * is the event number).
     * It is started by the GO transition and runs while the state of the module is ACTIVE.
     * </p>
     * <p>When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * selects an output by taking the next one from a simple iterator. This thread then creates
     * data transport records with payload banks containing ROC raw records and places them on the
     * output DataChannel.
     * </p>
     */
    class EventGeneratingThread extends Thread {

        private final int myId;
        private int  myRocRecordId;
        private long myEventNumber, timestamp = 100;
        /** Ring buffer containing ByteBuffers - used to hold events for writing. */
        private ByteBufferSupply bbSupply;
        // Number of data words in each event
        private int generatedDataWords;
        private int generatedDataBytes;
        private ByteBuffer templateBuffer;

        /** Boolean used to kill this thread. */
        private volatile boolean killThd;

        // Streaming stuff
        private long frameNumber;
        int singleTSBsize;


        EventGeneratingThread(int id, ThreadGroup group, String name) {
            super(group, name);
            this.myId = id;

            // Is we streamin'?
            if (isStreamingData()) {
                //generatedDataWords = 40*75;
                generatedDataWords = 5;
                //ByteBuffer singleTSB = createSingleTimeSliceBuffer(generatedDataWords, frameNumber, timestamp);
                //singleTSBsize = singleTSB.limit() - 8;
//System.out.println("  Roc mod: single TSB bytes size = " + singleTSBsize + ", words = " + (singleTSBsize/4));
                //templateBuffer = createDualTimeSliceBuffer(generatedDataWords, frameNumber, timestamp);
System.out.println("\n  Roc mod: Starting sim ROC frame at " + frameNumber + "\n");
                templateBuffer = createSingleTimeSliceBuffer(generatedDataWords, frameNumber, timestamp);
//Utilities.printBuffer(templateBuffer, 0, 56, "TEMPLATE BUFFER");
                eventWordSize = templateBuffer.remaining()/4;
                frameNumber++;
                timestamp += 10;
            }
            // or ain't we?
            else {
                // Set & initialize values
                myRocRecordId = rocRecordId + myId;
                myEventNumber = 1L + myId * eventBlockSize;
                timestamp = myId * 4 * eventBlockSize;

                // Need to coordinate amount of data words
                generatedDataWords = eventBlockSize * eventSize;
                generatedDataBytes = 4 * generatedDataWords;
                System.out.println("  Roc mod: generatedDataWords = " + generatedDataWords);


                eventWordSize = getSingleEventBufferWords(generatedDataWords);
                System.out.println("  Roc mod: eventWordSize = " + eventWordSize);

                templateBuffer = createSingleEventBuffer(generatedDataWords, myEventNumber, timestamp);


                System.out.println("  Roc mod: start With (id=" + myId + "):\n    record id = " + myRocRecordId +
                        ", ev # = " + myEventNumber + ", ts = " + timestamp +
                        ", blockSize = " + eventBlockSize);
            }
        }


        /**
         * Kill this thread which is sending messages/data to other end of emu socket.
         */
        final void endThread() {
            killThd = true;
            this.interrupt();
        }


        public void run() {

            int  skip=3, syncBitLoop = syncBitCount;
            int frameChange, fChange;

            // We can count either frames or events depending if we're streaming or not
            int   userCountLoop = syncCount;
            //int   userCountLoop = 5;

            // Stat time
            long totalT=0L, t1, deltaT, t2;

            // Event stats
            long oldVal=0L, totalCount=0L, bufCounter=0L;

            // Slice stats
            long oldFrameVal=0L, totalFrameCount=0L;
            double frameRate = 0., avgFrameRate = 0.;

            ByteBuffer buf;
            ByteBufferItem bufItem;
            boolean copyWholeBuf = true;

            if (rocNumber == 1) {
                frameChange = fChange = 1;
            }
            else {
                frameChange = fChange = 2;
            }

            // We need for the # of buffers in our bbSupply object to be >=
            // the # of ring buffer slots in the output channel or we can get
            // a deadlock. Although we get the value from the first channel's
            // first ring, it's the same for all output channels.
            if (outputChannelCount > 0) {
                bufSupplySize = outputChannels.get(0).getRingBuffersOut()[0].getBufferSize();
            }
            else {
                bufSupplySize = 1024;
            }

            // Now create our own buffer supply to match
            bbSupply = new ByteBufferSupply(bufSupplySize, 4*eventWordSize, ByteOrder.BIG_ENDIAN, false);

            try {
                t1 = System.currentTimeMillis();

                // Use dummy arg that's overwritten later
                boolean noBuildableEvents = false;

                // Send user event right after prestart and go events
//                if (emu.name().equals("Roc1")) {
//System.out.println("  Roc mod: write FIRST event after go for Roc1");
//
//                    // Put in User event
//                    PayloadBuffer pBuf = createUserBuffer(outputOrder, true, 8);
//                    eventToOutputChannel(pBuf, 0, 0);

//System.out.println("\n\n  Roc mod: write USER event after go for Roc1 ***************\n");
//                    PayloadBuffer pBuf2 = createUserBuffer(outputOrder, false, 9);
//                    eventToOutputChannel(pBuf2, 0, 0);

//                    eventCountTotal++;
//                    wordCountTotal  += 7;
//                    eventCountTotal += 2;
//                    wordCountTotal  += 2*7;
                    //myRocRecordId += 2;
//                }

                System.out.println("SETTING loops to " + loops);

                while (moduleState == CODAState.ACTIVE || paused) {

                    if (gotResetCommand) {
                        return;
                    }

                    if (noBuildableEvents) {
                        if (killThd) return;

                        Thread.sleep(500);

                        // Do the requisite number of iterations before syncing up
                        if (synced && --userCountLoop < 1) {
                            // Did we receive the END command yet? ("moduleState" is volatile)
                            if (moduleState == CODAState.DOWNLOADED) {
                                // END command has arrived
//System.out.println("  Roc mod: end has arrived");
                                gotEndCommand = true;
                            }

                            // Send message to synchronizer that we're waiting
                            // whether or not we got the END command. Only
                            // want 1 msg sent, so have the first thread do it.
                            if (myId == 0) {
                                sendMsgToSynchronizer(gotEndCommand);
                            }

                            // Wait for synchronizer's response before continuing
//System.out.println("  Roc mod: phaser await advance, ev count = " + eventCountTotal);
                            phaser.arriveAndAwaitAdvance();
//System.out.println("  Roc mod: phaser PAST advance, ev count = " + eventCountTotal);

                            // Every ROC has received the END command and completed the
                            // same number of iterations, therefore it's time to quit.
                            if (timeToEnd) {
//System.out.println("  Roc mod: arrive, SYNC told me to quit");
                                endPhaser.arriveAndDeregister();
                                return;
                            }

                            userCountLoop = 5;
                        }
                    }
                    else {
                        if (killThd) return;

                        // Add ROC Raw Records as PayloadBuffer objects
                        // Get buffer from recirculating supply.
                        bufItem = bbSupply.get();
                        buf = bufItem.getBuffer();

                        // Some logic to allow us to copy everything into buffer
                        // only once. After that, just update it.
                        if (copyWholeBuf) {
                            // Only need to do this once too
                            buf.order(outputOrder);

                            if (++bufCounter > bufSupplySize) {
                                copyWholeBuf = false;
                            }
                        }
//System.out.println("  Roc mod: write event");

                        if (isStreamingData()) {
//                            writeDualTimeSliceBuffer(buf, templateBuffer, frameNumber,
//                                    timestamp, copyWholeBuf, bytesPerDataBank, singleTSBsize);
                            writeTimeSliceBuffer(buf, templateBuffer, frameNumber,
                                                 timestamp, copyWholeBuf, bytesPerDataBank);
                        }
                        else {
                            if (--syncBitLoop == 0) {
                                syncBitLoop = syncBitCount;
                                // Set the sync bit
                                writeEventBuffer(buf, templateBuffer, myEventNumber,
                                        timestamp, true, copyWholeBuf,
                                        generatedDataBytes);
                            } else {
                                writeEventBuffer(buf, templateBuffer, myEventNumber,
                                        timestamp, false, copyWholeBuf,
                                        generatedDataBytes);
                            }
                        }

//Thread.sleep(1000);
                        if (killThd) return;

                        // Put generated events into output channel
                        eventToOutputRing(myId, buf, bufItem, bbSupply);
//Utilities.printBuffer(buf, 0, 56, "EVENT BUFFER");

                        if (isStreamingData()) {
                            wordCountTotal += eventWordSize;
                            // Switch frame and timestamp, every other send
                            if (--fChange < 1) {
                                frameNumber++;
                                timestamp += 10;
                                fChange = frameChange;
                                eventCountTotal++;
                                userCountLoop--;
                            }
                        }
                        else {
                            eventCountTotal += eventBlockSize;
                            wordCountTotal  += eventWordSize;
                            myEventNumber   += eventProducingThreads * eventBlockSize;
                            timestamp       += 4 * eventProducingThreads * eventBlockSize;
                            userCountLoop--;
                        }

//                        if (userEventLoop == userEventLoopMax - 10) {
//System.out.println("  Roc mod: INSERT USER EVENT");
//                            eventToOutputChannel(Evio.createUserBuffer(outputOrder), 0, 0);
//                        }

                        // Do the requisite number of iterations before syncing up
                        if (synced && userCountLoop < 1) {
                            // Did we receive the END command yet? ("moduleState" is volatile)
                            if (moduleState == CODAState.DOWNLOADED) {
                                // END command has arrived
System.out.println("  Roc mod: end has arrived");
                                gotEndCommand = true;
                            }

                            // Send message to synchronizer that we're waiting
                            // whether or not we got the END command. Only
                            // want 1 msg sent, so have the first thread do it.
                            if (myId == 0) {
                                sendMsgToSynchronizer(gotEndCommand);
                            }

                            // Wait for synchronizer's response before continuing
System.out.println("  Roc mod: phaser await advance, ev count = " + eventCountTotal);
                            phaser.arriveAndAwaitAdvance();
System.out.println("  Roc mod: phaser PAST advance, ev count = " + eventCountTotal);

                            // Every ROC has received the END command and completed the
                            // same number of iterations, therefore it's time to quit.
                            if (timeToEnd) {
System.out.println("  Roc mod: arrive, SYNC told me to quit");
                                endPhaser.arriveAndDeregister();
                                return;
                            }

                            // For better testing change synCount by 1 each time
                            userCountLoop = ++syncCount;
                        }
                    }

                    t2 = emu.getTime();
                    deltaT = t2 - t1;

                    if (myId == 0 && deltaT > 2000) {
                        if (isStreamingData()) {
                            if (skip-- < 1) {
                                totalT += deltaT;
                                totalFrameCount += frameNumber - oldFrameVal;
                                frameRate = ((frameNumber - oldFrameVal) * 1000. / deltaT);
                                avgFrameRate = (totalFrameCount * 1000.) / totalT;
                            }
                            System.out.println("  Roc mod: frame rate = " + String.format("%.3g", frameRate) +
                                    " Hz,  avg = " + String.format("%.3g", avgFrameRate));
                            System.out.println("  Roc mod: slice rate = " + String.format("%.3g", 2.*frameRate) +
                                    " Hz,  avg = " + String.format("%.3g", 2.*avgFrameRate));
                            t1 = t2;
                            oldFrameVal = frameNumber;
                        }
                        else {
                            if (skip-- < 1) {
                                totalT += deltaT;
                                totalCount += myEventNumber - oldVal;
                                System.out.println("  Roc mod: event rate = " + String.format("%.3g", ((myEventNumber - oldVal) * 1000. / deltaT)) +
                                                   " Hz,  avg = " + String.format("%.3g", (totalCount * 1000.) / totalT));
                            } else {
                                System.out.println("  Roc mod: event rate = " + String.format("%.3g", ((myEventNumber - oldVal) * 1000. / deltaT)) + " Hz");
                            }
                            t1 = t2;
                            oldVal = myEventNumber;
                        }
                    }

                }
            }
            catch (InterruptedException e) {
                // End or Reset most likely
            }
            catch (Exception e) {
                e.printStackTrace();
                // If we haven't yet set the cause of error, do so now & inform run control
                errorMsg.compareAndSet(null, e.getMessage());
                moduleState = CODAState.ERROR;
                emu.sendStatusMessage();
            }
        }

    }


    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        gotResetCommand = true;
System.out.println("  Roc mod: reset()");
        Date theDate = new Date();
        CODAStateIF previousState = moduleState;
        moduleState = CODAState.CONFIGURED;

        eventRate = wordRate = 0F;
        frameCountTotal = eventCountTotal = wordCountTotal = 0L;

        // rb.next() can block in endThreads() when doing a RESET.
        killThreads();

        if (synced) {
            // Unsubscribe
            try {
                if (cmsgSubHandle != null) {
                    cMsgServer.unsubscribe(cmsgSubHandle);
                    cmsgSubHandle = null;
                }
            } catch (cMsgException e) {}
        }

        paused = false;

        if (previousState.equals(CODAState.ACTIVE)) {
            // set end-of-run time in local XML config / debug GUI
            try {
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            }
            catch (DataNotFoundException e) {}
        }
    }


    /** {@inheritDoc} */
    public void end() throws CmdExecException {
        paused = false;
        gotEndCommand = true;

        // We're the only module, and the END event has made it through
        endCallback.endWait();

        // Skip over this if not synced or go never received
        if (gotGoCommand && synced) {
            // Wait until all threads are done writing events
System.out.println("  Roc mod: end(), endPhaser block here");
            try {
                endPhaser.awaitAdvanceInterruptibly(endPhaser.arrive());
            }
            catch (InterruptedException e) {
                e.printStackTrace();
System.out.println("  Roc mod: end(), endPhaser interrupted, XXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
                return;
            }
System.out.println("  Roc mod: end(), past endPhaser");
        }

        // Put this line down here so we don't pop out of event-generating
        // loop before END is properly dealt with.
        moduleState = CODAState.DOWNLOADED;

        if (synced) {
            // Unsubscribe
            try {
                if (cmsgSubHandle != null) {
System.out.println("  Roc mod: end(), UNSUBSCRIBE");
                    cMsgServer.unsubscribe(cmsgSubHandle);
                    cmsgSubHandle = null;
                }
            } catch (cMsgException e) {}
        }

        // set end-of-run time in local XML config / debug GUI
        try {
            // Set end-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
//System.out.println("  Roc mod: end(), kill threads");

        killThreads();

        // Putting the following 2 calls right after setting moduleState
        // results in the END event not making it through the output channel on occasion.
        // This is probably because there is a race condition as the killThreads()
        // may not have yet stopped the event producing threads from inserting events
        // while the eventToOutputChannel call (see below) was doing the same thing
        // simultaneously.

        // Put in END event
        PayloadBuffer pBuf = Evio.createControlBuffer(ControlType.END, 0, 0,
                                                      (int)eventCountTotal, (int)frameCountTotal, 0,
                                                      outputOrder, name, false, isStreamingData());
        // Send to first ring on ALL channels
        for (int i=0; i < outputChannelCount; i++) {
            if (i > 0) {
                // copy buffer and use that
                pBuf = new PayloadBuffer(pBuf);
            }
            try {
                eventToOutputChannel(pBuf, i, 0);
            }
            catch (InterruptedException e) {
                return;
            }
            System.out.println("  Roc mod: inserted END event to channel " + i);
        }
    }


    /** {@inheritDoc} */
    public void prestart() {

System.out.println("  Roc mod: PRESTART");
        moduleState = CODAState.PAUSED;

        // Reset some variables
        gotGoCommand = gotEndCommand = gotResetCommand = false;
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;
        rocRecordId = 1;

        if (synced) {
 System.out.println("  Roc mod: PRESTART, (re)create phasers");
            phaser    = new Phaser(eventProducingThreads + 1);
            endPhaser = new Phaser(eventProducingThreads + 2);
            timeToEnd = false;
            gotEndCommand = false;
        }

        // create threads objects (but don't start them yet)
        RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), emu.name()+":watcher");


//        boolean sendUser = true;
//
//        // Send user events right before prestart
//        if (sendUser && emu.name().equals("Roc1")) {
//            try {
//                // Put in User events
//                System.out.println("  Roc mod: write USER event for Roc1");
//                PayloadBuffer pBuf = createUserBuffer(outputOrder, false, 1);
//                eventToOutputChannel(pBuf, 0, 0);
//                rocRecordId++;
//
////                System.out.println("  Roc mod: write FIRST event for Roc1");
////                pBuf = createUserBuffer(outputOrder, true, 2);
////                eventToOutputChannel(pBuf, 0, 0);
////                rocRecordId++;
////
////                System.out.println("  Roc mod: write USER event for Roc1");
////                pBuf = createUserBuffer(outputOrder, false, 3);
////                eventToOutputChannel(pBuf, 0, 0);
////                rocRecordId++;
////
////                for (int i=0; i < 8200; i++) {
////                    System.out.println("  Roc mod: write FIRST event for Roc1");
////                    pBuf = createUserBuffer(outputOrder, true, i);
////                    eventToOutputChannel(pBuf, 0, 0);
////                    rocRecordId++;
////
////                }
//            }
//            catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//            eventCountTotal++;
//            wordCountTotal  += 7;
////            eventCountTotal += 3;
////            wordCountTotal  += 3*7;
////            eventCountTotal += 8200 + 0;
////            wordCountTotal  += (8200 + 0)*7;
//        }


        // Create PRESTART event
        PayloadBuffer pBuf = Evio.createControlBuffer(ControlType.PRESTART, emu.getRunNumber(),
                                                      emu.getRunTypeId(), 0, 0,0,
                                                      outputOrder, name, false, isStreamingData());
        // Send to first ring on ALL channels
        for (int i=0; i < outputChannelCount; i++) {
            // Copy buffer and use that
            PayloadBuffer pBuf2 = new PayloadBuffer(pBuf);
            try {
                eventToOutputChannel(pBuf2, i, 0);
            }
            catch (InterruptedException e) {
                return;
            }
            System.out.println("  Roc mod: inserted PRESTART event to channel " + i);
        }

        rocRecordId++;

//        // Send more user events right after prestart
//        if (sendUser && emu.name().equals("Roc1")) {
//            try {
//                // Put in User events
//                System.out.println("  Roc mod: write USER event after prestart for Roc1");
//                pBuf = createUserBuffer(outputOrder, false, 5);
//                eventToOutputChannel(pBuf, 0, 0);
//                rocRecordId++;
//
////                System.out.println("  Roc mod: write FIRST event after prestart for Roc1");
////                pBuf = createUserBuffer(outputOrder, true, 6);
////                eventToOutputChannel(pBuf, 0, 0);
////                rocRecordId++;
////
////                System.out.println("  Roc mod: write USER event after prestart for Roc1");
////                pBuf = createUserBuffer(outputOrder, false, 7);
////                eventToOutputChannel(pBuf, 0, 0);
////                rocRecordId++;
//            }
//            catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//            eventCountTotal++;
//            wordCountTotal  += 7;
////            eventCountTotal += 3;
////            wordCountTotal  += 21;
//        }



        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}

        // Subscribe to cMsg server for ROC synchronization purposes
        if (synced) {
            cMsgServer = emu.getCmsgPortal().getCmsgServer();
            try {
                System.out.println("  Roc mod: prestart(), SUBSCRIBE");
                cmsgSubHandle = cMsgServer.subscribe("sync", "ROC", callback, null);
            }
            catch (cMsgException e) {/* never happen */}
        }
//        System.out.println("  Roc mod: after PRESTART, rocRecordId = " + rocRecordId);
    }


    /** {@inheritDoc} */
    public void go() {
        gotGoCommand = true;

        if (moduleState == CODAState.ACTIVE) {
//System.out.println("  Roc mod: we must have hit go after PAUSE");
        }

        // Create GO event
        PayloadBuffer pBuf = Evio.createControlBuffer(ControlType.GO, 0, 0,
                                                      (int) eventCountTotal, (int)frameCountTotal, 0,
                                                      outputOrder, name, false, isStreamingData());
        // Send to first ring on ALL channels
        for (int i=0; i < outputChannelCount; i++) {
            // Copy buffer and use that
            PayloadBuffer pBuf2 = new PayloadBuffer(pBuf);
            try {
                eventToOutputChannel(pBuf2, i, 0);
            }
            catch (InterruptedException e) {
                return;
            }
            System.out.println("  Roc mod: inserted GO event to channel " + i);
        }

        rocRecordId++;
//        System.out.println("  Roc mod: after GO sent, rocRecordId = " + rocRecordId);

//        try {
//            Thread.sleep(500);
//        }
//        catch (InterruptedException e) {}

        moduleState = CODAState.ACTIVE;

        // start up all threads
        if (RateCalculator == null) {
            RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), emu.name()+":watcher");
        }

        if (RateCalculator.getState() == Thread.State.NEW) {
            RateCalculator.start();
        }

        if (!noPhysics) {
            for (int i = 0; i < eventProducingThreads; i++) {
                if (eventGeneratingThreads[i] == null) {
System.out.println("  Roc mod: create new event generating thread ");
                    eventGeneratingThreads[i] = new EventGeneratingThread(i, emu.getThreadGroup(),
                                                                          emu.name() + ":generator");
                }
                else if (!eventGeneratingThreads[i].isAlive()) {
System.out.println("  Roc mod: create new event generating thread, since old one is DEAD");
                    eventGeneratingThreads[i] = new EventGeneratingThread(i, emu.getThreadGroup(),
                                                                          emu.name() + ":generator");
                }

System.out.println("  Roc mod: event generating thread " + eventGeneratingThreads[i].getName() + " isAlive = " +
                           eventGeneratingThreads[i].isAlive());
                if (eventGeneratingThreads[i].getState() == Thread.State.NEW) {
System.out.println("  Roc mod: starting event generating thread");
                    eventGeneratingThreads[i].start();
                }
            }
        }

        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}

    }


}