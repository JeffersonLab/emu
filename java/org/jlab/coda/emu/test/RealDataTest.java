/*
 * Copyright (c) 2019, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.test;


import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.DataType;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class is designed to find the bottleneck in using
 * real Hall D data in simulated ROC. It's a hacked-up
 * version of RocSimulation.
 *
 * @author timmer
 * (Jun 5, 2019)
 */
public class RealDataTest {

    private boolean debug;

    /** Amount of real data bytes in hallDdata array. */
    private int arrayBytes;

    /** Put extracted real data in here. */
    private byte[] hallDdata;

    private int eventWordSize;

    private ByteBuffer templateBuffer;
    private int eventBlockSize = 40;
    private int eventSize = 75;
    private int generatedDataWords = eventBlockSize * eventSize;
    private int generatedDataBytes = 4*generatedDataWords;

    private boolean useRealData = true;

    private int hallDdataPosition = 0;

    private int detectorId = 111;

    private ByteBufferSupply bbSupply;

    /** ID number of this module obtained from config file. */
    private int id = 3;

    private ByteOrder outputOrder = ByteOrder.BIG_ENDIAN;

    // Value for trigger type from trigger supervisor
    private int triggerType = 15;
    private int myId = 0;

    /** Total number of evio events written to the outputs. */
    private long eventCountTotal;

    /** Sum of the sizes, in 32-bit words, of all evio events written to the outputs. */
    private long wordCountTotal;



    /** Constructor. */
    private RealDataTest(String[] args) {
        decodeCommandLine(args);
    }


    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private void decodeCommandLine(String[] args) {

        // loop over all args
        for (int i = 0; i < args.length; i++) {

            if (args[i].equalsIgnoreCase("-h")) {
                usage();
                System.exit(-1);
            }
//            else if (args[i].equalsIgnoreCase("-t")) {
//                String threads = args[i + 1];
//                i++;
//                try {
//                    buildThreadCount = Integer.parseInt(threads);
//                    if (buildThreadCount < 2) {
//                        buildThreadCount = 2;
//                    }
//                }
//                catch (NumberFormatException e) {
//                }
//            }
//            else if (args[i].equalsIgnoreCase("-g")) {
//                global = true;
//            }
//            else if (args[i].equalsIgnoreCase("-v")) {
//                volLong = true;
//            }
            else if (args[i].equalsIgnoreCase("-debug")) {
                debug = true;
            }
            else {
                usage();
                System.exit(-1);
            }
        }
    }


    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage:\n\n" +
            "   java BuildThreadTest\n" +
            "        [-debug]         turn on printout\n" +
            "        [-h]             print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
        try {
            RealDataTest receiver = new RealDataTest(args);
            receiver.run();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /** This method is executed as a thread. */
    public void run() {

        getRealData();

        int myId = 0;
        long myEventNumber = 1L + myId*eventBlockSize;
        long timestamp = myId*4*eventBlockSize;

        eventWordSize  = getSingleEventBufferWords(generatedDataWords);
        System.out.println("eventWordSize = " + eventWordSize);

        templateBuffer = createSingleEventBuffer(generatedDataWords, myEventNumber, timestamp);

        writeToBuffer();
    }


    public void writeToBuffer() {

        int skip = 3;
        long oldVal=0L, totalT=0L, totalCount=0L, bufCounter=0L;
        long t1, deltaT, t2;
        ByteBuffer buf;
        ByteBufferItem bufItem;
        boolean copyWholeBuf = true;
        int bufSupplySize = 4096;
        long myEventNumber=1, timestamp=0;

        int counter = 0;


        // Now create a buffer supply
        bbSupply = new ByteBufferSupply(bufSupplySize, 4*eventWordSize, ByteOrder.BIG_ENDIAN, false);

        try {
            t1 = System.currentTimeMillis();

            while (true) {

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

                writeEventBuffer(buf, templateBuffer, myEventNumber,
                                 timestamp, false, copyWholeBuf,
                                 generatedDataBytes);

                bbSupply.release(bufItem);

                eventCountTotal += eventBlockSize;
                wordCountTotal += eventWordSize;

                myEventNumber += eventBlockSize;
                timestamp += 4 * eventBlockSize;


                if (counter++ % 10000000 == 0) {

                    t2 = System.currentTimeMillis();
                    deltaT = t2 - t1;

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
        catch (Exception e) {
            e.printStackTrace();
        }
    }



    private int getSingleEventBufferWords(int generatedDataWords) {
        int dataWordLength = 1 + generatedDataWords;
        return (6 + 4*eventBlockSize + dataWordLength);
    }


    public static int createCodaTag(boolean sync, boolean error, boolean isBigEndian,
                                    boolean singleEventMode, int id) {
        int status = 0;

        if (sync)            status |= 0x1000;
        if (error)           status |= 0x2000;
        if (isBigEndian)     status |= 0x4000;
        if (singleEventMode) status |= 0x8000;

        return ( status | (id & 0x0fff) );
    }


    /**
     * Method to get real data from an existing file of data previously extracted from
     * a Hall D data file.
     * There are 9 files, each containing 16 MB of unique real Hall D data.
     *
     * @return true if hall D data found and available, else false.
     */
    private boolean getRealData() {

        // Try to get the last digit at end of name if any
        int num = 1;

        // First check to see if we already have some data in a file
        String filename = System.getenv("CODA");
        if (filename != null) {
            filename += "/common/bin/hallDdata" + num + ".bin";
        }
        else {
            filename = "/Users/timmer/coda/coda3/common/bin/hallDdata" + num + ".bin";
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

        System.out.println("getRealData: successfully read in file " + filename + ", size = " + arrayBytes);
        return true;
    }



    private ByteBuffer createSingleEventBuffer(int generatedDataWords, long eventNumber,
                                               long timestamp ) {
        int writeIndex=0;
        int dataLen = 1 + generatedDataWords;

        // Note: buf limit is set to its capacity in allocate()
        ByteBuffer buf = ByteBuffer.allocate(4*eventWordSize);
        buf.order(outputOrder);

        // Event bank header
        // sync, error, isBigEndian, singleEventMode
        int rocTag = createCodaTag(false, false, true, false, id);

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


        int dataTag = createCodaTag(false, false, true, false, detectorId);
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
                // Write a bunch of 1s
                buf.putInt(writeIndex, 1);
                writeIndex += 4;
            }
        }

        // buf is ready to read
        return buf;
    }



    void writeEventBuffer(ByteBuffer buf, ByteBuffer templateBuf,
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
        buf.position(0).limit(templateBuf.limit());

        // Set sync bit in event bank header
        // sync, error, isBigEndian, singleEventMode
        int rocTag = createCodaTag(syncBit, false, true, false, id);

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
        // Each event has eventBlockSize * eventSize (40*75 = 3000) data bytes.
        // 3k bytes * 4096 events = 12.3MB. This works out nicely since we have
        // retrieved 16MB from a single Hall D data file.
        // However, each Roc has the same data which will lend itself to more compression.
        // So the best thing is for each ROC to have different data.
        //if (false) {
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
                buf.position(0).limit(templateBuf.limit());

            }

            //System.out.print(".");

            hallDdataPosition += generatedDataBytes;
        }
    }





}
