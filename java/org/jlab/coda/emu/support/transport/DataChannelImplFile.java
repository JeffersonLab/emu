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
import org.jlab.coda.emu.support.data.EventType;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.emu.support.data.PayloadBank;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.jevio.*;

import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
public class DataChannelImplFile implements DataChannel {

    /** EMU object that created this channel. */
    private Emu emu;

    /** Logger associated with this EMU. */
    private Logger logger;

    /** Transport object that created this channel. */
    private final DataTransportImplFile dataTransport;

    /** Channel name */
    private final String name;

    /** Channel id (corresponds to sourceId of file). */
    private int id;

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

    /** Field queue - filled buffer queue */
    private final BlockingQueue<EvioBank> queue;

    /** Name of file currently being written to. */
    private String fileName;

    /** Evio data file. */
    private EvioReader evioFile;

    /** Object to write evio file. */
    private EventWriter evioFileWriter;

    /** Is this channel an input (true) or output (false) channel? */
    private boolean input;

    /** Byte order of output data. */
    private ByteOrder byteOrder;



    /**
     * Constructor DataChannelImplFifo creates a new DataChannelImplFifo instance.
     *
     * @param name          name of file channel
     * @param dataTransport DataTransport object that created this channel
     * @param attrib        the hashmap of config file attributes for this channel
     * @param input         true if this is an input
     * @param emu           emu this channel belongs to
     *
     * @throws DataTransportException
     *          - if unable to create fifo buffer.
     */
    DataChannelImplFile(String name, DataTransportImplFile dataTransport,
                      Map<String, String> attrib, boolean input,
                      Emu emu) throws DataTransportException {

        this.dataTransport = dataTransport;
        this.input = input;
        this.name = name;
        this.emu  = emu;
        logger = emu.getLogger();

        // Set option whether or not to enforce evio block header
        // numbers to be sequential (throw an exception if not).
        boolean blockNumberChecking = false;
        String attribString = attrib.get("blockNumCheck");
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

        // Set id number. Use any defined in config file else use default (0)
        id = 0;
        attribString = attrib.get("id");
        if (attribString != null) {
            try {
                id = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {  }
        }
//logger.info("      DataChannel File: id = " + id);

        // Directory given in config file?
        try {
            directory = attrib.get("dir");
//logger.info("      DataChannel File: config file dir = " + directory);
        } catch (Exception e) {
        }

        // Filename given in config file?
        try {
            fileName = attrib.get("fileName");

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
        } catch (Exception e) {
        }

        // Filename given in config file?
        String prefix = null;
        try {
            prefix = attrib.get("prefix");
//logger.info("      DataChannel File: config file prefix = " + prefix);
        } catch (Exception e) {
        }

        // Split parameter given in config file?
        try {
            String splitStr = attrib.get("split");
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
        } catch (Exception e) {
        }

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

        // set queue capacity
        int capacity = 40;
        try {
            capacity = dataTransport.getIntAttr("capacity");
            if (capacity < 1) capacity = 40;
        } catch (Exception e) {
            logger.info("      DataChannel File : " +  e.getMessage() + ", default to " + capacity + " records.");
        }
        queue = new ArrayBlockingQueue<EvioBank>(capacity);

        try {
            if (input) {
logger.info("      DataChannel File: try opening input file of " + fileName);
                evioFile = new EvioReader(fileName, blockNumberChecking);
                DataInputHelper helper = new DataInputHelper();
                dataThread = new Thread(emu.getThreadGroup(), helper, getName() + " data input");
                dataThread.start();
                helper.waitUntilStarted();

            } else {
                // set endianness of data
                byteOrder = ByteOrder.BIG_ENDIAN;
                try {
                    String order = attrib.get("endian");
                    if (order != null && order.equalsIgnoreCase("little")) {
                        byteOrder = ByteOrder.LITTLE_ENDIAN;
                    }
                } catch (Exception e) {
                    logger.info("      DataChannel File: no output data endianness specified, default to big.");
                }

logger.info("      DataChannel File: try opening output file of " + fileName);
                // Tell emu what that output name is for stat reporting
                emu.setOutputDestination(fileName);

                evioFileWriter = new EventWriter(fileName, false, byteOrder);
                DataOutputHelper helper = new DataOutputHelper();
                dataThread = new Thread(emu.getThreadGroup(), helper, getName() + " data out");
                dataThread.start();
                helper.waitUntilStarted();
            }

        } catch (Exception e) {
            if (input) {
                throw new DataTransportException("DataChannelImplFile : Cannot open data file " + e.getMessage(), e);
            }
            else {
                throw new DataTransportException("DataChannelImplFile : Cannot create data file" + e.getMessage(), e);
            }
        }
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
        @Override
        public void run() {

            int counter = 0;

            // I've started
            latch.countDown();

            try {
                while (!dataThread.isInterrupted()) {
                    EvioBank bank = evioFile.parseNextEvent();
                    if (bank == null) {
                        break;
                    }

                    queue.put(bank);  // will block
                    counter++;
                }

                // Put in END event
                EvioEvent controlEvent = Evio.createControlEvent(EventType.END, 0, 0, counter, 0);
                queue.put(controlEvent);  // will block

            } catch (InterruptedException e) {
                // time to quit
            } catch (Exception e) {
//logger.warn("      DataChannel File (" + name + "): close file");
//logger.warn("      DataChannel File (" + name + "): exit " + e.getMessage());
                emu.getCauses().add(e);
                dataTransport.state = CODAState.ERROR;
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
        @Override
        public void run() {

            // I've started
            latch.countDown();

            PayloadBank bank;
            int bankBytes;
            long numBytesWritten = 0L;
            boolean gotPrestart = false, gotEnd = false;

            try {

                while (!dataThread.isInterrupted()) {
                    // This bank is a data transport record (DTR)
                    bank = (PayloadBank)queue.take(); // will block

                    if (bank.getType() == EventType.END) {
                        gotEnd = true;
logger.info("      DataChannel File (" + name + "): got END, close file " + fileName);
                    }
                    else if (bank.getType() == EventType.PRESTART) {
logger.info("      DataChannel File (" + name + "): got PRESTART");
                        gotPrestart = true;
                    }
                    else {
//logger.info("      DataChannel File (" + name + "): got bank of type " + bank.getType());
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
                        evioFileWriter.close();
                        numBytesWritten = 0L;
                        fileName = String.format("%s%06d", outputFilePrefix, (++fileCount));
                        if (directory != null) {
                            fileName = directory + "/" + fileName;
                        }
//logger.info("      DataChannel File (" + name + "): split, new file = " + fileName);
                        evioFileWriter = new EventWriter(fileName);
                    }

//logger.info("      DataChannel File (" + name + "): try writing into file" + fileName);
                    evioFileWriter.writeEvent(bank);
                    numBytesWritten += bankBytes;

                    if (gotEnd) {
                        try { evioFileWriter.close(); }
                        catch (Exception e) {}
                        return;
                    }
                }

                logger.info("      DataChannel File (" + name + "): close file " + fileName);

            } catch (InterruptedException e) {
                // time to quit
            } catch (Exception e) {
//logger.warn("      DataChannel File (" + name + "): exit, " + e.getMessage());
                emu.getCauses().add(e);
                dataTransport.state = CODAState.ERROR;
            }

            try { evioFileWriter.close(); }
            catch (Exception e) {}
        }

    }


    /** {@inheritDoc} */
    @Override
    public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public int getID() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isInput() {
        return input;
    }

    /** {@inheritDoc} */
    @Override
    public DataTransport getDataTransport() {
        return dataTransport;
    }

    /** {@inheritDoc} */
    @Override
    public EvioBank receive() throws InterruptedException {
        return queue.take();
    }

    /** {@inheritDoc} */
    @Override
    public void send(EvioBank data) {
        queue.add(data);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (dataThread != null) dataThread.interrupt();
        try {
            if (evioFile != null) evioFile.close();
        } catch (Exception e) {
            //ignore
        }
        queue.clear();
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
        close();
    }

    /** {@inheritDoc} */
    @Override
    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }

}