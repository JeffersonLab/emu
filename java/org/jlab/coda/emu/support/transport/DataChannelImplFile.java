package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.data.EventType;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.emu.support.data.PayloadBank;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.jevio.*;

import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Implementation of a DataChannel reading/writing from/to a file in EVIO format.
 *
 * @author heyes
 * @author timmer
 * @date Nov 10, 2008
 */
public class DataChannelImplFile implements DataChannel {

    /** Field transport */
    private final DataTransportImplFile dataTransport;

    /** Map of config file attributes. */
    private Map<String, String> attributeMap;

    /** Field name */
    private final String name;

    /** Channel id (corresponds to sourceId of file). */
    private int id;

    /** Field dataThread */
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
    String fileName;

    /** Evio data file. */
    private EvioReader evioFile;

    /** Object to write evio file. */
    private EventWriter evioFileWriter;

    /** Is this channel an input (true) or output (false) channel? */
    boolean input;

    private Emu emu;
    private Logger logger;


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
        this.attributeMap = attrib;
        this.input = input;
        this.name = name;
        this.emu  = emu;
        logger = emu.getLogger();


        // Default output file name =   <session>_<run#>.dat<seq#>
        String session = emu.getSession();
        int runNumber  = emu.getRunNumber();
        outputFilePrefix = session + "_" + runNumber + ".dat";
        String outputFileName = String.format("%s%06d", outputFilePrefix, fileCount);
        String defaultInputFileName = "codaDataFile.evio";

        // Set id number. Use any defined in config file else use default (0)
        id = 0;
        String attribString = attributeMap.get("id");
        if (attribString != null) {
            try {
                id = Integer.parseInt(attribString);
            }
            catch (NumberFormatException e) {  }
        }
//logger.info("      DataChannel File: id = " + id);

        // Directory given in config file?
        try {
            directory = attributeMap.get("dir");
//logger.info("      DataChannel File: config file dir = " + directory);
        } catch (Exception e) {
        }

        // Filename given in config file?
        try {
            fileName = attributeMap.get("fileName");
            // scan for %d which must be replaced by the run number
            fileName = fileName.replace("%d", ""+runNumber);
//logger.info("      DataChannel File: config file name = " + fileName);
        } catch (Exception e) {
        }

        // Filename given in config file?
        String prefix = null;
        try {
            prefix = attributeMap.get("prefix");
//logger.info("      DataChannel File: config file prefix = " + prefix);
        } catch (Exception e) {
        }

        // Split parameter given in config file?
        try {
            String splitStr = attributeMap.get("split");
            if (splitStr != null) {
                split = Long.parseLong(splitStr);
//logger.info("      DataChannel File: split = " + split);
            }
        } catch (Exception e) {
        }

        if (fileName == null) {
            if (prefix != null) {
                outputFilePrefix = prefix + "_" + runNumber + ".dat";
                outputFileName = String.format("%s%06d", outputFilePrefix, fileCount);
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

        int capacity = 40;
        queue = new ArrayBlockingQueue<EvioBank>(capacity);

        try {
            if (input) {
logger.info("      DataChannel File: try opening input file of " + fileName);
                evioFile = new EvioReader(fileName);
                dataThread = new Thread(emu.getThreadGroup(), new DataInputHelper(), getName() + " data input");
            } else {
logger.info("      DataChannel File: try opening output file of " + fileName);
                evioFileWriter = new EventWriter(fileName);
                dataThread = new Thread(emu.getThreadGroup(), new DataOutputHelper(), getName() + " data out");
            }

            dataThread.start();

        } catch (Exception e) {
            if (input) {
                throw new DataTransportException("DataChannelImplFile : Cannot open data file " + e.getMessage(), e);
            }
            else {
                throw new DataTransportException("DataChannelImplFile : Cannot create data file" + e.getMessage(), e);
            }
        }

        // TODO: possible race condition, should make sure threads are started before returning
    }


    /**
     * Class <b>DataInputHelper </b>
     * This class reads data from the file and queues it on the fifo.
     * Don't know if this will ever be useful. Might as well generate
     * an END event when file is fully read.
     */
    private class DataInputHelper implements Runnable {

        int recordId = 0;

        public void run() {
            try {
                while (!dataThread.isInterrupted()) {
                    EvioBank bank = evioFile.parseNextEvent();
                    if (bank == null) {
                        break;
                    }

                    queue.put(bank);  // will block
                }

                // Put in END event (not DTR)
                EvioEvent controlEvent = Evio.createControlEvent(EventType.END, 0);
                queue.put(controlEvent);  // will block

//logger.info("      DataChannel File (" + name + "): close file");

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

        public void run() {
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

    /**
     * Class <b>DataOutputHelper </b>
     * Handles sending data.
     */
    private class DataOutputHelperOrig implements Runnable {

        public void run() {
            EvioBank bank, dtrBank;
            Vector<BaseStructure> kids;
            int bankBytes, numKids;
            long numBytesWritten = 0L;
            boolean gotGO = false;

            try {

                while (!dataThread.isInterrupted()) {
                    // This bank is a data transport record (DTR)
                    dtrBank = queue.take(); // will block

                    if (Evio.dtrHasEndEvent(dtrBank)) {
                        evioFileWriter.close();
logger.warn("      DataChannel File (" + name + "): got END, close file " + fileName);
                        return;
                    }
                    else if (Evio.dtrHasGoEvent(dtrBank)) {
//logger.warn("      DataChannel File (" + name + "): got GO");
                        gotGO = true;
                        continue;
                    }
                    else if (Evio.dtrHasControlEvent(dtrBank)) {
//logger.info("      DataChannel File (" + name + "): got control event of type " + Evio.getEventType(dtrBank));
                        continue;
                    }

                    if (!Evio.dtrHasPhysicsEvents(dtrBank)) {
//logger.info("      DataChannel File (" + name + "): got event of type " + Evio.getEventType(dtrBank) +
//                    " but expecting PHYSICS");
                        throw new EmuException("expecting Physics event");
                    }

                    if (!gotGO) {
                        // Got physics event before we got GO event, ignore it
//logger.info("      DataChannel File (" + name + "): got PHYSICS event but NO GO");
                        continue;
                    }

//logger.info("      DataChannel File (" + name + "): got PHYSICS event!");

                    // Dig out the physics events from DTR
                    // and deal with them individually.
                    kids = dtrBank.getChildren();
                    numKids = kids.size();

                    // write all banks except the first one containing record ID
                    for (int i=1; i < numKids; i++) {
                        bank = (EvioBank) kids.get(i);

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
logger.info("      DataChannel File (" + name + "): split, new file = " + fileName);
                            evioFileWriter = new EventWriter(fileName);
                        }

logger.info("      DataChannel File (" + name + "): try writing into file" + fileName);
                        evioFileWriter.writeEvent(bank);
                        numBytesWritten += bankBytes;
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

    public String getName() {
        return name;
    }

    // TODO: return something reasonable
    public int getID() {
        return 0;
    }

    public boolean isInput() {
        return input;
    }

    public DataTransport getDataTransport() {
        return dataTransport;
    }

    /**
     * Method receive ...
     *
     * @return EvioBank containing data
     * @throws InterruptedException on wakeup with no data
     */
    public EvioBank receive() throws InterruptedException {
        return queue.take();
    }

    /**
     * Method send ...
     *
     * @param data in EvioBank format
     */
    public void send(EvioBank data) {
        queue.add(data);
    }

    /** {@inheritDoc} */
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
    public void reset() {
        close();
    }

    /**
     * Method getQueue returns the queue of this DataChannel object.
     *
     * @return the queue (type BlockingQueue<EvioBank>) of this DataChannel object.
     */
    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }

}