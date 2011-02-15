package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.jevio.EvioBank;
import org.jlab.coda.jevio.EvioException;
import org.jlab.coda.jevio.EvioReader;
import org.jlab.coda.jevio.EventWriter;

import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 10, 2008
 * Time: 1:37:43 PM
 * Implementation of a DataChannel reading/writing from/to a file in EVIO format.
 */
public class DataChannelImplFile implements DataChannel {

    /** Field transport */
    private final DataTransportImplFile dataTransport;

    /** Field name */
    private final String name;

    /** Field dataThread */
    private Thread dataThread;

    /** Field size - the default size of the buffers used to receive data */
    private int size = 20000;

    /** Field queue - filled buffer queue */
    private final BlockingQueue<EvioBank> queue;

    private File file;

    /** Evio data file. */
    private EvioReader evioFile;

    /** Object to write evio file. */
    private EventWriter evioFileWriter;

    /** Is this channel an input (true) or output (false) channel? */
    boolean input;


    /**
     * Constructor DataChannelImplFifo creates a new DataChannelImplFifo instance.
     *
     * @param name          of type String
     * @param dataTransport of type DataTransport
     * @param input         true if this is an input
     *
     * @throws org.jlab.coda.emu.support.transport.DataTransportException
     *          - unable to create fifo buffer.
     */
    DataChannelImplFile(String name, DataTransportImplFile dataTransport, boolean input) throws DataTransportException {

        this.dataTransport = dataTransport;
        this.input = input;
        this.name = name;

        String fileName = "dataFile.coda";
        try {
            fileName = dataTransport.getAttr("filename");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to file name" + fileName);
        }

        int capacity = 40;
        queue = new ArrayBlockingQueue<EvioBank>(capacity);

        try {
            if (input) {
                evioFile = new EvioReader(fileName);
                dataThread = new Thread(Emu.THREAD_GROUP, new DataInputHelper(), getName() + " data input");
            } else {
                evioFileWriter = new EventWriter(fileName);
                dataThread = new Thread(Emu.THREAD_GROUP, new DataOutputHelper(), getName() + " data out");
            }
            dataThread.start();
        } catch (Exception e) {
            if (input) throw new DataTransportException("DataChannelImplFile : Cannot open data file " + e.getMessage(), e);
            else throw new DataTransportException("DataChannelImplFile : Cannot create data file" + e.getMessage(), e);
        }
    }

    /**
     * <pre>
     * Class <b>DataInputHelper </b>
     * This class reads data from the file and queues it on the fifo.
     * It checks that the data buffer is large enough and allocates a
     * bigger buffer if needed.
     * <p/>
     * TODO : the acknowledge written is fixed at 0xaa, it should be some feedback to the sender
     * </pre>
     */
    private class DataInputHelper implements Runnable {

        /** Method run ... */
        public void run() {
            try {
                Logger.info("Data Input helper for File");
                while (!dataThread.isInterrupted()) {
                    EvioBank bank = evioFile.parseNextEvent();
                    if (bank == null) {
                        break;
                    }
                    queue.put(bank);
                }
                Logger.warn(name + " - File closed");
            } catch (Exception e) {
                Logger.warn("DataInputHelper exit " + e.getMessage());
                Logger.warn(name + " - File closed");
                CODAState.ERROR.getCauses().add(e);
                dataTransport.state = CODAState.ERROR;
            }

        }

    }

    /**
     * <pre>
     * Class <b>DataOutputHelper </b>
     * </pre>
     * Handles sending data.
     * TODO : the ack should be some feedback from the receiver.
     */
    private class DataOutputHelper implements Runnable {

        /** Method run ... */
        public void run() {
            try {
                EvioBank bank;
                Logger.info("Data Output helper for File");
                while (!dataThread.isInterrupted()) {
                    bank = queue.take();
                    evioFileWriter.writeEvent(bank);
                }
                Logger.warn(name + " - data file closed");
            } catch (Exception e) {
                Logger.warn("DataOutputHelper exit " + e.getMessage());
                CODAState.ERROR.getCauses().add(e);
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

    /** Method close ... */
    public void close() {
        if (dataThread != null) dataThread.interrupt();
        try {

            if (evioFile != null) evioFile.close();
        } catch (Exception e) {
            //ignore
        }
        queue.clear();

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