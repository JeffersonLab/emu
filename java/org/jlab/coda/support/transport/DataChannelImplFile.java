package org.jlab.coda.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.support.codaComponent.CODAState;
import org.jlab.coda.support.data.DataBank;
import org.jlab.coda.support.data.DataFile;
import org.jlab.coda.support.logger.Logger;

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

    /** Field full - filled buffer queue */
    private final BlockingQueue<DataBank> queue;

    private File file;

    /** Field out */
    private DataFile dataFile = null;

    /**
     * Constructor DataChannelImplFifo creates a new DataChannelImplFifo instance.
     *
     * @param name          of type String
     * @param dataTransport of type DataTransport
     * @param input         true if this is an input
     *
     * @throws org.jlab.coda.support.transport.DataTransportException
     *          - unable to create fifo buffer.
     */
    DataChannelImplFile(String name, DataTransportImplFile dataTransport, boolean input) throws DataTransportException {

        this.dataTransport = dataTransport;
        this.name = name;
        String fileName = "dataFile.coda";
        try {
            fileName = dataTransport.getAttr("filename");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to file name" + fileName);
        }

        int capacity = 40;
        queue = new ArrayBlockingQueue<DataBank>(capacity);

        try {
            if (input) {
                dataFile = new DataFile(new DataInputStream(new FileInputStream(fileName)));
                dataThread = new Thread(Emu.THREAD_GROUP, new DataInputHelper(), getName() + " data input");

                dataThread.start();

            } else {

                dataFile = new DataFile(new DataOutputStream(new FileOutputStream(fileName)));
                dataThread = new Thread(Emu.THREAD_GROUP, new DataOutputHelper(), getName() + " data out");

                dataThread.start();

            }
        } catch (Exception e) {
            if (input) throw new DataTransportException("DataChannelImplCMsg : Cannot open data file " + e.getMessage(), e);
            else throw new DataTransportException("DataChannelImplCMsg : Cannot create data file" + e.getMessage(), e);
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

                    DataBank dr = dataFile.read();

                    queue.put(dr);
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
                DataBank d;
                Logger.info("Data Output helper for File");
                while (!dataThread.isInterrupted()) {
                    d = queue.take();

                    dataFile.write(d);

                }
                Logger.warn(name + " - data file closed");
            } catch (Exception e) {
                Logger.warn("DataOutputHelper exit " + e.getMessage());
                CODAState.ERROR.getCauses().add(e);
                dataTransport.state = CODAState.ERROR;
            }

        }

    }

    /** @see DataChannel#getName() */
    public String getName() {
        return name;
    }

    /**
     * Method receive ...
     *
     * @return int[]
     *
     * @throws InterruptedException on wakeup with no data
     */
    public DataBank receive() throws InterruptedException {
        return dataTransport.receive(this);
    }

    /**
     * Method send ...
     *
     * @param data of type long[]
     */
    public void send(DataBank data) {
        dataTransport.send(this, data);
    }

    /** Method close ... */
    public void close() {
        if (dataThread != null) dataThread.interrupt();
        try {

            if (dataFile != null) dataFile.close();
        } catch (Exception e) {
            //ignore
        }
        queue.clear();

    }

    /**
     * Method getFull returns the full of this DataChannel object.
     *
     * @return the full (type BlockingQueue<DataBank>) of this DataChannel object.
     */
    public BlockingQueue<DataBank> getQueue() {
        return queue;
    }

}