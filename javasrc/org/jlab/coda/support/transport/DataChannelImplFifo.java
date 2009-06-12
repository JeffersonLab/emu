package org.jlab.coda.support.transport;

import org.jlab.coda.support.data.DataBank;
import org.jlab.coda.support.logger.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 10, 2008
 * Time: 1:37:43 PM
 * Implementation of a DataChannel using BlockingQueue (FIFO)
 */
@SuppressWarnings({"RedundantThrows"})
public class DataChannelImplFifo implements DataChannel {

    /** Field transport */
    private final DataTransportImplFifo dataTransport;

    /** Field name */
    private final String name;

    /** Field dataThread */
    private Thread dataThread;

    /** Field full - filled buffer queue */
    private final BlockingQueue<DataBank> queue;

    /**
     * Constructor DataChannelImplFifo creates a new DataChannelImplFifo instance.
     *
     * @param name          of type String
     * @param dataTransport of type DataTransport
     * @param input         true if this is an input
     *
     * @throws DataTransportException - unable to create fifo buffer.
     */
    @SuppressWarnings({"UnusedParameters"})
    DataChannelImplFifo(String name, DataTransportImplFifo dataTransport, boolean input) throws DataTransportException {

        this.dataTransport = dataTransport;
        this.name = name;
        int capacity = 40;
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to " + capacity + " records.");
        }

        int size = 20000;
        try {
            size = dataTransport.getIntAttr("size");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to " + size + " byte records.");
        }

        queue = new ArrayBlockingQueue<DataBank>(capacity);

    }

    /** @see org.jlab.coda.support.transport.DataChannel#getName() */
    public String getName() {
        return name;
    }

    /**
     * Method receive ...
     *
     * @return int[]
     *
     * @throws InterruptedException on wakeup of fifo with no data
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

        queue.clear();

    }

    /**
     * Method getFull returns the full of this DataChannel object.
     *
     * @return the full (type BlockingQueue<DataRecord>) of this DataChannel object.
     */
    public BlockingQueue<DataBank> getQueue() {
        return queue;
    }

}
