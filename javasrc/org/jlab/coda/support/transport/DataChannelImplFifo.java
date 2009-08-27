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

    /** Field name */
    private final String name;

    /** bug bug: what is this??? Field dataThread */
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

    /**
     * {@inheritDoc}
     * @return {@inheritDoc}
     */
    public String getName() {
        return name;
    }

    /**
     * This method receives or gets DataBank objects from this object's queue.
     *
     * @return DataBank object containing int[]
     * @throws InterruptedException on wakeup of fifo with no data
     */
    public DataBank receive() throws InterruptedException {
        return queue.take();
    }

    /**
     * {@inheritDoc}
     * DataBank is sent to this object's queue
     * @param data {@inheritDoc} -- containing long[]
     */
    public void send(DataBank data) {
        queue.add(data);
    }

    /** {@inheritDoc} */
    public void close() {
        if (dataThread != null) dataThread.interrupt();
        queue.clear();
    }

    /**
     * {@inheritDoc}
     * @return {@inheritDoc}
     */
    public BlockingQueue<DataBank> getQueue() {
        return queue;
    }

}
