package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.jevio.EvioBank;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

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
    private final BlockingQueue<EvioBank> queue;

    /** Is this channel an input (true) or output (false) channel? */
    boolean input;


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
    DataChannelImplFifo(String name, DataTransportImplFifo dataTransport,
                        boolean input, Emu emu) throws DataTransportException {

        this.name = name;
        this.input = input;

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

        queue = new ArrayBlockingQueue<EvioBank>(capacity);

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
     * This method receives or gets EvioBank objects from this object's queue.
     *
     * @return EvioBank object containing data
     * @throws InterruptedException on wakeup of fifo with no data
     */
    public EvioBank receive() throws InterruptedException {
        return queue.take();
    }

    /**
     * {@inheritDoc}
     * EvioBank is sent to this object's queue.
     * @param data {@inheritDoc}
     */
    public void send(EvioBank data) {
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
    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }

}
