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

package org.jlab.coda.support.transport;

import org.jlab.coda.emu.EMUComponentImpl;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.evio.DataRecord;
import org.jlab.coda.support.log.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * -----------------------------------------------------
 * Copyright (c) 2008 Jefferson lab data acquisition group
 * Class DataChannelImplSO ...
 *
 * @author heyes
 *         Created on Sep 12, 2008
 */
public class DataChannelImplCMsg implements DataChannel {

    /** Field transport */
    private final TransportImplCMsg transport;

    /** Field name */
    private final String name;

    /** Field dataSocket */
    private Socket dataSocket;

    /** Field dataThread */
    private Thread dataThread;

    /** Field size - the default size of the buffers used to receive data */
    private int size = 20000;

    /** Field capacity - number of records that will fit in the fifos */
    private int capacity = 40;

    /** Field full - filled buffer queue */
    private final BlockingQueue<DataRecord> full;

    /** Field empty - empty buffer queue */
    private final BlockingQueue<DataRecord> empty;

    /** Field out */
    private DataOutputStream out;

    /** Field in */
    private DataInputStream in;

    /**
     * Constructor DataChannelImplSO creates a new DataChannelImplSO instance.
     *
     * @param name      of type String
     * @param transport of type DataTransport
     * @throws TransportException - unable to create buffers or socket.
     */
    DataChannelImplCMsg(String name, TransportImplCMsg transport) throws TransportException {

        this.transport = transport;
        this.name = name;

        try {
            capacity = transport.getIntAttr("capacity");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to " + capacity + " records.");
        }

        try {
            size = transport.getIntAttr("size");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to " + size + " byte records.");
        }

        full = new ArrayBlockingQueue<DataRecord>(capacity);
        empty = new ArrayBlockingQueue<DataRecord>(capacity);

        // Fill the queue with records of default size
        while (empty.remainingCapacity() > 0) {
            try {
                empty.put(new DataRecord(size));
            } catch (InterruptedException e) {
                throw new TransportException("DataChannelImplCMsg :can't fill the pool of empty buffers ", e);
            }
        }

        // If we are a server the accept helper in the transport implementation will
        // handle connections
        if (!transport.isServer()) {

            try {
                dataSocket = new Socket(transport.getHost(), transport.getPort());

                dataSocket.setTcpNoDelay(true);

                out = new DataOutputStream(dataSocket.getOutputStream());
                in = new DataInputStream(dataSocket.getInputStream());
                // Always write a 4 byte length followed by data
                out.write(name.length());
                out.write(name.getBytes());

                startOutputHelper();

            } catch (Exception e) {
                throw new TransportException("DataChannelImplCMsg : Cannot create data channel", e);
            }
        }

    }

    /** @see org.jlab.coda.support.transport.DataChannel#getName() */
    public String getName() {
        return name;
    }

    /**
     * Method receive ...
     *
     * @return int[]
     */
    public int[] receive() {
        return transport.receive(this);
    }

    /**
     * Method send ...
     *
     * @param data of type long[]
     */
    public void send(long[] data) {
        transport.send(this, data);
    }

    /** Method close ... */
    public void close() {
        transport.closeChannel(this);
    }

    /**
     * Method getDataSocket returns the dataSocket of this DataChannelImplSO object.
     *
     * @return the dataSocket (type Socket) of this DataChannelImplSO object.
     */
    public Socket getDataSocket() {
        return dataSocket;
    }

    /**
     * Method setDataSocket sets the dataSocket of this DataChannelImplSO object.
     *
     * @param incoming the dataSocket of this DataChannelImplSO object.
     * @throws TransportException - the socket closed while we were in here, unlikely.
     */
    public void setDataSocket(Socket incoming) throws TransportException {
        this.dataSocket = incoming;
        try {
            out = new DataOutputStream(dataSocket.getOutputStream());
            in = new DataInputStream(dataSocket.getInputStream());
        } catch (Exception e) {
            throw new TransportException("setDataSocket failed ", e);
        }
    }

    /**
     * <pre>
     * Class <b>DataInputHelper </b>
     * This class reads data from the socket and queues it on the fifo.
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

                while (dataSocket.isConnected()) {
                    // take an empty buffer
                    DataRecord dr = empty.take();

                    in.readFully(dr.getData(), 0, 4);

                    int rl = dr.getRecordLength();
                    int bl = dr.getBufferLength();

                    // rl is the count of long words in the buffer.
                    // the buffer needs to be (rl+1)*4 bytes long
                    // the extra 4 bytes hold the length
                    if ((rl + 1) * 4 > bl) {
                        // new data buffer 10% bigger than needed
                        dr = new DataRecord((rl + 1 + rl / 10) * 4);
                        // put back the length
                        dr.setRecordLength(rl);
                    }

                    // read in the payload, remember to offset 4 bytes so
                    // we don't overwrite the length
                    in.readFully(dr.getData(), 4, rl * 4);

                    out.write(0xaa);
                    full.put(dr);
                }
                Logger.warn(name + " - data socket disconnected");
            } catch (Exception e) {
                Logger.warn("DataInputHelper exit " + e.getMessage());
                e.printStackTrace();
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
                DataRecord d;

                while (dataSocket.isConnected()) {
                    d = full.take();

                    out.write(d.getData(), 0, 4);
                    out.write(d.getData(), 4, d.getRecordLength() * 4);
                    int ack = in.read();
                    if (ack != 0xaa) {
                        throw new CmdExecException("DataOutputHelper : ack = " + ack);
                    }
                    d.setRecordLength(0);
                    empty.put(d);
                }
                Logger.warn(name + " - data socket disconnected");
            } catch (Exception e) {
                Logger.warn("DataOutputHelper exit " + e.getMessage());
            }

        }

    }

    /** Method startInputHelper ... */
    public void startInputHelper() {
        dataThread = new Thread(EMUComponentImpl.THREAD_GROUP, new DataInputHelper(), getName() + " data input");

        dataThread.start();
    }

    /** Method startOutputHelper ... */
    public void startOutputHelper() {
        dataThread = new Thread(EMUComponentImpl.THREAD_GROUP, new DataOutputHelper(), getName() + " data out");

        dataThread.start();
    }

    /**
     * Method getFull returns the full of this DataChannel object.
     *
     * @return the full (type BlockingQueue<DataRecord>) of this DataChannel object.
     */
    public BlockingQueue<DataRecord> getFull() {
        return full;
    }

    /**
     * Method getEmpty returns the empty of this DataChannel object.
     *
     * @return the empty (type BlockingQueue<DataRecord>) of this DataChannel object.
     */
    public BlockingQueue<DataRecord> getEmpty() {
        return empty;
    }
}