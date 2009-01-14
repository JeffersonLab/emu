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

import org.jlab.coda.emu.Emu;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.evio.DataTransportRecord;
import org.jlab.coda.support.logger.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
    private final DataTransportImplCMsg dataTransport;

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
    private final BlockingQueue<DataTransportRecord> queue;

    /** Field out */
    private DataOutputStream out;

    /** Field in */
    private DataInputStream in;

    private boolean isInput = false;

    /**
     * Constructor DataChannelImplSO creates a new DataChannelImplSO instance.
     *
     * @param name          of type String
     * @param dataTransport of type DataTransport
     * @param input
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplCMsg(String name, DataTransportImplCMsg dataTransport, boolean input) throws DataTransportException {

        this.dataTransport = dataTransport;
        this.name = name;
        this.isInput = input;
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to " + capacity + " records.");
        }

        try {
            size = dataTransport.getIntAttr("size");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to " + size + " byte records.");
        }

        queue = new ArrayBlockingQueue<DataTransportRecord>(capacity);

        // If we are a server the accept helper in the dataTransport implementation will
        // handle connections
        if (!isInput) {
            try {
                dataSocket = new Socket(dataTransport.getHost(), dataTransport.getPort());

                dataSocket.setTcpNoDelay(true);

                out = new DataOutputStream(dataSocket.getOutputStream());
                in = new DataInputStream(dataSocket.getInputStream());
                // Always write a 1 byte length followed by data
                out.writeInt(0xC0DA2008);
                out.write(name.length());
                out.write(name.getBytes());

                startOutputHelper();

            } catch (Exception e) {
                throw new DataTransportException("DataChannelImplCMsg : Cannot create data channel", e);
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
    public DataTransportRecord receive() throws InterruptedException {
        return dataTransport.receive(this);
    }

    /**
     * Method send ...
     *
     * @param data of type long[]
     */
    public void send(DataTransportRecord data) {
        dataTransport.send(this, data);
    }

    /** Method close ... */
    public void close() {
        if (dataThread != null) dataThread.interrupt();
        try {
            if (dataSocket != null) dataSocket.close();
        } catch (IOException e) {
            // ignore
        }
        queue.clear();

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
     * @throws DataTransportException - the socket closed while we were in here, unlikely.
     */
    public void setDataSocket(Socket incoming) throws DataTransportException {
        this.dataSocket = incoming;
        try {
            out = new DataOutputStream(dataSocket.getOutputStream());
            in = new DataInputStream(dataSocket.getInputStream());
            Logger.info("socket : " + dataSocket + " associated with channel " + name);
        } catch (Exception e) {
            throw new DataTransportException("setDataSocket failed ", e);
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

                    int length = in.readInt();

                    DataTransportRecord dr = new DataTransportRecord(length * 4 + 8);
                    // setr the length
                    dr.setLength(length);

                    // read in the payload, remember to offset 4 bytes so
                    // we don't overwrite the length
                    in.readFully(dr.getData(), 4, length * 4);

                    // Send ack
                    out.write(0xaa);
                    queue.put(dr);
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
                DataTransportRecord d;

                while (dataSocket.isConnected()) {
                    d = queue.take();
                    System.out.println("Block------------");
                    d.dumpHeader();
                    out.write(d.getData(), 0, (d.length() + 1) * 4);
                    int ack = in.read();
                    if (ack != 0xaa) {
                        throw new CmdExecException("DataOutputHelper : ack = " + ack);
                    }
                    d.setLength(0);

                }
                Logger.warn(name + " - data socket disconnected");
            } catch (Exception e) {
                Logger.warn("DataOutputHelper exit " + e.getMessage());
            }

        }

    }

    /** Method startInputHelper ... */
    public void startInputHelper() {
        dataThread = new Thread(Emu.THREAD_GROUP, new DataInputHelper(), getName() + " data input");

        dataThread.start();
    }

    /** Method startOutputHelper ... */
    public void startOutputHelper() {
        dataThread = new Thread(Emu.THREAD_GROUP, new DataOutputHelper(), getName() + " data out");

        dataThread.start();
    }

    /**
     * Method getFull returns the full of this DataChannel object.
     *
     * @return the full (type BlockingQueue<DataRecord>) of this DataChannel object.
     */
    public BlockingQueue<DataTransportRecord> getQueue() {
        return queue;
    }

}