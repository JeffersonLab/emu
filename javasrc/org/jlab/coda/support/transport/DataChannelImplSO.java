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
import org.jlab.coda.support.data.DataBank;
import org.jlab.coda.support.data.DataTransportRecord;
import org.jlab.coda.support.logger.Logger;

import java.io.*;
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
public class DataChannelImplSO implements DataChannel {

    /** Field transport */
    private final DataTransport transport;

    /** Field name */
    private final String name;

    /** Field dataSocket */
    private Socket dataSocket;

    /** Field dataThread */
    private Thread dataThread;

    /** Field full */
    private final BlockingQueue<DataBank> queue;

    /**
     * Constructor DataChannelImplSO creates a new DataChannelImplSO instance.
     *
     * @param pname of type String
     * @param ti    of type DataTransport
     */
    DataChannelImplSO(String pname, DataTransport ti) {

        transport = ti;
        name = pname;

        int capacity = 40;
        try {
            capacity = transport.getIntAttr("capacity");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to " + capacity + " records.");
        }

        int size = 20000;
        try {
            size = transport.getIntAttr("size");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to " + size + " byte records.");
        }

        queue = new ArrayBlockingQueue<DataBank>(capacity);

    }

    /** @see DataChannel#getName() */
    public String getName() {
        return name;
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
     */
    public void setDataSocket(Socket incoming) {
        this.dataSocket = incoming;
    }

    private class DataInputHelper implements Runnable {

        /** Method run ... */
        @SuppressWarnings({"InfiniteLoopStatement"})
        public void run() {
            try {

                DataInputStream in = new DataInputStream(dataSocket.getInputStream());

                OutputStream os = dataSocket.getOutputStream();

                while (dataSocket.isConnected()) {
                    // take an empty buffer

                    DataTransportRecord dr = (DataTransportRecord) DataBank.read(in);

                    os.write(0xaa);
                    queue.put(dr);
                }
            } catch (Exception e) {
                Logger.warn("DataInputHelper exit " + e.getMessage());

            }

        }

    }

    private class DataOutputHelper implements Runnable {

        /** Method run ... */
        @SuppressWarnings({"InfiniteLoopStatement"})
        public void run() {
            try {

                DataOutputStream out = new DataOutputStream(dataSocket.getOutputStream());

                DataBank d;
                int ack;
                InputStream is = dataSocket.getInputStream();

                while (true) {
                    d = queue.take();

                    DataBank.write(out, d);

                    ack = is.read();
                    if (ack != 0xaa) {
                        throw new CmdExecException("DataOutputHelper : ack = " + ack);
                    }

                }
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
    public BlockingQueue<DataBank> getQueue() {
        return queue;
    }

    /**
     * Method send ...
     *
     * @param data of type long[]
     */
    public void send(DataBank data) {
        transport.send(this, data);
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

}
