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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Implementation of the DataChannel interface using cMsg
 * as the communication protocol.
 *
 * @author heyes
 *         Created on Sep 12, 2008
 */
@SuppressWarnings({"WeakerAccess"})
public class DataChannelImplCMsg implements DataChannel {

    /** Field transport */
    private final DataTransportImplCMsg dataTransport;

    /** Field name */
    private final String name;

    /** Field dataSocket */
    private Socket dataSocket;

    /** Field dataThread */
    private Thread dataThread;

    /** Field full - filled buffer queue */
    private final BlockingQueue<DataBank> queue;

    /** Field out */
    private DataOutputStream out;

    /** Field in */
    private DataInputStream in;

    /**
     * Constructor to create a new DataChannelImplCmsg instance.
     * Used only by {@link DataTransportImplCMsg#createChannel} which is
     * only used during PRESTART in the EmuModuleFactory.
     * 
     * bug bug : Why are we creating a socket here? Shouldn't we be using cMsg??
     *
     * @param name          the name of this channel
     * @param dataTransport the DataTransport object that this channel belongs to
     * @param input         true if this is an input data channel, otherwise false
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplCMsg(String name, DataTransportImplCMsg dataTransport, boolean input) throws DataTransportException {

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

        // If we are a server (data sender) the AcceptHelper thread in
        // the dataTransport implementation will handle connections and
        // create the dataSocket. Otherwise, run code below.
        if (!input) {
            try {
                dataSocket = new Socket(dataTransport.getHost(), dataTransport.getPort());

                dataSocket.setTcpNoDelay(true);
                // bug bug : channel should be null
                //dataSocket.getChannel();
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
     * Take a DataBank off the queue.
     *
     * @return int[]
     *
     * @throws InterruptedException on wakeup of fifo without data.
     */
    public DataBank receive() throws InterruptedException {
        // bug bug why not do: queue.take(); ?  why invoke the transport object?
        return dataTransport.receive(this);
    }

    /**
     * Add a DataBank to the queue.
     *
     * @param data is the bank to send
     */
    public void send(DataBank data) {
        // bug bug why not do: queue.add(data); ?  why invoke the transport object?
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
     *
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

                    DataTransportRecord dr = (DataTransportRecord) DataTransportRecord.read(in);

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
                DataBank d;

                while (dataSocket.isConnected()) {
                    d = queue.take();

                    DataBank.write(out, d);
                    int ack = in.read();
                    if (ack != 0xaa) {
                        throw new CmdExecException("DataOutputHelper : ack = " + ack);
                    }

                }
                Logger.warn(name + " - data socket disconnected");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("DataOutputHelper exit " + e.getMessage());
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

}