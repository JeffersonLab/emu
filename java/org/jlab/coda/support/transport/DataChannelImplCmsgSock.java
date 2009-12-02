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
import org.jlab.coda.jevio.EvioBank;
import org.jlab.coda.jevio.ByteParser;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.nio.ByteBuffer;

/**
 * Implementation of the DataChannel interface using cMsg
 * as the communication protocol.
 *
 * @author heyes
 *         Created on Sep 12, 2008
 */
@SuppressWarnings({"WeakerAccess"})
public class DataChannelImplCmsgSock implements DataChannel {

    /** Field transport */
    private final DataTransportImplCmsgSock dataTransport;

    /** Field name */
    private final String name;

    /** Field dataSocket */
    private Socket dataSocket;

    /** Field dataThread */
    private Thread dataThread;

    /** Field queue - filled buffer queue */
    private final BlockingQueue<EvioBank> queue;

    /** Field out */
    private DataOutputStream out;

    /** Field in */
    private DataInputStream in;

    /**
     * Constructor to create a new DataChannelImplCmsg instance.
     * Used only by {@link DataTransportImplCmsgSock#createChannel} which is
     * only used during PRESTART in the EmuModuleFactory.
     * 
     * @param name          the name of this channel
     * @param dataTransport the DataTransport object that this channel belongs to
     * @param input         true if this is an input data channel, otherwise false
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplCmsgSock(String name, DataTransportImplCmsgSock dataTransport, boolean input) throws DataTransportException {

        this.dataTransport = dataTransport;
        this.name = name;
        int capacity = 40;
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
            Logger.info("      DataChannelImplCmsgSock.const : " +  e.getMessage() + ", default to " + capacity + " records.");
        }

        int size = 20000;
        try {
            size = dataTransport.getIntAttr("size");
        } catch (Exception e) {
            Logger.info("      DataChannelImplCmsgSock.const : " + e.getMessage() + ", default to " + size + " byte records.");
        }

        queue = new ArrayBlockingQueue<EvioBank>(capacity);

        // If we are a server (data receiver) the AcceptHelper thread in
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
                throw new DataTransportException("      DataChannelImplCmsgSock : cannot create data channel", e);
            }
        }

    }

    /** @see org.jlab.coda.support.transport.DataChannel#getName() */
    public String getName() {
        return name;
    }

    /**
     * Take a EvioBank off the queue.
     *
     * @return data
     * @throws InterruptedException on wakeup of fifo without data.
     */
    public EvioBank receive() throws InterruptedException {
        return queue.take();
    }

    /**
     * Add a EvBank to the queue.
     *
     * @param data is the bank to send
     */
    public void send(EvioBank data) {
        queue.add(data);
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
            Logger.info("      DataChannelImplCmsgSock.setDataSocket : " + dataSocket + " associated with channel " + name);
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
                ByteParser parser = new ByteParser();
                while (dataSocket.isConnected()) {
                    EvioBank bank = parser.readEvent(in);

                    // Send ack
                    out.write(0xaa);
                    queue.put(bank);
                }
                Logger.warn("      DataChannelImplCmsgSock.DataInputHelper : " + name + " - data socket disconnected");
            } catch (Exception e) {
                Logger.warn("      DataChannelImplCmsgSock.DataInputHelper : exit " + e.getMessage());
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
                int size;
                EvioBank bank;
                ByteBuffer bbuf = ByteBuffer.allocate(1000); // allocateDirect does(may) NOT have backing array

                while (dataSocket.isConnected()) {
                    bank = queue.take();

                    // TODO: make buffer handling more efficient
                    size = bank.getTotalBytes();  // bytes
                    if (bbuf.capacity() < size) {
                        bbuf = ByteBuffer.allocateDirect(size + 1000);
                    }
                    bbuf.clear();
                    bank.write(bbuf);
                    out.write(bbuf.array());
                    out.flush();

                    int ack = in.read();
                    if (ack != 0xaa) {
                        throw new CmdExecException("DataOutputHelper : ack = " + ack);
                    }

                }
                Logger.warn("      DataChannelImplCmsgSock.DataInputHelper : " + name + " - data socket disconnected");
            } catch (Exception e) {
                e.printStackTrace();
System.out.println("      DataChannelImplCmsgSock.DataOutputHelper : exit " + e.getMessage());
                Logger.warn("      DataChannelImplCmsgSock.DataOutputHelper : exit " + e.getMessage());
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
     * Method getQueue returns the queue of this DataChannel object.
     *
     * @return the queue (type BlockingQueue<EvioBank>) of this DataChannel object.
     */
    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }

}