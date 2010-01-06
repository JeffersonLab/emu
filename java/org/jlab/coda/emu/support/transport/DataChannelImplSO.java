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

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.jevio.EvioBank;
import org.jlab.coda.jevio.ByteParser;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.Map;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * TODO: This class still needs testing.
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
    private final BlockingQueue<EvioBank> queue;

    /** Byte order of either input or output stream. */
    ByteOrder byteOrder;

    /**
     * Constructor DataChannelImplSO creates a new DataChannelImplSO instance.
     *
     * @param name          the name of this channel
     * @param transport     the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplSO(String name, DataTransportImplSO transport,
                      Map <String, String> attributeMap, boolean input) {

        this.name = name;
        this.transport = transport;

        // set queue capacity
        int capacity = 40;
        try {
            capacity = transport.getIntAttr("capacity");
        } catch (Exception e) {
            Logger.info(e.getMessage() + " default to " + capacity + " records.");
        }
        queue = new ArrayBlockingQueue<EvioBank>(capacity);

        // set endianness of data
         byteOrder = ByteOrder.BIG_ENDIAN;
         try {
             String order = attributeMap.get("endian");
             if (order != null && order.equalsIgnoreCase("little")) {
                 byteOrder = ByteOrder.LITTLE_ENDIAN;
             }
         } catch (Exception e) {
             Logger.info("      DataChannelImplCmsgSock.const : no data endianness specifed, default to big.");
         }

        if (input) {
            startInputHelper();
        }
        else {
            startOutputHelper();
        }

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
                OutputStream    os = dataSocket.getOutputStream();
                ByteParser  parser = new ByteParser();

                while (dataSocket.isConnected()) {
                    EvioBank bank = parser.readEvent(in, byteOrder);

                    os.write(0xaa); // no need to swap for 1 byte
                    queue.put(bank);
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
                int size;
                EvioBank bank;
                ByteBuffer bbuf = ByteBuffer.allocate(1000); // allocateDirect does(may) NOT have backing array
                bbuf.order(byteOrder);
                DataOutputStream out = new DataOutputStream(dataSocket.getOutputStream());
                InputStream in = dataSocket.getInputStream();

                while (true) {
                    bank = queue.take();

                    size = bank.getTotalBytes();  // bytes
                    if (bbuf.capacity() < size) {
                        bbuf = ByteBuffer.allocateDirect(size + 1000);
                        bbuf.order(byteOrder);
                    }
                    bbuf.clear();
                    bank.write(bbuf);
                    out.write(bbuf.array());
                    out.flush();

                    int ack = in.read(); // no need to swap for 1 byte
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

    /** {@inheritDoc} */
    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }

    /** {@inheritDoc} */
    public EvioBank receive() throws InterruptedException {
        return queue.take();
    }

    /** {@inheritDoc} */
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

}
