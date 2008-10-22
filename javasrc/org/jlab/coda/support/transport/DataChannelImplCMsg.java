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

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
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

    /** Field size */
    private final int size = 20000;

    /** Field full */
    private final BlockingQueue<DataRecord> full;

    /** Field empty */
    private final BlockingQueue<DataRecord> empty;

    /**
     * Constructor DataChannelImplSO creates a new DataChannelImplSO instance.
     *
     * @param pname     of type String
     * @param transport of type DataTransport
     */
    DataChannelImplCMsg(String pname, TransportImplCMsg transport) {
        System.out.println("DataChannelImplCMsg : " + pname);
        this.transport = transport;
        name = pname;

        String capacityS = "40";
        String tmp = transport.getAttr("pool");
        if (capacityS != null) capacityS = tmp;

        int capacity = Integer.valueOf(capacityS);

        full = new ArrayBlockingQueue<DataRecord>(capacity);
        empty = new ArrayBlockingQueue<DataRecord>(capacity);

        for (int ix = 0; ix < capacity; ix++) {
            DataRecord d = new DataRecord(size);

            try {
                empty.put(d);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (transport.isServer()) {

        } else {
            try {
                System.out.println("Create data socket : " + transport.getHost() + " " + transport.getPort() + " " + transport);

                dataSocket = new Socket(transport.getHost(), transport.getPort());

                dataSocket.setTcpNoDelay(true);
                OutputStream out = dataSocket.getOutputStream();
                DataOutputStream dout = new DataOutputStream(out);

                System.out.println("Send name over data socket : " + name);
                dout.write(name.length());

                dout.write(name.getBytes());

                startOutputHelper();

            } catch (SocketException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
     */
    public void setDataSocket(Socket incoming) {
        this.dataSocket = incoming;
    }

    private class DataInputHelper implements Runnable {

        /** Method run ... */
        @SuppressWarnings({"InfiniteLoopStatement"})
        public void run() {
            try {

                DataInputStream in = new DataInputStream(dataSocket
                        .getInputStream());

                OutputStream os = dataSocket.getOutputStream();

                while (dataSocket.isConnected()) {
                    // take an empty buffer
                    DataRecord dr = empty.take();

                    in.readFully(dr.getData(), 0, 4);

                    int rl = dr.getRecordLength();
                    int bl = dr.getBufferLength();
                    System.out.println("Recieved length : " + rl + " buffer is " + bl);
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
                    System.out.println("got all data, send ack");
                    os.write(0xaa);
                    full.put(dr);
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

                DataOutputStream out = new DataOutputStream(dataSocket
                        .getOutputStream());

                DataRecord d;
                int len;
                int ack;
                InputStream is = dataSocket.getInputStream();

                while (true) {
                    d = full.take();
                    len = d.getRecordLength();
                    System.out.println("DataSocket " + dataSocket + " open = " + dataSocket.isConnected() + " length " + d.getData());
                    out.write(d.getData(), 0, 4);
                    out.write(d.getData(), 4, len * 4);
                    ack = is.read();
                    if (ack != 0xaa) {
                        throw new CmdExecException("DataOutputHelper : ack = " + ack);
                    }
                    d.setRecordLength(0);
                    empty.put(d);
                }
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