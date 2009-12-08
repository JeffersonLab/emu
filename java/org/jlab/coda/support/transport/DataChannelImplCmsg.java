/*
 * Copyright (c) 2009, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.support.transport;

import org.jlab.coda.jevio.EvioBank;
import org.jlab.coda.jevio.ByteParser;
import org.jlab.coda.jevio.EvioException;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.cMsg.cMsgSubscriptionHandle;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgCallbackAdapter;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.emu.Emu;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.Map;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author timmer
 * @Date Dec 2, 2009
 */
public class DataChannelImplCmsg implements DataChannel {

    /** Field transport */
    private final DataTransportImplCmsg dataTransport;

    /** Field name */
    private final String name;

    /** Field queue - filled buffer queue */
    private final BlockingQueue<EvioBank> queue;

    /** Field dataThread */
    private Thread dataThread;

    /** Object for parsing evio data contained in incoming messages. */
    private ByteParser parser;

    /** cMsg subscription for receiving messages with data. */
    private cMsgSubscriptionHandle sub;

    /** Map of config file attributes. */
    Map<String, String> attributeMap;

    /**
     * This class defines the callback to be run when a message matching the subscription arrives.
     */
    class ReceiveMsgCallback extends cMsgCallbackAdapter {
        /**
         * Callback method definition.
         *
         * @param msg message received from domain server
         * @param userObject object passed as an argument which was set when the
         *                   client orginally subscribed to a subject and type of
         *                   message.
         */
        public void callback(cMsgMessage msg, Object userObject) {
            byte[] data = msg.getByteArray();
            if (data == null) return;

            try {
                EvioBank bank = parser.parseEvent(data, ByteOrder.BIG_ENDIAN);
                queue.put(bank);
            }
            catch (EvioException e) {
                e.printStackTrace();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Define "getMaximumCueSize" to set max number of unprocessed messages kept locally
        // before things "back up" (potentially slowing or stopping senders of messages of
        // this subject and type). Default = 1000.
    }

    /**
     * Constructor to create a new DataChannelImplCmsg instance.
     * Used only by {@link DataTransportImplCmsg#createChannel} which is
     * only used during PRESTART in the EmuModuleFactory.
     *
     * @param name          the name of this channel
     * @param dataTransport the DataTransport object that this channel belongs to
     * @param input         true if this is an input data channel, otherwise false
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplCmsg(String name, DataTransportImplCmsg dataTransport, Map<String, String> attributeMap, boolean input)
            throws DataTransportException {

        this.dataTransport = dataTransport;
        this.attributeMap  = attributeMap;
        this.name = name;

        int capacity = 40;
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
            Logger.info("      DataChannelImplCmsg.const : " +  e.getMessage() + ", default to " + capacity + " records.");
        }
        queue = new ArrayBlockingQueue<EvioBank>(capacity);


        if (input) {
            try {
                // create subscription for receiving messages containing data

                // first use subject & type defined in config file; if none, use defaults
                String subject = attributeMap.get("subject");
                if (subject == null) subject = name;
                String type = attributeMap.get("type");
                if (type == null) type = "data";
                
                ReceiveMsgCallback cb = new ReceiveMsgCallback();
                sub = dataTransport.getCmsgConnection().subscribe(subject, type, cb, null);
            }
            catch (cMsgException e) {
                Logger.info("      DataChannelImplCmsg.const : " + e.getMessage());
                throw new DataTransportException(e);
            }
            parser = new ByteParser();
        }
        else {
            startOutputHelper();
        }
    }

    /** {@inheritDoc} */
    public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    public EvioBank receive() throws InterruptedException {
        return queue.take();
    }

    /** {@inheritDoc} */
     public void send(EvioBank bank) {
        //queue.add(bank);   // throws exception if capacity reached
        //queue.offer(bank); // returns false if capacity reached
        try {
            queue.put(bank); // blocks if capacity reached
        }
        catch (InterruptedException e) {
            // ignore
        }
    }

    /**
     * {@inheritDoc}
     * Close this channel by unsubscribing from cmsg server and ending the data sending thread.
     */
    public void close() {
        if (dataThread != null) dataThread.interrupt();
        try {
            if (sub != null) {
                dataTransport.getCmsgConnection().unsubscribe(sub);
            }
        } catch (cMsgException e) {
            // ignore
        }
        queue.clear();
    }


    /**
     * <pre>
     * Class <b>DataOutputHelper</b>
     * </pre>
     * Handles sending data.
     */
    private class DataOutputHelper implements Runnable {

        /** Method run ... */
        public void run() {
            try {
                int size;
                EvioBank bank;
                cMsgMessage msg = new cMsgMessage();
                ByteBuffer buffer = ByteBuffer.allocate(1000); // allocateDirect does(may) NOT have backing array

                while ( dataTransport.getCmsgConnection().isConnected()) {
                    bank = queue.take();  // blocks

                    size = bank.getTotalBytes();
                    if (buffer.capacity() < size) {
                        buffer = ByteBuffer.allocateDirect(size + 1000);
                    }
                    buffer.clear();
                    bank.write(buffer);

                    // put data into cmsg message
                    msg.setByteArrayNoCopy(buffer.array());
                    // TODO: take care of byte order

                    // send it
                    dataTransport.getCmsgConnection().send(msg);
                }

                Logger.warn("      DataChannelImplCmsg.DataInputHelper : " + name + " - disconnected from cmsg server");

            } catch (Exception e) {
                e.printStackTrace();
                Logger.warn("      DataChannelImplCmsg.DataOutputHelper : exit " + e.getMessage());
            }
        }

    }

    /** Method startOutputHelper ... */
    public void startOutputHelper() {
        dataThread = new Thread(Emu.THREAD_GROUP, new DataOutputHelper(), getName() + " data out");
        dataThread.start();
    }

    /**
     * Method getQueue returns the queue of this DataChannel object.
     *
     * @return the queue (type BlockingQueue&lt;EvioBank&gt;) of this DataChannel object.
     */
    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }

}
