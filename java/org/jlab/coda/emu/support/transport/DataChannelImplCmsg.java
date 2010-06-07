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

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.jevio.EvioBank;
import org.jlab.coda.jevio.EvioException;
import org.jlab.coda.jevio.ByteParser;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.Map;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author timmer
 * Dec 2, 2009
 */
public class DataChannelImplCmsg implements DataChannel {

    /** Field transport */
    private final DataTransportImplCmsg dataTransport;

    /** Field name */
    private final String name;

    /** ID of this channel (corresponds to sourceId of ROCs for CODA event building). */
    private int id;

    /** Subject of either subscription or outgoing messages. */
    private String subject;

    /** Type of either subscription or outgoing messages. */
    private String type;

    /** Field queue - filled buffer queue */
    private final BlockingQueue<EvioBank> queue;

    /** Field dataThread */
    private Thread dataThread;

    /** Do we pause the dataThread? */
    private boolean pause;

    /** Object for parsing evio data contained in incoming messages. */
    private ByteParser parser;

    /** Byte order of output data (input data's order is specified in msg). */
    ByteOrder byteOrder;

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
System.out.println("cmsg data channel " + name + ": got message in callback");
            byte[] data = msg.getByteArray();
            if (data == null) return;

            try {
                ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

                if (msg.getByteArrayEndian() == cMsgConstants.endianLittle) {
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                }

                EvioBank bank = parser.parseEvent(data, byteOrder);
                queue.put(bank);

//                System.out.println("\nReceiving msg:\n" + bank.toString());
//
//                ByteBuffer bbuf = ByteBuffer.allocate(1000);
//                bbuf.clear();
//                bank.write(bbuf);
//
//                StringWriter sw2 = new StringWriter(1000);
//                XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
//                bank.toXML(xmlWriter);
//                System.out.println("Receiving msg:\n" + sw2.toString());
//                bbuf.flip();

//                System.out.println("Receiving msg (bin):");
//                sw2.getBuffer().delete(0, sw2.getBuffer().capacity());
//                PrintWriter wr = new PrintWriter(sw2);
//                while (bbuf.hasRemaining()) {
//                    wr.printf("%#010x\n", bbuf.getInt());
//                }
//                System.out.println(sw2.toString() + "\n\n");
//            }
//            catch (XMLStreamException e) {
//                e.printStackTrace();
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
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplCmsg(String name, DataTransportImplCmsg dataTransport,
                        Map<String, String> attributeMap, boolean input)
            throws DataTransportException {

        this.dataTransport = dataTransport;
        this.attributeMap  = attributeMap;
        this.name = name;

        // set queue capacity
        int capacity = 40;
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
            Logger.info("      DataChannelImplCmsg.const : " +  e.getMessage() + ", default to " + capacity + " records.");
        }
        queue = new ArrayBlockingQueue<EvioBank>(capacity);

        // Set subject & type for either subscription (incoming msgs) or for outgoing msgs.
        // Use any defined in config file else use defaults.
        subject = attributeMap.get("subject");
        if (subject == null) subject = name;

        type = attributeMap.get("type");
        if (type == null) type = "data";
//System.out.println("\n\nDataChannel: subject = " + subject + ", type = " + type + "\n\n");
        
        // Set id number. Use any defined in config file else use default (0)
        id = 0;
        String idVal = attributeMap.get("id");
        if (idVal != null) {
            try {
                id = Integer.parseInt(idVal);
            }
            catch (NumberFormatException e) {  }
        }

        if (input) {
            try {
                // create subscription for receiving messages containing data
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
            // set endianness of data
            byteOrder = ByteOrder.BIG_ENDIAN;
            try {
                String order = attributeMap.get("endian");
                if (order != null && order.equalsIgnoreCase("little")) {
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                }
            } catch (Exception e) {
                Logger.info("      DataChannelImplCmsg.const : no output data endianness specifed, default to big.");
            }

            startOutputHelper();
        }
    }

    public String getName() {
        return name;
    }

    public int getID() {
        return id;
    }

    public EvioBank receive() throws InterruptedException {
        return queue.take();
    }

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
        Logger.warn("      DataChannelImplCmsg.close : " + name + " - closing this channel");
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
                msg.setSubject(subject);
                msg.setType(type);
                ByteBuffer buffer = ByteBuffer.allocate(1000); // allocateDirect does(may) NOT have backing array
                // by default ByteBuffer is big endian
                buffer.order(byteOrder);

                while ( dataTransport.getCmsgConnection().isConnected() ) {

                    if (pause) {
//Logger.warn("      DataChannelImplCmsg.DataOutputHelper : " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    bank = queue.take();  // blocks

                    size = bank.getTotalBytes();
                    if (buffer.capacity() < size) {
                        buffer = ByteBuffer.allocateDirect(size + 1000);
                        buffer.order(byteOrder);
                    }
                    buffer.clear();
                    bank.write(buffer);

                    // put data into cmsg message
                    msg.setByteArrayNoCopy(buffer.array());
                    // TODO: set byte array length ?
                    msg.setByteArrayEndian(byteOrder == ByteOrder.BIG_ENDIAN ? cMsgConstants.endianBig :
                                                                               cMsgConstants.endianLittle);

                    // send it
                    dataTransport.getCmsgConnection().send(msg);
                }

                Logger.warn("      DataChannelImplCmsg.DataOutputHelper : " + name + " - disconnected from cmsg server");

            } catch (InterruptedException e) {
                Logger.warn("      DataChannelImplCmsg.DataOutputHelper : interrupted, exiting");
            } catch (Exception e) {
                e.printStackTrace();
                Logger.warn("      DataChannelImplCmsg.DataOutputHelper : exit " + e.getMessage());
            }
        }

    }

    /**
     * Start the startOutputHelper thread which takes a bank from
     * the queue, puts it in a message, and sends it.
     */
    public void startOutputHelper() {
        dataThread = new Thread(Emu.THREAD_GROUP, new DataOutputHelper(), getName() + " data out");
        dataThread.start();
    }

    /**
     * Pause the startOutputHelper thread which takes a bank from
     * the queue, puts it in a message, and sends it.
     */
    public void pauseOutputHelper() {
        if (dataThread == null) return;
        pause = true;
    }

    /**
     * Resume the startOutputHelper thread which takes a bank from
     * the queue, puts it in a message, and sends it.
     */
    public void resumeOutputHelper() {
        if (dataThread == null) return;
        pause = false;
    }

    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }

}
