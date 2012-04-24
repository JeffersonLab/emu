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

import org.jlab.coda.emu.support.data.EventType;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.emu.support.data.PayloadBank;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.jevio.*;


import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.BitSet;
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
    private EvioReader parser;

    /** Byte order of output data (input data's order is specified in msg). */
    ByteOrder byteOrder;

    /** Enforce evio block header numbers to be sequential? */
    boolean blockNumberChecking;

    /** Read END event from input queue. */
    private volatile boolean haveInputEndEvent;

    /** Read END event from output queue. */
    private volatile boolean haveOutputEndEvent;

    /** Got END command from Run Control. */
    private volatile boolean gotEndCmd;

    /** Got RESET command from Run Control. */
    private volatile boolean gotResetCmd;

    /** cMsg subscription for receiving messages with data. */
    private cMsgSubscriptionHandle sub;

    /** Map of config file attributes. */
    Map<String, String> attributeMap;

    /** Is this channel an input (true) or output (false) channel? */
    boolean input;

    private Logger logger;

    private Emu emu;


    /**
     * This class defines the callback to be run when a message matching the subscription arrives.
     */
    class ReceiveMsgCallback extends cMsgCallbackAdapter {
        /**
         * Callback method definition.
         *
         * @param msg message received from domain server
         * @param userObject object passed as an argument which was set when the
         *                   client originally subscribed to a subject and type of
         *                   message.
         */
        public void callback(cMsgMessage msg, Object userObject) {
//System.out.println("cmsg data channel " + name + ": got message in callback");
            byte[] data = msg.getByteArray();
            if (data == null) {
                System.out.println("cmsg data channel " + name + ": ain't got no data!!!");
                return;
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);

            EvioBank bank;
            PayloadBank payloadBank;
            int evioVersion, payloadCount, sourceId;
            BlockHeaderV4 header4;
            EventType  type;
            EvioReader reader;


            try {
                reader = new EvioReader(buffer);

                // Have reader throw an exception if evio
                // block numbers are not sequential.
                if (blockNumberChecking) reader.checkBlockNumberSequence(true);

                // Speed things up since no EvioListeners are used - doesn't do much
                reader.getParser().setNotificationActive(false);

                IBlockHeader blockHeader = reader.getCurrentBlockHeader();
                evioVersion = blockHeader.getVersion();
                if (evioVersion < 4) {
                    throw new EvioException("Evio data needs to be written in version 4+ format");
                }
                header4      = (BlockHeaderV4)blockHeader;
                type         = EventType.getEventType(header4.getEventType());
                sourceId     = header4.getReserved1();
                payloadCount = header4.getEventCount();

                while ((bank = reader.parseNextEvent()) != null) {
                    if (payloadCount < 1) {
                        throw new EvioException("Evio header inconsistency");
                    }

                    // Not a real copy, just points to stuff in bank
                    payloadBank = new PayloadBank(bank);
                    // Add vital info from block header.
                    payloadBank.setRecordId(blockHeader.getNumber());
                    payloadBank.setType(type);
                    payloadBank.setSourceId(sourceId);

                    // Put evio bank (payload bank) on Q if it parses
                    queue.put(bank);

                    // Handle end event ...
                    if (Evio.isEndEvent(bank)) {
                        // There should be no more events coming down the pike so
                        // go ahead write out existing events and then shut this
                        // thread down.
//logger.info("      DataChannel cMsg : found END event");
                        haveInputEndEvent = true;
                        break;
                    }

                    payloadCount--;
                }

                if (haveInputEndEvent) {
                    // TODO: do something!!!
                }
            }
            catch (EvioException e) {
                // if ET event data NOT in evio format, skip over it
                logger.error("        DataChannel Et : " + name +
                                     " ET event data is NOT (latest) evio format, skip");
            }
            catch (IOException e) {
                // if buffer read failure (bad data format ?)
                logger.error("        DataChannel Et : " + name +
                                     " ET event data is NOT (latest) evio format, skip");
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }




//            try {
////                ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
////
////                if (msg.getByteArrayEndian() == cMsgConstants.endianLittle) {
////                    byteOrder = ByteOrder.LITTLE_ENDIAN;
////                }
////
////                buffer = ByteBuffer.wrap(data).order(byteOrder);
//                buffer = ByteBuffer.wrap(data);
//                parser = new EvioReader(buffer);
//                EvioBank bank = parser.parseNextEvent();
////System.out.println("cmsg data channel ("+ name +"): got bank over cmsg, try putting into channel Q");
//                queue.put(bank);
////System.out.println("cmsg data channel: put into channel Q");
//
////                System.out.println("\nReceiving msg:\n" + bank.toString());
////
////                ByteBuffer bbuf = ByteBuffer.allocate(1000);
////                bbuf.clear();
////                bank.write(bbuf);
////
////                StringWriter sw2 = new StringWriter(1000);
////                XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
////                bank.toXML(xmlWriter);
////                System.out.println("Receiving msg:\n" + sw2.toString());
////                bbuf.flip();
////
////                System.out.println("Receiving msg (bin):");
////                sw2.getBuffer().delete(0, sw2.getBuffer().capacity());
////                PrintWriter wr = new PrintWriter(sw2);
////                while (bbuf.hasRemaining()) {
////                    wr.printf("%#010x\n", bbuf.getInt());
////                }
////                System.out.println(sw2.toString() + "\n\n");
////            }
////            catch (XMLStreamException e) {
////                e.printStackTrace();
//            }
//            catch (IOException e) {
//                e.printStackTrace();
//            }
//            catch (EvioException e) {
//                e.printStackTrace();
//            }
//            catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//

        }

        // Define "getMaximumCueSize" to set max number of unprocessed messages kept locally
        // before things "back up" (potentially slowing or stopping senders of messages of
        // this subject and type). Default = 1000.
    }

    /**
     * Constructor to create a new DataChannelImplCmsg instance. Used only by
     * {@link DataTransportImplCmsg#createChannel(String, Map, boolean, Emu)}
     * which is only used during PRESTART in the EmuModuleFactory.
     *
     * @param name          the name of this channel
     * @param dataTransport the DataTransport object that this channel belongs to
     * @param attributeMap  the hashmap of config file attributes for this channel
     * @param input         true if this is an input data channel, otherwise false
     * @param emu           emu this channel belongs to
     *
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplCmsg(String name, DataTransportImplCmsg dataTransport,
                        Map<String, String> attributeMap, boolean input,
                        Emu emu)
            throws DataTransportException {

        this.dataTransport = dataTransport;
        this.attributeMap  = attributeMap;
        this.input = input;
        this.name = name;
        this.emu = emu;
        logger = emu.getLogger();

        // set option whether or not to enforce evio
        // block header numbers to be sequential
        String attribString = attributeMap.get("blockNumCheck");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("true") ||
                attribString.equalsIgnoreCase("on")   ||
                attribString.equalsIgnoreCase("yes"))   {
                blockNumberChecking = true;
            }
        }

        // set queue capacity
        int capacity = 40;
        try {
            capacity = dataTransport.getIntAttr("capacity");
        } catch (Exception e) {
            logger.info("      DataChannelImplCmsg.const : " +  e.getMessage() + ", default to " + capacity + " records.");
        }
        queue = new ArrayBlockingQueue<EvioBank>(capacity);

        // Set subject & type for either subscription (incoming msgs) or for outgoing msgs.
        // Use any defined in config file else use defaults.
        subject = attributeMap.get("subject");
        if (subject == null) subject = name;

        type = attributeMap.get("type");
        if (type == null) type = "data";
System.out.println("\n\nDataChannel: subscribe to subject = " + subject + ", type = " + type + "\n\n");
        
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
                logger.info("      DataChannelImplCmsg.const : " + e.getMessage());
                throw new DataTransportException(e);
            }
        }
        else {
            // Tell emu what that output name is for stat reporting
            emu.setOutputDestination("cMsg");

            // set endianness of data
            byteOrder = ByteOrder.BIG_ENDIAN;
            try {
                String order = attributeMap.get("endian");
                if (order != null && order.equalsIgnoreCase("little")) {
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                }
            } catch (Exception e) {
                logger.info("      DataChannelImplCmsg.const : no output data endianness specifed, default to big.");
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

    public boolean isInput() {
        return input;
    }

    public DataTransport getDataTransport() {
        return dataTransport;
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
     * Method to print out the bank for diagnostic purposes.
     * @param bank bank to print out
     * @param bankName name of bank for printout
     */
    private void printBank(EvioBank bank, String bankName) {
        try {
            StringWriter sw2 = new StringWriter(1000);
            XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
            bank.toXML(xmlWriter);
            if (bankName == null) {
                System.out.println("bank:\n" + sw2.toString());
            }
            else {
                System.out.println("bank " + bankName + ":\n" + sw2.toString());
            }
        }
        catch (XMLStreamException e) {
            e.printStackTrace();
        }
    }

    /**
     * {@inheritDoc}
     * Close this channel by unsubscribing from cmsg server and ending the data sending thread.
     */
    public void close() {
        logger.warn("      DataChannelImplCmsg.close : " + name + " - closing this channel");
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

    // TODO: make close end things more gracefully with cMsg server
    /**
     * {@inheritDoc}
     * Kill/close this channel by unsubscribing from cmsg server and ending the data sending thread.
     */
    public void reset() {
        close();
    }


    /**
     * <pre>
     * Class <b>DataOutputHelper</b>
     * </pre>
     * Handles sending data.
     */
    private class DataOutputHelper implements Runnable {


        public void run() {
            try {
                int size;
                PayloadBank bank;
                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(subject);
                msg.setType(type);
                // TODO: set the proper size of the buffer later ...
                ByteBuffer buffer = ByteBuffer.allocate(2048); // allocateDirect does(may) NOT have backing array
                // by default ByteBuffer is big endian
                buffer.order(byteOrder);
                EventWriter evWriter = null;

                while ( dataTransport.getCmsgConnection().isConnected() ) {

                    if (pause) {
//logger.warn("      DataChannelImplCmsg.DataOutputHelper : " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    bank = (PayloadBank)queue.take();  // blocks

                    size = bank.getTotalBytes();
                    if (buffer.capacity() < size) {
//logger.warn("      DataChannelImplCmsg.DataOutputHelper : increasing buffer size to " + (size + 1000));
                        buffer = ByteBuffer.allocate(size + 1000);
                        buffer.order(byteOrder);
                    }
                    buffer.clear();

                    try {
                        // encode the event type into bits
                        BitSet bitInfo = new BitSet(24);
                        BlockHeaderV4.setEventType(bitInfo, bank.getType().getValue());

                        evWriter = new EventWriter(buffer, 128000, 10, null, bitInfo, emu.getCodaid());
                    }
                    catch (EvioException e) {e.printStackTrace();/* never happen */}

                    evWriter.writeEvent(bank);
                    evWriter.close();
                    buffer.flip();

                    // put data into cmsg message
                    msg.setByteArrayNoCopy(buffer.array(), 0, buffer.limit());
                    msg.setByteArrayEndian(byteOrder == ByteOrder.BIG_ENDIAN ? cMsgConstants.endianBig :
                                                                               cMsgConstants.endianLittle);
                    dataTransport.getCmsgConnection().send(msg);
                }

                logger.warn("      DataChannelImplCmsg.DataOutputHelper : " + name + " - disconnected from cmsg server");

            } catch (InterruptedException e) {
                logger.warn("      DataChannelImplCmsg.DataOutputHelper : interrupted, exiting");
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("      DataChannelImplCmsg.DataOutputHelper : exit " + e.getMessage());
            }
        }

    }

    /**
     * Start the startOutputHelper thread which takes a bank from
     * the queue, puts it in a message, and sends it.
     */
    public void startOutputHelper() {
        dataThread = new Thread(emu.getThreadGroup(), new DataOutputHelper(), getName() + " data out");
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
