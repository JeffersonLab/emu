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

import org.jlab.coda.emu.support.data.ControlType;
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
import java.io.StringWriter;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.concurrent.*;
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

    /** Do we pause the dataThread? */
    private boolean pause;

    /** Byte order of output data (input data's order is specified in msg). */
    private ByteOrder byteOrder;

    /** Enforce evio block header numbers to be sequential? */
    private boolean blockNumberChecking;

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
    private Map<String, String> attributeMap;

    /** Is this channel an input (true) or output (false) channel? */
    private boolean input;

    private Logger logger;

    private Emu emu;

    // OUTPUT

    /** Number of writing threads to ask for in copying data from banks to cMsg messages. */
    private int writeThreadCount;

    /** Number of data output helper threads each of which has a pool of writeThreadCount. */
    private int outputThreadCount;

    /** Array of threads used to output data. */
    private DataOutputHelper[] dataOutputThreads;

    /** Order number of next array of cMsg messages (containing
     *  bank lists) to be sent to cMsg server. */
    private int outputOrder;

    /** Order of array of bank lists (to put into array of cMsg messages) taken off Q. */
    private int inputOrder;

    /** Synchronize getting banks off Q for multiple DataOutputHelpers. */
    private Object lockIn  = new Object();

    /** Synchronize sending cMsg messages to cMsg server for multiple DataOutputHelpers. */
    private Object lockOut = new Object();

    /** Place to store a bank off the queue for the next message out. */
    private PayloadBank firstBankFromQueue;

    /**
     * Fill up a message with banks until it reaches this size limit in bytes
     * before another is used. Unless you have one big one which gets sent by itself.
     */
    private int outputSizeLimit = 256000;

    /** Fill up a message with at most this number of banks before another is used. */
    private int outputCountLimit = 100;


// TODO: does this need to be reset? I think so ...
    /** Use the evio block header's block number as a record id. */
    private int recordId;



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
            int evioVersion, sourceId, recordId;
            BlockHeaderV4 header4;
            EventType eventType, bankType;
            ControlType controlType;
            EvioReader reader;


            try {
                reader = new EvioReader(buffer, blockNumberChecking);

                // Speed things up since no EvioListeners are used - doesn't do much
                reader.getParser().setNotificationActive(false);

                IBlockHeader blockHeader = reader.getCurrentBlockHeader();
                evioVersion = blockHeader.getVersion();
                if (evioVersion < 4) {
                    throw new EvioException("Evio data needs to be written in version 4+ format");
                }
                header4     = (BlockHeaderV4)blockHeader;
                eventType   = EventType.getEventType(header4.getEventType());
                controlType = null;
                sourceId    = header4.getReserved1();
                // The recordId associated with each bank is taken from the first
                // evio block header in a single data buffer. For a physics or
                // ROC raw type, it should start at zero and increase by one in the
                // first evio block header of the next data buffer.
                // There may be multiple banks from the same buffer and
                // they will all have the same recordId.
                //
                // Thus, only the first block header # is significant. It is set sequentially
                // by the evWriter object & incremented once per message with physics
                // or ROC data (set to -1 for other data types). Copy it into each bank.
                // Even though many banks will have the same number, it should only
                // increment by one. This should work just fine as all evio events in
                // a single message should always be there (not possible to skip any)
                // since it is transferred all together.
                //
                // When the QFiller thread of the event builder gets a physics or ROC
                // evio event, it checks to make sure this number is in sequence and
                // prints a warning if it isn't.
                recordId = header4.getNumber();

                while ((bank = reader.parseNextEvent()) != null) {
                    // Complication: from the ROC, we'll be receiving USER events
                    // mixed in with and labeled as ROC Raw events. Check for that
                    // and fix it.
                    bankType = eventType;
                    if (eventType == EventType.ROC_RAW) {
                        if (Evio.isUserEvent(bank)) {
                            bankType = EventType.USER;
                        }
                    }
                    else if (eventType == EventType.CONTROL) {
                        // Find out exactly what type of control event it is
                        // (May be null if there is an error)
                        // TODO: It may NOT be enough just to check the tag
                        controlType = ControlType.getControlType(bank.getHeader().getTag());
                        if (controlType == null) {
                            throw new EvioException("Found unidentified control event");
                        }
                    }

                    // Not a real copy, just points to stuff in bank
                    payloadBank = new PayloadBank(bank);
                    // Add vital info from block header.
                    payloadBank.setEventType(bankType);
                    payloadBank.setControlType(controlType);
                    payloadBank.setSourceId(sourceId);
                    payloadBank.setRecordId(recordId);

                    // Put evio bank (payload bank) on Q if it parses
                    queue.put(bank);

                    // Handle end event ...
                    if (controlType == ControlType.END) {
                        // There should be no more events coming down the pike so
                        // go ahead write out existing events and then shut this
                        // thread down.
//logger.info("      DataChannel cMsg : found END event");
                        haveInputEndEvent = true;
                        break;
                    }
                }

                if (haveInputEndEvent) {
                    // TODO: do something?
                }
            }
            catch (EvioException e) {
                // if message data NOT in evio format, skip over it
                logger.error("        DataChannel cMsg : " + name +
                                     " cMsg message data is NOT (latest) evio format, skip");
            }
            catch (IOException e) {
                // if buffer read failure (bad data format ?)
                logger.error("        DataChannel cMsg : " + name +
                                     " cMsg message data is NOT (latest) evio format, skip");
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
            if (capacity < 1) capacity = 40;
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
                logger.info("      DataChannelImplCmsg.const : no output data endianness specified, default to big.");
            }

            // How may cMsg message buffer filling threads for each data output thread?
            writeThreadCount = 1;
            attribString = attributeMap.get("wthreads");
            if (attribString != null) {
                try {
                    writeThreadCount = Integer.parseInt(attribString);
                    if (writeThreadCount <  1) writeThreadCount = 1;
                    if (writeThreadCount > 10) writeThreadCount = 10;
                }
                catch (NumberFormatException e) {}
            }
//logger.info("      DataChannel cMsg : write threads = " + writeThreadCount);

            // How may data writing threads?
            outputThreadCount = 1;
            attribString = attributeMap.get("othreads");
            if (attribString != null) {
                try {
                    outputThreadCount = Integer.parseInt(attribString);
                    if (outputThreadCount <  1) outputThreadCount = 1;
                    if (outputThreadCount > 10) outputThreadCount = 10;
                }
                catch (NumberFormatException e) {}
            }
//logger.info("      DataChannel cMsg : output threads = " + outputThreadCount);


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
     * cMsg.disconnect() is done in the cMsg transport object.
     */
    public void closeOrig() {
        logger.warn("      DataChannelImplCmsg.close : " + name + " - closing this channel");
//        if (dataThread != null) dataThread.interrupt();
        try {
            if (sub != null) {
                dataTransport.getCmsgConnection().unsubscribe(sub);
            }
        } catch (cMsgException e) {
            // ignore
        }
        queue.clear();
    }

    // TODO: make close() end things more gracefully with cMsg server
    /**
     * {@inheritDoc}
     * Kill/close this channel by unsubscribing from cmsg server and ending the data sending thread.
     */
    public void resetOrig() {
        close();
    }




    /** {@inheritDoc} */
    public void close() {
        logger.warn("      DataChannel cMsg close() : " + name + " - closing this channel");

        gotEndCmd = true;
        gotResetCmd = false;

        // Do NOT interrupt threads which are communicating with the cMsg server.
        // This will mess up future communications !!!

        // How long do we wait for each output thread
        // to end before we just terminate them?
        // The total time for an emu to wait for the END transition
        // is emu.endingTimeLimit. Dividing that by the number of
        // output threads is probably a good guess.
        long waitTime;

        // Don't unsubscribe until helper threads are done
        try {
            if (dataOutputThreads != null) {
                waitTime = emu.getEndingTimeLimit() / outputThreadCount;
                for (int i=0; i < outputThreadCount; i++) {
//System.out.println("        try joining output thread #" + i + " for " + (waitTime/1000) + " sec");
                    dataOutputThreads[i].join(waitTime);
                    // kill everything since we waited as long as possible
                    dataOutputThreads[i].interrupt();
                    dataOutputThreads[i].shutdown();
//System.out.println("        out thread done");
                }
            }
//System.out.println("      helper thds done");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        // At this point all threads should be done
        if (sub != null) {
            try {
                dataTransport.getCmsgConnection().unsubscribe(sub);
            } catch (cMsgException e) {/* ignore */}
        }

        queue.clear();
//System.out.println("      close() is done");
    }


    /**
     * {@inheritDoc}
     * Reset this channel by interrupting the message sending threads and unsubscribing.
     */
    public void reset() {
logger.debug("      DataChannel cMsg reset() : " + name + " - resetting this channel");

        gotEndCmd   = false;
        gotResetCmd = true;

        // Don't unsubscribe until helper threads are done
        if (dataOutputThreads != null) {
            for (int i=0; i < outputThreadCount; i++) {
//System.out.println("        interrupt output thread #" + i + " ...");
                dataOutputThreads[i].interrupt();
                dataOutputThreads[i].shutdown();
                // Make sure all threads are done.
                try {dataOutputThreads[i].join(1000);}
                catch (InterruptedException e) {}
//System.out.println("        output thread done");
            }
        }

        // At this point all threads should be done
        if (sub != null) {
            try {
                dataTransport.getCmsgConnection().unsubscribe(sub);
            } catch (cMsgException e) {/* ignore */}
        }

        queue.clear();
//logger.debug("      DataChannel cMsg reset() : " + name + " - done");
    }




    /**
     * <pre>
     * Class <b>DataOutputHelper</b>
     * </pre>
     * Handles sending data.
     */
    private class DataOutputHelperOrig implements Runnable {


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
 // TODO: bug bug !!! need to account for block headers!!!
                    if (buffer.capacity() < size) {
//logger.warn("      DataChannelImplCmsg.DataOutputHelper : increasing buffer size to " + (size + 1000));
                        buffer = ByteBuffer.allocate(size + 1000);
                        buffer.order(byteOrder);
                    }
                    buffer.clear();

                    try {
                        // encode the event type into bits
                        BitSet bitInfo = new BitSet(24);
                        BlockHeaderV4.setEventType(bitInfo, bank.getEventType().getValue());

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
     * Class used to take Evio banks from Q, write them into cMsg messages.
     */
    private class DataOutputHelper extends Thread {

        /** Used to sync things before putting new cMsg messages. */
        private CountDownLatch latch;

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Thread pool for writing Evio banks into new cMsg messages. */
        private ExecutorService writeThreadPool;

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch startLatch = new CountDownLatch(1);



         /** Constructor. */
        DataOutputHelper(ThreadGroup group, String name) {
            super(group, name);

            // Thread pool with "writeThreadCount" number of threads & queue.
            writeThreadPool = Executors.newFixedThreadPool(writeThreadCount);
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
            }
        }


        /** Stop all this object's threads. */
        private void shutdown() {
            // Cancel queued jobs and call interrupt on executing threads
            writeThreadPool.shutdown();

            // Only wait for threads to terminate if shutting
            // down gracefully for an END command.
            if (gotEndCmd) {
                try { writeThreadPool.awaitTermination(100L, TimeUnit.MILLISECONDS); }
                catch (InterruptedException e) {}
            }
        }


        /**
         * This method is used to send an array of cMsg messages to an cMsg server.
         * It allows coordination between multiple DataOutputHelper threads so that
         * event order is preserved.
         *
         * @param msgs           the cMsg messages to send to cMsg server
         * @param inputOrder     the order in which evio events were grabbed off Q
         * @param messages2Write number of messages to write
         *
         * @throws InterruptedException if wait interrupted
         * @throws IOException cMsg communication error
         * @throws cMsgException problems sending message(s) to cMsg server
         */
        private void writeMessages(cMsgMessage[] msgs, int inputOrder, int messages2Write)
                throws InterruptedException, cMsgException {

            if (dataOutputThreads.length > 1) {
                synchronized (lockOut) {
                    // Are the banks we grabbed next to be output? If not, wait.
                    while (inputOrder != outputOrder) {
                        lockOut.wait();
                    }

                    // Send messages to cMsg server
System.out.println("multithreaded put: array len = " + msgs.length + ", send " + messages2Write +
                     " # of messages to cMsg server");

                    // Send all cMsg messages
                    for (cMsgMessage msg : msgs) {
                        dataTransport.getCmsgConnection().send(msg);
                    }

                    // Next one to be put on output channel
                    outputOrder = ++outputOrder % Integer.MAX_VALUE;
                    lockOut.notifyAll();
                }
            }
            else {
System.out.println("singlethreaded put: array len = " + msgs.length + ", send " + messages2Write +
 " # of messages to cMsg server");
                for (cMsgMessage msg : msgs) {
                    dataTransport.getCmsgConnection().send(msg);
                }
            }
        }


        /** {@inheritDoc} */
        @Override
        public void run() {

            // Tell the world I've started
            startLatch.countDown();

            try {
                EventType previousType, pBanktype;
                PayloadBank pBank;
                LinkedList<PayloadBank> bankList;
                boolean gotNothingYet;
                int nextMessageIndex, thisMessageIndex, pBankSize, listTotalSizeMax;
                int messages2Write, myInputOrder;
                int[] recordIds = new int[writeThreadCount];
                int[] bankListSize = new int[writeThreadCount];

                cMsgMessage[] msgs = new cMsgMessage[writeThreadCount];

                // Create an array of lists of PayloadBank objects by 2-step
                // initialization to avoid "generic array creation" error.
                // Create one list for each write thread.
                LinkedList<PayloadBank>[] bankListArray = new LinkedList[writeThreadCount];
                for (int i=0; i < writeThreadCount; i++) {
                    bankListArray[i] = new LinkedList<PayloadBank>();
                    msgs[i] = new cMsgMessage();
                    msgs[i].setSubject(subject);
                    msgs[i].setType(type);
                }


                while ( dataTransport.getCmsgConnection().isConnected() ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) Thread.sleep(5);
                        continue;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        shutdown();
                        return;
                    }

                    // First, clear all the lists of banks we need -
                    // one list for each write thread.
                    for (int j=0; j < writeThreadCount; j++) {
                        bankListArray[j].clear();
                    }

                    // Init variables
                    messages2Write = 0;
                    nextMessageIndex = thisMessageIndex = 0;
                    previousType = null;
                    gotNothingYet = true;
                    listTotalSizeMax = 32;
                    bankList = bankListArray[nextMessageIndex];

                    // If more than 1 output thread, need to sync things
                    if (dataOutputThreads.length > 1) {

                        synchronized (lockIn) {

                            // Because "haveOutputEndEvent" is set true only in this
                            // synchronized code, we can check for it upon entering.
                            // If found already, we can quit.
                            if (haveOutputEndEvent) {
                                shutdown();
                                return;
                            }

                            // Grab a bank to put into a cMsg buffer,
                            // checking occasionally to see if we got an
                            // RESET command or someone found an END event.
                            do {
                                // Get bank off of Q, unless we already did so in a previous loop
                                if (firstBankFromQueue != null) {
                                    pBank = firstBankFromQueue;
                                    firstBankFromQueue = null;
                                }
                                else {
                                    pBank = (PayloadBank) queue.poll(100L, TimeUnit.MILLISECONDS);
                                }

                                // If wait longer than 100ms, and there are things to write,
                                // send them to the cMsg server.
                                if (pBank == null) {
                                    if (gotNothingYet) {
                                        continue;
                                    }
                                    break;
                                }

                                gotNothingYet = false;
                                pBanktype = pBank.getEventType();
                                pBankSize = pBank.getTotalBytes();
                                // Assume worst case of one block-header/bank when finding max size
                                listTotalSizeMax += pBankSize + 32;

                                // This the first time through the while loop
                                if (previousType == null) {
                                    // Add bank to the list since there's always room for one
                                    bankList.add(pBank);

                                    // First time thru loop nextMessageIndex = thisMessageIndex,
                                    // at least until it gets incremented below.
                                    //
                                    // Set recordId depending on what type this bank is
                                    if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
                                        recordIds[thisMessageIndex] = recordId++;
                                    }
                                    else {
                                        recordIds[thisMessageIndex] = -1;
                                    }

                                    // Keep track of list's maximum possible size
                                    bankListSize[thisMessageIndex] = listTotalSizeMax;

                                    // Index of next list
                                    nextMessageIndex++;
                                }
                                // Is this bank a diff type as previous bank?
                                // Will it not fit into the target size per message?
                                // Will it be > the target number of banks per message?
                                // In all these cases start using a new list.
                                else if ((previousType != pBanktype) ||
                                        (listTotalSizeMax >= outputSizeLimit) ||
                                        (bankList.size() + 1 > outputCountLimit)) {

                                    // Store final value of previous list's maximum possible size
                                    bankListSize[thisMessageIndex] = listTotalSizeMax - pBankSize - 32;

//                                    if (listTotalSizeMax >= outputSizeLimit) {
//                                        System.out.println("LIST IS TOO BIG, start another");
//                                    }
//                                    if (bankList.size() + 1 > outputCountLimit) {
//                                        System.out.println("LIST HAS TOO MANY entries, start another");
//                                    }

                                    // If we've already used up the max number of messages,
                                    // write things out first. Be sure to store what we just
                                    // pulled off the Q to be the next bank!
                                    if (nextMessageIndex >= writeThreadCount) {
//                                        System.out.println("Already used " +
//                                                            nextMessageIndex + " messages for " + writeThreadCount +
//                                                            " write threads, store bank for next round");
                                        firstBankFromQueue = pBank;
                                        break;
                                    }

                                    // Get new list
                                    bankList = bankListArray[nextMessageIndex];
                                    // Add bank to new list
                                    bankList.add(pBank);
                                    // Size of new list (64 -> take ending header into account)
                                    bankListSize[nextMessageIndex] = listTotalSizeMax = pBankSize + 64;

                                    // Set recordId depending on what type this bank is
                                    if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
                                        recordIds[nextMessageIndex] = recordId++;
                                    }
                                    else {
                                        recordIds[nextMessageIndex] = -1;
                                    }

                                    // Index of this & next lists
                                    thisMessageIndex++;
                                    nextMessageIndex++;
                                }
                                // It's OK to add this bank to the existing list.
                                else {
                                    // Add bank to list since there's room and it's the right type
                                    bankList.add(pBank);
                                    // Keep track of list's maximum possible size
                                    bankListSize[thisMessageIndex] = listTotalSizeMax;
                                }

                                // Look for END event and mark it in attachment
                                if (Evio.isEndEvent(pBank)) {
                                    pBank.setAttachment(Boolean.TRUE);
                                    haveOutputEndEvent = true;
                                    break;
                                }

                                // Set this for next round
                                previousType = pBanktype;
                                pBank.setAttachment(Boolean.FALSE);

                            } while (!gotResetCmd && (thisMessageIndex < writeThreadCount));

                            // If I've been told to RESET ...
                            if (gotResetCmd) {
                                shutdown();
                                return;
                            }

                            myInputOrder = inputOrder;
                            inputOrder = ++inputOrder % Integer.MAX_VALUE;
                        }
                    }
                    else {

                       do {
                            if (firstBankFromQueue != null) {
                                pBank = firstBankFromQueue;
                                firstBankFromQueue = null;
                            }
                            else {
                                pBank = (PayloadBank) queue.poll(100L, TimeUnit.MILLISECONDS);
                            }

                            if (pBank == null) {
                                if (gotNothingYet) {
                                    continue;
                                }
                                break;
                            }

                            gotNothingYet = false;
                            pBanktype = pBank.getEventType();
                            pBankSize = pBank.getTotalBytes();
                            listTotalSizeMax += pBankSize + 32;

                            if (previousType == null) {
                                bankList.add(pBank);

                                if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
                                    recordIds[thisMessageIndex] = recordId++;
                                }
                                else {
                                    recordIds[thisMessageIndex] = -1;
                                }

                                bankListSize[thisMessageIndex] = listTotalSizeMax;

                                nextMessageIndex++;
                            }
                            else if ((previousType != pBanktype) ||
                                    (listTotalSizeMax >= outputSizeLimit) ||
                                    (bankList.size() + 1 > outputCountLimit)) {

                                bankListSize[thisMessageIndex] = listTotalSizeMax - pBankSize - 32;

                                if (nextMessageIndex >= writeThreadCount) {
                                   firstBankFromQueue = pBank;
                                    break;
                                }

                                bankList = bankListArray[nextMessageIndex];
                                bankList.add(pBank);
                                bankListSize[nextMessageIndex] = listTotalSizeMax = pBankSize + 64;

                                if (pBanktype.isAnyPhysics() || pBanktype.isROCRaw()) {
                                    recordIds[nextMessageIndex] = recordId++;
                                }
                                else {
                                    recordIds[nextMessageIndex] = -1;
                                }

                                thisMessageIndex++;
                                nextMessageIndex++;
                            }
                            else {
                                bankList.add(pBank);
                                bankListSize[thisMessageIndex] = listTotalSizeMax;
                            }

                            if (Evio.isEndEvent(pBank)) {
                                pBank.setAttachment(Boolean.TRUE);
                                haveOutputEndEvent = true;
                                break;
                            }

                            previousType = pBanktype;
                            pBank.setAttachment(Boolean.FALSE);

                        } while (!gotResetCmd && (thisMessageIndex < writeThreadCount));

                        if (gotResetCmd) {
                            shutdown();
                            return;
                        }

                        myInputOrder = inputOrder;
                        inputOrder = ++inputOrder % Integer.MAX_VALUE;
                    }

                    if (nextMessageIndex > 1) {
                        latch = new CountDownLatch(nextMessageIndex);
                    }

                    // For each cMsg message that can be filled with something ...
                    for (int i=0; i < nextMessageIndex; i++) {
                        // Get one of the list of banks to put into this cMsg message
                        bankList = bankListArray[i];

                        if (bankList.size() < 1) {
                            continue;
                        }

                        // Write banks' data into cMsg message in separate thread
                        EvWriter writer = new EvWriter(bankList, msgs[i],
                                                       bankListSize[i], recordIds[i]);
                        writeThreadPool.execute(writer);

                        // Keep track of how many messages we want to write
                        messages2Write++;

                        // Handle END event ...
                        for (PayloadBank bank : bankList) {
                            if (bank.getAttachment() == Boolean.TRUE) {
                                // There should be no more events coming down the pike so
                                // go ahead write out events and then shut this thread down.
                                break;
                            }
                        }
                    }

                    // Wait for all events to finish processing
                    if (nextMessageIndex > 1) {
                        latch.await();
                    }

                    // Write cMsg messages after gathering them all
System.out.println("      DataChannel cMsg: write " + messages2Write + " messages");
                    writeMessages(msgs, myInputOrder, messages2Write);


                    if (haveOutputEndEvent) {
System.out.println("Ending");
                        shutdown();
                        return;
                    }
                }

            } catch (InterruptedException e) {
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("      DataChannel cMsg DataOutputHelper : exit " + e.getMessage());
            }

        }

        /**
         * This class is designed to write an evio bank's
         * contents into a cMsg message by way of a thread pool.
         */
        private class EvWriter implements Runnable {

            /** List of evio banks to write. */
            private LinkedList<PayloadBank> bankList;

            /** cMsg message's data buffer. */
            private ByteBuffer  buffer;

            /** Object for writing banks into message's data buffer. */
            private EventWriter evWriter;

            /** Message to send with bank data inside. */
            private cMsgMessage msg;


            /**
             * Encode the event type into the bit info word
             * which will be in each evio block header.
             *
             * @param bSet bit set which will become part of the bit info word
             * @param type event type to be encoded
             */
            void setEventType(BitSet bSet, int type) {
                // check args
                if (type < 0) type = 0;
                else if (type > 15) type = 15;

                if (bSet == null || bSet.size() < 6) {
                    return;
                }
                // do the encoding
                for (int i=2; i < 6; i++) {
                    bSet.set(i, ((type >>> i - 2) & 0x1) > 0);
                }
            }


            /**
             * Constructor.
             * @param bankList list of banks to be written into a single cMsg message
             * @param msg cMsg message in which to write the list of banks
             * @param bankByteSize total size of the banks in bytes <b>including block headers</b>
             * @param myRecordId value of starting block header's block number
             */
            EvWriter(LinkedList<PayloadBank> bankList, cMsgMessage msg,
                     int bankByteSize, int myRecordId) {

                this.msg = msg;
                this.bankList = bankList;

                // Need to account for block headers + a little extra just in case
                buffer = ByteBuffer.allocate(bankByteSize);
                buffer.order(byteOrder);

                // Encode the event type into bits
                BitSet bitInfo = new BitSet(24);
                setEventType(bitInfo, bankList.getFirst().getEventType().getValue());

                try {
                    // Create object to write evio banks into message buffer
                    evWriter = new EventWriter(buffer, 550000, 200, null, bitInfo, emu.getCodaid());
                    evWriter.setStartingBlockNumber(myRecordId);
                }
                catch (EvioException e) {e.printStackTrace();/* never happen */}
            }


            /**
             * {@inheritDoc}<p>
             * Write bank into cMsg message buffer.
             */
            public void run() {
                try {
                    // Write banks into message buffer
                    for (PayloadBank bank : bankList) {
                        evWriter.writeEvent(bank);
                    }
                    evWriter.close();
                    buffer.flip();

                    // Put data into cMsg message
                    msg.setByteArrayNoCopy(buffer.array(), 0, buffer.limit());
                    msg.setByteArrayEndian(byteOrder == ByteOrder.BIG_ENDIAN ? cMsgConstants.endianBig :
                                                                               cMsgConstants.endianLittle);
                    // Tell the DataOutputHelper thread that we're done
                    latch.countDown();
                }
                catch (EvioException e) {
                    e.printStackTrace();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * Start the startOutputHelper thread which takes a bank from
     * the queue, puts it in a message, and sends it.
     */
    public void startOutputHelper() {
        dataOutputThreads = new DataOutputHelper[outputThreadCount];

        for (int i=0; i < outputThreadCount; i++) {
            DataOutputHelper helper = new DataOutputHelper(emu.getThreadGroup(),
                                                           getName() + " data out" + i);
            dataOutputThreads[i] = helper;
            dataOutputThreads[i].start();
            helper.waitUntilStarted();
        }
    }

    /**
     * Pause the startOutputHelper thread which takes a bank from
     * the queue, puts it in a message, and sends it.
     */
    public void pauseOutputHelper() {
        pause = true;
    }

    /**
     * Resume the startOutputHelper thread which takes a bank from
     * the queue, puts it in a message, and sends it.
     */
    public void resumeOutputHelper() {
        pause = false;
    }

    public BlockingQueue<EvioBank> getQueue() {
        return queue;
    }

}
