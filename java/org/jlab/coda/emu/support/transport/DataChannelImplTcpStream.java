/*
 * Copyright (c) 2022, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.transport;


import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.SpinCountBackoffWaitStrategy;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.cMsg.common.cMsgMessageFull;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.*;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * <p>
 * This class is copied from the DataChannelImpEmu class.
 * It main purpose is to read streaming format data over TCP from VTPs.
 * One way that it differs from the original class is that there is no
 * "fat pipe" or multiple socket option for one stream.
 * This is not necessary for the VTP which can send 4 individual streams
 * to get the desired bandwidth.
 * Some old code may be left in place in order to use the EmuDomainTcpServer
 * for convenience.</p>
 *
 *
 * To preferentially use IPv6, give following command line option to JVM:
 * -Djava.net.preferIPv6Addresses=true
 * Java sockets use either IPv4 or 6 automatically depending on what
 * kind of addresses are used. No need (or possibility) for setting
 * this explicitly.
 *
 * @author timmer
 * (8/8/2022)
 */
public class DataChannelImplTcpStream extends DataChannelAdapter {

    /** Data transport subclass object for Emu. */
    private final DataTransportImplTcpStream dataTransportImplTcpStream;

    /** Do we pause the dataThread? */
    private volatile boolean pause;

    /** Read END event from input ring. */
    private volatile boolean haveInputEndEvent;

    /**
     * If true, dump incoming data immediately so it does not get put on
     * the ring but does get parsed. Use this for testing incoming data rate.
     */
    private boolean dumpData;

    // OUTPUT

    /** Thread used to output data. */
    private DataOutputHelper dataOutputThread;

    /** UDP port of emu domain server. */
    private int sendPort;

    /** TCP send buffer size in bytes. */
    private int tcpSendBuf;

    /** TCP no delay setting. */
    private boolean noDelay;

    /** Time in seconds to wait for connection to emu server. */
    private int connectTimeout;

    /** Subnet client uses to connect to server if possible. */
    private String preferredSubnet;

    /** List of IP addresses to connect to ordered by: 1) on preferred subnet,
     *  2) on local subnet, 3) everything else. */
    private List<String> orderedIpAddrs;

    /** Coda id of the data source. */
    private int sourceId;

    /** Connection to emu domain server. */
    private cMsg emuDomain;

    // INPUT

    /**
     * Store locally whether this channel's module is an ER or not.
     * If so, don't parse incoming data so deeply - only top bank header.
     */
    private boolean isER;

    /** Threads used to read incoming data. */
    private DataInputHelper dataInputThread;

    /** Thread to parse incoming data and merge it into 1 ring if coming from multiple sockets. */
    private ParserMerger parserMergerThread;

    /** Data input streams from TCP sockets. */
    private DataInputStream in;

    /** SocketChannels used to receive data. */
    private SocketChannel socketChannel;

    /** Socket used to receive data. */
    private Socket socket;

    /** TCP receive buffer size in bytes. */
    private int tcpRecvBuf;

    /**
     * Node pools is used to get top-level EvioNode objects.
     * First index is socketCount, second is number of buffers
     * (64 total) in ByteBufferSupplys.
     */
    private EvioNodePool[] nodePools;

    // INPUT & OUTPUT

    /**
     * Biggest chunk of data sent by data producer.
     * Allows good initial value of ByteBuffer size.
     */
    private int maxBufferSize;

    /** Use the evio block header's block number as a record id. */
    private int recordId = 1;

    /** Use direct ByteBuffer? */
    private boolean direct;

    // Disruptor (RingBuffer)  stuff

    private long nextRingItem;

    /** Ring buffer holding ByteBuffers when using EvioCompactEvent reader for incoming events. */
    protected ByteBufferSupply bbInSupply;


    /**
     * Constructor to create a new DataChannelImplEt instance. Used only by
     * {@link DataTransportImplEt#createChannel(String, Map, boolean, Emu, EmuModule, int, int)}
     * which is only used during PRESTART in {@link Emu}.
     *
     * @param name         the name of this channel
     * @param transport    the DataTransport object that this channel belongs to
     * @param attributeMap the hashmap of config file attributes for this channel
     * @param input        true if this is an input data channel, otherwise false
     * @param emu          emu this channel belongs to
     * @param module       module this channel belongs to
     * @param outputIndex  order in which module's events will be sent to this
     *                     output channel (0 for first output channel, 1 for next, etc.).
     * @throws DataTransportException - unable to create buffers or socket.
     */
    DataChannelImplTcpStream(String name, DataTransportImplTcpStream transport,
                             Map<String, String> attributeMap, boolean input, Emu emu,
                             EmuModule module, int outputIndex, int streamNumber)
            throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module, outputIndex);

        dataTransportImplTcpStream = transport;
        this.streamNumber = streamNumber;

        if (input) {
            logger.info("      DataChannel TcpStream: creating input channel " + name);
        }
        else {
            logger.info("      DataChannel TcpStream: creating output channel " + name);
        }

        // Use direct ByteBuffers or not, faster & more stable with non-direct.
        // Turn this off since performance is better.
        String attribString = attributeMap.get("direct");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("false") ||
                    attribString.equalsIgnoreCase("off") ||
                    attribString.equalsIgnoreCase("no")) {
                direct = false;
            }
        }
        direct = false;

        // if INPUT channel
        if (input) {
            isER = (emu.getCodaClass() == CODAClass.ER);

            if (isER) {
                // In the special case of an ER, override the default to provide a ring
                // which has a timeout of 10 seconds on the main data channel (emu socket).
                // (Nornally there is none.)
                // This makes a significant difference only if there are 2 input channels
                // (1 ET and 1 emu socket) since the ER must switch between looking at both.
                // Otherwise, if there was no data flowing, then the secondary (ET) channel
                // would never be looked at.
                ringBufferIn = createSingleProducer (
                                  new RingItemFactory(),
                                  inputRingItemCount,
                                  new SpinCountBackoffWaitStrategy(
                                              30000,
                                              new LiteTimeoutBlockingWaitStrategy(10L, TimeUnit.SECONDS)));
            }

            // size of TCP receive buffer (0 means use operating system default)
            //tcpRecvBuf = 3000000;     // THIS VALUE DOES NOT WORK FOR 1G Ethernet!!!
            tcpRecvBuf = 0;
            attribString = attributeMap.get("recvBuf");
            if (attribString != null) {
                try {
                    tcpRecvBuf = Integer.parseInt(attribString);
                    if (tcpRecvBuf < 0) {
                        tcpRecvBuf = 0;
                    }
                    logger.info("      DataChannel TcpStream: set recvBuf to " + tcpRecvBuf);
                }
                catch (NumberFormatException e) {}
            }

            logger.info("      DataChannel TcpStream: recvBuf = " + tcpRecvBuf);

            // set "data dump" option on
            // Currently unused.
            attribString = attributeMap.get("dump");
            if (attribString != null) {
                if (attribString.equalsIgnoreCase("true") ||
                    attribString.equalsIgnoreCase("on") ||
                    attribString.equalsIgnoreCase("yes")) {
                    dumpData = true;
                }
            }
            
//            dumpData = true;
//logger.info("      DataChannel TcpStream: dumpData = " + dumpData);


        }
        // if OUTPUT channel
        else {
            // set TCP_NODELAY option on
            noDelay = true;
            attribString = attributeMap.get("noDelay");
            if (attribString != null) {
                if (attribString.equalsIgnoreCase("false") ||
                    attribString.equalsIgnoreCase("off") ||
                    attribString.equalsIgnoreCase("no")) {
                    noDelay = false;
                }
            }
            logger.info("      DataChannel TcpStream: noDelay = " + noDelay);

            // size of TCP send buffer (0 means use operating system default)
            //tcpSendBuf = 3000000;     // THIS VALUE DOES NOT WORK FOR 1G Ethernet!!!
            tcpSendBuf = 0;
            attribString = attributeMap.get("sendBuf");
            if (attribString != null) {
                try {
                    tcpSendBuf = Integer.parseInt(attribString);
                    if (tcpSendBuf < 0) {
                        tcpSendBuf = 0;
                    }
                    logger.info("      DataChannel TcpStream: sendBuf = " + tcpSendBuf);
                }
                catch (NumberFormatException e) {}
            }
            logger.info("      DataChannel TcpStream: set sendBuf to " + tcpSendBuf);

            // Send port
            sendPort = cMsgNetworkConstants.emuTcpPort;
            attribString = attributeMap.get("port");
            if (attribString != null) {
                try {
                    sendPort = Integer.parseInt(attribString);
                    if (sendPort < 1024 || sendPort > 65535) {
                        sendPort = cMsgNetworkConstants.emuTcpPort;
                    }
                }
                catch (NumberFormatException e) {}
            }
            logger.info("      DataChannel TcpStream: sending on port " + sendPort);


            // Size of max buffer
            maxBufferSize = 4000000;
            attribString = attributeMap.get("maxBuf");
            if (attribString != null) {
logger.info("      DataChannel TcpStream: max buf size attribute string = " + attribString);
                try {
                    maxBufferSize = Integer.parseInt(attribString);
                    if (maxBufferSize < 0) {
                        maxBufferSize = 4000000;
                    }
                }
                catch (NumberFormatException e) {}
            }
            logger.info("      DataChannel TcpStream: max buf size = " + maxBufferSize);

            // Emu domain connection timeout in sec
            connectTimeout = -1;
            attribString = attributeMap.get("timeout");
            if (attribString != null) {
                try {
                    connectTimeout = Integer.parseInt(attribString);
                    if (connectTimeout < 0) {
                        connectTimeout = 3;
                    }
                }
                catch (NumberFormatException e) {}
            }
            logger.info("      DataChannel TcpStream: timeout = " + connectTimeout);

            // Emu domain preferred subnet in dot-decimal format
            preferredSubnet = null;
            attribString = attributeMap.get("subnet");
            if (attribString != null && cMsgUtilities.isDottedDecimal(attribString) == null) {
                preferredSubnet = null;
            }
            else {
                preferredSubnet = attribString;
            }
            logger.info("      DataChannel TcpStream: over subnet " + preferredSubnet);
        }

        // State after prestart transition -
        // during which this constructor is called
        channelState = CODAState.PAUSED;
    }


    /**
     * Once a client connects to the Emu domain server in the Emu transport object,
     * that socket is passed to this method and a thread is spawned to handle all
     * communications over it. Only used for input channel.<p>
     *     
     * This method is called synchronously by a single thread in the
     * EmuDomainTcpServer class.
     *
     * @param channel        data input socket/channel
     * @param sourceId       CODA id # of data source
     * @param maxBufferSize  biggest chunk of data expected to be sent by data producer
     *
     * @throws IOException   if exception dealing with socket or input stream
     */
    final void attachToInput(SocketChannel channel, int sourceId, int maxBufferSize) throws IOException {

        // Create a ring buffer full of empty ByteBuffer objects
        // in which to copy incoming data from client.
        // Using direct buffers works but performance is poor and fluctuates
        // quite a bit in speed.
        //
        // A DC with 13 inputs can quickly consume too much memory if we're not careful.
        // Put a limit on the total amount of memory used for all emu socket input channels.
        // Total limit is 1GB. This is probably the easiest way to figure out how many buffers to use.
        // Number of bufs must be a power of 2 with a minimum of 16 and max of 128.
        //
        // int channelCount = emu.getInputChannelCount();
        // int numBufs = 1024000000 / (maxBufferSize * channelCount);
        //
        // numBufs = numBufs <  16 ?  16 : numBufs;
        // numBufs = numBufs > 128 ? 128 : numBufs;
        //
        // Make power of 2, round up
        // numBufs = EmuUtilities.powerOfTwo(numBufs, true);
        // logger.info("\n\n      DataChannel TcpStream in: " + numBufs + " buffers in input supply\n\n");

        // Reducing numBufs to 32 increases barrier.waitfor() time from .02% to .4% of EB time
        int numBufs = 32;

        // Initialize things
        parserMergerThread = new ParserMerger();
        nodePools = new EvioNodePool[numBufs];
        this.sourceId = sourceId;
        this.maxBufferSize = maxBufferSize;

        // Set socket options
        socket = channel.socket();

        // Set TCP receive buffer size
        if (tcpRecvBuf > 0) {
            socket.setPerformancePreferences(0,0,1);
            socket.setReceiveBufferSize(tcpRecvBuf);
        }

        // Use buffered streams for efficiency
        socketChannel = channel;
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

        // EBs release events sequentially if there's only 1 build thread,
        // else the release is NOT sequential.
        boolean sequentialRelease = true;
        if (module.getEventProducingThreadCount() > 1) {
            sequentialRelease = false;
        }

        // If ER
        if (isER) {
            List<DataChannel> outChannels = emu.getOutChannels();

            // if (0 output channels or 1 file output channel) ...
            if (((outChannels.size() < 1) ||
                    (outChannels.size() == 1 &&
                            (outChannels.get(0).getTransportType() == TransportType.FILE)))) {
                // Since ER has only 1 recording thread and every event is processed in order,
                // and since the file output channel also processes all events in order,
                // the byte buffer supply does not have to be synchronized as byte buffers are
                // released in order. Will make things faster.

                // UPDATE, user events coming over same channel as physics are COPIED and
                // buffers from this supply are released. They are released while a previous
                // physics buffer is still being used to write events to (ie nodes in
                // the process of being written). So there is NO sequential release.
                sequentialRelease = false;
            }
            else {
                // If ER has more than one output, buffers may not be released sequentially
                sequentialRelease = false;
            }
        }

//logger.info("      DataChannel TcpStream in: seq release of buffers = " + sequentialRelease);

        // Create the EvioNode pools,
        // each of which contain 3500 EvioNodes to begin with. These are used for
        // the nodes of each event.
        for (int i = 0; i < numBufs; i++) {
            nodePools[i] = new EvioNodePool(3500);
        }
//logger.info("      DataChannel TcpStream in: created " + (numBufs) + " node pools for socket " + index + ", " + name());

        bbInSupply = new ByteBufferSupply(numBufs, 32,
                                          ByteOrder.BIG_ENDIAN, direct,
                                          sequentialRelease, nodePools);

logger.info("      DataChannel TcpStream in: connection made from " + name);

        // Start thread to handle socket input
        dataInputThread = new DataInputHelper();
        dataInputThread.start();

        // If this is the last socket, make sure all threads are started up before proceeding
        parserMergerThread.start();
        dataInputThread.waitUntilStarted();
logger.info("      DataChannel TcpStream in: last connection made, parser thd started, input threads running");
    }


    /**
     * Open a client output channel to the EmuSocket server.
     * @throws cMsgException if communication problems with server.
     */
    private void openOutputChannel() throws cMsgException {

        if (orderedIpAddrs != null && orderedIpAddrs.size() > 0) {
            directOutputChannel();
        }
        else {
            multicastOutputChannel();
        }
    }

    
    /**
     * Open a client output channel to the EmuSocket server using multicasting.
     * @throws cMsgException if communication problems with server.
     */
    private void multicastOutputChannel() throws cMsgException {

        // UDL ->  emu://port/expid/destCompName?codaId=id&timeout=sec&bufSize=size&tcpSend=size&noDelay

        // "name" is name of this channel which also happens to be the
        // destination CODA component we want to connect to.
        StringBuilder builder = new StringBuilder(256);

        builder.append("emu://multicast:").append(sendPort).append('/').append(emu.getExpid());
        builder.append('/').append(name).append("?codaId=").append(getID());

        if (maxBufferSize > 0) {
            builder.append("&bufSize=").append(maxBufferSize);
        }
        else {
            builder.append("&bufSize=4000000");
        }

        if (connectTimeout > -1) {
            builder.append("&timeout=").append(connectTimeout);
        }

        if (tcpSendBuf > 0) {
            builder.append("&tcpSend=").append(tcpSendBuf);
        }

        if (preferredSubnet != null) {
            builder.append("&subnet=").append(preferredSubnet);
        }

        if (noDelay) {
            builder.append("&noDelay");
        }

 logger.info("      DataChannel TcpStream out: will connect to server w/ multicast UDL = " + builder.toString());
        // This connection will contain "sockCount" number of sockets
        // which are all used to send data.
        emuDomain = new cMsg(builder.toString(), name, "emu domain client");
        emuDomain.connect();

        startOutputThread();
    }


    /**
     * Open a direct client output channel to the EmuSocket TCP server.
     * @throws cMsgException if communication problems with server.
     */
    private void directOutputChannel() throws cMsgException {

        // "name" is name of this channel which also happens to be the
        // destination CODA component we want to connect to.

        StringBuilder builder = new StringBuilder(256);

        for (String ip : orderedIpAddrs) {
            builder.append("emu://").append(ip).append(':').append(sendPort);
            builder.append('/').append(emu.getExpid()).append('/').append(name);
            builder.append("?codaId=").append(getID());

            if (preferredSubnet != null) {
                builder.append("&subnet=").append(preferredSubnet);
            }

            if (maxBufferSize > 0) {
                builder.append("&bufSize=").append(maxBufferSize);
            }
            else {
                builder.append("&bufSize=4000000");
            }

            if (connectTimeout > -1) {
                builder.append("&timeout=").append(connectTimeout);
            }

            if (tcpSendBuf > 0) {
                builder.append("&tcpSend=").append(tcpSendBuf);
            }

            if (noDelay) {
                builder.append("&noDelay");
            }

            // This connection will contain "sockCount" number of sockets
            // which are all used to send data.
            try {
logger.info("      DataChannel TcpStream out: will directly connect to server w/ UDL = " + builder.toString());
                emuDomain = new cMsg(builder.toString(), name, "emu domain client");
                emuDomain.connect();
                startOutputThread();
                return;
            }
            catch (cMsgException e) {
                logger.info("      DataChannel TcpStream out: could not connect to server at " + ip);
                builder.delete(0, builder.length());
                continue;
            }
        }

        throw new cMsgException("Cannot connect to any given IP addresses directly");
    }


    private void closeOutputChannel() throws cMsgException {
        if (input) return;
        // flush and close sockets
        emuDomain.disconnect();
    }


    private void closeInputSockets() {
        if (!input) return;
//        logger.info("      DataChannel TcpStream in: close input sockets from " + name);

        try {
             in.close();
             // Will close socket, associated channel & streams
             socket.close();
        }
        catch (IOException e) {}
    }


    /** {@inheritDoc} */
    public TransportType getTransportType() {
        return TransportType.EMU;
    }


    /** {@inheritDoc} */
    public int getInputLevel() {
        int supplyLevel, level = 0;

        if (bbInSupply == null) {
            supplyLevel = 0;
        }
        else {
            supplyLevel = bbInSupply.getFillLevel();
        }
        level = level > supplyLevel ? level : supplyLevel;
        return level;
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        super.prestart();
        haveInputEndEvent = false;

        if (input) {
            channelState = CODAState.PAUSED;
            return;
        }
        try {
            // Before we create a socket, order the destination IP addresses
            // according to any preferred subnet.
            orderedIpAddrs = cMsgUtilities.orderIPAddresses(Arrays.asList(ipAddrList),
                                                            Arrays.asList(bAddrList),
                                                            preferredSubnet);
            System.out.println("Ordered destination IP list:");
            for (String ip : orderedIpAddrs) {
                System.out.println("  " + ip);
            }

            openOutputChannel();
        }
        catch (Exception e) {
            channelState = CODAState.ERROR;
            emu.setErrorState("      DataChannel TcpStream out: " + e.getMessage());
            throw new CmdExecException(e);
        }

        channelState = CODAState.PAUSED;
    }

    /** {@inheritDoc} */
    public void go() {
        pause = false;
        channelState = CODAState.ACTIVE;
    }

    /** {@inheritDoc} */
    public void pause() {
        pause = true;
        channelState = CODAState.PAUSED;
    }


    /**
     * Interrupt all threads.
     */
    private void interruptThreads() {
        if (dataInputThread != null) {
            // The parser merger thread needs to be interrupted first,
            // otherwise the parseToRing method may get stuck waiting
            // on further data in a loop around parkNanos().
//logger.debug("      DataChannel TcpStream: end/reset(), interrupt parser/merger thread");
            parserMergerThread.interrupt();
            try {Thread.sleep(10);}
            catch (InterruptedException e) {}

            if (dataInputThread != null) {
                dataInputThread.interrupt();
//logger.debug("      DataChannel TcpStream: end/reset(), interrupt input thread " + i);
            }
        }

        if (dataOutputThread != null) {
//logger.debug("      DataChannel TcpStream: end/reset(), interrupt main output thread");
            dataOutputThread.interrupt();

//logger.debug("      DataChannel TcpStream: end/reset(), interrupt output thread");
                dataOutputThread.sender.endThread();
        }
    }

    
    /**
     * Try joining all threads, up to 1 sec each.
     */
    private void joinThreads() {
        if (dataInputThread != null) {
            try {parserMergerThread.join(1000);}
            catch (InterruptedException e) {}
//logger.debug("      DataChannel TcpStream: end/reset(), joined parser/merger thread");

                if (dataInputThread != null) {
                    try {dataInputThread.join(1000);}
                    catch (InterruptedException e) {}
                }
//logger.debug("      DataChannel TcpStream: end/reset(), joined input thread " + i);
        }

        if (dataOutputThread != null) {

            try {dataOutputThread.join(1000);}
            catch (InterruptedException e) {}

//logger.debug("      DataChannel TcpStream: end/reset(), joined main output thread ");

                try {dataOutputThread.sender.join(1000);}
                catch (InterruptedException e) {}
//logger.debug("      DataChannel TcpStream: end/reset(), joined output thread ");
        }
    }
    

    /** {@inheritDoc}. Formerly this code was the close() method. */
    public void end() {
        gotEndCmd   = true;
        gotResetCmd = false;

        // The emu's emu.end() method first waits (up to 60 sec) for the END event to be read
        // by input channels, processed by the module, and finally to be sent by
        // the output channels. Then it calls everyone's end() method including this one.
        // Threads and sockets can be shutdown quickly, since we've already
        // waited for the END event.

        interruptThreads();
        joinThreads();

        // Clean up
        if (dataInputThread != null) {
            dataInputThread = null;
            parserMergerThread = null;
            closeInputSockets();
        }

        if (dataOutputThread != null) {
            dataOutputThread.sender = null;
            dataOutputThread = null;

            try {
logger.debug("      DataChannel TcpStream: end(), close output channel " + name);
                closeOutputChannel();
            }
            catch (cMsgException e) {}
        }

        channelState = CODAState.DOWNLOADED;
    }


    /**
     * {@inheritDoc}
     * Reset this channel by interrupting the data sending threads and closing ET system.
     */
    public void reset() {
        gotEndCmd = false;
        gotResetCmd = true;

        interruptThreads();
        joinThreads();

        // Clean up
        if (dataInputThread != null) {
            dataInputThread = null;
            parserMergerThread = null;
            closeInputSockets();
        }

        if (dataOutputThread != null) {
            dataOutputThread.sender = null;
            dataOutputThread = null;

            try {
logger.debug("      DataChannel TcpStream: end(), close output channel " + name);
                closeOutputChannel();
            }
            catch (cMsgException e) {}
        }

        errorMsg.set(null);
        channelState = CODAState.CONFIGURED;
    }


//    /**
//     * For input channel, start the DataInputHelper thread which takes Evio
//     * file-format data, parses it, puts the parsed Evio banks into the ring buffer.
//     */
//    private final void startInputThread() {
//        dataInputThread = new DataInputHelper();
//        dataInputThread.start();
//        dataInputThread.waitUntilStarted();
//    }


    private void startOutputThread() {
        dataOutputThread = new DataOutputHelper();
        dataOutputThread.start();
        dataOutputThread.waitUntilStarted();
    }


    /**
     * Class used to get data over network and put into ring buffer.
     * There is one of these for each of the "socketCount" number of TCP sockets.
     * Currently only one.
     */
    private final class DataInputHelper extends Thread {

        /** Variable to print messages when paused. */
        private int pauseCounter = 0;

        /** Let a single waiter know that the main thread has been started. */
        private final CountDownLatch latch = new CountDownLatch(1);

        /** Data input stream from TCP socket. */
        private final DataInputStream inStream;

        /** SocketChannel used to receive data. */
        private final SocketChannel sockChannel;

        /** Supply of ByteBuffers to use for this socket. */
        private final ByteBufferSupply bbSupply;


        /** Constructor. */
        DataInputHelper() {
            super(emu.getThreadGroup(), name() + "_data_in");
            inStream = in;
            bbSupply = bbInSupply;
            sockChannel = socketChannel;
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {}
        }


        /** {@inheritDoc} */
        @Override
        public void run() {

            // Tell the world I've started
            latch.countDown();

            long word;
            int cmd, size;
            boolean delay = false;
            // For use with direct buffers
            ByteBuffer wordCmdBuf = ByteBuffer.allocate(8);
            IntBuffer ibuf = wordCmdBuf.asIntBuffer();

            ByteBuffer buf;
            ByteBufferItem item;

            try {

                while (true) {
                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        return;
                    }

                    if (delay) {
                        Thread.sleep(5);
                        delay = false;
                    }

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
                            logger.warn("      DataChannel TcpStream in: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }


                    // Sets the producer sequence
                    item = bbSupply.get();
//System.out.println("      DataChannel TcpStream in: GOT item " + item.myIndex + " from ByteBuffer supply");

                    // First read the command & size with one read, into a long.
                    // These 2, 32-bit ints are sent in network byte order, cmd first.
                    // Reading a long assumes big endian so cmd, which is sent
                    // first, should appear in most significant bytes.
                    if (direct) {
//System.out.println("      DataChannel TcpStream in: Try reading buffer hdr words into direct buf");
                        sockChannel.read(wordCmdBuf);
                        cmd  = ibuf.get();
                        size = ibuf.get();
                        ibuf.position(0);
                        wordCmdBuf.position(0);

                        item.ensureCapacity(size);
                        buf = item.getBuffer();
                        buf.limit(size);

//System.out.println("      DataChannel TcpStream in: got cmd = " + cmd + ", size = " + size + ", now read in data ...");
                        // Be sure to read everything
                        while (buf.hasRemaining()) {
                            sockChannel.read(buf);
                        }
//System.out.println("      DataChannel TcpStream in: done reading in data");
                        buf.flip();
                    }
                    else {
//System.out.println("      DataChannel TcpStream in: Try reading buffer hdr words");
                        word = inStream.readLong();
                        cmd  = (int) ((word >>> 32) & 0xffL);
                        size = (int)   word;   // just truncate for lowest 32 bytes
                        item.ensureCapacity(size);
                        buf = item.getBuffer();
                        buf.limit(size);

//System.out.println("      DataChannel TcpStream in: got cmd = " + cmd + ", size = " + size + ", now read in data ...");
                        inStream.readFully(item.getBuffer().array(), 0, size);
//System.out.println("      DataChannel TcpStream in: done reading in data");
                    }
//System.out.println("      DataChannel TcpStream in: " + name + ", incoming buf size = " + size);
//Utilities.printBuffer(item.getBuffer(), 0, size/4, "PRESTART EVENT, buf lim = " + buf.limit());
                    bbSupply.publish(item);

                    // We just received the END event
                    if (cmd == cMsgConstants.emuEvioEndEvent) {
System.out.println("      DataChannel TcpStream in: " + name + ", got END event, exit reading thd");
                        return;
                    }
                }
            }
            catch (InterruptedException e) {
                logger.warn("      DataChannel TcpStream in: " + name + ", interrupted, exit reading thd");
            }
            catch (AsynchronousCloseException e) {
                logger.warn("      DataChannel TcpStream in: " + name + ", socket closed, exit reading thd");
            }
            catch (EOFException e) {
                // Assume that if the other end of the socket closes, it's because it has
                // sent the END event and received the end() command.
                logger.warn("      DataChannel TcpStream in: " + name + ", other end of socket closed, exit reading thd");
            }
            catch (Exception e) {
                if (haveInputEndEvent) {
System.out.println("      DataChannel TcpStream in: " + name +
                   ", exception but already have END event, so exit reading thd");
                    return;
                }
                e.printStackTrace();
                channelState = CODAState.ERROR;
                // If error msg already set, this will not
                // set it again. It will send it to rc.
                String errString = "DataChannel TcpStream in: error reading " + name;
                if (e.getMessage() != null) {
                    errString += ' ' + e.getMessage();
                }
                emu.setErrorState(errString);
            }
        }
    }


    /**
     * Class to consume all buffers read from the socket, parse them into evio events,
     * and place events into this channel's single ring buffer. It no longer "merges"
     * data from multiple inputs as it did in the EMU channel, but it does allow
     * separation of the socket reading code from the parsing code.
     */
    private final class ParserMerger extends Thread {

        /** Keep track of record ids coming in to make sure they're sequential. */
        private int expectedRecordId = 1;

        /** Object used to read/parse incoming evio data. */
        private EvioCompactReader reader;


        /** Constructor. */
        ParserMerger() {
            super(emu.getThreadGroup(), name() + "_parser_merger");
        }
        

        public void run() {
            try {
                // Simplify things when there's only 1 socket for better performance
                ByteBufferSupply bbSupply = bbInSupply;
                while (true) {
                    // Sets the consumer sequence
                    ByteBufferItem item = bbSupply.consumerGet();
                    if (parseStreamingToRing(item, bbSupply)) {
                        logger.info("      DataChannel TcpStream in: 1 quit streaming parser/merger thread for END event from " + name);
                        break;
                    }
                }
            }
            catch (InterruptedException e) {
//                logger.warn("      DataChannel TcpStream in: " + name +
//                            " parserMerger thread interrupted, quitting ####################################");
            }
            catch (EvioException e) {
                // Bad data format or unknown control event.
                e.printStackTrace();
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel TcpStream in: " + e.getMessage());
            }
        }


        /**
         * Parse the buffer into evio bits that get put on this channel's ring.
         *
         * @param item        ByteBufferSupply item containing buffer to be parsed.
         * @param bbSupply    ByteBufferSupply item.
         * @return is the last evio event parsed the END event?
         * @throws EvioException
         * @throws InterruptedException
         */
        private boolean parseStreamingToRing(ByteBufferItem item, ByteBufferSupply bbSupply)
                throws EvioException, InterruptedException {

            RingItem ri;
            boolean hasFirstEvent, isUser=false;
            ControlType controlType = null;
            EvioNodeSource pool;

            // Get buffer from an item from ByteBufferSupply - one per channel
            ByteBuffer buf = item.getBuffer();
//Utilities.printBytes(buf, 0, 100, "Incoming buf");

//            // Do this for possibly compressed data. Make sure the buffer we got from the
//            // supply is big enough to hold the uncompressed data. If not, created a new,
//            // bigger buffer and copy everything into it.
//            ByteBuffer newBuf = EvioCompactReaderUnsync.ensureUncompressedCapacity(buf);
//            item.setBuffer(newBuf);

            try {
                // Pool of EvioNodes associated with this buffer which grows as needed
                pool = (EvioNodePool)item.getMyObject();
                // Each pool must be reset only once!
                pool.reset();
                if (reader == null) {
//System.out.println("      DataChannel TcpStream in: create reader, buf's pos/lim = " + buf.position() + "/" + buf.limit());
                    reader = new EvioCompactReader(buf, pool, false);
//System.out.println("      DataChannel TcpStream in: incoming data's evio version = " + reader.getEvioVersion());
                }
                else {
//System.out.println("      DataChannel TcpStream in: set buffer, expected id = " + expectedRecordId);
                    reader.setBuffer(buf, pool);
                }

                // If buf contained compressed data
                if (reader.isCompressed()) {
                    // Data may have been uncompressed into a different, larger buffer.
                    // If so, ditch the original and use the new one.
                    ByteBuffer biggerBuf = reader.getByteBuffer();
                    if (biggerBuf != buf) {
                        item.setBuffer(biggerBuf);
                    }
                }
            }
            catch (EvioException e) {
                System.out.println("      DataChannel TcpStream in: data NOT evio format 1");
                e.printStackTrace();
                Utilities.printBytes(buf, 0, 80, "BAD BUFFER TO PARSE");
                throw e;
            }

            // First block header in buffer
            IBlockHeader blockHeader = reader.getFirstBlockHeader();
            if (blockHeader.getVersion() < 4) {
                throw new EvioException("Data not in evio but in version " +
                        blockHeader.getVersion());
            }

            hasFirstEvent = blockHeader.hasFirstEvent();

            int evtType = blockHeader.getEventType();
            EventType eventType = EventType.getEventType(blockHeader.getEventType());
            if (eventType == null || !eventType.isEbFriendly()) {
                System.out.println("bad evio format or improper event type (" + evtType + ")\n");
                Utilities.printBytes(buf, 0, 200, "Incoming (bad format bytes");
                throw new EvioException("bad evio format or improper event type (" + evtType + ")");
            }

            recordId = blockHeader.getNumber();

            // Check record for sequential record id
            expectedRecordId = Evio.checkRecordIdSequence(recordId, expectedRecordId, false,
                                                          eventType, DataChannelImplTcpStream.this);
//System.out.println("      DataChannel TcpStream in: expected record id = " + expectedRecordId +
//                    ", actual = " + recordId);
//System.out.println("      DataChannel TcpStream in: event type = " + eventType + ", event count = " + reader.getEventCount() + " from " + name + "\n");
//            EvioNode nd = reader.getScannedEvent(1, pool);
//Utilities.printBytes(nd.getBuffer(), 0, nd.getTotalBytes(), "First event bytes");

            int eventCount = reader.getEventCount();
            boolean gotRocRaw  = eventType.isFromROC();
            boolean gotPhysics = eventType.isAnyPhysics();

            // For streaming ROC Raw, there is a ROC bank with at least 2 children -
            // one of which is a stream info bank (SIB) and the others which are
            // data banks, each of which must be parsed.
            if (gotRocRaw) {
                EvioNode topNode = reader.getScannedEvent(1, pool);
                if (topNode == null) {
                    throw new EvioException("Empty buffer arriving into input channel ???");
                }

                if (topNode.getChildCount() < 2) {
                    throw new EvioException("ROC Raw bank should have at least 2 children, not " + topNode.getChildCount());
                }
            }
            else if (gotPhysics) {
                EvioNode topNode = reader.getScannedEvent(1, pool);

                if (topNode == null) {
                    throw new EvioException("Empty buffer arriving into input channel ???");
                }

                if (!CODATag.isStreamingPhysics(topNode.getTag()))  {
                    throw new EvioException("Wrong tag for streaming Physics bank, got " +
                            CODATag.getName(topNode.getTag()));
                }
            }

            // Each PayloadBuffer contains a reference to the buffer it was
            // parsed from (buf).
            // This cannot be released until the module is done with it.
            // Keep track by counting users (# time slice banks parsed from same buffer).
            item.setUsers(eventCount);

            for (int i = 1; i < eventCount+1; i++) {

                int frame = 0;
                long timestamp = 0L;
                EvioNode topNode;

                if (isER) {
                    // Don't need to parse all bank headers, just top level.
                    topNode = reader.getEvent(i);
                }
                else {
                    // getScannedEvent will clear child and allNodes lists
                    topNode = reader.getScannedEvent(i, pool);
                }

                // This should NEVER happen
                if (topNode == null) {
                    System.out.println("      DataChannel TcpStream in: WARNING, event count = " + eventCount +
                            " but get(Scanned)Event(" + i + ") is null - evio parsing bug");
                    continue;
                }

                // RocRaw's, Time Slice Bank
                EvioNode node = topNode;

                if (gotRocRaw) {
                    // Complication: from the ROC, we'll be receiving USER events mixed
                    // in with and labeled as ROC Raw events. Check for that & fix it.
                    if (Evio.isUserEvent(node)) {
                        isUser = true;
                        eventType = EventType.USER;
                        if (hasFirstEvent) {
                            System.out.println("      DataChannel TcpStream in: " + name + "  FIRST event from ROC RAW");
                        } else {
                            System.out.println("      DataChannel TcpStream in: " + name + " USER event from ROC RAW");
                        }
                    }
                    else {
                        // Pick this raw data event apart a little
                        if (!node.getDataTypeObj().isBank()) {
                            DataType eventDataType = node.getDataTypeObj();
                            throw new EvioException("ROC raw record contains " + eventDataType +
                                    " instead of banks (data corruption?)");
                        }

                        // Find the frame and timestamp now for later ease of use (skip over 5 ints)
                        int pos = node.getPosition();
                        ByteBuffer buff = node.getBuffer();
                        frame = buff.getInt(20 + pos);
                        timestamp = EmuUtilities.intsToLong(buff.getInt(24 + pos), buff.getInt(28 + pos));
//System.out.println("      DataChannel TcpStream in: roc raw has frame = " + frame + ", timestamp = " + timestamp + ", pos = " + pos);
                    }
                }
                else if (eventType.isBuildable()) {
                    // If time slices coming from DCAG, SAG, or PAG
                    // Physics or partial physics event must have BANK as data type
                    if (!node.getDataTypeObj().isBank()) {
                        DataType eventDataType = node.getDataTypeObj();
                        throw new EvioException("physics record contains " + eventDataType +
                                " instead of banks (data corruption?)");
                    }

                    int pos = node.getPosition();
                    // Find the frame and timestamp now for later ease of use (skip over 4 ints)
                    ByteBuffer buff = node.getBuffer();
                    frame = buff.getInt(16 + pos);
                    timestamp = EmuUtilities.intsToLong(buff.getInt(20 + pos), buff.getInt(24 + pos));
//System.out.println("      DataChannel TcpStream in: buildable has frame = " + frame + ", timestamp = " + timestamp + ", pos = " + pos);
                }
                else if (eventType == EventType.CONTROL) {
                    // Find out exactly what type of control event it is
                    // (May be null if there is an error).
                    controlType = ControlType.getControlType(node.getTag());
logger.info("      DataChannel TcpStream in: got " + controlType + " event from " + name);
                    if (controlType == null) {
                        logger.info("      DataChannel TcpStream in: found unidentified control event");
                        throw new EvioException("Found unidentified control event");
                    }
                }
                else if (eventType == EventType.USER) {
                    isUser = true;
                    if (hasFirstEvent) {
                        logger.info("      DataChannel TcpStream in: " + name + " got FIRST event");
                    } else {
                        logger.info("      DataChannel TcpStream in: " + name + " got USER event");
                    }
//                } else if (evType == EventType.MIXED) {
//                        // Mix of event types.
//                        // Can occur for combo of user, ROC RAW and possibly control events.
//                        // Only occurs when a user inserts a User event during the End transition.
//                        // What happens is that the User Event gets put into a EVIO Record which can
//                        // also contain ROC RAW events. The evio record gets labeled as containing
//                        // mixed events.
//                        //
//                        // This will NOT occur in ER, so headers are all parsed at this point.
//                        // Look at the very first header, second word.
//                        // num = 0  --> it's a control or User event (tag tells which):
//                        //          0xffd0 <= tag <= 0xffdf --> control event
//                        //          else                    --> User event
//                        // num > 0  --> block level for ROC RAW
//
//                        node = topNode;
//                        int num = node.getNum();
//                        if (num == 0) {
//                            int tag = node.getTag();
//                            if (ControlType.isControl(tag)) {
//                                controlType = ControlType.getControlType(tag);
//                                evType = EventType.CONTROL;
//                                logger.info("      DataChannel TcpStream in: " + name + " mixed type to " + controlType.name());
//                            }
//                            else {
//                                isUser = true;
//                                evType = EventType.USER;
//                                logger.info("      DataChannel TcpStream in: " + name + " mixed type to user type");
//                            }
//                        }
//                        else {
//                            logger.info("      DataChannel TcpStream in: " + name + " mixed type to ROC RAW");
//                            evType = EventType.ROC_RAW;
//                            // Pick this raw data event apart a little
//                            if (!node.getDataTypeObj().isBank()) {
//                                DataType eventDataType = node.getDataTypeObj();
//                                throw new EvioException("ROC raw record contains " + eventDataType +
//                                        " instead of banks (data corruption?)");
//                            }
//                        }
                }

                nextRingItem = ringBufferIn.nextIntr(1);
                ri = ringBufferIn.get(nextRingItem);


//                 if (dumpData) {
//                     bbSupply.release(item);
//
//                     // Handle end event ...
//                     if (controlType == ControlType.END) {
////TODO: what to do about having got the next ringItem
//                         // There should be no more events coming down the pike so
//                         // go ahead write out existing events and then shut this
//                         // thread down.
//                         haveInputEndEvent = true;
//                         // Run callback saying we got end event
//                         if (endCallback != null) endCallback.endWait();
//                         break;
//                     }
//
//                     // Send control events on to module so we can prestart, go and take data
//                     if (!eventType.isBuildable()) {
//                         ri.setAll(null, null, node, eventType, controlType,
//                                   isUser, hasFirstEvent, module.isStreamingData(), id, recordId, sourceId,
//                                   1, name, item, bbSupply);
//
//                         ringBufferIn.publish(nextRingItem);
//                     }
//
//                     continue;
//                 }

                // Set & reset all parameters of the ringItem
                if (eventType.isBuildable()) {
//logger.info("      DataChannel TcpStream in: put buildable event into channel ring, event from " + name);
                    ri.setAll(null, null, node, eventType, controlType,
                            isUser, hasFirstEvent, module.isStreamingData(), id, recordId, sourceId,
                            node.getNum(), name, item, bbSupply);
                    ri.setTimeFrame(frame);
                    ri.setTimestamp(timestamp);
                } else {
logger.info("      DataChannel TcpStream in: put CONTROL (user?) event into channel ring, event from " + name);
                    ri.setAll(null, null, node, eventType, controlType,
                            isUser, hasFirstEvent, module.isStreamingData(), id, recordId, sourceId,
                            1, name, item, bbSupply);
                }

                // Only the first event of first block can be "first event"
                isUser = hasFirstEvent = false;

                ringBufferIn.publish(nextRingItem);

                // Handle end event ...
                if (controlType == ControlType.END) {
                    // There should be no more events coming down the pike so
                    // go ahead write out existing events and then shut this
                    // thread down.
                    haveInputEndEvent = true;
                    // Run callback saying we got end event
                    if (endCallback != null) endCallback.endWait();
logger.info("      DataChannel TcpStream in: BREAK from loop, got END event");
                    break;
                }
            }

            return haveInputEndEvent;
        }
    }



    /**
     * Class used to take Evio banks from ring buffer (placed there by a module),
     * and write them over network to an Emu domain input channel using the Emu
     * domain output channel.
     */
    private final class DataOutputHelper extends Thread {

        /** Help in pausing DAQ. */
        private int pauseCounter;

        /** Let a single waiter know that the main thread has been started. */
        private final CountDownLatch startLatch = new CountDownLatch(1);

        /** Object to write (marshall) input buffers into larger, output evio buffer (next member). */
        private EventWriterUnsync writer;

        /** Buffer to write events into so it can be sent in a cMsg message. */
        private ByteBuffer currentBuffer;

        /** ByteBuffer supply item that currentBuffer comes from. */
        private ByteBufferItem currentBBitem;

        /** Entry in evio block header. */
        private final BitSet bitInfo = new BitSet(24);

        /** Type of last event written out. */
        private EventType previousEventType;

        /** What state is this thread in? */
        private volatile ThreadState threadState;

        /** Time at which events were sent over socket. */
        private volatile long lastSendTime;

        /** Sender thread to send data over network. */
        private SocketSender sender;

        /** ByteBufferSupply for sender. */
        private final ByteBufferSupply bbOutSupply;

        /** When regulating output buffer flow, the last time a buffer was sent. */
        private long lastBufSendTime;

        /** When regulating output buffer flow, the current
         * number of physics events written to buffer. */
        private int currentEventCount;

        /** Used to implement a precision sleep when regulating output data rate. */
        private final long SLEEP_PRECISION = TimeUnit.MILLISECONDS.toNanos(2);
        
         /** Used to implement a precision sleep when regulating output data rate. */
        private final long SPIN_YIELD_PRECISION = TimeUnit.MICROSECONDS.toNanos(2);


        ByteBuffer bufferPrev;

        /**
         * This class is a separate thread used to write filled data
         * buffers over the emu socket.
         */
        private final class SocketSender extends Thread {

            /** Boolean used to kill this thread. */
            private volatile boolean killThd;

            /** The ByteBuffers to send. */
            private final ByteBufferSupply supply;

            /** cMsg message into which out going data is placed in order to be written. */
            private final cMsgMessageFull outGoingMsg;


            SocketSender(ByteBufferSupply supply) {
                super(emu.getThreadGroup(), name() + "_sender");

                this.supply = supply;

                // Need do this only once
                outGoingMsg = new cMsgMessageFull();
                // Message format
                outGoingMsg.setUserInt(cMsgConstants.emuEvioFileFormat);
            }

            /**
             * Kill this thread which is sending messages/data to other end of emu socket.
             */
            final void endThread() {
System.out.println("SocketSender: killThread, set flag, interrupt");
                killThd = true;
                this.interrupt();
            }


            /**
             * Send the events currently marshalled into a single buffer.
             */
            public void run() {
                boolean isEnd;

                while (true) {
                    if (killThd) {
System.out.println("SocketSender thread told to return");
                        return;
                    }

                    try {
//                        Thread.sleep(2000);
                        
                        // Get a buffer filled by the other thread
                        ByteBufferItem item = supply.consumerGet();
                        ByteBuffer buf = item.getBufferAsIs();
//Utilities.printBuffer(buf, 0, 40, "PRESTART EVENT, buf lim = " + buf.limit());

                        // Put data into message
                        if (direct) {
                            outGoingMsg.setByteArray(buf);
                        }
                        else {
                            outGoingMsg.setByteArrayNoCopy(buf.array(), buf.arrayOffset(),
                                                           buf.remaining());
                        }

                        // User boolean is true if this buf contains END event,
                        // so signify that in command (user int).
                        isEnd = item.getUserBoolean();
                        if (isEnd) {
                            outGoingMsg.setUserInt(cMsgConstants.emuEvioEndEvent);
                        }
                        else {
                            outGoingMsg.setUserInt(cMsgConstants.emuEvioFileFormat);
                        }

                        // Send it
                        emuDomain.send(outGoingMsg);

                        // Force things out over socket
                        if (item.getForce()) {
                            try {
                                emuDomain.flush(0);
                            }
                            catch (cMsgException e) {
                            }
                        }

                        // Run callback saying we got and are done with end event
                        if (isEnd) {
                            endCallback.endWait();
                        }

                        // Release this buffer so it can be filled again
//System.out.println("release " + item.getMyId() + ", rec # = " + item.getConsumerSequence());
                        supply.release(item);
//System.out.println("released rec # = " + item.getConsumerSequence());
                    }
                    catch (InterruptedException e) {
System.out.println("SocketSender thread interrupted");
                        return;
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        channelState = CODAState.ERROR;
                        emu.setErrorState("DataChannel TcpStream out: " + e.getMessage());
                        return;
                    }

                    lastSendTime = emu.getTime();
                }
            }
        }


        /** Constructor. */
        DataOutputHelper() {
            super(emu.getThreadGroup(), name() + "_data_out");

            // All buffers will be released in order in this code.
            // This will improve performance since mutexes can be avoided.
            boolean orderedRelease = true;

            bufferPrev = ByteBuffer.allocate(maxBufferSize).order(byteOrder);

                // A mini ring of buffers, 16 is the best size
System.out.println("DataOutputHelper constr: making BB supply of 8 bufs @ bytes = " + maxBufferSize);
                bbOutSupply = new ByteBufferSupply(16, maxBufferSize, byteOrder,
                                                      direct, orderedRelease);

                // Start up sender thread
                sender = new SocketSender(bbOutSupply);
                sender.start();

            // Create writer to write events into file format
            try {

                // Start out with a single buffer from the supply just created
                currentBBitem = bbOutSupply.get();
                currentBuffer = currentBBitem.getBuffer();
//System.out.println("\nFirst current buf -> rec # = " + currentBuffer.getInt(4) +
//                           ", " + System.identityHashCode(currentBuffer));

                writer = new EventWriterUnsync(currentBuffer);
                //writer = new EventWriterUnsync(currentBuffer, 0, 0, null, 1, null, 0);

// TODO: This writes a trailer into currentBuffer
                writer.close();
            }
            catch (InterruptedException e) {/* never happen */}
            catch (EvioException e) {/* never happen */}
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {}
        }


        /**
         * Put the current buffer of events back into the bbOutSupply ring for its
         * consumer which is the writing thread.
         *
         * @param force    if true, force data over socket
         * @param userBool user boolean to be set in byte buffer item. In this case,
         *                 if true, event being flushed is single END event.
         * @param isData   if true, current item is data (not control or user event).
         * @throws InterruptedException
         */
        private void flushEvents(boolean force, boolean userBool, boolean isData)
                throws InterruptedException {
            
            // Position the buffer
            writer.close();

            // We must have something to write
            if (writer.getEventsWritten() < 1) {
                return;
            }

            // If we're regulating the flow of data buffers to send at a fixed rate ...
            if (isData && regulateBufferRate) {
                long now = System.nanoTime();
                long elapsedTime = now - lastBufSendTime;
                lastBufSendTime = now;

                // If not enough time has elapsed since last send, wait
                if (elapsedTime < nanoSecPerBuf) {
                    // New, highly accurate sleep method
                    long timeLeft = nanoSecPerBuf - elapsedTime;
                    final long end = now + timeLeft;
                    do {
                        if (timeLeft > SLEEP_PRECISION) {
                            try {Thread.sleep(1);}
                            catch (InterruptedException e) {}
                        } else {
                            if (timeLeft > SPIN_YIELD_PRECISION) {
                                Thread.yield();
                            }
                        }
                        timeLeft = end - System.nanoTime();

                    } while (timeLeft > 0);

                    lastBufSendTime = System.nanoTime();
                }
            }

            currentEventCount = 0;

            // Store flags for future use
            currentBBitem.setForce(force);
            currentBBitem.setUserBoolean(userBool);

            // Put the written-into buffer back into the supply so the consumer -
            // the thread which writes it over the network - can get it and
            // write it.
            ByteBuffer bb = writer.getByteBuffer();
            currentBuffer.flip();
            currentBuffer.limit(writer.getBytesWrittenToBuffer());

//System.out.println("flushEvents: reading buf limit = " + bb.limit());
//System.out.println("flushEvents: setting current buf lim = " + currentBuffer.limit());

            bbOutSupply.publish(currentBBitem);

            // Get another buffer from the supply so writes can continue.
            // It'll block if none available.

//Thread.sleep(200);
            currentBBitem = bbOutSupply.get();
            currentBuffer = currentBBitem.getBuffer();
//System.out.println("flushEvents: out\n");
        }


        /**
         * Flush already written events over sockets.
         * This is only called when data rate is slow and data must
         * be forced over the network.
         * @throws InterruptedException
         */
        private void flushExistingEvioData() throws InterruptedException {
            // Don't write nothin'
            if (currentEventCount == 0) {
                return;
            }

            if (previousEventType.isBuildable()) {
                flushEvents(true, false, true);
            }
            else {
                flushEvents(true, false, false);
            }
        }


        /**
         * Write events into internal buffer and, if need be, flush
         * them over socket. Force all non-buildable events, like control
         * and user events, to be sent immediately.
         *
         * @param rItem event to write
         * @throws IOException if error writing evio data to buf
         * @throws EvioException if error writing evio data to buf (bad format)
         * @throws EmuException if no data to write or buffer is too small to hold 1 event.
         * @throws InterruptedException if thread interrupted.
         */
        private void writeEvioData(RingItem rItem)
                throws IOException, EvioException, EmuException, InterruptedException {

            int blockNum;
            EventType eType = rItem.getEventType();
            boolean isBuildable = eType.isBuildable();
            int eventsWritten = writer.getEventsWritten();

            // If we're sending out 1 event by itself ...
            if (singleEventOut || !isBuildable) {
                currentEventCount = 0;

                // If we already have something stored-up to write, send it out first
                if (eventsWritten > 0 && !writer.isClosed()) {
                    if (previousEventType.isBuildable()) {
                        flushEvents(false, false, true);
                    }
                    else {
                        flushEvents(true, false, false);
                    }
                }

                if (isBuildable) {
                    blockNum = recordId;
                }
                else {
                    blockNum = -1;
                }

                recordId++;
//System.out.println("      DataChannel TcpStream out: writeEvioData: record Id set to " + blockNum +
//                  ", then incremented to " + recordId);

                // Make sure there's enough room for that one event
                if (rItem.getTotalBytes() > currentBuffer.capacity()) {
                    currentBBitem.ensureCapacity(rItem.getTotalBytes() + 1024);
                    currentBuffer = currentBBitem.getBuffer();
//System.out.println("\n  &&&&&  DataChannel TcpStream out: writeEvioData:  expand 1 current buf -> rec # = " + currentBuffer.getInt(4));
                }

                // Write the event ..
                EmuUtilities.setEventType(bitInfo, eType);
                if (rItem.isFirstEvent()) {
                    EmuUtilities.setFirstEvent(bitInfo);
                }
//System.out.println("      DataChannel TcpStream out: writeEvioData: single write into buffer");
                writer.setBuffer(currentBuffer, bitInfo, blockNum);

                // Unset first event for next round
                EmuUtilities.unsetFirstEvent(bitInfo);

                ByteBuffer buf = rItem.getBuffer();
                if (buf != null) {
                    try {
//System.out.println("      DataChannel TcpStream out: writeEvioData: single ev buf, pos = " + buf.position() +
//", lim = " + buf.limit() + ", cap = " + buf.capacity());
                        boolean fit = writer.writeEvent(buf);
                        if (!fit) {
                            // Our buffer is too small to fit even 1 event!
                            throw new EmuException("emu socket's buffer size must be increased in jcedit");
                        }
//                        Utilities.printBufferBytes(buf, 0, 20, "control?");
                    }
                    catch (Exception e) {
System.out.println("      c: single ev buf, pos = " + buf.position() +
                   ", lim = " + buf.limit() + ", cap = " + buf.capacity());
                        Utilities.printBytes(buf, 0, 20, "bad END?");
                        throw e;
                    }
                }
                else {
                    EvioNode node = rItem.getNode();
                    if (node != null) {
                        boolean fit = writer.writeEvent(node, false);
                        if (!fit) {
                            // Our buffer is too small to fit even 1 event!
                            throw new EmuException("emu socket's buffer size must be increased in jcedit");
                        }
                    }
                    else {
                        throw new EmuException("no data to write");
                    }
                }
                rItem.releaseByteBuffer();

                // Force over socket if control/user event
//                if (eType.isControl()) {
//                    if (rItem.getControlType() == ControlType.END) {
//                        flushEvents(true, true, false);
//                    }
//                    else {
//                        flushEvents(true, false, false);
//                    }
//                }
//                else if (eType.isUser()) {
//                    flushEvents(true, false, false);
//                }
//                else {
//                    flushEvents(false, false, true);
//                }

                if (isBuildable) {
//System.out.println("      DataChannel TcpStream out: writeEvioData: flush " + eType + " type event, don't force ");
                    flushEvents(false, false, true);
                }
                else {
//System.out.println("      DataChannel TcpStream out: writeEvioData: flush " + eType + " type event, FORCE");
                    if (rItem.getControlType() == ControlType.END) {
//System.out.println("      DataChannel TcpStream out: writeEvioData: call flushEvents for END");
                        flushEvents(true, true, false);
                    }
                    else {
//System.out.println("      DataChannel TcpStream out: writeEvioData: call flushEvents for non-END");
                        flushEvents(true, false, false);
                    }
                }
            }
            // If we're marshalling events into a single buffer before sending ...
            else {
//System.out.println("      DataChannel TcpStream out: writeEvioData: events into buf, written = " + eventsWritten +
//", closed = " + writer.isClosed());
                // If we've already written at least 1 event AND
                // (we have no more room in buffer OR we're changing event types),
                // write what we have.
                if ((eventsWritten > 0 && !writer.isClosed())) {
                    // If previous type not data ...
                    if (previousEventType != eType) {
//System.out.println("      DataChannel TcpStream out: writeEvioData *** switch types, call flush at current event count = " + currentEventCount);
                        flushEvents(false, false, false);
                    }
                    // Else if there's no more room or have exceeded event count limit ...
                    else if (!writer.hasRoom(rItem.getTotalBytes()) ||
                            (regulateBufferRate && (currentEventCount >= eventsPerBuffer))) {
//System.out.println("      DataChannel TcpStream out: writeEvioData *** no room so call flush at current event count = " + currentEventCount);
                        flushEvents(false, false, true);
                    }
//                    else {
//System.out.println("      DataChannel TcpStream out: writeEvioData *** PLENTY OF ROOM, has room = " +
//                           writer.hasRoom(rItem.getTotalBytes()));
//                    }
                    // Flush closes the writer so that the next "if" is true
                }

                boolean writerClosed = writer.isClosed();

                // Initialize writer if nothing written into buffer yet
                if (eventsWritten < 1 || writerClosed) {
                    // If we're here, we're writing the first event into the buffer.
                    // Make sure there's enough room for at least that one event.
                    if (rItem.getTotalBytes() > currentBuffer.capacity()) {
                        currentBBitem.ensureCapacity(rItem.getTotalBytes() + 1024);
                        currentBuffer = currentBBitem.getBuffer();
                    }

                    // Reinitialize writer
                    EmuUtilities.setEventType(bitInfo, eType);
//System.out.println("\nwriteEvioData: setBuffer, eventsWritten = " + eventsWritten + ", writer -> " +
//                           writer.getEventsWritten());
                    writer.setBuffer(currentBuffer, bitInfo, recordId++);
//System.out.println("\nwriteEvioData: after setBuffer, eventsWritten = " + writer.getEventsWritten());
                }

//System.out.println("      DataChannel TcpStream write: write ev into buf");
                // Write the new event ..
                ByteBuffer buf = rItem.getBuffer();
                if (buf != null) {
                    //System.out.print("b");
                    boolean fit = writer.writeEvent(buf);
                    if (!fit) {
                        // Our buffer is too small to fit even 1 event!
                        throw new EmuException("emu socket's buffer size must be increased in jcedit");
                    }
                }
                else {
                    EvioNode node = rItem.getNode();
                    if (node != null) {
                        // Since this is an emu-socket output channel,
                        // it is getting events from either the building thread of an event builder
                        // or the event generating thread of a simulated ROC. In both cases,
                        // any node passed into the following function has a backing buffer only
                        // used by that single node. (This is NOT like an input channel when an
                        // incoming buffer has several nodes all parsed from that one buffer).
                        // In this case, we do NOT need to "duplicate" the buffer to avoid interfering
                        // with other threads using the backing buffer's limit & position because there
                        // are no such threads. Thus, we set the duplicate arg to false which should
                        // generate fewer objects and save computing power/time.
                        //
                        // In reality, however, all data coming from EB or ROC will be in buffers and
                        // not in node form, so this method will never be called. This is just here
                        // for completeness.
                        boolean fit = writer.writeEvent(node, false, false);
                        if (!fit) {
                            // Our buffer is too small to fit even 1 event!
                            throw new EmuException("emu socket's buffer size must be increased in jcedit");
                        }
                    }
                    else {
                        throw new EmuException("no data to write");
                    }
                }
                currentEventCount++;
                rItem.releaseByteBuffer();
            }

            previousEventType = eType;
        }


        /** {@inheritDoc} */
        @Override
        public void run() {
            threadState = ThreadState.RUNNING;

            // Tell the world I've started
            startLatch.countDown();

            try {
                RingItem ringItem;
                EventType pBankType;
                ControlType pBankControlType;
                boolean gotPrestart = false;

                // Time in milliseconds for writing if time expired
                long timeout = 2000L;
                lastSendTime = System.currentTimeMillis();

                // The 1st event may be a user event or a prestart.
                // After the prestart, the next event may be "go", "end", or a user event.
                // The non-END control events are placed on ring 0 of all output channels.
                // The END event is placed in the ring in which the next data event would
                // have gone. The user events are placed on ring 0 of only the first output
                // channel.

                // Keep reading user & control events (all of which will appear in ring 0)
                // until the 2nd control event (go or end) is read.
                while (true) {
                    // Read next event
                    ringItem = getNextOutputRingItem(0);
                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    // If control event ...
                    if (pBankType == EventType.CONTROL) {
                        // if prestart ..
                        if (pBankControlType == ControlType.PRESTART) {
                            if (gotPrestart) {
                                throw new EmuException("got 2 prestart events");
                            }
logger.info("      DataChannel TcpStream out " + outputIndex + ": send prestart event");
                            gotPrestart = true;
                            writeEvioData(ringItem);
                        }
                        else {
                            if (!gotPrestart) {
                                throw new EmuException("prestart, not " + pBankControlType +
                                                               ", must be first control event");
                            }

                            if (pBankControlType != ControlType.GO &&
                                pBankControlType != ControlType.END) {
                                throw new EmuException("second control event must be go or end");
                            }

logger.info("      DataChannel TcpStream out " + outputIndex + ": send " + pBankControlType + " event");
                            writeEvioData(ringItem);

                            // Release and go to the next event
                            releaseCurrentAndGoToNextOutputRingItem(0);

                            // Done looking for the 2 control events
                            break;
                        }
                    }
                    // If user event ...
                    else if (pBankType == EventType.USER) {
logger.debug("      DataChannel TcpStream out " + outputIndex + ": send user event");
                        // Write user event
                        writeEvioData(ringItem);
                    }
                    // Only user and control events should come first, so error
                    else {
                        throw new EmuException(pBankType + " type of events must come after go event");
                    }

                    // Keep reading events till we hit go/end
                    releaseCurrentAndGoToNextOutputRingItem(0);
                }


                if (pBankControlType == ControlType.END) {
                    // END event automatically flushed in writeEvioData()
logger.info("      DataChannel TcpStream out: " + name + " got END event, quitting 1");
                    threadState = ThreadState.DONE;
                    return;
                }

                while (true) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) {
                            try {
                                Thread.sleep(5);
                            }
                            catch (InterruptedException e1) {
                            }
                        }
                        continue;
                    }

                    try {
                        ringItem = getNextOutputRingItem(ringIndex);
                    }
                    catch (InterruptedException e) {
                        return;
                    }

                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    try {
                        writeEvioData(ringItem);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        errorMsg.compareAndSet(null, "Cannot write data: " + e.getMessage());
                        throw e;
                    }

//logger.info("      DataChannel TcpStream out: send seq " + nextSequences[ringIndex] + ", release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);

                    // Do not go to the next ring if we got a control or user event.
                    // All prestart, go, & users go to the first ring. Just keep reading
                    // until we get to a built event. Then start keeping count so
                    // we know when to switch to the next ring.
                    if (outputRingCount > 1 && pBankControlType == null && !pBankType.isUser()) {
                        setNextEventAndRing();
//logger.info("      DataChannel TcpStream out, " + name + ": for seq " + nextSequences[ringIndex] + " SWITCH TO ring = " + ringIndex);
                    }

                    if (pBankControlType == ControlType.END) {
                        // END event automatically flushed in writeEvioData()
logger.info("      DataChannel TcpStream out: " + name + " got END event, quitting 2");
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
logger.info("      DataChannel TcpStream out: " + name + " got RESET cmd, quitting");
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // Time expired so send out events we have
//System.out.println("time = " + emu.getTime() + ", lastSendTime = " + lastSendTime);
                    long t = emu.getTime();
                    if (!regulateBufferRate && (t - lastSendTime > timeout)) {
System.out.println("TIME FLUSH ******************, time = " + t + ", last time = " + lastSendTime +
        ", delta = " + (t - lastSendTime));
                        flushExistingEvioData();
                    }
                }

            }
            catch (InterruptedException e) {
                logger.warn("      DataChannel TcpStream out: " + name + "  interrupted thd, quitting");
            }
            catch (Exception e) {
                e.printStackTrace();
                channelState = CODAState.ERROR;
System.out.println("      DataChannel TcpStream out:" + e.getMessage());
                emu.setErrorState("DataChannel TcpStream out: " + e.getMessage());
            }
        }

    }

}
