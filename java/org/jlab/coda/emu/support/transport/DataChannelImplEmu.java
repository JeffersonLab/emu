/*
 * Copyright (c) 2014, Jefferson Science Associates
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
 * This class implement a data channel which
 * gets-data-from/sends-data-to an Emu domain client/server.
 *
 * @author timmer
 * (4/23/2014)
 */
public class DataChannelImplEmu extends DataChannelAdapter {

    /** Data transport subclass object for Emu. */
    private final DataTransportImplEmu dataTransportImplEmu;

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
    private DataInputHelper[] dataInputThread;

    /** Thread to parse incoming data and merge it into 1 ring if coming from multiple sockets. */
    private ParserMerger parserMergerThread;

    /** Data input streams from TCP sockets. */
    private DataInputStream[] in;

    /** SocketChannels, up to 2, used to receive data. */
    private SocketChannel socketChannel[];

    /** Sockets, up to 2, used to receive data. */
    private Socket socket[];

    /** TCP receive buffer size in bytes. */
    private int tcpRecvBuf;

    /**
     * Node pools is used to get top-level EvioNode objects.
     * First index is socketCount, second is number of buffers
     * (64 total) in ByteBufferSupplys.
     */
    private EvioNodePool[][] nodePools;

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

    /**
     * In order to get a higher throughput for fast networks (e.g. infiniband),
     * this emu channel may use multiple sockets underneath. Defaults to 1.
     */
    private int socketCount;

    /** Number of sockets created so far. */
    private int socketsConnected;

    // Disruptor (RingBuffer)  stuff

    private long nextRingItem;

    /** Ring buffer holding ByteBuffers when using EvioCompactEvent reader for incoming events.
     *  One per socket (socketCount total). */
    protected ByteBufferSupply[] bbInSupply;


    /**
     * Constructor to create a new DataChannelImplEt instance. Used only by
     * {@link DataTransportImplEt#createChannel(String, Map, boolean, Emu, EmuModule, int)}
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
    DataChannelImplEmu(String name, DataTransportImplEmu transport,
                       Map<String, String> attributeMap, boolean input, Emu emu,
                       EmuModule module, int outputIndex)
            throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module, outputIndex);

        dataTransportImplEmu = transport;

        if (input) {
            logger.info("      DataChannel Emu: creating input channel " + name);
        }
        else {
            logger.info("      DataChannel Emu: creating output channel " + name);
        }

        // Use direct ByteBuffers or not, faster & more stable with non-direct
        direct = false;
        String attribString = attributeMap.get("direct");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("false") ||
                    attribString.equalsIgnoreCase("off") ||
                    attribString.equalsIgnoreCase("no")) {
                direct = false;
            }
        }

        // How many sockets to use underneath
        socketCount = 1;
        attribString = attributeMap.get("sockets");
        if (attribString != null) {
            try {
                socketCount = Integer.parseInt(attribString);
                if (socketCount < 1) {
                    socketCount = 1;
                }
            }
            catch (NumberFormatException e) {}
        }
logger.info("      DataChannel Emu: TCP socket count = " + socketCount);
        if (socketCount > 1) {
            logger.info("      DataChannel Emu: ************** FAT PIPE ***************");
        }

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
                    logger.info("      DataChannel Emu: set recvBuf to " + tcpRecvBuf);
                }
                catch (NumberFormatException e) {}
            }

//logger.info("      DataChannel Emu: recvBuf = " + tcpRecvBuf);

            // set "data dump" option on
            attribString = attributeMap.get("dump");
            if (attribString != null) {
                if (attribString.equalsIgnoreCase("true") ||
                    attribString.equalsIgnoreCase("on") ||
                    attribString.equalsIgnoreCase("yes")) {
                    dumpData = true;
                }
            }
            
//            dumpData = true;
//logger.info("      DataChannel Emu: dumpData = " + dumpData);


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
logger.info("      DataChannel Emu: noDelay = " + noDelay);

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
                    logger.info("      DataChannel Emu: sendBuf = " + tcpSendBuf);
                }
                catch (NumberFormatException e) {}
            }
//logger.info("      DataChannel Emu: set sendBuf to " + tcpSendBuf);

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
logger.info("      DataChannel Emu: sending on port " + sendPort);


            // Size of max buffer
            maxBufferSize = 4000000;
            attribString = attributeMap.get("maxBuf");
            if (attribString != null) {
                try {
                    maxBufferSize = Integer.parseInt(attribString);
                    if (maxBufferSize < 0) {
                        maxBufferSize = 4000000;
                    }
                }
                catch (NumberFormatException e) {}
            }
logger.info("      DataChannel Emu: max buf size = " + maxBufferSize);

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
logger.info("      DataChannel Emu: timeout = " + connectTimeout);

            // Emu domain preferred subnet in dot-decimal format
            preferredSubnet = null;
            attribString = attributeMap.get("subnet");
            if (attribString != null && cMsgUtilities.isDottedDecimal(attribString) == null) {
                preferredSubnet = null;
            }
            else {
                preferredSubnet = attribString;
            }
logger.info("      DataChannel Emu: over subnet " + preferredSubnet);
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
     * @param socketCount    total # of sockets data producer will be making
     * @param socketPosition position with respect to other data producers: 1, 2 ...
     *                       
     * @throws IOException   if exception dealing with socket or input stream
     */
    final void attachToInput(SocketChannel channel, int sourceId, int maxBufferSize,
                       int socketCount, int socketPosition) throws IOException {

        // Create a ring buffer full of empty ByteBuffer objects
        // in which to copy incoming data from client.
        // Using direct buffers works but performance is poor and fluctuates
        // quite a bit in speed.
        //
        // A DC with 13 inputs can quickly consume too much memory if we're not careful.
        // Put a limit on the total amount of memory used for all emu socket input channels.
        // Total limit is 1GB. This is probably the easiest way to figure out how many buffers to use.
        // Number of bufs must be a power of 2 with a minimum of 16 and max of 128.
        int channelCount = emu.getInputChannelCount();
        int numBufs = 1024000000 / (maxBufferSize * channelCount);
        numBufs = numBufs <  16 ?  16 : numBufs;
        numBufs = numBufs > 128 ? 128 : numBufs;
        // Reducing numBufs to 32 increases barrier.waitfor() time from .02% to .4% of EB time
        numBufs = 32;

        // Make power of 2, round up
        numBufs = EmuUtilities.powerOfTwo(numBufs, true);
//logger.info("\n\n      DataChannel Emu in: " + numBufs + " buffers in input supply, socketCount = " +
//                    socketCount + "\n\n");

        // Initialize things once
        if (socketChannel == null) {
            in = new DataInputStream[socketCount];
            socket = new Socket[socketCount];
            bbInSupply = new ByteBufferSupply[socketCount];
            socketChannel = new SocketChannel[socketCount];
            dataInputThread = new DataInputHelper[socketCount];
            parserMergerThread = new ParserMerger();
            nodePools = new EvioNodePool[socketCount][numBufs];
//logger.info("      DataChannel Emu in: allocated " + (socketCount * numBufs) + " node pools in array");
        }
        // If establishing multiple sockets for this single emu channel,
        // make sure their settings are compatible.
        else {
            if (socketCount != this.socketCount) {
                System.out.println("Bad socketCount: " + socketCount + ", != previous " + this.socketCount);
            }
            
            if (sourceId != this.sourceId) {
                System.out.println("Bad sourceId: " + sourceId + ", != previous " + this.sourceId);
            }
        }

        this.sourceId = sourceId;
        this.socketCount = socketCount;
        this.maxBufferSize = maxBufferSize;

        socketsConnected++;

        int index = socketPosition - 1;

        // Set socket options
        Socket sock = socket[index] = channel.socket();

        // Set TCP receive buffer size
        if (tcpRecvBuf > 0) {
            sock.setPerformancePreferences(0,0,1);
            sock.setReceiveBufferSize(tcpRecvBuf);
        }

        // Use buffered streams for efficiency
        socketChannel[index] = channel;
        in[index] = new DataInputStream(new BufferedInputStream(sock.getInputStream()));

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

//logger.info("      DataChannel Emu in: seq release of buffers = " + sequentialRelease);

        // Create the EvioNode pools - each socket gets numBuf number of pools -
        // each of which contain 3500 EvioNodes to begin with. These are used for
        // the nodes of each event.
        for (int i = 0; i < numBufs; i++) {
            nodePools[index][i] = new EvioNodePool(3500);
        }
//logger.info("      DataChannel Emu in: created " + (numBufs) + " node pools for socket " + index + ", " + name());

        bbInSupply[index] = new ByteBufferSupply(numBufs, maxBufferSize,
                                                 ByteOrder.BIG_ENDIAN, direct,
                                                 sequentialRelease, nodePools[index]);

//logger.info("      DataChannel Emu in: seq release = " + sequentialRelease);

logger.info("      DataChannel Emu in: connection made from " + name);

        // Start thread to handle socket input
        dataInputThread[index] = new DataInputHelper(socketPosition);
        dataInputThread[index].start();

        // If this is the last socket, make sure all threads are started up before proceeding
        if (socketsConnected == socketCount) {
            parserMergerThread.start();
            for (int i=0; i < socketCount; i++) {
                dataInputThread[i].waitUntilStarted();
            }
//logger.info("      DataChannel Emu in: last connection made, parser thd started, input threads running");
        }
    }


    /**
     * Open a client output channel to the EmuSocket server.
     * @throws cMsgException if communication problems with server.
     */
    private final void openOutputChannel() throws cMsgException {

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
    private final void multicastOutputChannel() throws cMsgException {

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

        if (socketCount > 1) {
            builder.append("&sockets=").append(socketCount);
        }

        if (noDelay) {
            builder.append("&noDelay");
        }

 logger.info("      DataChannel Emu out: will connect to server w/ multicast UDL = " + builder.toString());
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
    private final void directOutputChannel() throws cMsgException {

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

            if (socketCount > 1) {
                builder.append("&sockets=").append(socketCount);
            }

            if (noDelay) {
                builder.append("&noDelay");
            }

            // This connection will contain "sockCount" number of sockets
            // which are all used to send data.
            try {
logger.info("      DataChannel Emu out: will directly connect to server w/ UDL = " + builder.toString());
                emuDomain = new cMsg(builder.toString(), name, "emu domain client");
                emuDomain.connect();
                startOutputThread();
                return;
            }
            catch (cMsgException e) {
                logger.info("      DataChannel Emu out: could not connect to server at " + ip);
                builder.delete(0, builder.length());
                continue;
            }
        }

        throw new cMsgException("Cannot connect to any given IP addresses directly");
    }


    private final void closeOutputChannel() throws cMsgException {
        if (input) return;
        // flush and close sockets
        emuDomain.disconnect();
    }


    private final void closeInputSockets() {
        if (!input) return;
//        logger.info("      DataChannel Emu in: close input sockets from " + name);

        try {
            for (int i=0; i < socketCount; i++) {
                in[i].close();
                // Will close socket, associated channel & streams
                socket[i].close();
            }
        }
        catch (IOException e) {}
    }


    /** {@inheritDoc} */
    public TransportType getTransportType() {
        return TransportType.EMU;
    }


    /** {@inheritDoc} */
    public int getInputLevel() {
        // Pick out the fullest of the socket buffer supplies
        int supplyLevel, level = 0;
        for (int i = 0; i < socketCount; i++)  {
            supplyLevel = bbInSupply[i].getFillLevel();
            level = level > supplyLevel ? level : supplyLevel;
        }
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
            emu.setErrorState("      DataChannel Emu out: " + e.getMessage());
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
//logger.debug("      DataChannel Emu: end/reset(), interrupt parser/merger thread");
            parserMergerThread.interrupt();
            try {Thread.sleep(10);}
            catch (InterruptedException e) {}

            for (int i=0; i < socketCount; i++) {
                if (dataInputThread[i] == null) {
                    continue;
                }
//logger.debug("      DataChannel Emu: end/reset(), interrupt input thread " + i);
                dataInputThread[i].interrupt();
            }
        }

        if (dataOutputThread != null) {
//logger.debug("      DataChannel Emu: end/reset(), interrupt main output thread ");
            dataOutputThread.interrupt();

            for (int i=0; i < socketCount; i++) {
//logger.debug("      DataChannel Emu: end/reset(), interrupt output thread " + i);
                dataOutputThread.sender[i].endThread();
            }
        }
    }

    
    /**
     * Try joining all threads, up to 1 sec each.
     */
    private void joinThreads() {
        if (dataInputThread != null) {
            try {parserMergerThread.join(1000);}
            catch (InterruptedException e) {}

//logger.debug("      DataChannel Emu: end/reset(), joined parser/merger thread");

            for (int i=0; i < socketCount; i++) {
                if (dataInputThread[i] == null) {
                    continue;
                }

                try {dataInputThread[i].join(1000);}
                catch (InterruptedException e) {}

//logger.debug("      DataChannel Emu: end/reset(), joined input thread " + i);
            }
        }

        if (dataOutputThread != null) {

            try {dataOutputThread.join(1000);}
            catch (InterruptedException e) {}

//logger.debug("      DataChannel Emu: end/reset(), joined main output thread ");

            for (int i=0; i < socketCount; i++) {
                try {dataOutputThread.sender[i].join(1000);}
                catch (InterruptedException e) {}
//logger.debug("      DataChannel Emu: end/reset(), joined output thread " + i);
            }
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
            for (int i=0; i < socketCount; i++) {
                dataInputThread[i] = null;
            }
            dataInputThread = null;
            parserMergerThread = null;
            closeInputSockets();
        }

        if (dataOutputThread != null) {
            for (int i=0; i < socketCount; i++) {
                dataOutputThread.sender[i] = null;
            }
            dataOutputThread = null;

            try {
logger.debug("      DataChannel Emu: end(), close output channel " + name);
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
            for (int i=0; i < socketCount; i++) {
                dataInputThread[i] = null;
            }
            dataInputThread = null;
            parserMergerThread = null;
            closeInputSockets();
        }

        if (dataOutputThread != null) {
            for (int i=0; i < socketCount; i++) {
                dataOutputThread.sender[i] = null;
            }
            dataOutputThread = null;

            try {
logger.debug("      DataChannel Emu: end(), close output channel " + name);
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


    private final void startOutputThread() {
        dataOutputThread = new DataOutputHelper();
        dataOutputThread.start();
        dataOutputThread.waitUntilStarted();
    }


    /**
     * Class used to get data over network and put into ring buffer.
     * There is one of these for each of the "socketCount" number of TCP sockets.
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

        private final int socketPosition;

        /** Constructor. */
        DataInputHelper(int socketPosition) {
            super(emu.getThreadGroup(), name() + "_data_in");
            int socketIndex = socketPosition - 1;
            this.socketPosition = socketPosition;
            inStream = in[socketIndex];
            bbSupply = bbInSupply[socketIndex];
            sockChannel = socketChannel[socketIndex];
        }


        /** A single waiter can call this method which returns when thread was started. */
        private final void waitUntilStarted() {
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
            int cmd=-1, size=-1;
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
                            logger.warn("      DataChannel Emu in: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }


                    // Sets the producer sequence
                    item = bbSupply.get();
//System.out.println("      DataChannel Emu in: GOT pool " + item.myIndex);

                    // First read the command & size with one read, into a long.
                    // These 2, 32-bit ints are sent in network byte order, cmd first.
                    // Reading a long assumes big endian so cmd, which is sent
                    // first, should appear in most significant bytes.
                    if (direct) {
                        sockChannel.read(wordCmdBuf);
                        cmd  = ibuf.get();
                        size = ibuf.get();
                        ibuf.position(0);
                        wordCmdBuf.position(0);

                        item.ensureCapacity(size);
                        buf = item.getBuffer();
                        buf.limit(size);

                        // Be sure to read everything
                        while (buf.hasRemaining()) {
                            sockChannel.read(buf);
                        }
                        buf.flip();
                    }
                    else {
                        word = inStream.readLong();
                        cmd  = (int) ((word >>> 32) & 0xffL);
                        size = (int)   word;   // just truncate for lowest 32 bytes
                        item.ensureCapacity(size);
                        buf = item.getBuffer();
                        buf.limit(size);

                        inStream.readFully(item.getBuffer().array(), 0, size);
                    }

                    bbSupply.publish(item);

                    // We just received the END event
                    if (cmd == cMsgConstants.emuEvioEndEvent) {
System.out.println("      DataChannel Emu in: " + name + ", got END event on sock " + socketPosition + ", exit reading thd");
                        return;
                    }
                }
            }
            catch (InterruptedException e) {
                logger.warn("      DataChannel Emu in: " + name + ", interrupted, exit reading thd");
            }
            catch (AsynchronousCloseException e) {
                logger.warn("      DataChannel Emu in: " + name + ", socket closed, exit reading thd");
            }
            catch (EOFException e) {
                // Assume that if the other end of the socket closes, it's because it has
                // sent the END event and received the end() command.
                logger.warn("      DataChannel Emu in: " + name + ", other end of socket closed for sock " +
                                    socketPosition + ", exit reading thd");
            }
            catch (Exception e) {
                if (haveInputEndEvent) {
System.out.println("      DataChannel Emu in: " + name +
                   ", exception but aleady have END event, so exit reading thd");
                    return;
                }
                channelState = CODAState.ERROR;
                // If error msg already set, this will not
                // set it again. It will send it to rc.
                String errString = "DataChannel Emu in: error reading " + name;
                if (e.getMessage() != null) {
                    errString += ' ' + e.getMessage();
                }
                emu.setErrorState(errString);
            }
        }


    }


    /**
     * Class to consume all buffers read from all sockets, parse them into evio events,
     * and merge this data from multiple sockets by placing them into this
     * channel's single ring buffer.
     */
    private final class ParserMerger extends Thread {

        /** Keep track of record ids coming in to make sure they're sequential. */
        private int expectedRecordId = 1;

        /** Object used to read/parse incoming evio data. */
        private EvioCompactReaderUnsync reader;


        /** Constructor. */
        ParserMerger() {
            super(emu.getThreadGroup(), name() + "_parser_merger");
        }
        

        public void run() {
            try {
                // Simplify things when there's only 1 socket for better performance
                if (socketCount == 1) {
                    ByteBufferSupply bbSupply = bbInSupply[0];
                    while (true) {
                        // Sets the consumer sequence
                        ByteBufferItem item = bbSupply.consumerGet();
                        if (parseToRing(item, bbSupply)) {
                            logger.info("      DataChannel Emu in: 1 quit parser/merger thread for END event from " + name);
                            break;
                        }
                    }
                }
                else {
                    int sockIndex = 0;

                    toploop:
                    while (true) {
                        for (ByteBufferSupply bbSupply : bbInSupply) {
                            // Alternate by getting one buffer from each supply in order
                            ByteBufferItem item = bbSupply.consumerGet();
                            if (parseToRing(item, bbSupply)) {
                                logger.info("      DataChannel Emu in: 2 quit parser/merger thread for END event from " + name);
                                break toploop;
                            }

                            sockIndex = (sockIndex + 1) % 2;
                            // This buffer will be released when EB/ER is done with it
                        }
                    }
                }
            }
            catch (InterruptedException e) {
//                logger.warn("      DataChannel Emu in: " + name +
//                            " parserMerger thread interrupted, quitting ####################################");
            }
            catch (EvioException e) {
                // Bad data format or unknown control event.
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel Emu in: " + e.getMessage());
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
        private final boolean parseToRing(ByteBufferItem item, ByteBufferSupply bbSupply)
                throws EvioException, InterruptedException {

            RingItem ri;
            EvioNode node;
            boolean hasFirstEvent, isUser=false;
            ControlType controlType = null;
            EvioNodePool pool;

            // Get buffer from an item from ByteBufferSupply - one per channel
            ByteBuffer buf = item.getBuffer();

            try {
                // Pool of EvioNodes associated with this buffer
                pool = (EvioNodePool)item.getMyObject();
                // Each pool must be reset only once!
                pool.reset();

                if (reader == null) {
                    reader = new EvioCompactReaderUnsync(buf, pool);
                }
                else {
                    reader.setBuffer(buf, pool);
                }
            }
            catch (EvioException e) {
                System.out.println("      DataChannel Emu in: data NOT evio v4 format 1");
                e.printStackTrace();
                Utilities.printBufferBytes(buf, 0, 80, "BAD BUFFER TO PARSE");
                throw e;
            }

            // First block header in buffer
            BlockHeaderV4 blockHeader = reader.getFirstBlockHeader();
            if (blockHeader.getVersion() < 4) {
                throw new EvioException("Data not in evio v4 but in version " +
                                                blockHeader.getVersion());
            }

            hasFirstEvent = blockHeader.hasFirstEvent();

            EventType eventType = EventType.getEventType(blockHeader.getEventType());
            if (eventType == null || !eventType.isEbFriendly()) {
                throw new EvioException("bad evio format or improper event type");
            }

            recordId = blockHeader.getNumber();

            // Check record for sequential record id
            expectedRecordId = Evio.checkRecordIdSequence(recordId, expectedRecordId, true,
                                                          eventType, DataChannelImplEmu.this);

            // Each PayloadBuffer contains a reference to the buffer it was
             // parsed from (buf).
             // This cannot be released until the module is done with it.
             // Keep track by counting users (# events parsed from same buffer).
             int eventCount = reader.getEventCount();
             item.setUsers(eventCount);

//    System.out.println("      DataChannel Emu in: block header, event type " + eventType +
//                       ", recd id = " + recordId + ", event cnt = " + eventCount);

             for (int i = 1; i < eventCount + 1; i++) {
                 // Type may change if there are mixed types in record
                 EventType evType = eventType;

                 nextRingItem = ringBufferIn.nextIntr(1);
                 ri = ringBufferIn.get(nextRingItem);

                 if (isER) {
                     // Don't need to parse all bank headers, just top level.
                     node = reader.getEvent(i);
                 }
                 else {
                     // getScannedEvent will clear child and allNodes lists
                     node = reader.getScannedEvent(i, pool);
                 }

                 // This should NEVER happen
                 if (node == null) {
System.out.println("      DataChannel Emu in: WARNING, event count = " + eventCount +
                   " but get(Scanned)Event(" + i + ") is null - evio parsing bug");
                     continue;
                 }

                 // Complication: from the ROC, we'll be receiving USER events
                 // mixed in with and labeled as ROC Raw events. Check for that
                 // and fix it.
                 if (evType == EventType.ROC_RAW) {
//logger.info("      DataChannel Emu in: got ROC Raw event type from " + name);
                     if (Evio.isUserEvent(node)) {
                         isUser = true;
                         evType = EventType.USER;
                         if (hasFirstEvent) {
                             System.out.println("      DataChannel Emu in: " + name + "  FIRST event from ROC RAW");
                         }
                         else {
                             System.out.println("      DataChannel Emu in: " + name + " USER event from ROC RAW");
                         }
                     }
                     else {
                         // Pick this raw data event apart a little
                         if (!node.getDataTypeObj().isBank()) {
                             DataType eventDataType = node.getDataTypeObj();
                             throw new EvioException("ROC raw record contains " + eventDataType +
                                                     " instead of banks (data corruption?)");
                         }
                     }
                 }
                 else if (evType == EventType.CONTROL) {
                     // Find out exactly what type of control event it is
                     // (May be null if there is an error).
                     controlType = ControlType.getControlType(node.getTag());
logger.info("      DataChannel Emu in: got " + controlType + " event from " + name);
                     if (controlType == null) {
                         logger.info("      DataChannel Emu in: found unidentified control event");
                         throw new EvioException("Found unidentified control event");
                     }
                 }
                 else if (evType == EventType.USER) {
                     isUser = true;
                     if (hasFirstEvent) {
                         logger.info("      DataChannel Emu in: " + name + " got FIRST event");
                     }
                     else {
                         logger.info("      DataChannel Emu in: " + name + " got USER event");
                     }
                 }
                 else if (evType == EventType.MIXED) {
                     // Mix of event types.
                     // Can occur for combo of user, ROC RAW and possibly control events.
                     // Only occurs when a user inserts a User event during the End transition.
                     // What happens is that the User Event gets put into a EVIO Record which can
                     // also contain ROC RAW events. The evio record gets labeled as containing
                     // mixed events.
                     //
                     // This will NOT occur in ER, so headers are all parsed at this point.
                     // Look at the very first header, second word.
                     // num = 0  --> it's a control or User event (tag tells which):
                     //          0xffd0 <= tag <= 0xffdf --> control event
                     //          else                    --> User event
                     // num > 0  --> block level for ROC RAW

                     int num = node.getNum();
                     if (num == 0) {
                         int tag = node.getTag();
                         if (ControlType.isControl(tag)) {
                             controlType = ControlType.getControlType(tag);
                             evType = EventType.CONTROL;
                         }
                         else {
                             isUser = true;
                             evType = EventType.USER;
                         }
                     }
                     else {
                         evType = EventType.ROC_RAW;
                         // Pick this raw data event apart a little
                         if (!node.getDataTypeObj().isBank()) {
                             DataType eventDataType = node.getDataTypeObj();
                             throw new EvioException("ROC raw record contains " + eventDataType +
                                     " instead of banks (data corruption?)");
                         }
                     }
                 }
                 else {
                     // Physics or partial physics event must have BANK as data type
                     if (!node.getDataTypeObj().isBank()) {
                         DataType eventDataType = node.getDataTypeObj();
                         throw new EvioException("physics record contains " + eventDataType +
                                                 " instead of banks (data corruption?)");
                     }
                 }

                 if (dumpData) {
                     bbSupply.release(item);

                     // Handle end event ...
                     if (controlType == ControlType.END) {
//TODO: what to do about having got the next ringItem
                         // There should be no more events coming down the pike so
                         // go ahead write out existing events and then shut this
                         // thread down.
                         haveInputEndEvent = true;
                         // Run callback saying we got end event
                         if (endCallback != null) endCallback.endWait();
                         break;
                     }

                     // Send control events on to module so we can prestart, go and take data
                     if (!evType.isBuildable()) {
                         ri.setAll(null, null, node, evType, controlType,
                                   isUser, hasFirstEvent, id, recordId, sourceId,
                                   1, name, item, bbSupply);

                         ringBufferIn.publish(nextRingItem);
                     }

                     continue;
                 }

                 // Set & reset all parameters of the ringItem
                 if (evType.isBuildable()) {
                     ri.setAll(null, null, node, evType, controlType,
                               isUser, hasFirstEvent, id, recordId, sourceId,
                               node.getNum(), name, item, bbSupply);
                 }
                 else {
                     ri.setAll(null, null, node, evType, controlType,
                               isUser, hasFirstEvent, id, recordId, sourceId,
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

        /** Index into sender array to SocketSender currently being used. */
        private int currentSenderIndex;

        /** Entry in evio block header. */
        private final BitSet bitInfo = new BitSet(24);

        /** Type of last event written out. */
        private EventType previousEventType;

        /** What state is this thread in? */
        private volatile ThreadState threadState;

        /** Time at which events were sent over socket. */
        private volatile long lastSendTime;

        /** Sender threads to send data over network. */
        private final SocketSender[] sender;

        /** One ByteBufferSupply for each sender/socket. */
        private final ByteBufferSupply[] bbOutSupply;

        /** When regulating output buffer flow, the last time a buffer was sent. */
        private long lastBufSendTime;

        /** When regulating output buffer flow, the current
         * number of physics events written to buffer. */
        private int currentEventCount;

        /** Used to implement a precision sleep when regulating output data rate. */
        private final long SLEEP_PRECISION = TimeUnit.MILLISECONDS.toNanos(2);
        
         /** Used to implement a precision sleep when regulating output data rate. */
        private final long SPIN_YIELD_PRECISION = TimeUnit.MICROSECONDS.toNanos(2);



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


            SocketSender(ByteBufferSupply supply, int socketIndex) {
                super(emu.getThreadGroup(), name() + "_sender_"+ socketIndex);

                this.supply = supply;
                // Need do this only once
                outGoingMsg = new cMsgMessageFull();
                // Message format
                outGoingMsg.setUserInt(cMsgConstants.emuEvioFileFormat);
                // Tell cmsg which socket to use
                outGoingMsg.setSysMsgId(socketIndex);
            }

            /**
             * Kill this thread which is sending messages/data to other end of emu socket.
             */
            final void endThread() {
//System.out.println("SocketSender: killThread, set flag, interrupt");
                killThd = true;
                this.interrupt();
            }


            /**
             * Send the events currently marshalled into a single buffer.
             * @force if true, force data over socket
             */
            public void run() {
                boolean isEnd = false;

                while (true) {
                    if (killThd) {
//System.out.println("SocketSender thread told to return");
                        return;
                    }

                    try {
                        // Get a buffer filled by the other thread
                        ByteBufferItem item = supply.consumerGet();
                        ByteBuffer buf = item.getBufferAsIs();

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
                        supply.release(item);
                    }
                    catch (InterruptedException e) {
System.out.println("SocketSender thread interrupted");
                        return;
                    }
                    catch (Exception e) {
                        channelState = CODAState.ERROR;
                        emu.setErrorState("DataChannel Emu out: " + e.getMessage());
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

            sender = new SocketSender[socketCount];
            bbOutSupply = new ByteBufferSupply[socketCount];

            for (int i=0; i < socketCount; i++) {
                // A mini ring of buffers, 16 is the best size
                bbOutSupply[i] = new ByteBufferSupply(16, maxBufferSize, byteOrder,
                                                      direct, orderedRelease);

                // Start up sender thread
                sender[i] = new SocketSender(bbOutSupply[i], i);
                sender[i].start();
            }

            // Create writer to write events into file format
            try {

                // Start out with a single buffer from the first supply just created
                currentSenderIndex = 0;
                currentBBitem = bbOutSupply[currentSenderIndex].get();
                currentBuffer = currentBBitem.getBuffer();

                writer = new EventWriterUnsync(currentBuffer);
                writer.close();
            }
            catch (InterruptedException e) {/* never happen */}
            catch (EvioException e) {/* never happen */}
        }


        /** A single waiter can call this method which returns when thread was started. */
        private final void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
            }
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
        private final void flushEvents(boolean force, boolean userBool, boolean isData)
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
            currentBuffer.flip();
            bbOutSupply[currentSenderIndex].publish(currentBBitem);

            // Get another buffer from the supply so writes can continue.
            // It'll block if none available.

            // Index must switch between sockets.
            // The following is the equivalent of the mod operation
            // but is much faster (x mod 2^n == x & (2^n - 1))
            if (socketCount > 1) {
                //currentSenderIndex = (currentSenderIndex + 1) & 1; // works for 2 sockets only
                currentSenderIndex = (currentSenderIndex + 1) % socketCount;
            }

            currentBBitem = bbOutSupply[currentSenderIndex].get();
            currentBuffer = currentBBitem.getBuffer();
        }


        /**
         * Flush already written events over sockets.
         * This is only called when data rate is slow and data must
         * be forced over the network.
         * @throws InterruptedException
         */
        private final void flushExistingEvioData() throws InterruptedException {
            // Don't write nothin'
            if (currentEventCount == 0) {
//System.out.println("      DataChannel Emu out: flushExistingData: nothing to flush ......");
                return;
            }

            if (previousEventType.isBuildable()) {
//System.out.println("      DataChannel Emu out: flushExistingData: flush data");
                flushEvents(true, false, true);
            }
            else {
//System.out.println("      DataChannel Emu out: flushExistingData: flush control/user event");
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
         * @throws EmuException if no data to write
         * @throws InterruptedException
         */
        private final void writeEvioData(RingItem rItem)
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

                // If we're here, we're writing the first event into the buffer.
                // Make sure there's enough room for that one event.
                if (rItem.getTotalBytes() > currentBuffer.capacity()) {
                    currentBBitem.ensureCapacity(rItem.getTotalBytes() + 1024);
                    currentBuffer = currentBBitem.getBuffer();
                }

                // Write the event ..
                EmuUtilities.setEventType(bitInfo, eType);
                if (rItem.isFirstEvent()) {
                    EmuUtilities.setFirstEvent(bitInfo);
                }
                writer.setBuffer(currentBuffer, bitInfo, blockNum);

                // Unset first event for next round
                EmuUtilities.unsetFirstEvent(bitInfo);

                ByteBuffer buf = rItem.getBuffer();
                if (buf != null) {
                    try {
//System.out.println("      DataChannel Emu out: single ev buf, pos = " + buf.position() +
//", lim = " + buf.limit() + ", cap = " + buf.capacity());
                        writer.writeEvent(buf);
//                        Utilities.printBufferBytes(buf, 0, 20, "control?");
                    }
                    catch (Exception e) {
System.out.println("      DataChannel Emu out: single ev buf, pos = " + buf.position() +
                   ", lim = " + buf.limit() + ", cap = " + buf.capacity());
                        Utilities.printBufferBytes(buf, 0, 20, "bad END?");
                        throw e;
                    }
                }
                else {
                    EvioNode node = rItem.getNode();
                    if (node != null) {
                        writer.writeEvent(node, false);
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
//System.out.println("      DataChannel Emu out: writeEvioData: flush " + eType + " type event, don't force ");
                    flushEvents(false, false, true);
                }
                else {
//System.out.println("      DataChannel Emu out: writeEvioData: flush " + eType + " type event, FORCE");
                    if (rItem.getControlType() == ControlType.END) {
                        flushEvents(true, true, false);
                    }
                    else {
                        flushEvents(true, false, false);
                    }
                }
            }
            // If we're marshalling events into a single buffer before sending ...
            else {
                // If we've already written at least 1 event AND
                // (we have no more room in buffer OR we're changing event types),
                // write what we have.
                if ((eventsWritten > 0 && !writer.isClosed())) {
                    // If previous type not data ...
                    if (previousEventType != eType) {
//System.out.println("      DataChannel Emu out: switch types, call flush at current event count = " + currentEventCount);
                        flushEvents(false, false, false);
                    }
                    // Else if there's no more room or have exceeded event count limit ...
                    else if (!writer.hasRoom(rItem.getTotalBytes()) ||
                            (regulateBufferRate && (currentEventCount >= eventsPerBuffer))) {
//System.out.println("      DataChannel Emu out: call flush at current event count = " + currentEventCount);
                        flushEvents(false, false, true);
                    }
                    // Flush closes the writer so that the next "if" is true
                }

                // Initialize writer if nothing written into buffer yet
                if (eventsWritten < 1 || writer.isClosed()) {
                    // If we're here, we're writing the first event into the buffer.
                    // Make sure there's enough room for at least that one event.
                    if (rItem.getTotalBytes() > currentBuffer.capacity()) {
                        currentBBitem.ensureCapacity(rItem.getTotalBytes() + 1024);
                        currentBuffer = currentBBitem.getBuffer();
                    }

                    // Init writer
                    EmuUtilities.setEventType(bitInfo, eType);
                    writer.setBuffer(currentBuffer, bitInfo, recordId++);
//System.out.println("      DataChannel Emu write: init writer");
                }

//System.out.println("      DataChannel Emu write: write ev into buf");
                // Write the new event ..
                ByteBuffer buf = rItem.getBuffer();
                if (buf != null) {
                    writer.writeEvent(buf);
                }
                else {
                    EvioNode node = rItem.getNode();
                    if (node != null) {
                        writer.writeEvent(node, false);
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
logger.info("      DataChannel Emu out " + outputIndex + ": send prestart event");
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

logger.info("      DataChannel Emu out " + outputIndex + ": send " + pBankControlType + " event");
                            writeEvioData(ringItem);

                            // Release and go to the next event
                            releaseCurrentAndGoToNextOutputRingItem(0);

                            // Done looking for the 2 control events
                            break;
                        }
                    }
                    // If user event ...
                    else if (pBankType == EventType.USER) {
logger.debug("      DataChannel Emu out " + outputIndex + ": send user event");
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
logger.info("      DataChannel Emu out: " + name + " got END event, quitting 1");
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

//logger.info("      DataChannel Emu out: send seq " + nextSequences[ringIndex] + ", release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);

                    // Do not go to the next ring if we got a control or user event.
                    // All prestart, go, & users go to the first ring. Just keep reading
                    // until we get to a built event. Then start keeping count so
                    // we know when to switch to the next ring.
                    if (outputRingCount > 1 && pBankControlType == null && !pBankType.isUser()) {
                        setNextEventAndRing();
//logger.info("      DataChannel Emu out, " + name + ": for seq " + nextSequences[ringIndex] + " SWITCH TO ring = " + ringIndex);
                    }

                    if (pBankControlType == ControlType.END) {
                        // END event automatically flushed in writeEvioData()
logger.info("      DataChannel Emu out: " + name + " got END event, quitting 2");
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
logger.info("      DataChannel Emu out: " + name + " got RESET cmd, quitting");
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // Time expired so send out events we have
//System.out.println("time = " + emu.getTime() + ", lastSendTime = " + lastSendTime);
                    if (!regulateBufferRate && (emu.getTime() - lastSendTime > timeout)) {
//System.out.println("TIME FLUSH ******************");
                        flushExistingEvioData();
                    }
                }

            }
            catch (InterruptedException e) {
                logger.warn("      DataChannel Emu out: " + name + "  interrupted thd, quitting");
            }
            catch (Exception e) {
                e.printStackTrace();
                channelState = CODAState.ERROR;
System.out.println("      DataChannel Emu out:" + e.getMessage());
                emu.setErrorState("DataChannel Emu out: " + e.getMessage());
            }
        }

    }

}
