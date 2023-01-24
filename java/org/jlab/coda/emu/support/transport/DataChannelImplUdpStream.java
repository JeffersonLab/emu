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

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.*;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousCloseException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;


import org.jlab.coda.emu.support.data.tuple.*;


/**
 * <p>
 * This class implement a data channel which
 * gets data from a hardware front-end via UDP packets.
 * This class acts as "client" which does a connect to the front-end
 * which acts as a "server" and does a bind.
 * This will be streaming format data.</p>
 *
 * To preferentially use IPv6, give following command line option to JVM:
 * -Djava.net.preferIPv6Addresses=true
 * Java sockets use either IPv4 or 6 automatically depending on what
 * kind of addresses are used. No need (or possibility) for setting
 * this explicitly.
 *
 * @author timmer
 * (3/10/2022)
 */
public class DataChannelImplUdpStream extends DataChannelAdapter {

    /** Data transport subclass object for Emu. */
    private final DataTransportImplUdpStream dataTransportImplUdpStream;

    /** Do we pause the dataThread? */
    private volatile boolean pause;

    /** Read END event from input ring. */
    private volatile boolean haveInputEndEvent;

    /**
     * Biggest chunk of data sent by data producer.
     * Allows good initial value of ByteBuffer size.
     * Also used for fake ROC output.
     */
    private int bufSize;

    // FROM ORACLE about use of IPv4 and IPv6:
    // -  There should be no change in Java application code if everything has been done appropriately.
    //    For example, there are no direct references to IPv4 literal addresses; instead, host names are used.
    // -  All the address or socket type information is encapsulated in the Java networking API.
    // -  Through setting system properties, address type and/or socket type preferences can be set.
    // -  For new applications IPv6-specific new classes and APIs can be used.
    // see  https://docs.oracle.com/javase/8/docs/technotes/guides/net/ipv6_guide/

    // OUTPUT

    /** Host to send packets to. */
    private String destHost;

    /** Address to send packets to. */
    private InetAddress destAddr;

    /** UDP send buffer size. */
    private int sendBufSize;

    /** Thread used to write outgoing data. */
    private DataOutputHelper dataOutputThread;

    /** Are we sending UDP packets to the EJFAT load balancer? */
    private boolean useEjfatLoadBalancer;

    /** Are we sending ERSAP style reassembly header? */
    private boolean useErsapReHeader;

    /** FPGA Load Balancer reassembly protocol. */
    private final int lbProtocol = 1;
    /** FPGA Load Balancer reassembly version. LB only works for version = 2. */
    private final int lbVersion = 2;
    /** VTP reassembly version */
    private final int reVersion = 1;

    /** Size of EJFAT load balancing header, by default, nothing */
    private int LB_HEADER_BYTES = 0;
    /** Size of UDP CODA reassembly header. */
    private int RE_HEADER_BYTES = 8;
    /** Size of all headers. By default, do NOT include EJFAT header. */
    private int HEADER_BYTES = RE_HEADER_BYTES;
    /** Default MTU to be used if it cannot be found programmatically. */
    private final int DEFAULT_MTU = 1400;

    // INPUT

    /** Socket used to receive data. */
    private DatagramSocket inSocket;

    /** Threads used to read incoming data. */
    private DataInputHelper dataInputThread;

    /** Thread to parse incoming data and merge it into 1 ring if coming from multiple sockets. */
    private ParserMerger parserMergerThread;

    /** UDP receive buffer size. */
    private int recvBufSize;

    /** UDP port that the VTP/ROC binds to and that we must connect to. */
    private final int port;

    /** Supply of ByteBuffers in which each stores recontructed data from UDP packets. */
    private ByteBufferSupply bbSupply;

    /**
     * A node pool is used to get top-level EvioNode objects.
     * The index is the number of buffers in the ByteBufferSupply.
     */
    private EvioNodePool[] nodePools;

//    /** Use the evio block header's block number as a record id. */
//    private int recordId = 1;

//    // TODO: In Dave's scheme, record id identifies the lower bits of a specific time frame,
//    // TODO: So separate the idea of evio block number from record id!!
//    // TODO: In fact, block number can always be 0 since each buffer is
//    // isolated from the others. AT LEAST I think so.
//    private final int blockNum = 1;

    /** Do not use direct ByteBuffers, only those with a backing array. */
    private final boolean direct = false;

    // Disruptor (RingBuffer)  stuff

    private long nextRingItem;



    /**
     * Constructor to create a new DataChannelImplUdpStream instance. Used only by
     * {@link DataTransportImplUdpStream#createChannel(String, Map, boolean, Emu, EmuModule, int, int)}
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
    DataChannelImplUdpStream(String name, DataTransportImplUdpStream transport,
                             Map<String, String> attributeMap , boolean input, Emu emu,
                             EmuModule module, int outputIndex)
            throws DataTransportException {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module, outputIndex);

        dataTransportImplUdpStream = transport;

        if (input) {
            logger.info("    DataChannel UDP: creating input channel " + name);
        }
        else {
            logger.info("    DataChannel UDP: creating output channel " + name);
        }

        String attribString;

        // We need a supplies of buffers - 1 supply for each data source.
        // First we need to know how big to make them to start with.
        // They'll be expanded as needed.
        // TODO: see how many bytes Dave A. sends at once

        bufSize = 100000;
        attribString = attributeMap.get("bufSize");
        if (attribString != null) {
            try {
                bufSize = Integer.parseInt(attribString);
                if (bufSize < 9000) {
                    // Each buffer should hold at least 1 Jumbo frames worth of data
                    bufSize = 9000;
                }
            }
            catch (NumberFormatException e) {
                throw new DataTransportException("bad port number in config");
            }
        }
        logger.info("    DataChannel UDP stream: single buffer byte size = " + bufSize);


        // Either destination port to send UDP datagrams to, or port to receive on
        attribString = attributeMap.get("port");
        if (attribString != null) {
            try {
                port = Integer.parseInt(attribString);
                if (port < 1024 || port > 65535) {
                    throw new DataTransportException("out of range port number in config");
                }
            }
            catch (NumberFormatException e) {
                throw new DataTransportException("bad port number in config");
            }
        }
        else {
            throw new DataTransportException("no port number in config");
        }

        // Use the ERSAP style reassembly header?
        // Default is Dave Abbott's header from VTP.
        attribString = attributeMap.get("useErsapReHeader");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("true") ||
                    attribString.equalsIgnoreCase("on")   ||
                    attribString.equalsIgnoreCase("yes"))   {
                useErsapReHeader = true;
            }
        }

        if (useErsapReHeader) {
            RE_HEADER_BYTES = 16;
            HEADER_BYTES = RE_HEADER_BYTES;
        }


        if (input) {
            // This transport gets data from some source which, in turn, means that source
            // is calling send, sendto, or sendmsg (C, C++) to a specific host and port.
            // Thus this transport must be bound to a specific port.

            // Let's try 12MB buf by default, which will most likely get doubled to 24MB by the system
            recvBufSize = 12000000;
            String recvBuf = attributeMap.get("recvBufSize");
            if (recvBuf != null) {
                try {
                    recvBufSize = Integer.parseInt(recvBuf);
                    if (recvBufSize < 128000) {
                        recvBufSize = 128000;
                    }
                }
                catch (NumberFormatException e) {/* go with default */}
            }

            try {
                // on a dual-stack host, this dual-binds to IPv6 + IPv4
                inSocket = new DatagramSocket(port);
                inSocket.setReceiveBufferSize(recvBufSize);

// TODO: to wake up the socket, send a packet. The timeout feature SLOWS socket down!!
                // Timeout of 10 seconds
//                inSocket.setSoTimeout(10000);
                // Only enable this if killing and restarting emu gives problems trying to bind to this port
                // socket.setReuseAddress(true);

                logger.debug("    DataChannel UDP: create UDP receiving socket at port " + port +
                        " with " + inSocket.getReceiveBufferSize() + " byte UDP receive buffer, on host " +
                        inSocket.getLocalAddress().getHostName());
            }
            catch (SocketException e) {
                throw new DataTransportException(e);
            }


            int ringSize = 256;

            nodePools = new EvioNodePool[ringSize];
            // Create the EvioNode pools - the BBsupply gets ringSize number of pools -
            // each of which contain 3500 EvioNodes to begin with. These are used for
            // the nodes of each event.
            for (int j = 0; j < ringSize; j++) {
                nodePools[j] = new EvioNodePool(3500);
            }


            // module releases events sequentially if there's only 1 build thread,
            // else the release is NOT sequential.
            boolean sequentialRelease = false;
            if (module.getEventProducingThreadCount() > 1) {
                sequentialRelease = false;
            }

            bbSupply = new ByteBufferSupply(ringSize, bufSize,
                                            ByteOrder.BIG_ENDIAN, direct,
                                            sequentialRelease, nodePools);

            // Start thread to handle socket input
            dataInputThread = new DataInputHelper();
            dataInputThread.start();

            parserMergerThread = new ParserMerger();
            parserMergerThread.start();

            logger.info("    DataChannel UDP stream in: seq release of buffers = " + sequentialRelease);
            logger.info("    DataChannel UDP stream in: created " + ringSize + " node pools for source");

            //            dumpData = true;
//logger.info("    DataChannel UDP stream: dumpData = " + dumpData);

        }
        else {

            // The situations in which this transport acts as a server
            // is when it's used as:
            // 1) the output of the simulated ROC,
            // 2) the output of an aggregtator to an EJFAT load balancer
            // In these cases it's port number is ephemeral.

            // Do use the EJFAT load balancer?
            attribString = attributeMap.get("useLoadBalancer");
            if (attribString != null) {
                if (attribString.equalsIgnoreCase("true") ||
                    attribString.equalsIgnoreCase("on")   ||
                    attribString.equalsIgnoreCase("yes"))   {
                    useEjfatLoadBalancer = true;
                }
            }

            if (useEjfatLoadBalancer) {
                // Include LB header
                LB_HEADER_BYTES = 16;
                HEADER_BYTES = RE_HEADER_BYTES + LB_HEADER_BYTES;
logger.info("    DataChannel UDP stream: total header bytes = " + HEADER_BYTES);
            }

            // Let's try 12MB buf by default, which will most likely get doubled to 24MB by the system
            sendBufSize = 12000000;
            String sendBuf = attributeMap.get("sendBufSize");
            if (sendBuf != null) {
                try {
                    sendBufSize = Integer.parseInt(sendBuf);
                    if (sendBufSize < 128000) {
                        sendBufSize = 128000;
                    }
                }
                catch (NumberFormatException e) {/* go with default */}
            }

            // Host to use for a sending UDP socket
            attribString = attributeMap.get("host");
            if (attribString != null) {
                destHost = attribString;
            }
            else {
                throw new DataTransportException("no destination host in config");
            }

            // Start thread to handle socket input
            dataOutputThread = new DataOutputHelper();
            dataOutputThread.start();
        }

        // State after prestart transition -
        // during which this constructor is called
        channelState = CODAState.PAUSED;
    }


    /**
     * <p>
     * Returns the maximum transmission unit of the network interface used by
     * {@code socket}, or a reasonable default if there's an error retrieving
     * it from the socket.</p>
     *
     * The returned value should only be used as an optimization; such as to
     * size buffers efficiently.
     *
     * @param socket socket to get MTU from.
     * @return MTU value.
     */
    public int getMTU(DatagramSocket socket) {
        try {
            NetworkInterface networkInterface = NetworkInterface.getByInetAddress(
                    socket.getLocalAddress());
            if (networkInterface != null) {
                int mtu = networkInterface.getMTU();
                mtu = mtu > 9000 ? 9000 : mtu;
                return mtu;
            }
            return DEFAULT_MTU;
        } catch (SocketException exception) {
            return DEFAULT_MTU;
        }
    }


    /** {@inheritDoc} */
    public TransportType getTransportType() {
        return TransportType.EMU;
    }


    /** {@inheritDoc} */
    public int getInputLevel() {
        return bbSupply.getFillLevel();
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        super.prestart();
        haveInputEndEvent = false;
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
//logger.debug("    DataChannel UDP stream: end/reset(), interrupt parser/merger thread");
            parserMergerThread.interrupt();
            try {Thread.sleep(10);}
            catch (InterruptedException e) {}

                if (dataInputThread != null) {
                    dataInputThread.interrupt();
                }
//logger.debug("    DataChannel UDP stream: end/reset(), interrupt input thread");
        }

    }

    
    /**
     * Try joining all threads, up to 1 sec each.
     */
    private void joinThreads() {
        if (dataInputThread != null) {
            try {parserMergerThread.join(1000);}
            catch (InterruptedException e) {}

//logger.debug("    DataChannel UDP stream: end/reset(), joined parser/merger thread");

            try {
                dataInputThread.join(1000);
            }
            catch (InterruptedException e) {}
//logger.debug("    DataChannel UDP stream: end/reset(), joined input thread");
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
            if (inSocket != null) inSocket.close();
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
            if (inSocket != null) inSocket.close();
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


    /**
     * <p>
     * Parse the reassembly header at the start of the given array.
     * Return parsed values in array. The following is to viewed as
     * 2 integers with LSB at 0 bit and MSB at 31.
     * These will be send in network byte order - big endian.
     * This format is what is coming from the CODA's VTP.
     * </p>
     * <p>
     * The "Record ID" is LIKE the "block or record number" in the evio header
     * (which is 4 bytes in length and has a value of -1 if banks in block are control or user events).
     * In evio, if the block contains buildable events, the record id starts at 1
     * and increments by 1 for each successive block. In this header, there
     * are only 8 bits or 1 byte so a value of 0 is given to control or user events.
     *
     * This value is somewhat equivalent to the "tick" in the Ersap format.
     * </p>
     * <pre>
     *    3                   2                   1
     *  1 0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |            Source ID          |L|F|Rsv|  Record ID    |Version|
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |   Total Packets in Record     |      Packet # or Sequence     |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * </pre>
     *
     * @param buffer     buffer to parse.
     * @param off        offset into buffer to begin parsing.
     * @param parsedVals array to hold parsed values
     */
    static void parseReHeader(byte[] buffer, int off, int[] parsedVals) {
        //if (parsedVals == null || parsedVals.length < 7) return;

        int first  = 0;
        int second = 0;
        try {
            first  = ByteDataTransformer.toInt(buffer, ByteOrder.BIG_ENDIAN, off);
            second = ByteDataTransformer.toInt(buffer, ByteOrder.BIG_ENDIAN, off+4);
        } catch (EvioException e) {/* never happen */}

        // version
        parsedVals[0] = first & 0xf;
        // record id (lower bits of frame #)
        parsedVals[1] = (first >> 4) & 0xff;
        // first
        parsedVals[2] = (first >> 14) & 0x1;
        // last
        parsedVals[3] = (first >> 15) & 0x1;
        // source id
        parsedVals[4] = first >>> 16;

        // sequence
        parsedVals[5] = second & 0xffff;
        // packet count
        parsedVals[6] = second >>> 16;
    }


    /**
     * <p>
     * Parse the reassembly header at the start of the given array.
     * Return parsed values in array. The following is to viewed as
     * 2 integers with LSB at 0 bit and MSB at 31.
     * These will be send in network byte order - big endian.
     * This format is what is coming from the CODA's VTP.
     * </p>
     * <p>
     * The "Record ID" is LIKE the "block or record number" in the evio header
     * (which is 4 bytes in length and has a value of -1 if banks in block are control or user events).
     * In evio, if the block contains buildable events, the record id starts at 1
     * and increments by 1 for each successive block. In this header, there
     * are only 8 bits or 1 byte so a value of 0 is given to control or user events.
     *
     * This value is somewhat equivalent to the "tick" in the Ersap format.
     * </p>
     * <pre>
     *    3                   2                   1
     *  1 0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |            Source ID          |L|F|Rsv|  Record ID    |Version|
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |   Total Packets in Record     |      Packet # or Sequence     |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * </pre>
     *
     * @param buffer buffer to parse.
     * @param off    offset into buffer.
     * @param parsedVals array to hold parsed values
     */
    static void parseReHeader(ByteBuffer buffer, int off, int[] parsedVals) {
        //if (parsedVals == null || parsedVals.length < 7) return;

        int first  = buffer.getInt(off);
        int second = buffer.getInt(off + 4);

        // version
        parsedVals[0] = first & 0xf;
        // record id (lower bits of frame #)
        parsedVals[1] = (first >> 4) & 0xff;
        // first
        parsedVals[2] = (first >> 14) & 0x1;
        // last
        parsedVals[3] = (first >> 15) & 0x1;
        // source id
        parsedVals[4] = first >> 16;

        // sequence
        parsedVals[5] = second & 0xffff;
        // packet count
        parsedVals[6] = second >> 16;
    }


    /**
     * <p>
     * Parse the reassembly header at the start of the given array.
     * Return parsed values in array. The following is to viewed as
     * 4 32-bit integers with LSB at 0 bit and MSB at 31.
     * These will be send in network byte order - big endian.
     * This format is used with ERSAP and its use of the U280 FPGA load balancer.
     * </p>
     * <pre>
     *  0                   1                   2                   3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |Version|        Rsvd       |F|L|            Data-ID            |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                  UDP Packet Offset                            |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                                                               |
     *  +                              Tick                             +
     *  |                                                               |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  *   Padding 1   |   Padding 2   |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * </pre>
     *
     * @param buffer     buffer to parse.
     * @param off        offset into buffer to begin parsing.
     * @param parsedVals array to hold 5 parsed values
     * @param tick array to hold tick
     */
    static void parseErsapReHeader(byte[] buffer, int off, int[] parsedVals, long[] tick) {
        // Speed things up by skipping arg checks
        //if (parsedVals == null || parsedVals.length < 5 || tick == null || tick.length < 1) return;

        // version high 4 bits of first byte
        parsedVals[0] = (buffer[off] >> 4) & 0xf;
        // first
        parsedVals[1] = (buffer[off+1] >> 1) & 0x1;
        // last
        parsedVals[2] =  buffer[off+1] & 0x1;
        // data id (big end byte first)
        parsedVals[3] = ByteDataTransformer.toShort(buffer[off+2], buffer[off+3], ByteOrder.BIG_ENDIAN) & 0xffff;

        // sequence (big end byte first)
        parsedVals[4] = ByteDataTransformer.toInt(buffer[off+4], buffer[off+5], buffer[off+6], buffer[off+7],
                                                  ByteOrder.BIG_ENDIAN);
        // tick (big end byte first)
        tick[0] = ByteDataTransformer.toLong(buffer[off+8],  buffer[off+9],  buffer[off+10], buffer[off+11],
                                             buffer[off+12], buffer[off+13], buffer[off+14], buffer[off+15],
                                             ByteOrder.BIG_ENDIAN);
    }

    /**
     * <p>
     * Parse the reassembly header at the start of the given array.
     * Return parsed values in array. The following is to viewed as
     * 4 32-bit integers with LSB at 0 bit and MSB at 31.
     * These will be send in network byte order - big endian.
     * This format is used with ERSAP and its use of the U280 FPGA load balancer.
     * </p>
     * <pre>
     *  0                   1                   2                   3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |Version|        Rsvd       |F|L|            Data-ID            |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                  UDP Packet Offset                            |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                                                               |
     *  +                              Tick                             +
     *  |                                                               |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  *   Padding 1   |   Padding 2   |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * </pre>
     *
     * @param buffer     ByteBuffer to parse.
     * @param off        offset into buffer.
     * @param parsedVals array to hold 5 parsed values
     * @param tick array to hold tick
     */
    static void parseErsapReHeader(ByteBuffer buffer, int off, int[] parsedVals, long[] tick) {
        // Speed things up by skipping arg checks
        //if (parsedVals == null || parsedVals.length < 5 || tick == null || tick.length < 1) return;

        // version high 4 bits of first byte
        parsedVals[0] = (buffer.get(off) >> 4) & 0xf;
        // first
        parsedVals[1] = (buffer.get(off+1) >> 1) & 0x1;
        // last
        parsedVals[2] =  buffer.get(off+1) & 0x1;
        // data id (big end byte first)
        parsedVals[3] = ByteDataTransformer.toShort(buffer.get(off+2), buffer.get(off+3),
                                                    ByteOrder.BIG_ENDIAN) & 0xffff;

        // sequence (big end byte first)
        parsedVals[4] = ByteDataTransformer.toInt(buffer.get(off+4), buffer.get(off+5),
                                                  buffer.get(off+6), buffer.get(off+7),
                                                  ByteOrder.BIG_ENDIAN);
        // tick (big end byte first)
        tick[0] = ByteDataTransformer.toLong(buffer.get(off+8),  buffer.get(off+9),
                                             buffer.get(off+10), buffer.get(off+11),
                                             buffer.get(off+12), buffer.get(off+13),
                                             buffer.get(off+14), buffer.get(off+15),
                                             ByteOrder.BIG_ENDIAN);
    }


    /**
     * Return a new ByteBuffer, its size multiplied by "factor" of the size of given buf,
     * and have all data copied over up to limit. If given buf is direct,
     * so is the returned buffer. Order is set to match arg's.
     * Will not work for direct ByteBuffers.
     *
     * @param buf buffer to increase size of.
     * @param bytes number of bytes to copy.
     * @param factor number to multiply with given buffer size to give new size.
     * @return new, bigger buffer with same data as original.
     */
    static ByteBuffer expandBuffer(ByteBuffer buf, int bytes, int factor) {
        ByteBuffer newBuf;
        newBuf = ByteBuffer.allocate(factor * buf.capacity());
        newBuf.order(buf.order());
        System.arraycopy(buf.array(), 0, newBuf.array(), 0, bytes);
        return newBuf;
    }



    /**
     * Class used to get data over network and put into ring buffer.
     */
    private final class DataInputHelper extends Thread {

        /**
         * Variable to print messages when paused.
         */
        private int pauseCounter = 0;

        /**
         * Let a single waiter know that the main thread has been started.
         */
        private final CountDownLatch latch = new CountDownLatch(1);

        // Each stream is from 1 data source

        // Map to hold out-of-order packets, sorted by key:
        //     key   = sequence (unique for each record id)
        //     value = tuple of {byte array holding packet data bytes, data length,
        //                       is last packet, is first packet}
        private final TreeMap<Integer,
                Quartet<byte[], Integer, Boolean, Boolean>> outOfOrderPackets = new TreeMap<>();

        // Max bytes per packet for this data source:
        private int maxBytesPerPacket;

        // Flag used to quit thread
        private final AtomicBoolean quitThread = new AtomicBoolean(false);


        /**
         * Constructor.
         */
        DataInputHelper() {
            super(emu.getThreadGroup(), name() + "_data_in");
        }


        /**
         * A single waiter can call this method which returns when thread was started.
         */
        private void waitUntilStarted() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
            }
        }


        /**
         * Call in order to exit this thread.
         */
        public void exitThread() {
            quitThread.set(true);
        }


        /**
         * It's the observation of this programmer that in the simple network environments in which
         * this code is run, any irregularity in the sequence of packets is due to dropped packets
         * and not to simple reordering. Thus this is the assumption made in attempts to recover
         * from errors. Little attempt is made to hold on to out-of-order packets. If a packet
         * appears from a new record id, all the saved packets are dropped and any previous partially
         * filled buffers are dumped as well.
         */
        //  @Override
        public void run() {

            // Tell the everyone I've started
            latch.countDown();
            logger.info("    DataChannel UDP stream in: " + name + " - started");

            // Statistics
            // "Record ID" in VTP format, euivalent to "tick" in Ersap format
            long prevTick = -1;
            long packetTick;
            int packetDataId, sequence, prevSequence = 0, pktCount = 0;
            // Next expected sequence or packet # from UDP reassembly header
            int expectedSequence = 0;

            boolean dumpTick = false;
            boolean packetFirst, packetLast;
            boolean prevPacketLast = true;
            boolean firstReadForBuf = false;
            int bytesRead;
            // Buffer in which to place recontructed data from packets
            byte[] buffer;
            int nBytes;
            int version;

            // Each pktBuffer is 90KB which contains up to 10 Jumbo packets
            int chunk = 10;
            int bufIndex = chunk;
            ByteBuffer itemBB;
            ByteBufferItem item;
            ByteBufferItem[] items = new ByteBufferItem[chunk];

            boolean veryFirstRead;
            // Indexes in buffer where to write the header, data
            int writeHeaderAt, putDataAt;
            int bufLen, remainingLen;
            // What's the max # bytes source is sending in a single packet?
            int maxPacketBytes = 0;

            // Storage for packet / header
            int biggestPacketLen = 9100;
            byte[] pkt = new byte[biggestPacketLen];
            byte[] headerStorage = new byte[HEADER_BYTES];

            // Allocate once to minimize garbage. Contains data from a single UDP reassembly header.
            int[] reHeader = new int[7];
            byte[] recvBuf = new byte[biggestPacketLen];
            DatagramPacket packet = new DatagramPacket(recvBuf, biggestPacketLen);
            long[] tick = new long[1];

            // Need to handle a complication. The VTP sends buffers in which the first packet has sequence 1, not 0!
            // The simulated FPGA and any secondary aggregators, on the other hand, use the output section of this
            // class to send buffers and start with sequence 0, not 1.
            // The incoming sequence has 1 substracted from it to handle VTPs.
            // We know it's not from a VTP if we get a -1 value for
            // the sequence, indicating that the sender has started its sequence from 0. Follow me?
            // To account for this, assume input is from VTP, if -1 is received as a sequence,
            // set boolean indicating source is not a VTP.
            boolean vtpSource = true;

            try {

                while (true) {
                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        return;
                    }

                    if (pause) {
                        if (pauseCounter++ % 400 == 0)
                            logger.warn("    DataChannel UDP stream in: " + name + " - PAUSED");
                        Thread.sleep(5);
                        continue;
                    }

                    // We need to get another chunk of buffers to read valid data into
                    // since we've used them all.
                    if (bufIndex == chunk) {
                        // Grab "chunk" empty buffers from ByteBufferSupply at once for efficiency
                        bbSupply.get(chunk, items);
                        // Start with first buffer retrieved
                        bufIndex = 0;
                    }

                    item = items[bufIndex++];
                    // Get reference to item's byte array
                    itemBB = item.getClearedBuffer();
                    buffer = itemBB.array();
                    packet.setData(buffer);
                    putDataAt = 0;
                    // How big is this array?
                    bufLen = buffer.length;
                    remainingLen = bufLen;
                    veryFirstRead = true;


                    while (true) {

                        // Another packet of data might exceed buffer space, so expand
                        if (remainingLen < 9000) {
                            // Double buffer size
                            itemBB = expandBuffer(itemBB, putDataAt, 2);
                            item.setBuffer(itemBB);
                            buffer = itemBB.array();
                            bufLen = buffer.length;
                            remainingLen = bufLen - putDataAt;
System.out.println("reallocated buffer to " + bufLen + " bytes");
                        }

                        if (veryFirstRead) {

                            maxPacketBytes   = 0;
                            expectedSequence = 0;
                            putDataAt        = 0;
                            remainingLen     = bufLen;

                            // Read in first packet into temporary storage
                            packet.setData(pkt, 0, biggestPacketLen);
//System.out.println("\nWait to receive packet on port " + inSocket.getLocalPort() + ", other end is port " + inSocket.getPort());
                            inSocket.receive(packet);

                            // Bytes in packet
                            bytesRead = packet.getLength();
                            // Number of data bytes not counting RE header
                            nBytes = bytesRead - HEADER_BYTES;

                            if (useErsapReHeader) {
                                // Parse Ersap format RE header
                                parseErsapReHeader(pkt, 0, reHeader, tick);

                                version      = reHeader[0];
                                packetFirst  = reHeader[1] == 1 ? true : false;
                                packetLast   = reHeader[2] == 1 ? true : false;
                                packetDataId = reHeader[3];
                                sequence     = reHeader[4];
                                packetTick   = tick[0];
                            }
                            else {
                                // Parse VTP format RE header
                                parseReHeader(pkt, 0, reHeader);

                                version      = reHeader[0];
                                // recordId (1 byte), must be sequential
                                packetTick   = reHeader[1];
                                packetFirst  = reHeader[2] == 1 ? true : false;
                                packetLast   = reHeader[3] == 1 ? true : false;
                                packetDataId = reHeader[4];
                                pktCount     = reHeader[6];

                                // VTP starts at 1, not 0. HOWEVER, the simulated fpga / secondary agg start at 0!!
                                sequence = reHeader[5];
System.out.println("     first seq = " + sequence);
                                if (vtpSource) {
                                    sequence--;
                                    System.out.println("     vtp seq -> " + sequence);
                                    if (sequence == -1) {
                                        vtpSource = false;
                                        sequence++;
                                        System.out.println("     non-vtp seq -> " + sequence);
                                    }
                                }
                            }

                            // Copy data (NOT header) into main buffer
                            System.arraycopy(pkt, HEADER_BYTES, buffer, 0, nBytes);

                            // When receiving from a ROC or a CODA reassembly header, the "packetTick"
                            // (a concept from ERSAP reassembly) is really LIKE the "record id" or block/record
                            // number of the evio header containing event being sent
                            // (even if control or user event). Starts at 0.
                            // In addition we are not going to stop receiving if
                            // a buffer is dropped and the record id is not sequential.

//System.out.println("Got what should be first packet of buf, seq " + sequence);
                            veryFirstRead = false;
                        }
                        else {
                            writeHeaderAt = putDataAt - HEADER_BYTES;
                            // Copy part of buffer that we'll temporarily overwrite
                            System.arraycopy(buffer, writeHeaderAt, headerStorage, 0, HEADER_BYTES);

                            // Read data right into final buffer (including RE header).
                            // Set where to write incoming data
                            packet.setData(buffer, writeHeaderAt, biggestPacketLen);
                            inSocket.receive(packet);

                            // TODO:: is this total data in buffer, or just data of last read?
                            bytesRead = packet.getLength();
                            nBytes = bytesRead - HEADER_BYTES;

                            if (nBytes == 0) {
                                // Something clearly wrong. There should be SOME data besides header returned.
System.out.println("Internal error: got packet with no data, buf's unused bytes = " + remainingLen);
                                outOfOrderPackets.clear();
                                throw new EmuException("Got packet with no data, internal error");
                            }

                            // Parse header
                            if (useErsapReHeader) {
                                // Parse Ersap format RE header
                                parseErsapReHeader(buffer, writeHeaderAt, reHeader, tick);

                                version      = reHeader[0];
                                packetFirst  = reHeader[1] == 1 ? true : false;
                                packetLast   = reHeader[2] == 1 ? true : false;
                                packetDataId = reHeader[3];
                                sequence     = reHeader[4];
                                packetTick   = tick[0];
                            }
                            else {
                                // Parse VTP format RE header
                                parseReHeader(buffer, writeHeaderAt, reHeader);

                                version      = reHeader[0];
                                packetTick   = reHeader[1]; // recordId (1 byte)
                                packetFirst  = reHeader[2] == 1 ? true : false;
                                packetLast   = reHeader[3] == 1 ? true : false;
                                packetDataId = reHeader[4]; // source id
                                pktCount     = reHeader[6];

                                // VTP starts at 1, not 0. HOWEVER, the simulated fpga / secondary agg start at 0!!
                                sequence = reHeader[5];
                                if (vtpSource) {
                                    sequence--;
                                }
                                System.out.println("     plain seq = " + sequence);
                            }
//System.out.println("Got packet, seq " + sequence);

                            // Replace what was written over
                            System.arraycopy(headerStorage, 0, buffer, writeHeaderAt, HEADER_BYTES);
                        }

                        if (useErsapReHeader) {
                            System.out.println("\nPkt hdr: ver = " + version + ", first = " + packetFirst + ", last = " +
                                    packetLast + ", tick = " + packetTick + ", seq = " + sequence +
                                    ", source id = " + packetDataId + ", nBytes = " + nBytes);
                        }
                        else {
                            System.out.println("\nPkt hdr: ver = " + version + ", first = " + packetFirst + ", last = " +
                                    packetLast + ", recordId = " + packetTick + ", seq = " + sequence +
                                    ", pktCount = " +pktCount + ", source id = " + packetDataId + ", nBytes = " + nBytes);
                        }

                        //
                        // The assumption here is that:
                        // If the buffer associated with a particular record-id/tick is fully reassembled,
                        // then it is very unlikely that any partially reassembled previous record
                        // will ever receive the missing packets and be completed.
                        // In practice, since it's much simpler, once a packet from a new recordId/tick
                        // arrives, all the packets associated with the previous tick are abandoned
                        // and that tick is lost.

                        // This if statement is what enables the packet reading/parsing to keep
                        // up an input rate that is too high (causing dropped packets) and still
                        // salvage much of what is coming in.
                        // For the VTP, the "tick" is one byte so it rolls over constantly,
                        // but is not a problem.

                        // If we've moved on to another buffer
                        if (packetTick != prevTick) {

                            // If we're here, either we've just read the very first legitimate packet,
                            // or we've dropped some packets and advanced to another tick/recordId in the process.
                            expectedSequence = 0;

                            if (sequence != 0) {
                                // Already have trouble, looks like we dropped the first packet of a tick,
                                // and possibly others after it.
                                // So go ahead and dump the rest of the tick in an effort to keep up.
System.out.println("New rec: dropped at least first pkt");
System.out.println("Hdr: recId = " + packetTick + " (prev " + prevTick +
                       "), seq = " + sequence + " (prev " + prevSequence +
                       "), first = " + packetFirst + ", last = " + packetLast + "\n");

//Utilities.printBytes(buffer, 0, 200, "Streamed buf");
                                veryFirstRead = true;
                                dumpTick = true;
                                prevTick = packetTick;
                                prevSequence = sequence;
                                prevPacketLast = packetLast;

                                continue;
                            }

                            if (putDataAt != 0) {
                                // The last tick's buffer was not fully contructed
                                // before this new tick showed up!
System.out.println("New rec: previous rec unfinished");
System.out.println("Hdr: recId = " + packetTick + " (prev " + prevTick +
                       "), seq = " + sequence + " (prev " + prevSequence +
                       "), first = " + packetFirst + ", last = " + packetLast + "\n");

                                // We have a problem here, the first packet of this tick, unfortunately,
                                // is at the end of the buffer storing the previous tick. We must move it
                                // to the front of the buffer and overwrite the previous tick.
                                // This will happen if the end of the previous tick is completely dropped
                                // and the first packet of the new tick is read.
                                System.arraycopy(buffer, putDataAt, buffer, 0, nBytes);

                                maxPacketBytes   = 0;
                                putDataAt        = 0;
                                remainingLen     = bufLen;
                            }

                            // See if we skipped an entire buffer / record / frame / tick
                            // How do we get the difference in ticks taking rollover into account?
                            // packetTick is a long, but CODA uses only 8 bits, so
                            // prevTick is fine, but packetTick may need adjustment.
                            //
                            // The other "gotcha" is that an out-of-order packet may arrive from a previous tick.
                            // It may have seq = 0, in which case we end up here.
                            //
                            // I don't know of a way to distinguish these in every case ...
                            if ((packetTick - prevTick) > 1) {
System.out.println("New rec: MAY have skipped whole record(s)");
System.out.println("Hdr: recId = " + packetTick + " (prev " + prevTick +
                       "), seq = " + sequence + " (prev " + prevSequence +
                       "), first = " + packetFirst + ", last = " + packetLast + "\n");
                            }


                            // If here, new record, seq = 0
                            // There's a chance we can construct a full buffer.

                            // Dump everything we saved from previous record.
                            // Delete all out-of-seq packets.
                            outOfOrderPackets.clear();
                            dumpTick = false;
                        }
                        // Same record as last packet
                        else {

                            if (sequence - prevSequence <= 0) {
System.out.println("Got SAME or DECREASING seq, " + sequence + " (from " + prevSequence + "), recId = " + packetTick);
                                continue;
                            }

                            if (dumpTick || (sequence - prevSequence > 1)) {
                                // If here, the sequence hopped by at least 2,
                                // probably dropped at least 1,
                                // so drop rest of packets for record.
                                // This branch of the "if" will no longer
                                // be executed once the next record shows up.
System.out.println("Same rec: missing seq");
System.out.println("Hdr: recId = " + packetTick + " (prev " + prevTick +
                       "), seq = " + sequence + " (prev " + prevSequence +
                       "), first = " + packetFirst + ", last = " + packetLast + "\n");
                                veryFirstRead = true;
                                dumpTick = true;
                                prevSequence = sequence;
                                prevPacketLast = packetLast;

                                continue;
                            }
                        }

                        // TODO: What if we get a zero-length packet???

                        if (sequence == 0) {
                            firstReadForBuf = true;
                            putDataAt = 0;
                        }

                        prevTick = packetTick;
                        prevSequence = sequence;
                        prevPacketLast = packetLast;

//                        System.out.println("Received " + nBytes + " bytes from sender " + id + ", tick " + packetTick +
//                                            ", seq " + sequence + ", last = " + packetLast);

                        // Check to see if packet is out-of-sequence
                        // The challenge here is to TEMPORARILY store the out-of-order packets.
                        if (sequence != expectedSequence) {
                             System.out.println("\n   ID " + id + ": got seq " + sequence +
                                                ", expecting " + expectedSequence);

                            // If we get a sequence that we already received, ERROR!
                            if (sequence < expectedSequence) {
                                outOfOrderPackets.clear();
                                throw new EmuException("got seq " + sequence + " before!");
                            }

                            // Set a limit on how much we're going to store (100 packets) while we wait
                            if (outOfOrderPackets.size() >= 100) {
                                outOfOrderPackets.clear();
                                throw new EmuException("Reached limit of stored out-of-order packets\n");
                            }

                            // Since it's out of order, what was written into buffer will need to be
                            // copied and stored. And that written data will eventually need to be
                            // overwritten with the correct packet data.
                            byte[] dataCopy = new byte[nBytes];
                            System.arraycopy(buffer, putDataAt, dataCopy, 0, nBytes);

//                            if (debug) System.out.println("    Save & store packet " + sequence +
//                                            ", packetLast = " + packetLast + ", storage has " + outOfOrderPacketsNew.size());

                            // Put it into map. The key = sequence.
                            Quartet<byte[], Integer, Boolean, Boolean> val =
                                    new Quartet<>(dataCopy, nBytes, packetLast, packetFirst);
                            outOfOrderPackets.put(sequence, val);

//                            System.out.println(" +" + outOfOrderPacketsNew.size() + ", id " + recordId + ", seq " + sequence + ", x " + expectedSequence);
                            // Read next packet
                            continue;
                        }

                        while (true) {
                            //if (debug) System.out.println("Packet " + sequence + " in order, last = " + packetLast);

                            // Packet was in proper order. Get ready to look for next in sequence.
                            putDataAt += nBytes;
                            remainingLen -= nBytes;
                            expectedSequence++;

                            // If it's the first read of a sequence, and there are more reads to come,
                            // the # of bytes it read will be max possible. Remember that.
                            if (firstReadForBuf) {
                                maxPacketBytes = nBytes;
                                firstReadForBuf = false;

                                // Error check
                                if (!packetFirst) {
                                    System.out.println("Expecting first bit to be set on very first read but wasn't");
                                    outOfOrderPackets.clear();
                                    throw new EmuException("Expecting first bit to be set on very first read but wasn't");
                                }
                            }
                            else if (packetFirst) {
                                System.out.println("Expecting first bit NOT to be set on read but was");
                                outOfOrderPackets.clear();
                                throw new EmuException("Expecting first bit NOT to be set on read but was");
                            }

//                            System.out.println("remainingLen = " + remainingLen + ", expected seq = " +
//                                               expectedSequence + ", first = " +
//                                               packetFirst + ", last = "+ packetLast);

                            // If no stored, out-of-order packets ...
                            if (outOfOrderPackets.isEmpty()) {
                                // If very last packet, on to next buffer
                                if (packetLast) {
                                    // For UDP we don't need to track if ticks are sequential
                                    //item.setUserInt((int)packetTick);
                                    break;
                                }
                            }
                            // If there were previous packets out-of-order, they may now be in order.
                            // If so, write them into buffer.
                            // Remember the map already sorts them into proper sequence.
                            else {
                                // System.out.println("We also have stored packets");
                                // Go to first stored packet since this is a map sorted by sequence
                                Map.Entry<Integer, Quartet<byte[], Integer, Boolean, Boolean>> entry =
                                        outOfOrderPackets.firstEntry();

                                // If it's truly the next packet ...
                                if (entry.getKey() == expectedSequence) {
                                    Quartet<byte[], Integer, Boolean, Boolean> val = entry.getValue();

                                    byte[] dat = val.getValue0();
                                    nBytes = val.getValue1();
                                    packetLast = val.getValue2();
                                    packetFirst = val.getValue3();
                                    sequence = expectedSequence;

                                    // Not enough room for this packet
                                    if (remainingLen < nBytes) {
                                        // There is only 1 user of this item/buffer at the moment, so expanding it
                                        // is possible without multithreading issues.
                                        itemBB = expandBuffer(itemBB, putDataAt, 2);
                                        item.setBuffer(itemBB);
                                        buffer = itemBB.array();
                                        bufLen = buffer.length;
                                        remainingLen = bufLen - putDataAt;
                                        System.out.println("reallocated buffer to " + bufLen + " bytes");
                                    }

                                    System.arraycopy(dat, 0, buffer, putDataAt, nBytes);

                                    // Remove packet from map
                                    outOfOrderPackets.remove(expectedSequence, val);

//                                    System.out.println("Add stored packet " + expectedSequence +
//                                                       ", size of map = " + outOfOrderPacketsNew.size() +
//                                                       ", last = packetLast");
                                    continue;
                                }
                            }

                            break;
                        }

                        veryFirstRead = false;

                        if (packetLast) {
                            break;
                        }

                        // read next packet
                    }


//                    // Print out part of buffer
//                    Utilities.printBytes(buffer, 0, 100, "Streamed buf");
                    
                    //System.out.println("publish, seq " + sequence);
                    itemBB.limit(putDataAt);
                    bbSupply.publish(item);
                }
            }
            catch (InterruptedException e) {
                logger.warn("    DataChannel UDP stream in: " + name + ", interrupted, exit reading thd");
            }
            catch (AsynchronousCloseException e) {
                logger.warn("    DataChannel UDP stream in: " + name + ", socket closed, exit reading thd");
            }
            catch (IOException e) {
                // Assume that if the other end of the socket closes, it's because it has
                // sent the END event and received the end() command.
                logger.warn("    DataChannel UDP stream in: " + name + ", socket I/O error");
            }
            catch (Exception e) {
                if (haveInputEndEvent) {
                    System.out.println("    DataChannel UDP stream in: " + name +
                            ", exception but already have END event, so exit reading thd");
                    return;
                }
                e.printStackTrace();
                channelState = CODAState.ERROR;
                // If error msg already set, this will not
                // set it again. It will send it to rc.
                String errString = "DataChannel UDP stream in: error reading " + name;
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
        private EvioCompactReader reader;


        /** Constructor. */
        ParserMerger() {
            super(emu.getThreadGroup(), name() + "_parser_merger");
        }


        public void run() {
            try {
                // Simplify things when there's only 1 socket for better performance
                while (true) {
                    // Sets the consumer sequence
                    ByteBufferItem item = bbSupply.consumerGet();
                    if (parseStreamingToRing(item, bbSupply)) {
                        logger.info("    DataChannel UDP stream in: 1 quit streaming parser/merger thread for END event from " + name);
                        break;
                    }
                }
            }
            catch (InterruptedException e) {
//                logger.warn("    DataChannel UDP stream in: " + name +
//                            " parserMerger thread interrupted, quitting ####################################");
            }
            catch (EvioException e) {
                // Bad data format or unknown control event.
                e.printStackTrace();
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel UDP stream in: " + e.getMessage());
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

            try {
                // Pool of EvioNodes associated with this buffer which grows as needed
                pool = (EvioNodePool)item.getMyObject();
                // Each pool must be reset only once!
                pool.reset();

                // The following will set the endianness of the buffer when it's scanned,
                // which overwrites the default big endian setting in creating bbSupply.
                if (reader == null) {
//System.out.println("    DataChannel UDP stream in: create reader, buf's pos/lim = " + buf.position() + "/" + buf.limit());
                    reader = new EvioCompactReader(buf, pool, false);
                }
                else {
//System.out.println("    DataChannel UDP stream in: set buffer, expected id = " + expectedRecordId);
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
                System.out.println("    DataChannel UDP stream in: data NOT evio format 1");
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

            EventType eventType = EventType.getEventType(blockHeader.getEventType());
            if (eventType == null || !eventType.isEbFriendly()) {
                throw new EvioException("bad evio format or improper event type");
            }

            // For UDP, we don't need to track if records are sequential

//            // Stored the record id previously when reassembling packets
//            recordId = item.getUserInt();
//
//            // Check record for sequential record id
//            expectedRecordId = Evio.checkRecordIdSequence(recordId, expectedRecordId, false,
//                                                          eventType, DataChannelImplUdpStream.this);

//System.out.println("    DataChannel UDP stream in: expected record id = " + expectedRecordId +
//                    ", actual = " + recordId);
//System.out.println("    DataChannel UDP stream in: event type = " + eventType + ", event count = " + reader.getEventCount() + " from " + name);

            int eventCount = reader.getEventCount();
            boolean gotRocRaw  = eventType.isFromROC();
            boolean gotPhysics = eventType.isAnyPhysics();
//System.out.println("    DataChannel UDP stream in: gotRocRaw = " + gotRocRaw + ", is physics = " + gotPhysics);

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

                // getScannedEvent will clear child and allNodes lists
                topNode = reader.getScannedEvent(i, pool);

                // This should NEVER happen
                if (topNode == null) {
                    System.out.println("    DataChannel UDP stream in: WARNING, event count = " + eventCount +
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
                            System.out.println("    DataChannel UDP stream in: " + name + "  FIRST event from ROC RAW");
                        } else {
                            System.out.println("    DataChannel UDP stream in: " + name + " USER event from ROC RAW");
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
//System.out.println("    DataChannel UDP stream in: read ts, node pos = " + pos + ", datalen = " + 4*node.getDataLength());
                        ByteBuffer buff = node.getBuffer();
                        frame = buff.getInt(20 + pos);
                        timestamp = EmuUtilities.intsToLong(buff.getInt(24 + pos), buff.getInt(28 + pos));
//System.out.println("    DataChannel UDP stream in: roc raw has frame = " + frame + ", timestamp = " + timestamp + ", pos = " + pos);
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
                    frame = buff.getInt(20 + pos);
                    timestamp = EmuUtilities.intsToLong(buff.getInt(24 + pos), buff.getInt(28 + pos));
//System.out.println("    DataChannel UDP stream in: buildable has frame = " + frame + ", timestamp = " + timestamp + ", pos = " + pos);
                }
                else if (eventType == EventType.CONTROL) {
                    // Find out exactly what type of control event it is
                    // (May be null if there is an error).
                    controlType = ControlType.getControlType(node.getTag());
                    logger.info("    DataChannel UDP stream in: got " + controlType + " event from " + name);
                    if (controlType == null) {
                        logger.info("    DataChannel UDP stream in: found unidentified control event");
                        throw new EvioException("Found unidentified control event");
                    }
                }
                else if (eventType == EventType.USER) {
                    isUser = true;
                    if (hasFirstEvent) {
                        logger.info("    DataChannel UDP stream in: " + name + " got FIRST event");
                    } else {
                        logger.info("    DataChannel UDP stream in: " + name + " got USER event");
                    }
                }

                nextRingItem = ringBufferIn.nextIntr(1);
                ri = ringBufferIn.get(nextRingItem);

                // Set & reset all parameters of the ringItem
                if (eventType.isBuildable()) {
//logger.info("    DataChannel UDP stream in: put buildable event into channel ring, event from " + name);
                    ri.setAll(null, null, node, eventType, controlType,
                            isUser, hasFirstEvent, module.isStreamingData(), id, recordId, id,
                            node.getNum(), name, item, bbSupply);
                    ri.setTimeFrame(frame);
                    ri.setTimestamp(timestamp);
                } else {
//logger.info("    DataChannel UDP stream in: put CONTROL (user?) event into channel ring, event from " + name);
                    ri.setAll(null, null, node, eventType, controlType,
                            isUser, hasFirstEvent, module.isStreamingData(), id, recordId, id,
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
                    logger.info("    DataChannel UDP stream in: BREAK from loop, got END event");
                    break;
                }
            }

            return haveInputEndEvent;
        }
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////
    // OUTPUT
    //////////////////////////////////////////////////////////////////////////////////////////////////


    /**
     * <p>
     * Write the reassembly header at the start of the given byte array.
     * The following is to be viewed as
     * 2 integers with LSB at 0 bit and MSB at 31.
     * These will be send in network byte order - big endian.
     * </p>
     * <pre>
     *    3                   2                   1
     *  1 0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |            Source ID          |L|F|Rsv|  Record ID    |Version|
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |   Total Packets in Record     |      Packet # or Sequence     |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * </pre>
     *
     * @param buffer        byte array in which to write.
     * @param offset        index in buffer to start writing.
     * @param sourceId      data source id.
     * @param first         is this the first packet of a record/buffer being sent?
     * @param last          is this the last packet of a record/buffer being sent?
     * @param recordId      record id.
     * @param version       version of meta data.
     * @param totalPackets  total number of packets comprising record.
     * @param sequence      sequence of this packet with respect to others in the record.
     * @throws EmuException if offset &lt; 0 or buffer overflow.
     */
    static void writeReHeader(byte[] buffer, int offset, int sourceId,
                              boolean first, boolean last,
                              int recordId, int version,
                              int totalPackets, int sequence) throws EmuException {

        // Data will be send big endian (Java default)

        if (offset < 0 || (offset + 8 > buffer.length)) {
            throw new EmuException("offset arg < 0 or buf too small");
        }

        int firstInt = first ? 1 : 0;
        int lastInt  =  last ? 1 : 0;

        int word1 = (version & 0xf)| ((recordId & 0xff) << 4) | (firstInt << 14) | (lastInt << 15) | (sourceId << 16);
        int word2 = (sequence & 0xffff) | (totalPackets << 16);

        try {
            ByteDataTransformer.toBytes(word1, ByteOrder.BIG_ENDIAN, buffer, offset);
            ByteDataTransformer.toBytes(word2, ByteOrder.BIG_ENDIAN, buffer, offset+4);
        }
        catch (EvioException e) {/* never happen */}
    }


    /**
     * <p>
     * Write the reassembly header at the start of the given buffer.
     * The following is to be viewed as
     * 2 integers with LSB at 0 bit and MSB at 31.
     * These will be send in network byte order - big endian.
     * </p>
     * <pre>
     *    3                   2                   1
     *  1 0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |            Source ID          |L|F|Rsv|  Record ID    |Version|
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |   Total Packets in Record     |      Packet # or Sequence     |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * </pre>
     *
     * @param buffer        buffer in which to write.
     * @param offset        index in buffer to start writing.
     * @param sourceId      data source id.
     * @param first         is this the first packet of a record/buffer being sent?
     * @param last          is this the last packet of a record/buffer being sent?
     * @param recordId      record id.
     * @param version       version of meta data.
     * @param totalPackets  total number of packets comprising record.
     * @param sequence      sequence of this packet with respect to others in the record.
     * @throws EmuException if offset &lt; 0 or buffer overflow.
     */
    static void writeReHeader(ByteBuffer buffer, int offset, int sourceId,
                              boolean first, boolean last,
                              int recordId, int version,
                              int totalPackets, int sequence) throws EmuException {

        // Data will be send big endian (Java default)

        if (offset < 0 || (offset + 8 > buffer.limit())) {
            throw new EmuException("offset arg < 0 or buf too small");
        }

        int firstInt = first ? 1 : 0;
        int lastInt  =  last ? 1 : 0;

        int word1 = (version & 0xf)| ((recordId & 0xff) << 4) | (firstInt << 14) | (lastInt << 15) | (sourceId << 16);
        int word2 = (sequence & 0xffff) | (totalPackets << 16);

        buffer.putInt(offset, word1);
        buffer.putInt(offset+4, word2);
    }


    /**
     * <p>
     * Write the reassembly header, at the start of the given byte array,
     * in the format used in ERSAP project.
     * The first 16 bits go as ordered. The dataId is put in network byte order.
     * The offset and tick are also put into network byte order.</p>
     * </p>
     * <pre>
     *  protocol 'Version:4, Rsvd:10, First:1, Last:1, Data-ID:16, Offset:32'
     *
     *  0                   1                   2                   3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |Version|        Rsvd       |F|L|            Data-ID            |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                  UDP Packet Offset                            |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                                                               |
     *  +                              Tick                             +
     *  |                                                               |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * </pre>
     *
     * @param buffer        byte array in which to write.
     * @param offset        index in buffer to start writing.
     * @param version       version of meta data.
     * @param first         is this the first packet of a record/buffer being sent?
     * @param last          is this the last packet of a record/buffer being sent?
     * @param dataId        data source id.
     * @param packetOffset  packet sequence.
     * @param tick          tick value.
     * @throws EmuException if offset &lt; 0 or buffer overflow.
     */
    static void writeErsapReHeader(byte[] buffer, int offset,
                                  int version, boolean first, boolean last,
                                  short dataId, int packetOffset, long tick)
            throws EmuException {

        if (offset < 0 || (offset + 16 > buffer.length)) {
            throw new EmuException("offset arg < 0 or buf too small");
        }

        buffer[offset] = (byte) (version << 4);
        int fst = first ? 1 : 0;
        int lst =  last ? 1 : 0;
        buffer[offset + 1] = (byte) ((fst << 1) + lst);

        try {
            ByteDataTransformer.toBytes(dataId, ByteOrder.BIG_ENDIAN, buffer, offset + 2);
            ByteDataTransformer.toBytes(packetOffset, ByteOrder.BIG_ENDIAN, buffer, offset + 4);
            ByteDataTransformer.toBytes(tick, ByteOrder.BIG_ENDIAN, buffer, offset + 8);
        }
        catch (EvioException e) {/* never happen */}
    }


    /**
     * Set the Load Balancer header data.
     * The first four bytes go as ordered.
     * The entropy goes as a single, network byte ordered, 16-bit int.
     * The tick goes as a single, network byte ordered, 64-bit int.
     *
     * <pre>
     *  protocol 'L:8,B:8,Version:8,Protocol:8,Reserved:16,Entropy:16,Tick:64'
     *
     *  0                   1                   2                   3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |       L       |       B       |    Version    |    Protocol   |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  3               4                   5                   6
     *  2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |              Rsvd             |            Entropy            |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  6                                               12
     *  4 5       ...           ...         ...         0 1 2 3 4 5 6 7
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                                                               |
     *  +                              Tick                             +
     *  |                                                               |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * </pre>
     *
     * @param buffer   buffer in which to write the header.
     * @param off      index in buffer to start writing.
     * @param tick     unsigned 64 bit tick number used to tell the load balancer
     *                 which backend host to direct the packet to.
     * @param version  version of load balancer metadata.
     * @param protocol protocol this software uses.
     * @param entropy  entropy field used to determine destination port.
     * @return bytes written.
     * @throws EmuException if offset &lt; 0 or buffer overflow.
     */
    static int writeLbHeader(ByteBuffer buffer, int off, long tick, int version, int protocol, int entropy)
                    throws EmuException{

        if (off < 0 || (off + 16 > buffer.limit())) {
            throw new EmuException("offset arg < 0 or buf too small");
        }

        buffer.put(off, (byte)('L'));
        buffer.put(off+1, (byte)('B'));
        buffer.put(off+2, (byte)version);
        buffer.put(off+3, (byte)protocol);
        buffer.putShort(off+6, (short)entropy);
        buffer.putLong(off+8, tick);
        return 16;
    }


    /**
     * Set the Load Balancer header data.
     * The first four bytes go as ordered.
     * The entropy goes as a single, network byte ordered, 16-bit int.
     * The tick goes as a single, network byte ordered, 64-bit int.
     *
     * <pre>
     *  protocol 'L:8,B:8,Version:8,Protocol:8,Reserved:16,Entropy:16,Tick:64'
     *
     *  0                   1                   2                   3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |       L       |       B       |    Version    |    Protocol   |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  3               4                   5                   6
     *  2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |              Rsvd             |            Entropy            |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  6                                               12
     *  4 5       ...           ...         ...         0 1 2 3 4 5 6 7
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |                                                               |
     *  +                              Tick                             +
     *  |                                                               |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * </pre>
     *
     * @param buffer   buffer in which to write the header.
     * @param off      index in buffer to start writing.
     * @param tick     unsigned 64 bit tick number used to tell the load balancer
     *                 which backend host to direct the packet to.
     * @param version  version of load balancer metadata.
     * @param protocol protocol this software uses.
     * @param entropy  entropy field used to determine destination port.
     * @return bytes written.
     * @throws EmuException if offset &lt; 0 or buffer overflow.
     */
    static int writeLbHeader(byte[] buffer, int off, long tick, int version, int protocol, int entropy)
                    throws EmuException{

        if (off < 0 || (off + 16 > buffer.length)) {
            throw new EmuException("offset arg < 0 or buf too small");
        }

        buffer[off]   = (byte) 'L';
        buffer[off+1] = (byte) 'B';
        buffer[off+2] = (byte) version;
        buffer[off+3] = (byte) protocol;
        try {
            ByteDataTransformer.toBytes((short)entropy, ByteOrder.BIG_ENDIAN, buffer, off+6);
            ByteDataTransformer.toBytes(tick, ByteOrder.BIG_ENDIAN, buffer, off+8);
        }
        catch (EvioException e) {/* never happen */}
        return 16;
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

        /** Sender threads to send data over network. */
        private final SocketSender sender;

        /** ByteBufferSupply for each sender socket. */
        private final ByteBufferSupply bbOutSupply;

        /** ByteBufferSupply for each sender socket. */
        private final DatagramSocket outSocket;

        private int maxUdpPayload;

        private long tick;
        private int entropy;


        /** When regulating output buffer flow, the current
         * number of physics events written to buffer. */
        private int currentEventCount;


        /** Do we call connect on the UDP socket? */
        private boolean useConnectedSocket = true;



        /**
         * This class is a separate thread used to write filled data
         * buffers over the UDP socket.
         */
        private final class SocketSender extends Thread {

            /** Boolean used to kill this thread. */
            private volatile boolean killThd;

            /** The ByteBuffers to send. */
            private final ByteBufferSupply supply;

            private final InetAddress destAddr;
            private final int destPort;
            private DatagramPacket packet;


            SocketSender(ByteBufferSupply bufSupply, InetAddress addr, int port) {
                super(emu.getThreadGroup(), name() + "_sender");

                supply = bufSupply;
                destAddr = addr;
                destPort = port;
logger.info("    DataChannel UDP out: constructed SocketSender thread");
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
                boolean debug = true;
                int[] packetsSent = new int[1];
                byte[] packetStorage = new byte[10000];
                int delay = 0;

                // Use dummy values just to create packet object, overwritten later
                if (useConnectedSocket) {
                    packet = new DatagramPacket(packetStorage, 10000, destAddr, port);
                }
                else {
                    packet = new DatagramPacket(packetStorage, 10000);
                }

                while (true) {
                    if (killThd) {
System.out.println("SocketSender thread told to return");
                        return;
                    }

                    try {
//                        Thread.sleep(2000);

                        // Get a buffer filled by the other thread
//logger.info("    DataChannel UDP stream out: get BB from BBsupply");
                        ByteBufferItem item = supply.consumerGet();
                        ByteBuffer buf = item.getBufferAsIs();
                        boolean isBuildable = item.getUserInt() == 1;
                        isEnd = item.getUserBoolean();

                        packetsSent[0] = 0;

//Utilities.printBuffer(buf, 0, 40, "PRESTART EVENT, buf lim = " + buf.limit());

                        // Put data into message:
                        // - first the meta data for reassembly
                        // - second the actual data

                        // If toLoadBalancer false, ignore all LB quantities:
                        // tick, entropy, protocol, and lbVersion

                        // Fast version overwrites part of incoming data buffer, but that's OK.
                        // It's not used after this.

//logger.info("    DataChannel UDP stream out: " + name + " - send packetized buffer of len " + buf.limit());
                        sendPacketizedBufferFast(
                                buf.array(), 0, buf.limit(),
                                packetStorage, maxUdpPayload,
                                outSocket, packet,
                                tick, entropy, lbProtocol, lbVersion,
                                recordId, id, reVersion,
                                delay, debug, packetsSent);


//                            sendPacketizedBufferSend(buf.array(), 0, buf.limit(),
//                                                           packetStorage, maxUdpPayload,
//                                                           outSocket, packet,
//                                                           tick, entropy, lbProtocol, lbVersion,
//                                                           recordId, id, reVersion,
//                                                           delay, debug, packetsSent);

                        // Increment record id or tick depending on if we're using VTP or Ersap RE header
                        recordId++;
                        tick++;

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
                        emu.setErrorState("DataChannel UDP stream out: " + e.getMessage());
                        return;
                    }

                    lastSendTime = emu.getTime();
                }
            }
        }


        /** Constructor. */
        DataOutputHelper() throws DataTransportException {
            super(emu.getThreadGroup(), name() + "_data_out");

            try {
                // Create UDP socket
logger.info("    DataChannel UDP out: create UDP sending socket");
                outSocket = new DatagramSocket();
                // Implementation dependent send buffer size
                outSocket.setSendBufferSize(sendBufSize);
                destAddr = InetAddress.getByName(destHost);
                //destAddr = InetAddress.getByName("192.168.0.125");
//logger.info("    DataChannel UDP out: connect UDP socket to dest " + destAddr.getHostName() + " port " + port);
                if (useConnectedSocket) {
                    outSocket.connect(destAddr, port);
                }

                logger.debug("    DataChannel UDP out: create UDP sending socket with " +
                        " with " + outSocket.getSendBufferSize() + " byte send buffer, on port " +
                        outSocket.getLocalPort() + " to port " + outSocket.getPort() + " to host " + destAddr.getHostName());
            }
            catch (Exception e) {
                throw new DataTransportException(e);
            }

            // Break data into multiple packets of MTU size.
            // Attempt to get MTU progamatically.
            int mtu = getMTU(outSocket);
logger.debug("MTU on socket = " + mtu);

            // I don't know how to set the MTU in java, so skip this set for now

            // 20 bytes = normal IPv4 packet header, 8 bytes = max UDP packet header
            maxUdpPayload = (mtu - 20 - 8 - HEADER_BYTES);

            // A mini ring of buffers, 16 is the best size.
            // All buffers will be released in order in this code.
            // This will improve performance since mutexes can be avoided.
            boolean orderedRelease = true;
System.out.println("DataOutputHelper constr: making BB supply of 16 bufs @ bytes = " + bufSize);
            bbOutSupply = new ByteBufferSupply(16, bufSize, byteOrder, direct, orderedRelease);

            // Start up sender thread
            sender = new SocketSender(bbOutSupply, destAddr, port);
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
            catch (InterruptedException | EvioException e) {/* never happen */}
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
            }
        }


        /** <p>
         * Send a buffer to a given destination by breaking it up into smaller
         * packets and sending these by UDP.
         * The receiver is responsible for reassembling these packets back into the original data.</p>
         *
         * Optimize by minimizing copying of data and calling "send" on a connected socket.
         * The very first packet is sent in buffer of copied data.
         * However, subsequently it writes the new header into the
         * dataBuffer just before the data to be sent, and then sends.
         * <b>Be warned that the original buffer will be changed after calling this routine!
         * This should not be a big deal as emu output channels send out each event only on
         * ONE channel by round-robin. The ER is an exception but only allows file and ET
         * output channels. So things should be fine.</b>
         *
         * @param dataBuffer     data to be sent.
         * @param readFromIndex  index into dataBuffer to start reading.
         * @param dataLen        number of bytes to be sent.
         * @param packetStorage  array in which to build a packet to send.
         *                       It's passed in as a parameter to avoid object creation and its
         *                       attendant strain on the garbage collector with each call.
         * @param maxUdpPayload  maximum number of bytes to place into one UDP packet.
         *
         * @param clientSocket   UDP sending socket.
         * @param udpPacket      UDP sending packet.
         *
         * @param tick           value used by load balancer (LB) in directing packets to final host.
         * @param entropy        entropy used by LB header in directing packets to a specific port
         *                       (but currently unused).
         * @param lbProtocol     protocol in LB header.
         * @param lbVersion      verion of LB header.
         *
         * @param recordId       record id in reassembly (RE) header.
         * @param dataId         data id in RE header.
         * @param reVersion      version of RE header.
         *
         * @param delay          delay in millisec between each packet being sent.
         * @param debug          turn debug printout on & off.
         * @param packetsSent    Array with one element.
         *                       Used to return the number of packets sent over network
         *                              (valid even if error returned).
         * @throws IOException if error sending packets
         */
        void sendPacketizedBufferFast(byte[] dataBuffer, int readFromIndex, int dataLen,
                                      byte[] packetStorage, int maxUdpPayload,
                                      DatagramSocket clientSocket, DatagramPacket udpPacket,
                                      long tick, int entropy, int lbProtocol, int lbVersion,
                                      int recordId, int dataId, int reVersion,
                                      int delay, boolean debug,
                                      int[] packetsSent)
                        throws IOException {

            int bytesToWrite, sentPackets = 0;

            // How many total packets are we sending? Round up.
            int totalPackets = (dataLen + maxUdpPayload - 1)/maxUdpPayload;

            // Index into packetStorage to write
            int writeToIndex = 0;

            // If this packet is the very first packet sent for this data buffer
            boolean veryFirstPacket = true;
            // If this packet is the very last packet sent for this data buffer
            boolean veryLastPacket  = false;

            int packetCounter = 0;
            // Use this flag to allow transmission of a single zero-length buffer
            boolean firstLoop = true;

            while (firstLoop || dataLen > 0) {

                // The number of regular data bytes to write into this packet
                bytesToWrite = dataLen > maxUdpPayload ? maxUdpPayload : dataLen;

                // Is this the very last packet for buffer?
                if (bytesToWrite == dataLen) {
                    veryLastPacket = true;
                }

                if (debug) System.out.println("Send " + bytesToWrite +
                                              " bytes, very first = " + veryFirstPacket +
                                              ", very last = " + veryLastPacket +
                        ", record id = " + recordId +
                        ", total packets = " + totalPackets +
                        ", packet counter = " + packetCounter +
                        ", writing to LB = " + useEjfatLoadBalancer +
                        ", writeToIndex = " + writeToIndex);

                // Write LB meta data into buffer
                try {
                    if (useEjfatLoadBalancer) {
                        // Write LB meta data into byte array
//logger.info("    DataChannel UDP stream: LB header: tick = " + tick + ", entropy = " + entropy);
                        writeLbHeader(packetStorage, writeToIndex, tick, lbVersion, lbProtocol, entropy);
                    }

                    // Write RE meta data into byte array
                    if (useErsapReHeader) {
                        writeErsapReHeader(packetStorage, writeToIndex + LB_HEADER_BYTES,
                                           reVersion, veryFirstPacket, veryLastPacket, (short)dataId,
                                           packetCounter++, tick);
                    }
                    else {
                        writeReHeader(packetStorage, writeToIndex + LB_HEADER_BYTES,
                                      dataId, veryFirstPacket, veryLastPacket, recordId, reVersion,
                                      totalPackets, packetCounter++);
                    }
                }
                catch (EmuException e) {/* never happen */}

                if (firstLoop) {
                    // Copy data for very first packet only
                    System.arraycopy(dataBuffer, readFromIndex,
                                     packetStorage, writeToIndex + HEADER_BYTES,
                                     bytesToWrite);
                }

                // "UNIX Network Programming" points out that a connect call made on a UDP client side socket
                // figures out and stores all the state about the destination socket address in advance
                // (masking, selecting interface, etc.), saving the cost of doing so on every send call.
                // This book claims that a connected socket can be up to 3x faster because of this reduced overhead -
                // data can go straight to the NIC driver bypassing most IP stack processing.
                // In our case, the calling function connected the socket.

                // Send message to receiver
                udpPacket.setData(packetStorage, writeToIndex, bytesToWrite + HEADER_BYTES);
                clientSocket.send(udpPacket);

                if (firstLoop) {
                    // Switch from external array to writing from dataBuffer for rest of packets
                    packetStorage = dataBuffer;
                    writeToIndex = -1 * HEADER_BYTES;
                }

                sentPackets++;

                // delay if any
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    }
                    catch (InterruptedException e) {}
                }

                dataLen -= bytesToWrite;
                writeToIndex += bytesToWrite;
                readFromIndex += bytesToWrite;
                veryFirstPacket = false;
                firstLoop = false;

                if (debug) System.out.println("Sent pkt " + (packetCounter - 1) +
                                              ", remaining bytes = " + dataLen + "\n");
            }

            packetsSent[0] = sentPackets;
        }



        /**
         * <p>
         * Send a buffer to a given destination by breaking it up into smaller
         * packets and sending these by UDP.
         * The receiver is responsible for reassembling these packets back into the original data.</p>
         *
         * This routine calls "send" on a connected socket.
         * All data (header and actual data from dataBuffer arg) are copied into a separate
         * buffer and sent. Unlike the {@link #sendPacketizedBufferFast} routine, the
         * original data is unchanged.
         *
         * @param dataBuffer     data to be sent.
         * @param readFromIndex  index into dataBuffer to start reading.
         * @param dataLen        number of bytes to be sent.
         * @param packetStorage  array in which to build a packet to send.
         *                       It's pssed in as a parameter to avoid object creation and its
         *                       attendant strain on the garbage collector with each call.
         * @param maxUdpPayload  maximum number of bytes to place into one UDP packet.
         *
         * @param clientSocket   UDP sending socket.
         * @param udpPacket      UDP sending packet.
         *
         * @param tick           value used by load balancer (LB) in directing packets to final host.
         * @param entropy        entropy used by LB header in directing packets to a specific port
         *                       (but currently unused).
         * @param protocol       protocol in LB header.
         * @param lbVersion      verion of LB header.
         *
         * @param recordId       record id in reassembly (RE) header.
         * @param dataId         data id in RE header.
         * @param reVersion      version of RE header.
         *
         * @param delay          delay in millisec between each packet being sent.
         * @param debug          turn debug printout on & off.
         * @param packetsSent    Array with one element.
         *                       Used to return the number of packets sent over network
         *                              (valid even if error returned).
         *
         * @throws IOException if error sending packets
         */
        void sendPacketizedBufferSend(byte[] dataBuffer, int readFromIndex, int dataLen,
                                      byte[] packetStorage, int maxUdpPayload,
                                      DatagramSocket clientSocket, DatagramPacket udpPacket,
                                      long tick, int entropy, int protocol, int lbVersion,
                                      int recordId, int dataId, int reVersion,
                                      int delay, boolean debug,
                                      int[] packetsSent)
                throws IOException {

            int bytesToWrite, sentPackets = 0;

            // How many total packets are we sending? Round up.
            int totalPackets = (dataLen + maxUdpPayload - 1)/maxUdpPayload;

            // The very first packet goes in here
            //byte[] packetStorage = new byte[maxUdpPayload + HEADER_BYTES];
            // Index into packetStorage to write
            int writeToIndex = 0;

            // If this packet is the very first packet sent for this data buffer
            boolean veryFirstPacket = true;
            // If this packet is the very last packet sent for this data buffer
            boolean veryLastPacket  = false;

            int packetCounter = 0;
            // Use this flag to allow transmission of a single zero-length buffer
            boolean firstLoop = true;

            while (firstLoop || dataLen > 0) {

                // The number of regular data bytes to write into this packet
                bytesToWrite = dataLen > maxUdpPayload ? maxUdpPayload : dataLen;

                // Is this the very last packet for all buffers?
                if (bytesToWrite == dataLen) {
                    veryLastPacket = true;
                }

                if (debug) System.out.println("Send " + bytesToWrite +
                        " bytes, very first = " + veryFirstPacket +
                        ", very last = " + veryLastPacket);

                // Write LB meta data into buffer
                try {
                    if (useEjfatLoadBalancer) {
                        // Write LB meta data into buffer
                        writeLbHeader(packetStorage, writeToIndex, tick, lbVersion, protocol, entropy);
                    }

                    // Write RE meta data into byte array
                    if (useErsapReHeader) {
                        writeErsapReHeader(packetStorage, writeToIndex + LB_HEADER_BYTES,
                                reVersion, veryFirstPacket, veryLastPacket, (short)dataId,
                                packetCounter++, tick);
                    }
                    else {
                        writeReHeader(packetStorage, writeToIndex + LB_HEADER_BYTES,
                                dataId, veryFirstPacket, veryLastPacket, recordId, reVersion,
                                totalPackets, packetCounter++);
                    }

                }
                catch (EmuException e) {/* never happen */}

                // This is where and how many bytes to write for data
                System.arraycopy(dataBuffer, readFromIndex,
                                 packetStorage, writeToIndex + HEADER_BYTES,
                                 bytesToWrite);

                // "UNIX Network Programming" points out that a connect call made on a UDP client side socket
                // figures out and stores all the state about the destination socket address in advance
                // (masking, selecting interface, etc.), saving the cost of doing so on every ::sendto call.
                // This book claims that ::send vs ::sendto can be up to 3x faster because of this reduced overhead -
                // data can go straight to the NIC driver bypassing most IP stack processing.
                // In our case, the calling function connected the socket, so we call "send".

                // Send message to receiver
                udpPacket.setData(packetStorage, writeToIndex, bytesToWrite + HEADER_BYTES);
                clientSocket.send(udpPacket);
                sentPackets++;

                // delay if any
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    }
                    catch (InterruptedException e) {}
                }

                dataLen -= bytesToWrite;
                readFromIndex += bytesToWrite;
                veryFirstPacket = false;
                firstLoop = false;

                if (debug) System.out.println("Sent pkt " + (packetCounter - 1) +
                        ", remaining bytes = " + dataLen + "\n");
            }

            packetsSent[0] = sentPackets;

            if (debug) System.out.println("Set next offset to = " + packetCounter);
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

            currentEventCount = 0;

            // Store flags for future use
            currentBBitem.setForce(force);
            currentBBitem.setUserBoolean(userBool);
            if (isData) {
                // Distinguish between user/control vs data
                currentBBitem.setUserInt(1);
            }

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
                    blockNum = 0;
                }
                else {
                    blockNum = -1;
                }

                //recordId++;
//System.out.println("    DataChannel UDP stream out: writeEvioData: record Id set to " + blockNum +
//                   ", then incremented to " + recordId);

                // Make sure there's enough room for that one event
                if (rItem.getTotalBytes() > currentBuffer.capacity()) {
                    currentBBitem.ensureCapacity(rItem.getTotalBytes() + 1024);
                    currentBuffer = currentBBitem.getBuffer();
//System.out.println("\n  &&&&&  DataChannel UDP stream out: writeEvioData:  expand 1 current buf -> rec # = " + currentBuffer.getInt(4));
                }

                // Write the event ..
                EmuUtilities.setEventType(bitInfo, eType);
                if (rItem.isFirstEvent()) {
                    EmuUtilities.setFirstEvent(bitInfo);
                }
//System.out.println("    DataChannel UDP stream out: writeEvioData: single write into buffer");
                writer.setBuffer(currentBuffer, bitInfo, blockNum);

                // Unset first event for next round
                EmuUtilities.unsetFirstEvent(bitInfo);

                ByteBuffer buf = rItem.getBuffer();
                if (buf != null) {
                    try {
//System.out.println("    DataChannel UDP stream out: writeEvioData: single ev buf, pos = " + buf.position() +
//                   ", lim = " + buf.limit() + ", cap = " + buf.capacity());
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
//System.out.println("    DataChannel UDP stream out: writeEvioData: flush " + eType + " type event, don't force ");
                    flushEvents(false, false, true);
                }
                else {
//System.out.println("    DataChannel UDP stream out: writeEvioData: flush " + eType + " type event, FORCE");
                    if (rItem.getControlType() == ControlType.END) {
//System.out.println("    DataChannel UDP stream out: writeEvioData: call flushEvents for END");
                        flushEvents(true, true, false);
                    }
                    else {
//System.out.println("    DataChannel UDP stream out: writeEvioData: call flushEvents for non-END");
                        flushEvents(true, false, false);
                    }
                }
            }
            // If we're marshalling events into a single buffer before sending ...
            else {
//System.out.println("    DataChannel UDP stream out: writeEvioData: events into buf, written = " + eventsWritten +
//", closed = " + writer.isClosed());
                // If we've already written at least 1 event AND
                // (we have no more room in buffer OR we're changing event types),
                // write what we have.
                if ((eventsWritten > 0 && !writer.isClosed())) {
                    // If previous type not data ...
                    if (previousEventType != eType) {
//System.out.println("    DataChannel UDP stream out: writeEvioData *** switch types, call flush at current event count = " + currentEventCount);
                        flushEvents(false, false, false);
                    }
                    // Else if there's no more room or have exceeded event count limit ...
                    else if (!writer.hasRoom(rItem.getTotalBytes()) ||
                            (regulateBufferRate && (currentEventCount >= eventsPerBuffer))) {
//System.out.println("    DataChannel UDP stream out: writeEvioData *** no room so call flush at current event count = " + currentEventCount);
                        flushEvents(false, false, true);
                    }
//                    else {
//System.out.println("    DataChannel UDP stream out: writeEvioData *** PLENTY OF ROOM, has room = " +
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
                    writer.setBuffer(currentBuffer, bitInfo, 0);
                    //recordId++;
//System.out.println("\nwriteEvioData: after setBuffer, eventsWritten = " + writer.getEventsWritten());
                }

//System.out.println("    DataChannel UDP stream write: write ev into buf");
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
                        // Since this is an output channel,
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
logger.info("    DataChannel UDP stream out " + outputIndex + ": send PRESTART event");
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

logger.info("    DataChannel UDP stream out " + outputIndex + ": send " + pBankControlType + " event");
                            writeEvioData(ringItem);

                            // Release and go to the next event
                            releaseCurrentAndGoToNextOutputRingItem(0);

                            // Done looking for the 2 control events
                            break;
                        }
                    }
                    // If user event ...
                    else if (pBankType == EventType.USER) {
logger.debug("    DataChannel UDP stream out " + outputIndex + ": send user event");
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
logger.info("    DataChannel UDP stream out: " + name + " got END event, quitting 1");
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

//logger.info("    DataChannel UDP stream out: send seq " + nextSequences[ringIndex] + ", release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(ringIndex);

                    // Do not go to the next ring if we got a control or user event.
                    // All prestart, go, & users go to the first ring. Just keep reading
                    // until we get to a built event. Then start keeping count so
                    // we know when to switch to the next ring.
                    if (outputRingCount > 1 && pBankControlType == null && !pBankType.isUser()) {
                        setNextEventAndRing();
//logger.info("    DataChannel UDP stream out, " + name + ": for seq " + nextSequences[ringIndex] + " SWITCH TO ring = " + ringIndex);
                    }

                    if (pBankControlType == ControlType.END) {
                        // END event automatically flushed in writeEvioData()
logger.info("    DataChannel UDP stream out: " + name + " got END event, quitting 2");
                        threadState = ThreadState.DONE;
                        return;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
logger.info("    DataChannel UDP stream out: " + name + " got RESET cmd, quitting");
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
                logger.warn("    DataChannel UDP stream out: " + name + "  interrupted thd, quitting");
            }
            catch (Exception e) {
                e.printStackTrace();
                channelState = CODAState.ERROR;
System.out.println("    DataChannel UDP stream out:" + e.getMessage());
                emu.setErrorState("DataChannel UDP stream out: " + e.getMessage());
            }
        }

    }

}
