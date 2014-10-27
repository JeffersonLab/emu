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

package org.jlab.coda.emu.test;


import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.transport.DataTransportImplEmu;
import org.jlab.coda.jevio.*;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * This class is designed to be a test receiver for a module who
 * has emu domain output channels.
 * @author timmer
 * (Oct 23, 2014)
 */
public class EmuDomainReceiver {

    private boolean debug;
    private int tcpPort;
    private String expid;

    /** Constructor. */
    EmuDomainReceiver(String[] args) {
        decodeCommandLine(args);
    }


    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private void decodeCommandLine(String[] args) {

        // loop over all args
        for (int i = 0; i < args.length; i++) {

            if (args[i].equalsIgnoreCase("-h")) {
                usage();
                System.exit(-1);
            }
            else if (args[i].equalsIgnoreCase("-p")) {
                String port = args[i + 1];
                i++;
                try {
                    tcpPort = Integer.parseInt(port);
                    if (tcpPort < 1024 || tcpPort > 65535) {
                        tcpPort = cMsgNetworkConstants.emuTcpPort;
                    }
                }
                catch (NumberFormatException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
            else if (args[i].equalsIgnoreCase("-x")) {
                expid = args[i + 1];
                i++;
            }
            else if (args[i].equalsIgnoreCase("-debug")) {
                debug = true;
            }
            else {
                usage();
                System.exit(-1);
            }
        }

        if (expid == null) {
            expid = System.getenv("EXPID");
            if (expid == null) {
                System.out.println("Provide an EXPID either on the cmd line");
                System.out.println("or in an environmental variable");
                System.exit(-1);
            }
        }

        return;
    }


    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage:\n\n" +
            "   java EmuDomainReceiver\n" +
            "        [-p <port>]   TCP port to listen on for connections\n" +
            "        [-x <expid>]  EXPID of experiment\n" +
            "        [-debug]      turn on printout\n" +
            "        [-h]          print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
        try {
            EmuDomainReceiver receiver = new EmuDomainReceiver(args);
            receiver.run();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    /** This method is executed as a thread. */
    public void run() {

        try {
            LocalEmuDomainServer server = new LocalEmuDomainServer(tcpPort, expid, "emu server", null);
            server.start();
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }
    }


}



/** Local copy of EmuDomainServer class. */
class LocalEmuDomainServer extends Thread {


    String domain = "emu";

    /** This server's UDP listening port. */
    final int serverPort;

    /** The local port used temporarily while multicasting for other rc multicast servers. */
    int localTempPort;

    /** Signal to coordinate the multicasting and waiting for responses. */
    CountDownLatch multicastResponse = new CountDownLatch(1);

    /** The host of the responding server to initial multicast probes of the local subnet. */
    String respondingHost;

    /** Only allow response to clients if server is properly started. */
    volatile boolean acceptingClients;

    /** Socket over which to UDP multicast to and check for other rc multicast servers. */
    private MulticastSocket multicastSocket;

    /** Timeout in milliseconds to wait for server to respond to multicasts. Default is 2 sec. */
    private int multicastTimeout = 2000;

    /** Thread that listens for UDP multicasts to this server and then responds. */
    private LocalEmuDomainUdpListener listener;

    /** Thread that listens for TCP client connections and then handles client. */
    private LocalEmuDomainTcpServer tcpServer;

    private DatagramPacket udpPacket;

    private final String expid;
    private final String name;

    private int debug;

    final DataTransportImplEmu transport;




    public LocalEmuDomainServer(int port, String expid, String name,
                                DataTransportImplEmu transport) throws cMsgException {

        this.name = name;
        this.expid = expid;
        this.serverPort = port;
        this.transport = transport;

        //-------------------------------------------------------
        // Get a UDP packet ready to send out in order to
        // multicast on local subnet to find other servers.
        // There may only be 1, ONE, server for this expid
        // and component name!
        //-------------------------------------------------------

        // Create byte array for multicast
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream out = new DataOutputStream(baos);

        try {
            // Put our TCP listening port (irrelevant here), our name, and
            // the EXPID (experiment id string) into byte array.

            // This multicast is from an emu multicast domain server
            out.writeInt(cMsgNetworkConstants.magicNumbers[0]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[1]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[2]);
            out.writeInt(cMsgNetworkConstants.emuDomainMulticastServer);
            // Port is irrelevant in this case
            out.writeInt(0);
            out.writeInt(name.length());
            out.writeInt(expid.length());
            try {
                out.write(name.getBytes("US-ASCII"));
                out.write(expid.getBytes("US-ASCII"));
            }
            catch (UnsupportedEncodingException e) {
            }
            out.flush();
            out.close();

            // Create socket to send multicasts to other Emu multicast servers
            multicastSocket = new MulticastSocket();
            multicastSocket.setTimeToLive(32);
            localTempPort = multicastSocket.getLocalPort();

            InetAddress emuServerMulticastAddress = null;
            try {emuServerMulticastAddress = InetAddress.getByName(cMsgNetworkConstants.emuMulticast); }
            catch (UnknownHostException e) {}

            // Create packet to multicast from the byte array
            byte[] buf = baos.toByteArray();
            udpPacket = new DatagramPacket(buf, buf.length, emuServerMulticastAddress, port);
            baos.close();
        }
        catch (IOException e) {
            try { out.close();} catch (IOException e1) {}
            try {baos.close();} catch (IOException e1) {}
            if (multicastSocket != null) multicastSocket.close();

System.out.println("I/O Error: " + e);
            throw new cMsgException(e.getMessage());
        }
    }


    public LocalEmuDomainTcpServer getTcpServer() {
        return tcpServer;
    }


    /** Stop all communication with Emu domain clients. */
    public void stopServer() {
        listener.killAllThreads();
        tcpServer.killAllThreads();
    }


    public void run() {

        try {
            // Start TCP server thread
            tcpServer = new LocalEmuDomainTcpServer(this, serverPort, expid);
            tcpServer.start();

            // Wait for indication thread is running before continuing on
            synchronized (tcpServer) {
                if (!tcpServer.isAlive()) {
                    try {
                        tcpServer.wait();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }


            // Start listening for udp packets
            listener = new LocalEmuDomainUdpListener(this, serverPort, expid);
            listener.start();

            // Wait for indication listener thread is running before continuing on
            synchronized (listener) {
                if (!listener.isAlive()) {
                    try {
                        listener.wait();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            // First check to see if there is another Emu multicast server
            // on this port with this EXPID. If so, all hands abandon ship.

//System.out.println("Find other EmuDomainServers");
            // Create a thread which will send out our multicast
            Multicaster sender = new Multicaster(udpPacket);
            sender.start();

            // Wait up to multicastTimeout milliseconds
            boolean response = false;
            try {
//System.out.println("Wait for response");
                if (multicastResponse.await(multicastTimeout, TimeUnit.MILLISECONDS)) {
//System.out.println("Got a response!");
                    response = true;
                }
            }
            catch (InterruptedException e) { }

            sender.interrupt();

            if (response) {
                // Stop all server threads
                stopServer();
                multicastSocket.close();
                throw new cMsgException("Another Emu multicast server is running at port " + serverPort +
                                                " host " + respondingHost + " with EXPID = " + expid);
            }
//System.out.println("No other Emu mMulticast server is running, so start this one up!");
            acceptingClients = true;

            // Closing the socket AFTER THE ABOVE LINE diminishes the chance that
            // a client on the same host will grab that localTempPort and so be
            // filtered out as being this same server's multicast.
            multicastSocket.close();
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }

        return;
    }



//-----------------------------------------------------------------------------


    /**
     * This class defines a thread to multicast a UDP packet to the
     * Emu multicast servers every 1/3 second.
     */
    class Multicaster extends Thread {

        DatagramPacket packet;

        Multicaster(DatagramPacket udpPacket) {
            packet = udpPacket;
        }


        public void run() {
            try {
                /* A slight delay here will help the main thread
                 * to be already waiting for a response from the server when we
                 * multicast to the server here (prompting that response). This
                 * will help insure no responses will be lost.
                 */
                Thread.sleep(100);

                while (true) {

                    try {
//System.out.println("  Send multicast packet to Emu multicast servers over each interface");
                        Enumeration<NetworkInterface> enumer = NetworkInterface.getNetworkInterfaces();
                        while (enumer.hasMoreElements()) {
                            NetworkInterface ni = enumer.nextElement();

//System.out.println("Emu multicast server: found interface " + ni +
//                           ", up = " + ni.isUp() +
//                           ", loopback = " + ni.isLoopback() +
//                           ", has multicast = " + ni.supportsMulticast());

                            if (ni.isUp() && ni.supportsMulticast() && !ni.isLoopback()) {
//System.out.println("Emu multicast server: sending mcast packet over " + ni.getName());
                                multicastSocket.setNetworkInterface(ni);
                                multicastSocket.send(packet);
                            }
                        }
                    }
                    catch (IOException e) {
                        // Probably multicastSocket closed in run()
                        return;
                    }

                    Thread.sleep(300);
                }
            }
            catch (InterruptedException e) {
                // Time to quit
 //System.out.println("Interrupted sender");
            }
        }
    }



}




/** Local copy of EmuDomainUdpListener class. */
class LocalEmuDomainUdpListener extends Thread {

    /** Emu multicast server that created this object. */
    private LocalEmuDomainServer server;

    /** UDP port on which to listen for emu client multi/unicasts. */
    private int multicastPort;

    /** UDP port on which to listen for emu client multi/unicasts. */
    private int tcpPort;

    /** UDP socket on which to read packets sent from rc clients. */
    private MulticastSocket multicastSocket;

    /** Level of debug output for this class. */
    private int debug = cMsgConstants.debugError;

    private String expid;

    /** Setting this to true will kill all threads. */
    private volatile boolean killThreads;


    /** Kills this and all spawned threads. */
    void killAllThreads() {
        killThreads = true;
        this.interrupt();
    }



    /**
     * Constructor.
     * @param server emu server that created this object
     * @param port udp port on which to receive transmissions from emu clients
     */
    public LocalEmuDomainUdpListener(LocalEmuDomainServer server, int port, String expid) throws cMsgException {

        this.expid = expid;
        multicastPort = tcpPort = port;

        try {
            // Create a UDP socket for accepting multi/unicasts from the Emu client
            multicastSocket = new MulticastSocket(multicastPort);
            SocketAddress sa =
                new InetSocketAddress(InetAddress.getByName(cMsgNetworkConstants.emuMulticast), multicastPort);
            // Be sure to join the multicast address group of all network interfaces
            // (something not mentioned in any javadocs or books!).
            Enumeration<NetworkInterface> enumer = NetworkInterface.getNetworkInterfaces();
            while (enumer.hasMoreElements()) {
                NetworkInterface ni = enumer.nextElement();
                if (ni.isUp() && ni.supportsMulticast() && !ni.isLoopback()) {
//System.out.println("Join group for " + cMsgNetworkConstants.emuMulticast +
//                    ", port = " + multicastPort + ", ni = " + ni.getName());
                    multicastSocket.joinGroup(sa, ni);
                }
            }
            multicastSocket.setReceiveBufferSize(65535);
            multicastSocket.setReuseAddress(true);
            multicastSocket.setTimeToLive(32);
        }
        catch (IOException e) {
            throw new cMsgException("Port " + multicastPort + " is taken", e);
        }
        this.server = server;
        // die if no more non-daemon threads running
        setDaemon(true);
    }


    /** This method is executed as a thread. */
    public void run() {

        if (debug >= cMsgConstants.debugInfo) {
            System.out.println("Emu Multicast Listening Thread: running");
        }

        // Create a packet to be written into from client
        byte[] buf = new byte[2048];
        DatagramPacket packet = new DatagramPacket(buf, 2048);

        // Prepare a packet to be send back to the client
        byte[] outBuf = null;
        DatagramPacket sendPacket  = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream out       = new DataOutputStream(baos);

        // Get our local IP addresses, canonical first
        ArrayList<String> ipAddresses = new ArrayList<String>(cMsgUtilities.getAllIpAddresses());

        try {
            // Put our special #s, TCP listening port, expid,
            // and all IP addresses into byte array.
            out.writeInt(cMsgNetworkConstants.magicNumbers[0]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[1]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[2]);
            out.writeInt(tcpPort);
            out.writeInt(ipAddresses.size());
            try {
                for (String addr : ipAddresses) {
                    out.writeInt(addr.length());
                    out.write(addr.getBytes("US-ASCII"));
                }
            }
            catch (UnsupportedEncodingException e) { }
            out.flush();
            out.close();

            // Create buffer to multicast from the byte array
            outBuf = baos.toByteArray();
            baos.close();
        }
        catch (IOException e) {
            if (debug >= cMsgConstants.debugError) {
                System.out.println("I/O Error: " + e);
            }
        }

        // EmuDomainServer object is waiting for this thread to start in, so tell it we've started.
        synchronized (this) {
            notifyAll();
        }

        // Listen for multicasts and interpret packets
        try {
            while (true) {
                if (killThreads) { return; }

                packet.setLength(2048);
//System.out.println("Udp listener: WAITING TO RECEIVE PACKET");
                multicastSocket.receive(packet);   // blocks
                if (debug >= cMsgConstants.debugInfo) {
                    System.out.println("     ***** RECEIVED EMU DOMAIN MULTICAST PACKET *****");
                }

                if (killThreads) { return; }

                // Pick apart byte array received
                InetAddress multicasterAddress = packet.getAddress();
                String multicasterHost = multicasterAddress.getHostName();
                int multicasterUdpPort = packet.getPort();   // Port to send response packet to

                if (packet.getLength() < 4*4) {
                    if (debug >= cMsgConstants.debugWarn) {
                        System.out.println("got multicast packet that's too small");
                    }
                    continue;
                }

                int magic1  = cMsgUtilities.bytesToInt(buf, 0);
                int magic2  = cMsgUtilities.bytesToInt(buf, 4);
                int magic3  = cMsgUtilities.bytesToInt(buf, 8);
                if (magic1 != cMsgNetworkConstants.magicNumbers[0] ||
                    magic2 != cMsgNetworkConstants.magicNumbers[1] ||
                    magic3 != cMsgNetworkConstants.magicNumbers[2])  {
                    if (debug >= cMsgConstants.debugWarn) {
                        System.out.println("got multicast packet with bad magic #s");
                    }
                    continue;
                }

                int msgType = cMsgUtilities.bytesToInt(buf, 12); // What type of message is this ?

                switch (msgType) {
                    // Multicasts from emu clients
                    case cMsgNetworkConstants.emuDomainMulticastClient:
//System.out.println("Client wants to connect");
                        out.writeInt(cMsgNetworkConstants.emuDomainMulticastServer);
                        break;
                    // Multicasts from emu servers
                    case cMsgNetworkConstants.emuDomainMulticastServer:
//System.out.println("Server wants to connect");
                        break;
                    // Kill this server since one already exists on this port/expid
                    case cMsgNetworkConstants.emuDomainMulticastKillSelf:
//System.out.println("Emu multicast server : Told to kill myself by another multicast server");
                        server.respondingHost = multicasterHost;
                        server.multicastResponse.countDown();
                        return;
                    // Packet from client just trying to locate emu multicast servers.
                    // Send back a normal response but don't do anything else.
                    case cMsgNetworkConstants.emuDomainMulticastProbe:
//System.out.println("I was probed");
                        break;
                    // Ignore packets from unknown sources
                    default:
//System.out.println("Unknown command");
                        continue;
                }

                int cMsgVersion = cMsgUtilities.bytesToInt(buf, 16); // cMsg version (see cMsg.EmuDomain.EmuClient.java)
                int nameLen     = cMsgUtilities.bytesToInt(buf, 20); // length of sender's name (# chars)
                int expidLen    = cMsgUtilities.bytesToInt(buf, 24); // length of expid (# chars)
                int pos = 28;

                // sender's name
                String multicasterName = null;
                try {
                    multicasterName = new String(buf, pos, nameLen, "US-ASCII");
                    pos += nameLen;
                }
                catch (UnsupportedEncodingException e) {}

                // sender's EXPID
                String multicasterExpid = null;
                try {
                    multicasterExpid = new String(buf, pos, expidLen, "US-ASCII");
                    pos += expidLen;
                }
                catch (UnsupportedEncodingException e) {}

//                if (debug >= cMsgConstants.debugInfo) {
//                    System.out.println("multicaster's host = " + multicasterHost + ", UDP port = " + multicasterUdpPort +
//                        ", cMsg version = " + cMsgVersion + ", name = " + multicasterName +
//                        ", expid = " + multicasterExpid);
//                }

                // Check for conflicting expids
                if (!expid.equalsIgnoreCase(multicasterExpid)) {
                    if (debug >= cMsgConstants.debugInfo) {
                        System.out.println("Conflicting EXPID's, ignoring");
                    }
                    continue;
                }

                // Before sending a reply, check to see if we simply got a packet
                // from our self when first connecting. Just ignore our own probing
                // multicast.

//                System.out.println("RC multicast server: accepting Clients = " + server.acceptingClients);
//                System.out.println("                   : local host = " + InetAddress.getLocalHost().getCanonicalHostName());
//                System.out.println("                   : multicaster's packet's host = " + multicasterHost);
//                System.out.println("                   : multicaster's packet's UDP port = " + multicasterUdpPort);
//                System.out.println("                   : multicaster's name = " + multicasterName);
//                System.out.println("                   : multicaster's expid = " + multicasterExpid);
//                System.out.println("                   : our port = " + server.localTempPort);

                if (multicasterUdpPort == server.localTempPort) {
//System.out.println("Emu multicast server : ignore my own udp messages");
                    continue;
                }

                // if multicast probe or connection request from client ...
                if (msgType == cMsgNetworkConstants.emuDomainMulticastProbe  ||
                    msgType == cMsgNetworkConstants.emuDomainMulticastClient)  {
                    try {
                        sendPacket = new DatagramPacket(outBuf, outBuf.length, multicasterAddress, multicasterUdpPort);
//System.out.println("Send response-to-probe packet to client");
                        multicastSocket.send(sendPacket);
                    }
                    catch (IOException e) {
                        System.out.println("I/O Error: " + e);
                    }
                }
                // else if multicast from server ...
                else {
                    // Other Emu multicast servers send "feelers" just trying see if another
                    // server is on the same port with the same EXPID.
                    if (debug >= cMsgConstants.debugInfo) {
                        System.out.println("Another Emu multicast server probing this one");
                    }

                    // If this server was properly started, tell the one probing us to kill itself
                    if (server.acceptingClients) {
                        // Create packet to respond to multicast
                        cMsgUtilities.intToBytes(cMsgNetworkConstants.magicNumbers[0], buf, 0);
                        cMsgUtilities.intToBytes(cMsgNetworkConstants.magicNumbers[1], buf, 4);
                        cMsgUtilities.intToBytes(cMsgNetworkConstants.magicNumbers[2], buf, 8);
                        cMsgUtilities.intToBytes(cMsgNetworkConstants.emuDomainMulticastKillSelf, buf, 12);
                        DatagramPacket pkt = new DatagramPacket(buf, 16, multicasterAddress, multicastPort);
System.out.println("Send response packet (kill yourself) to server");
                        multicastSocket.send(pkt);
                    }
                    else {
System.out.println("Still starting up but have been probed by starting server. So quit");
                        server.respondingHost = multicasterHost;
                        server.multicastResponse.countDown();
                        return;
                    }
                }
            }
        }
        catch (IOException e) {
            if (debug >= cMsgConstants.debugError) {
                System.out.println("emuDomainUdpListener: I/O ERROR in emu multicast server");
                System.out.println("emuDomainUdpListener: close multicast socket, port = " +
                                           multicastSocket.getLocalPort());
            }
        }
        finally {
            if (!multicastSocket.isClosed())  multicastSocket.close();
        }

        return;
    }


}



/** Local copy of EmuDomainTcpServer class. */
class LocalEmuDomainTcpServer extends Thread {


    /** Level of debug output for this class. */
    private int debug = cMsgConstants.debugError;

    private final int serverPort;

    private final LocalEmuDomainServer server;

    /** Setting this to true will kill all threads. */
    private volatile boolean killThreads;


    /** Kills this and all spawned threads. */
    void killAllThreads() {
        killThreads = true;
        this.interrupt();
    }


    /**
     * Constructor.
     * @param server emu server that created this object
     * @param serverPort TCP port on which to receive transmissions from emu clients
     */
    public LocalEmuDomainTcpServer(LocalEmuDomainServer server, int serverPort, String expid) throws cMsgException {
        this.server = server;
        this.serverPort = serverPort;
    }


    /** This method is executed as a thread. */
    public void run() {
        if (debug >= cMsgConstants.debugInfo) {
            System.out.println("Emu domain TCP server: running");
        }

        // Direct buffer for reading 3 magic & 3 other integers with non-blocking IO
        int BYTES_TO_READ = 6*4;
        ByteBuffer buffer = ByteBuffer.allocateDirect(BYTES_TO_READ);

        Selector selector = null;
        ServerSocketChannel serverChannel = null;

        try {
            // Get things ready for a select call
            selector = Selector.open();

            // Bind to the given TCP listening port. If not possible, throw exception
            try {
                serverChannel = ServerSocketChannel.open();
                ServerSocket listeningSocket = serverChannel.socket();
                listeningSocket.setReuseAddress(true);
                // We prefer high bandwidth, low latency, & short connection times, in that order
                listeningSocket.setPerformancePreferences(0,1,2);
                listeningSocket.bind(new InetSocketAddress(serverPort));
            }
            catch (IOException ex) {
                System.out.println("Emu domain server: TCP port number " + serverPort + " in use.");
                System.exit(-1);
            }

            // Set non-blocking mode for the listening socket
            serverChannel.configureBlocking(false);

            // Register the channel with the selector for accepts
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            // EmuDomainServer object is waiting for this thread to start, so tell it we've started.
            synchronized (this) {
                notifyAll();
            }

            while (true) {
                // 3 second timeout
                int n = selector.select(3000);

                // If no channels (sockets) are ready, listen some more
                if (n == 0) {
                    // But first check to see if we've been commanded to die
                    if (killThreads) {
                        return;
                    }
                    continue;
                }
//System.out.println("Emu domain server: someone trying to connect");

                // Get an iterator of selected keys (ready sockets)
                Iterator it = selector.selectedKeys().iterator();

                // Look at each key
                keyLoop:
                while (it.hasNext()) {
                    SelectionKey key = (SelectionKey) it.next();

                    // Is this a new connection coming in?
                    if (key.isValid() && key.isAcceptable()) {

                        // Accept the connection from the client
                        SocketChannel channel = serverChannel.accept();

                        // Check to see if this is a legit cMsg client or some imposter.
                        // Don't want to block on read here since it may not be a cMsg
                        // client and may block forever - tying up the server.
                        int version, codaId=-1, bufferSizeDesired=-1;
                        int bytes, bytesRead=0, loops=0;
                        buffer.clear();
                        buffer.limit(BYTES_TO_READ);
                        channel.configureBlocking(false);

                        // Loop until all 6 integers of incoming data read or timeout
                        while (bytesRead < BYTES_TO_READ) {
                            if (debug >= cMsgConstants.debugInfo) {
                                System.out.println("Emu domain server: try reading rest of Buffer");
                            }

                            bytes = channel.read(buffer);

                            // Check for End-of-stream ...
                            if (bytes == -1) {
                                channel.close();
                                it.remove();
                                continue keyLoop;
                            }

                            bytesRead += bytes;

                            if (debug >= cMsgConstants.debugInfo) {
                                System.out.println("Emu domain server: bytes read = " + bytesRead);
                            }

                            // If we've read everything, look to see what we got ...
                            if (bytesRead >= BYTES_TO_READ) {
                                buffer.flip();

                                // Check for correct magic #s
                                int magic1 = buffer.getInt();
                                int magic2 = buffer.getInt();
                                int magic3 = buffer.getInt();
                                if (magic1 != cMsgNetworkConstants.magicNumbers[0] ||
                                    magic2 != cMsgNetworkConstants.magicNumbers[1] ||
                                    magic3 != cMsgNetworkConstants.magicNumbers[2])  {
                                    if (debug >= cMsgConstants.debugInfo) {
                                        System.out.println("Emu domain server: Magic #s did NOT match, ignore");
                                    }
                                    channel.close();
                                    it.remove();
                                    continue keyLoop;
                                }

                                // Check for server / client compatibility for cMsg version
                                version = buffer.getInt();
//System.out.println("Got version = " + version);
                                if (version != cMsgConstants.version) {
                                    if (debug >= cMsgConstants.debugInfo) {
                                        System.out.println("Emu domain server: version mismatch, got " +
                                                            version + ", needed " + cMsgConstants.version);
                                    }
                                    channel.close();
                                    it.remove();
                                    continue keyLoop;
                                }

                                // CODA id of sender
                                codaId = buffer.getInt();
//System.out.println("Got coda id = " + codaId);
                                if (codaId < 0) {
                                    if (debug >= cMsgConstants.debugInfo) {
                                        System.out.println("Emu domain server: bad coda id of sender (" +
                                                           codaId + ")");
                                    }
                                    channel.close();
                                    it.remove();
                                    continue keyLoop;
                                }

                                // Max size buffers to hold incoming data in bytes
                                bufferSizeDesired = buffer.getInt();
//System.out.println("Got buffer size = " + bufferSizeDesired);
                                if (bufferSizeDesired < 4*10) {
                                    // 40 bytes is smallest possible evio file format size
                                    if (debug >= cMsgConstants.debugInfo) {
                                        System.out.println("Emu domain server: bad buffer size from sender (" +
                                                           bufferSizeDesired + ")");
                                    }
                                    channel.close();
                                    it.remove();
                                    continue keyLoop;
                                }
                            }
                            else {
                                // Give client 10 loops (.1 sec) to send its stuff, else no deal
                                if (++loops > 10) {
                                    channel.close();
                                    it.remove();
                                    continue keyLoop;
                                }
                                try { Thread.sleep(30); }
                                catch (InterruptedException e) { }
                            }
                        }

                        // Go back to using streams
                        channel.configureBlocking(true);

                        // Create a channel new
                        InputDataChannelImplEmu emuChannel = new InputDataChannelImplEmu();

                        // The emu (not socket) channel will start a
                        // thread to handle all further communication.
                        try {
                            emuChannel.attachToInput(channel, codaId, bufferSizeDesired);
                        }
                        catch (IOException e) {
                            if (debug >= cMsgConstants.debugInfo) {
                                System.out.println("Emu domain server: " + e.getMessage());
                            }
                            channel.close();
                            it.remove();
                            continue;
                        }

                        if (debug >= cMsgConstants.debugInfo) {
                            System.out.println("Emu domain server: new connection");
                        }
                    }

                    // remove key from selected set since it's been handled
                    it.remove();
                }
            }
        }
        catch (IOException ex) {
            System.out.println("Emu domain server: main server IO error");
            ex.printStackTrace();
        }
        finally {
            try {if (serverChannel != null) serverChannel.close();} catch (IOException e) {}
            try {if (selector != null) selector.close();} catch (IOException e) {}
        }

        if (debug >= cMsgConstants.debugInfo) {
            System.out.println("Emu domain server: quitting");
        }
    }

}



/** This class is a simplified version of DataChannelImplEmu class. */
class InputDataChannelImplEmu {

    /** Read END event from input ring. */
    private volatile boolean haveInputEndEvent;

    /** TCP no delay setting. */
    private boolean noDelay;

    /** Coda id of the data source. */
    private int sourceId;

    // INPUT

    /** Thread used to input data. */
    private DataInputHelper dataInputThread;

    /** Data input stream from TCP socket. */
    private DataInputStream in;

    /** TCP receive buffer size in bytes. */
    private int tcpRecvBuf;

    // INPUT & OUTPUT

    /** Biggest chunk of data sent by data producer.
     *  Allows good initial value of ByteBuffer size.  */
    private int maxBufferSize;

    //-------------------------------------------
    // Disruptor (RingBuffer)  Stuff
    //-------------------------------------------
//    private long nextRingItem;

    /** Ring buffer holding ByteBuffers when using EvioCompactEvent reader for incoming events. */
    protected ByteBufferSupply bbSupply;

//    /** Ring buffer - one per input channel. */
//    protected RingBuffer<RingItem> ringBufferIn;
//
//    /** Number of items in input ring buffer. */
//     protected int inputRingItemCount  = 2048;
//


    /**
     * Constructor to create a new DataChannelImplEt instance. Used only by
     * {@link org.jlab.coda.emu.support.transport.DataTransportImplEt#createChannel(String, java.util.Map, boolean, org.jlab.coda.emu.Emu, org.jlab.coda.emu.EmuModule, int)}
     * which is only used during PRESTART in {@link org.jlab.coda.emu.Emu}.
     *
     *
     */
    InputDataChannelImplEmu()  {

//        ringBufferIn =
//                createSingleProducer(new RingItemFactory(ModuleIoType.PayloadBuffer),
//                                     inputRingItemCount, new YieldingWaitStrategy());

    }


    /**
     * Once a client connects to the Emu domain server in the Emu transport object,
     * that socket is passed to this method and a thread is spawned to handle all
     * communications over it. Only used for input channel.
     *
     * @param channel
     */
    void attachToInput(SocketChannel channel, int sourceId, int maxBufferSize) throws IOException {
        this.sourceId = sourceId;
        this.maxBufferSize = maxBufferSize;

        // Set socket options
        Socket socket = channel.socket();
        // Set TCP no-delay so no packets are delayed
        socket.setTcpNoDelay(noDelay);
        // Set TCP receive buffer size
        if (tcpRecvBuf > 0) {
            socket.setReceiveBufferSize(tcpRecvBuf);
        }

        // Use buffered streams for efficiency
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 256000));

        // Create a ring buffer full of empty ByteBuffer objects
        // in which to copy incoming data from client.
        bbSupply = new ByteBufferSupply(128, maxBufferSize);

        // Start thread to handle all socket input
        startInputThread();
    }



    /**
     * For input channel, start the DataInputHelper thread which takes Evio
     * file-format data, parses it, puts the parsed Evio banks into the ring buffer.
     */
    private final void startInputThread() {
        dataInputThread = new DataInputHelper();
        dataInputThread.start();
        dataInputThread.waitUntilStarted();
    }



    /**
     * Class used to get data over network events, parse them into Evio banks,
     * and put them onto a ring buffer.
     */
    private class DataInputHelper extends Thread {

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch latch = new CountDownLatch(1);

        /** Read into ByteBuffers. */
        private EvioCompactReader compactReader;



        /** Constructor. */
        DataInputHelper() {
System.out.println("      DataChannel Emu in: start EMU input thread");
        }


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {}
        }


        /** {@inheritDoc} */
        public void run() {

            // Tell the world I've started
            latch.countDown();

            try {
                int command;
                boolean delay = false;

                while ( true ) {

                    // Read the command first
                    command = in.readInt();
//System.out.println("      DataChannel Emu in: cmd = 0x" + Integer.toHexString(command));
//                    Thread.sleep(1000);

                    // 1st byte has command
                    switch (command & 0xff) {
                        case cMsgConstants.emuEvioFileFormat:
                            handleEvioFileToBuf();
                            break;

                        case cMsgConstants.emuEnd:
System.out.println("      DataChannel Emu in: get emuEnd cmd");
                            break;

                        default:
//System.out.println("      DataChannel Emu in: unknown command from Emu client = " + (command & 0xff));
                    }

                    if (haveInputEndEvent) {
                        break;
                    }
                }

            } catch (Exception e) {
                System.out.println("      DataChannel Emu in: exit thd: " + e.getMessage());
            }

        }


        private final void handleEvioFileToBuf() throws IOException, EvioException {

            EvioNode node;
            EventType bankType;
            PayloadBuffer payloadBuffer;
            ControlType controlType = null;

            // Get a reusable ByteBuffer
            ByteBufferItem bbItem = bbSupply.get();
            ByteBuffer buf = bbItem.getBuffer();

            // Read the length of evio file-format data to come
            int evioBytes = in.readInt();
//System.out.println("      DataChannel Emu in: len = " + evioBytes);
            // If buffer is too small, make a bigger one
            bbItem.ensureCapacity(evioBytes);

            // Read evio file-format data
            in.readFully(buf.array(), 0, evioBytes);

            try {
                if (compactReader == null) {
                    compactReader = new EvioCompactReader(buf);
                }
                else {
                    compactReader.setBuffer(buf);
                }
            }
            catch (EvioException e) {
                e.printStackTrace();
                throw e;
            }

            // First block header in buffer
            BlockHeaderV4 blockHeader = compactReader.getFirstBlockHeader();
            if (blockHeader.getVersion() < 4) {
                throw new EvioException("Evio data needs to be written in version 4+ format");
            }

            EventType eventType = EventType.getEventType(blockHeader.getEventType());

            // Each PayloadBuffer contains a reference to the buffer it was
            // parsed from (buf).
            // This cannot be released until the module is done with it.
            // Keep track by counting users (# events parsed from same buffer).
            int eventCount = compactReader.getEventCount();
            bbItem.setUsers(eventCount);

//logger.info("      DataChannel Emu in: " + name + " block header, event type " + eventType +
//            ", src id = " + sourceId + ", recd id = " + recordId + ", event cnt = " + eventCount);

            for (int i=1; i < eventCount+1; i++) {
                node = compactReader.getScannedEvent(i);

                // Complication: from the ROC, we'll be receiving USER events
                // mixed in with and labeled as ROC Raw events. Check for that
                // and fix it.
                bankType = eventType;
                if (eventType == EventType.ROC_RAW) {
                    if (Evio.isUserEvent(node)) {
                        bankType = EventType.USER;
                    }
                }
                else if (eventType == EventType.CONTROL) {
                    // Find out exactly what type of control event it is
                    // (May be null if there is an error).
                    // TODO: It may NOT be enough just to check the tag
                    controlType = ControlType.getControlType(node.getTag());
                    if (controlType == null) {
                        throw new EvioException("Found unidentified control event");
                    }
                }

                bbSupply.release(bbItem);

                // Handle end event ...
                if (controlType == ControlType.END) {
                    // There should be no more events coming down the pike so
                    // go ahead write out existing events and then shut this
                    // thread down.
System.out.println("      DataChannel Emu in: found END event");
                    haveInputEndEvent = true;
                    // run callback saying we got end event
                    break;
                }
            }
        }

    }



}