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

import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.support.codaComponent.CODAState;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.*;

/**
 * Basically a copy of RCMulticastDomain's rcListeningThread class.
 * @author timmer (4/18/14)
 */
public class EmuDomainUdpListener extends Thread {

    /** Emu multicast server that created this object. */
    private EmuDomainServer server;

    /** UDP port on which to listen for emu client multi/unicasts. */
    private int multicastPort;

    /** UDP port on which to listen for emu client multi/unicasts. */
    private int tcpPort;

    /** UDP socket on which to read packets sent from rc clients. */
    private MulticastSocket multicastSocket;

    /** Level of debug output for this class. */
    private int debug = cMsgConstants.debugError;

    private String expid;
    private String emuName;

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
    public EmuDomainUdpListener(EmuDomainServer server, int port,
                                String expid, String emuName) throws cMsgException {

        this.expid = expid;
        this.emuName = emuName;
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
//System.out.println("Emu listen: join group for " + cMsgNetworkConstants.emuMulticast +
//                    ", port = " + multicastPort + ", ni = " + ni.getName());
                    multicastSocket.joinGroup(sa, ni);
                }
            }
            multicastSocket.setReceiveBufferSize(65535);
            multicastSocket.setReuseAddress(true);
            multicastSocket.setTimeToLive(32);
        }
        catch (IOException e) {
            System.out.println("Emu domain server: UDP port number " + multicastPort + " already in use.");
            System.exit(-1);
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

        try {
            // Put our special #s, TCP listening port, expid,
            // and all IP addresses into byte array.
            out.writeInt(cMsgNetworkConstants.magicNumbers[0]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[1]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[2]);
            out.writeInt(tcpPort);

            try {
                // List of all IP data (no IPv6, no loopback, no down interfaces)
                List<InterfaceAddress> ifAddrs = cMsgUtilities.getAllIpInfo();
                int addrCount = ifAddrs.size();

                // Let folks know how many address pairs are coming
                out.writeInt(addrCount);

                for (InterfaceAddress ifAddr : ifAddrs) {
                    Inet4Address bAddr;
                    try { bAddr = (Inet4Address)ifAddr.getBroadcast(); }
                    catch (ClassCastException e) {
                        // should never happen since IPv6 already removed
                        continue;
                    }

                    String broadcastAddr = bAddr.getHostAddress();
                    String ipAddr = ifAddr.getAddress().getHostAddress();

                    out.writeInt(ipAddr.length());
//System.out.println("Emu listen: ip size = " + ipAddr.length());
                    out.write(ipAddr.getBytes("US-ASCII"));
//System.out.println("Emu listen: ip = " + ipAddr);
                    out.writeInt(broadcastAddr.length());
//System.out.println("Emu listen: broad size = " + broadcastAddr.length());
                    out.write(broadcastAddr.getBytes("US-ASCII"));
//System.out.println("Emu listen: broad = " + broadcastAddr);
                }
            }
            catch (UnsupportedEncodingException e) { /* never happen*/ }

            out.flush();
            out.close();

            // Create buffer to multicast from the byte array
            outBuf = baos.toByteArray();
            baos.close();
        }
        catch (IOException e) {
            server.transport.transportState = CODAState.ERROR;
            server.transport.emu.setErrorState("Transport Emu: IO error in emu UDP server");
            if (debug >= cMsgConstants.debugError) {
                System.out.println("Emu domain UDP server: main server IO error: " + e.getMessage());
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
//System.out.println("Emu UDP listen: WAITING TO RECEIVE PACKET");
                multicastSocket.receive(packet);   // blocks

                if (killThreads) { return; }

                // Pick apart byte array received
                InetAddress multicasterAddress = packet.getAddress();
                String multicasterHost = multicasterAddress.getHostName();
                int multicasterUdpPort = packet.getPort();   // Port to send response packet to

                if (packet.getLength() < 4*4) {
                    if (debug >= cMsgConstants.debugWarn) {
                        System.out.println("Emu UDP listen: got multicast packet that's too small");
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
                        System.out.println("Emu UDP listen: got multicast packet with bad magic #s");
                    }
                    continue;
                }

                int msgType = cMsgUtilities.bytesToInt(buf, 12); // What type of message is this ?

                switch (msgType) {
                    // Multicasts from emu clients
                    case cMsgNetworkConstants.emuDomainMulticastClient:
//System.out.println("Emu UDP listen: client wants to connect");
                        break;
                    // Packet from client just trying to locate emu multicast servers.
                    // Send back a normal response but don't do anything else.
                    case cMsgNetworkConstants.emuDomainMulticastProbe:
//System.out.println("Emu UDP listen: I was probed");
                        break;
                    // Ignore packets from unknown sources
                    default:
//System.out.println("Emu UDP listen: unknown command");
                        continue;
                }

                int cMsgVersion = cMsgUtilities.bytesToInt(buf, 16); // cMsg version (see cMsg.EmuDomain.EmuClient.java)
                int nameLen     = cMsgUtilities.bytesToInt(buf, 20); // length of sender's name (# chars)
                int expidLen    = cMsgUtilities.bytesToInt(buf, 24); // length of expid (# chars)
                int pos = 28;

                // Check for conflicting cMsg versions
                if (cMsgVersion != cMsgConstants.version) {
                    if (debug >= cMsgConstants.debugInfo) {
                        System.out.println("Emu UDP listen: conflicting cMsg versions, got " +
                                                   cMsgVersion + ", need " + cMsgConstants.version);
                    }
                    continue;
                }

                // destination/server CODA component name
                // (this component's if client, probing server if server)
                String componentName = null;
                try {
                    componentName = new String(buf, pos, nameLen, "US-ASCII");
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

                if (debug >= cMsgConstants.debugInfo) {
                    System.out.println("Emu UDP listen: multicaster's host = " + multicasterHost +
                                       ", UDP port = " + multicasterUdpPort +
                                       ", cMsg version = " + cMsgVersion + ", name = " +
                                       componentName + ", expid = " + multicasterExpid);
                }

                // Check for conflicting expids
                if (!expid.equalsIgnoreCase(multicasterExpid)) {
                    if (debug >= cMsgConstants.debugInfo) {
                        System.out.println("Emu listen: conflicting EXPIDs, got " + multicasterExpid +
                                           ", need " + expid);
                    }
                    continue;
                }

                // Before sending a reply, check to see if we simply got a packet
                // from our self when first connecting. Just ignore our own probing
                // multicast.

//                System.out.println("Emu UDP listen: accepting Clients = " + server.acceptingClients);
//                System.out.println("          : local host = " + InetAddress.getLocalHost().getCanonicalHostName());
//                System.out.println("          : multicaster's packet's host = " + multicasterHost);
//                System.out.println("          : multicaster's packet's UDP port = " + multicasterUdpPort);
//                System.out.println("          : multicaster's expid = " + multicasterExpid);
//                System.out.println("          : component's name = " + componentName);
//                System.out.println("          : our port = " + server.localTempPort);

                if (multicasterUdpPort == server.localTempPort) {
//System.out.println("Emu UDP listen: ignore my own udp messages");
                    continue;
                }

                // If connection request from client, don't accept if they're
                // looking to connect to a different emu name
                if (msgType == cMsgNetworkConstants.emuDomainMulticastClient &&
                    !componentName.equalsIgnoreCase(emuName)) {

                    if (debug >= cMsgConstants.debugInfo) {
                        System.out.println("Emu UDP listen: this emu wrong destination, I am " +
                                                   emuName + ", client looking for " + componentName);
                    }
                    continue;
                }

                try {
                    sendPacket = new DatagramPacket(outBuf, outBuf.length, multicasterAddress, multicasterUdpPort);
//System.out.println("Emu UDP listen: send response-to-probe packet to client");
                    multicastSocket.send(sendPacket);
                }
                catch (IOException e) {
                    System.out.println("I/O Error: " + e);
                }
            }
        }
        catch (IOException e) {
            server.transport.transportState = CODAState.ERROR;
            server.transport.emu.setErrorState("Transport Emu: IO error in emu UDP server");
            if (debug >= cMsgConstants.debugError) {
                System.out.println("Emu UDP server: IO error: " + e.getMessage());
            }
        }
        finally {
            if (!multicastSocket.isClosed())  multicastSocket.close();
        }

        if (debug >= cMsgConstants.debugInfo) {
            System.out.println("Emu UDP server: quitting");
        }
    }



}
