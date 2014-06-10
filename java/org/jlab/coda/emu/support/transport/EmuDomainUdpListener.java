package org.jlab.coda.emu.support.transport;

import org.jlab.coda.cMsg.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.*;
import java.util.jar.JarOutputStream;

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
    public EmuDomainUdpListener(EmuDomainServer server, int port, String expid) throws cMsgException {

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
