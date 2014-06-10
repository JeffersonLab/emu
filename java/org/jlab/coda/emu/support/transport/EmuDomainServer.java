package org.jlab.coda.emu.support.transport;

import org.jlab.coda.cMsg.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * Much of this copied from the the RCMulticastDomain's RCMulticast class.
 * @author timmer (4/18/14)
 */
public class EmuDomainServer extends Thread {


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
    private EmuDomainUdpListener listener;

    /** Thread that listens for TCP client connections and then handles client. */
    private EmuDomainTcpServer tcpServer;

    private DatagramPacket udpPacket;

    private final String expid;
    private final String name;

    private int debug;

    final DataTransportImplEmu transport;




    public EmuDomainServer(int port, String expid, String name,
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


    public EmuDomainTcpServer getTcpServer() {
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
            tcpServer = new EmuDomainTcpServer(this, serverPort, expid);
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
            listener = new EmuDomainUdpListener(this, serverPort, expid);
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
