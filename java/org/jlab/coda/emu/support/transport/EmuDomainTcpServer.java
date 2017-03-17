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

import org.jlab.coda.cMsg.cMsgConstants;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgNetworkConstants;
import org.jlab.coda.emu.support.codaComponent.CODAState;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * This class is the cMsg Emu domain TCP server run inside of an EMU.
 * It accepts connections from ROCs and SEBs. Its purpose is to implement
 * fast, efficient communication between those components and EBs and ERs.
 *
 * @author timmer (4/18/14)
 */
public class EmuDomainTcpServer extends Thread {


    /** Level of debug output for this class. */
    private int debug = cMsgConstants.debugError;

    private final int serverPort;

    private final EmuDomainServer server;

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
    public EmuDomainTcpServer(EmuDomainServer server, int serverPort) throws cMsgException {
        setName("Emu-socket TCP server");
        this.server = server;
        this.serverPort = serverPort;
    }


    /** This method is executed as a thread. */
    public void run() {
        if (debug >= cMsgConstants.debugInfo) {
            System.out.println("    Transport Emu: domain TCP server running");
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
                System.out.println("    Transport Emu: domain server, TCP port number " + serverPort + " already in use.");
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
//System.out.println("    Transport Emu: domain server, someone trying to connect");

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
                                System.out.println("    Transport Emu: domain server, try reading rest of Buffer");
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
                                System.out.println("    Transport Emu: domain server, bytes read = " + bytesRead);
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
                                        System.out.println("    Transport Emu: domain server, Magic #s did NOT match, ignore");
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
                                        System.out.println("    Transport Emu: domain server, version mismatch, got " +
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
                                        System.out.println("    Transport Emu: domain server, bad coda id of sender (" +
                                                           codaId + ')');
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
                                        System.out.println("    Transport Emu: domain server, bad buffer size from sender (" +
                                                           bufferSizeDesired + ')');
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

                        // Look up the associated channel in the transport object
                        DataChannelImplEmu emuChannel = server.transport.inputChannelTable.get(codaId);
                        if (emuChannel == null) {
                            if (debug >= cMsgConstants.debugInfo) {
                                System.out.println("    Transport Emu: domain server, no emu input channel found for CODA id = " +
                                                   codaId);
                            }
                            channel.close();
                            it.remove();
                            continue;
                        }

                        // The emu (not socket) channel will start a
                        // thread to handle all further communication.
                        try {
                            emuChannel.attachToInput(channel, codaId, bufferSizeDesired);
                        }
                        catch (IOException e) {
                            if (debug >= cMsgConstants.debugInfo) {
                                System.out.println("    Transport Emu: domain server, " + e.getMessage());
                            }
                            channel.close();
                            it.remove();
                            continue;
                        }

//System.out.println("    Transport Emu: domain server, new connection");
                    }

                    // remove key from selected set since it's been handled
                    it.remove();
                }
            }
        }
        catch (IOException ex) {
            server.transport.transportState = CODAState.ERROR;
            server.transport.emu.setErrorState("Transport Emu: IO error in emu TCP server");
            if (debug >= cMsgConstants.debugError) {
                System.out.println("    Transport Emu: domain server, IO error");
            }
        }
        finally {
            try {if (serverChannel != null) serverChannel.close();} catch (IOException e) {}
            try {if (selector != null) selector.close();} catch (IOException e) {}
        }

        if (debug >= cMsgConstants.debugInfo) {
            System.out.println("    Transport Emu: domain server, quitting");
        }
    }

}
