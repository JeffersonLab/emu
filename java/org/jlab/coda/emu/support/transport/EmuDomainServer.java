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


/**
 * Much of this copied from the the RCMulticastDomain's RCMulticast class.
 * @author timmer (4/18/14)
 */
public class EmuDomainServer extends Thread {


    String domain = "emu";

    /** This server's UDP listening port. */
    final int serverPort;

    /** Thread that listens for UDP multicasts to this server and then responds. */
    private EmuDomainUdpListener listener;

    /** Thread that listens for TCP client connections and then handles client. */
    private EmuDomainTcpServer tcpServer;

    private final String expid;

    /** Containing emu's name. */
    private final String name;

    final DataTransportImplEmu transport;




    public EmuDomainServer(int port, String expid, String name,
                           DataTransportImplEmu transport) throws cMsgException {

        this.name = name;
        this.expid = expid;
        this.serverPort = port;
        this.transport = transport;
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
            tcpServer = new EmuDomainTcpServer(this, serverPort);
            tcpServer.start();

            // Wait for indication thread is running before continuing on
            synchronized (tcpServer) {
                if (!tcpServer.isAlive()) {
                    try {
                        tcpServer.wait();
                    }
                    catch (InterruptedException e) {
                        transport.transportState = CODAState.ERROR;
                        transport.emu.setErrorState("Transport Emu: error starting emu TCP server");
                        return;
                    }
                }
            }


            // Start listening for udp packets
            listener = new EmuDomainUdpListener(this, serverPort, expid, name);
            listener.start();

            // Wait for indication listener thread is running before continuing on
            synchronized (listener) {
                if (!listener.isAlive()) {
                    try {
                        listener.wait();
                    }
                    catch (InterruptedException e) {
                        transport.transportState = CODAState.ERROR;
                        transport.emu.setErrorState("Transport Emu: error starting emu UDP server");
                        return;
                    }
                }
            }
        }
        catch (cMsgException e) {
            transport.transportState = CODAState.ERROR;
            transport.emu.setErrorState("Transport Emu: error starting emu domain server: " + e.getMessage());
        }

        return;
    }


}
