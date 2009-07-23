/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.support.codaComponent.CODAState;
import org.jlab.coda.support.codaComponent.CODATransition;
import org.jlab.coda.support.codaComponent.RunControl;
import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.data.DataTransportRecord;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.util.Map;

/**
 * <pre>
 * Class <b>TransportImplSO </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class DataTransportImplSO extends DataTransportCore implements DataTransport {

    /** Field connected */
    private boolean connected = false;

    /** Field multicastAddr */
    private InetAddress multicastAddr = null;

    /** Field multicastPort */
    private int multicastPort;

    /** Field localhost */
    private InetAddress localhost;

    /** Field multicastSocket */
    private MulticastSocket multicastSocket;

    /** Field serverSocket */
    private ServerSocket serverSocket;

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /**
     * Constructor TransportImplSO creates a new TransportImplSO instance.
     *
     * @param pname  of type String
     * @param attrib of type Map
     *
     * @throws DataNotFoundException when
     */
    public DataTransportImplSO(String pname, Map<String, String> attrib) throws DataNotFoundException {
        super(pname, attrib);
    }

    /** Method close ... */
    public void close() {
        connected = false;
        // close remaining channels.
        if (!channels.isEmpty()) {
            synchronized (channels) {
                for (DataChannel c : channels.values()) {
                    c.close();
                }
                channels.clear();
            }
        }
    }

    /**
     * <pre>
     * Class <b>MulticastHelper </b>
     * Listen on a multicast socket and respond if we receive
     * a packet with our name in it. Respond with our name, host
     * address and server socket port number. This is run if we
     * are a server and someone is looking for us.
     * </pre>
     *
     * @author heyes
     *         Created on Sep 17, 2008
     */
    private class MulticastHelper implements Runnable {

        /** Method run ... */
        @SuppressWarnings({"InfiniteLoopStatement"})
        public void run() {

            try {
                byte[] buffer = new byte[65535];
                DatagramPacket dp = new DatagramPacket(buffer, buffer.length);

                multicastSocket = new MulticastSocket(multicastPort);
                multicastSocket.joinGroup(multicastAddr);
                multicastSocket.setTimeToLive(3);
                while (true) {
                    // receive packet of client looking for server
                    multicastSocket.receive(dp);
                    String s = new String(dp.getData(), 0, dp.getLength());

                    if (s.matches(name())) {
                        // if names match send a response packet with our name, host, and server port
                        s = name() + " " + localhost.getHostAddress() + " " + serverSocket.getLocalPort();
                        dp.setData(s.getBytes());
                        multicastSocket.send(dp);
                    }
                }
            } catch (SocketException se) {
                System.err.println(se);
            } catch (IOException ie) {
                System.err.println(ie);
            }

        }

    }

    /**
     * <pre>
     * Class <b>AcceptHelper </b>
     * Listen on a TCP server socket, accept connection,
     * read in length and name, Use that name to retrieve
     * DataChannel, and the socket to receive data as part
     * of that DataChannel.
     *
     * </pre>
     *
     * @author heyes
     *         Created on Sep 17, 2008
     */
    private class AcceptHelper implements Runnable {

        /** Method run ... */
        public void run() {
            byte[] cbuf = new byte[100];
            int l;
            try {
                serverSocket.setReceiveBufferSize(100000);
            } catch (SocketException e1) {
                e1.printStackTrace();
            }
            while (true) {
                try {
                    Socket incoming = serverSocket.accept();
                    incoming.setTcpNoDelay(true);

                    DataInputStream in = new DataInputStream(incoming.getInputStream());

                    l = in.read();

                    int bytes = in.read(cbuf, 0, l);

                    String s = new String(cbuf, 0, bytes);

                    DataChannelImplSO c = (DataChannelImplSO) channels.get(s);

                    if (c != null) {
                        c.setDataSocket(incoming);
                        c.startInputHelper();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }

        }

    }

    /**
     * Method startServer ...
     *
     * @throws DataTransportException when
     */
    void startServer() throws DataTransportException {
        // set connected flag

        try {
            localhost = InetAddress.getLocalHost();
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
            throw new DataTransportException(e1.getMessage());
        }

        try {
            serverSocket = new ServerSocket(0, 1, localhost);
        } catch (IOException e1) {
            e1.printStackTrace();
            throw new DataTransportException(e1.getMessage());
        }

        // run this thread if we're a server and someone is looking for us by multicasting
        Thread multicastHelperThread = new Thread(Emu.THREAD_GROUP, new MulticastHelper(), name() + " multicast");
        multicastHelperThread.start();

        // listen on server socket for clients
        Thread acceptHelperThread = new Thread(Emu.THREAD_GROUP, new AcceptHelper(), name() + " accept");
        acceptHelperThread.start();
        
        connected = true;
    }

    /**
     * <pre>
     * Class <b>ConnectHelper </b>
     * Create a multicast socket and send out packets
     * with our name in it. If we're a client we do this
     * to find a server.
     * </pre>
     *
     * @author heyes
     *         Created on Sep 17, 2008
     */
    private class ConnectHelper implements Runnable {

        /** Method run ... */
        public void run() {
            try {
                byte[] buffer = new byte[65535];
                DatagramPacket dp = new DatagramPacket(buffer, buffer.length);

                multicastSocket = new MulticastSocket();
                multicastSocket.joinGroup(multicastAddr);
                multicastSocket.setTimeToLive(20);
                
                while (true) {

                    // send out a packet with our name in it
                    String s = name();
                    DatagramPacket odp = new DatagramPacket(s.getBytes(), s.length(), multicastAddr, multicastPort);
                    multicastSocket.send(odp);

                    // Receive a response packet if there is one from a server
                    // of the above name (s), else block until there is one.
                    multicastSocket.receive(dp);

                    // response should be string with space separation of: name, host, server port
                    String s1 = new String(dp.getData(), 0, dp.getLength());

                    String[] values = s1.split(" ");

                    // if the name matches ours ...
                    if (values[0].matches(name())) {

                        // create socket to the given host and port
                        Socket dataSocket = new Socket(values[1], Integer.parseInt(values[2]));

                        dataSocket.setTcpNoDelay(true);
                        OutputStream out = dataSocket.getOutputStream();
                        DataOutputStream dout = new DataOutputStream(out);

                        String emuName = Emu.INSTANCE.name();
                        String channelName = name() + ":" + emuName;

                        // first thing, send the channel name len and channel name
                        dout.write(channelName.length());
                        dout.write(channelName.getBytes());

                        DataChannelImplSO c = (DataChannelImplSO) channels.get(channelName);

                        if (c != null) {
                            c.setDataSocket(dataSocket);
                            c.startOutputHelper();
                        }
                        break;
                    }
                }
            } catch (SocketException se) {
                System.err.println(se);
            } catch (IOException ie) {
                System.err.println(ie);
            }

        }

    }

    /** This method connects to a server and finishes creating a channel by setting the socket. */
    void connect() {
        // start thread to connecto to server
        Thread connectHelperThread = new Thread(Emu.THREAD_GROUP, new ConnectHelper(), name() + " connect");
        connectHelperThread.start();
    }

    /**
     * Create a DataChannel for this transport implementation.
     *
     * @param name    of type String
     * @param isInput set to true if this is an input (client) channel
     *
     * @return DataChannel
     */
    public DataChannel createChannel(String name, boolean isInput) throws DataTransportException {
        DataChannel c = new DataChannelImplSO(name() + ":" + name, this);
        channels.put(c.getName(), c);
        return c;
    }

    /**
     * bug bug, what does this do?
     *
     * @param channel of type DataChannel
     *
     * @return int[]
     */
    public DataTransportRecord receive(DataChannel channel) {
        return null;
    }

    /**
     * This method returns the connection status of this DataTransport object
     * (true if connected).
     *
     * @return the connection status (boolean) of this DataTransport object (true if connected)
     */
    public boolean isConnected() {
        return connected;
    }

    /** @return the state */
    public State state() {
        return state;
    }

    /**
     * This method is only called by the DataTransportFactory's
     * (a singleton) execute method which is only called
     * by the EmuModuleFactory's (a singleton) execute method.
     *
     * @param cmd of type Command
     *
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    public void execute(Command cmd) {

        if (cmd.equals(CODATransition.PRESTART)) {
            try {
                multicastAddr = InetAddress.getByName("239.200.0.0");
                multicastPort = getIntAttr("port");

                if (server) startServer();
                else connect();
            } catch (Exception e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            state = cmd.success();
            return;
        }

        if (cmd.equals(CODATransition.END)) {

            state = cmd.success();
            return;
        }

        if (cmd.equals(RunControl.RESET)) {

            state = cmd.success();
            return;
        }

        // We don't implement other commands so assume success.
        if (state != CODAState.ERROR) state = cmd.success();

    }
}
