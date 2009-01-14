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
import org.jlab.coda.support.evio.DataTransportRecord;

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
                    multicastSocket.receive(dp);
                    String s = new String(dp.getData(), 0, dp.getLength());

                    if (s.matches(name())) {
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
    public void startServer() throws DataTransportException {
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

        Thread multicastHelperThread = new Thread(Emu.THREAD_GROUP, new MulticastHelper(), name() + " multicast");
        multicastHelperThread.start();
        Thread acceptHelperThread = new Thread(Emu.THREAD_GROUP, new AcceptHelper(), name() + " accept");
        acceptHelperThread.start();
        connected = true;
    }

    /**
     * <pre>
     * Class <b>ConnectHelper </b>
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
                    String s = name();
                    DatagramPacket odp = new DatagramPacket(s.getBytes(), s.length(), multicastAddr, multicastPort);
                    multicastSocket.send(odp);

                    multicastSocket.receive(dp);
                    String s1 = new String(dp.getData(), 0, dp.getLength());

                    String[] values = s1.split(" ");

                    if (values[0].matches(name())) {

                        Socket dataSocket = new Socket(values[1], Integer.parseInt(values[2]));

                        dataSocket.setTcpNoDelay(true);
                        OutputStream out = dataSocket.getOutputStream();
                        DataOutputStream dout = new DataOutputStream(out);

                        String emuName = Emu.INSTANCE.name();
                        String channelName = name() + ":" + emuName;

                        dout.write(channelName.length());

                        dout.write(channelName.getBytes());
                        DataChannelImplSO c = (DataChannelImplSO) channels.get(name() + ":" + emuName);

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

    /** Method connect ... */
    public void connect() {
        Thread connectHelperThread = new Thread(Emu.THREAD_GROUP, new ConnectHelper(), name() + " connect");

        connectHelperThread.start();

    }

    /**
     * Method createChannel ...
     *
     * @param name    of type String
     * @param isInput
     * @return DataChannel
     */
    public DataChannel createChannel(String name, boolean isInput) throws DataTransportException {
        DataChannel c = new DataChannelImplSO(name() + ":" + name, this);
        channels.put(c.getName(), c);
        return c;
    }

    /**
     * Method closeChannel ...
     *
     * @param channel of type DataChannel
     */
    public void closeChannel(DataChannel channel) {

    }

    /**
     * Method receive ...
     *
     * @param channel of type DataChannel
     * @return int[]
     */
    public DataTransportRecord receive(DataChannel channel) {
        return null;
    }

    /**
     * Method send ...
     *
     * @param channel of type DataChannel
     * @param data    of type long[]
     */
    public void send(DataChannel channel, long[] data) {
    }

    /**
     * Method isConnected returns the connected of this DataTransport object.
     *
     * @return the connected (type boolean) of this DataTransport object.
     */
    public boolean isConnected() {
        return connected;
    }

    /** @return the state */
    public State state() {
        return state;
    }

    /**
     * Method execute ...
     *
     * @param cmd of type Command
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    public void execute(Command cmd) {

        if (cmd.equals(CODATransition.prestart)) {
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

        if (cmd.equals(CODATransition.end)) {
            // TODO do something!
            state = cmd.success();
            return;
        }

        if (cmd.equals(RunControl.reset)) {
            // TODO do something!
            state = cmd.success();
            return;
        }

        // We don't implement other commands so assume success.
        if (state != CODAState.ERROR) state = cmd.success();

    }
}
