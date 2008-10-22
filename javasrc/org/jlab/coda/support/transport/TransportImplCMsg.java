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

import org.jlab.coda.cMsg.cMsgCallbackInterface;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.emu.EMUComponentImpl;
import org.jlab.coda.support.cmsg.CMSGPortal;
import org.jlab.coda.support.component.CODAState;
import org.jlab.coda.support.component.CODATransition;
import org.jlab.coda.support.component.RunControl;
import org.jlab.coda.support.config.DataNotFoundException;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.log.Logger;

import java.io.DataInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;

/**
 * <pre>
 * Class <b>TransportImplSO </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class TransportImplCMsg extends DataTransportCore implements DataTransport, cMsgCallbackInterface {

    /** Field connected */
    private boolean connected = false;

    /** Field localhost */
    private InetAddress localhost;

    /** Field serverSocket */
    private ServerSocket serverSocket;

    /** Field host */
    private String host = null;

    /** Field port */
    private int port = 0;

    /** Field state */
    private State state = CODAState.UNCONFIGURED;
    /** Field connectTimeout */
    private final int connectTimeout = 10000;

    /**
     * Constructor TransportImplSO creates a new TransportImplSO instance.
     *
     * @param pname  of type String
     * @param attrib of type Map
     * @throws org.jlab.coda.support.config.DataNotFoundException
     *          when
     */
    public TransportImplCMsg(String pname, Map<String, String> attrib) throws DataNotFoundException {
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
     * Method callback ...
     *
     * @param msg of type cMsgMessage
     * @param o   of type Object
     */
    public void callback(cMsgMessage msg, Object o) {

        String type = msg.getType();

        if (type.matches("get_socket")) {

            try {
                cMsgMessage rmsg = msg.response();

                rmsg.setText(localhost.getHostAddress());
                rmsg.setUserInt(serverSocket.getLocalPort());
                rmsg.setSubject(name());
                rmsg.setType("socket_ack");

                CMSGPortal.getServer().send(rmsg);
                CMSGPortal.getServer().flush(0);
            } catch (cMsgException e) {
                e.printStackTrace();
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
            }
        }

    }

    /**
     * Method maySkipMessages ...
     *
     * @return boolean
     */
    public boolean maySkipMessages() {
        return false;
    }

    /**
     * Method mustSerializeMessages ...
     *
     * @return boolean
     */
    public boolean mustSerializeMessages() {
        return true;
    }

    /**
     * Method getMaximumCueSize returns the maximumCueSize of this TransportImplCMsg object.
     *
     * @return the maximumCueSize (type int) of this TransportImplCMsg object.
     */
    public int getMaximumCueSize() {
        return connectTimeout;
    }

    /**
     * Method getSkipSize returns the skipSize of this TransportImplCMsg object.
     *
     * @return the skipSize (type int) of this TransportImplCMsg object.
     */
    public int getSkipSize() {
        return 1;
    }

    /**
     * Method getMaximumThreads returns the maximumThreads of this TransportImplCMsg object.
     *
     * @return the maximumThreads (type int) of this TransportImplCMsg object.
     */
    public int getMaximumThreads() {
        return 200;
    }

    /**
     * Method getMessagesPerThread returns the messagesPerThread of this TransportImplCMsg object.
     *
     * @return the messagesPerThread (type int) of this TransportImplCMsg object.
     */
    public int getMessagesPerThread() {
        return 1;
    }

    /**
     * <pre>
     * Class <b>AcceptHelper </b>
     * </pre>
     * <p/>
     * This thread listens on the serverSocket for incomming connections
     * It then looks for a channel with the name read from the socket and
     * associates the connection with that channel.
     */
    private class AcceptHelper implements Runnable {

        /** Method run ... */
        public void run() {

            try {
                serverSocket.setReceiveBufferSize(100000);
            } catch (SocketException e1) {
                e1.printStackTrace();
            }
            while (true) {
                try {
                    Socket incoming = serverSocket.accept();
                    incoming.setTcpNoDelay(true);

                    DataInputStream in = new DataInputStream(incoming
                            .getInputStream());
                    int magic = in.readInt();
                    if (magic != 0xC0DA2008) {
                        Logger.warn("Invalid connection, got " + magic + " expected " + 0xC0DA2008);
                    }
                    int l = in.readByte();
                    byte[] cbuf = new byte[l + 1];
                    in.readFully(cbuf, 0, l);

                    String cname = new String(cbuf, 0, l);
                    DataChannelImplCMsg c = (DataChannelImplCMsg) channels.get(cname);
                    Logger.info("transp " + name() + " accepts chan " + cname + " : " + incoming);
                    if (c != null) {
                        c.setDataSocket(incoming);
                        c.startInputHelper();
                    }
                } catch (Exception e) {
                    System.err.println("Exception " + e.getMessage());
                    e.printStackTrace();
                    CODAState.ERROR.getCauses().add(e);
                    state = CODAState.ERROR;
                    return;
                }
            }

        }

    }

    /**
     * Method startServer ...
     *
     * @throws TransportException
     * @see DataTransport#startServer()
     */
    public void startServer() throws TransportException {
        // set connected flag

        try {
            localhost = InetAddress.getLocalHost();

            serverSocket = new ServerSocket(0, 1, localhost);

            System.out.println("subscribe " + name());
            CMSGPortal.getServer().subscribe(name(), "get_socket", this, null);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TransportException(e.getMessage(), e);
        }

        Thread acceptHelperThread = new Thread(EMUComponentImpl.THREAD_GROUP, new AcceptHelper(), name() + " accept");
        acceptHelperThread.start();
        connected = true;
    }

    /** Method connect ... */
    public void connect() throws TransportException {
        try {
            cMsgMessage msg = new cMsgMessage();
            msg.setSubject(name());
            msg.setType("get_socket");
            msg.setText("no text");

            cMsgMessage reply = CMSGPortal.getServer().sendAndGet(msg, connectTimeout);

            host = reply.getText();
            port = reply.getUserInt();
        } catch (Exception e) {
            e.printStackTrace();
            throw new TransportException(e.getMessage(), e);
        }
    }

    /**
     * Method createChannel ...
     *
     * @param name of type String
     * @return DataChannel
     */
    public DataChannel createChannel(String name) throws TransportException {
        System.out.println("create channel " + name);
        DataChannel c = new DataChannelImplCMsg(name() + ":" + name, this);
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
    public int[] receive(DataChannel channel) {
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
     * @see org.jlab.coda.emu.EmuModule#execute(org.jlab.coda.support.control.Command)
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    public void execute(Command cmd) throws CmdExecException {

        if (cmd.equals(CODATransition.prestart)) {

            try {
                if (server) startServer();
                else connect();
            } catch (TransportException e) {
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

    /**
     * Method getHost returns the host of this TransportImplCMsg object.
     *
     * @return the host (type String) of this TransportImplCMsg object.
     */
    public String getHost() {
        return host;
    }

    /**
     * Method setHost sets the host of this TransportImplCMsg object.
     *
     * @param host the host of this TransportImplCMsg object.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Method getPort returns the port of this TransportImplCMsg object.
     *
     * @return the port (type int) of this TransportImplCMsg object.
     */
    public int getPort() {
        return port;
    }

    /**
     * Method setPort sets the port of this TransportImplCMsg object.
     *
     * @param port the port of this TransportImplCMsg object.
     */
    public void setPort(int port) {
        this.port = port;
    }
}