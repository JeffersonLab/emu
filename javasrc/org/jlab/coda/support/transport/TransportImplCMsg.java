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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.concurrent.TimeoutException;

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

    private String TEST_UDL = "cMsg://localhost:3456/cMsg/test";
    private Thread monitor;

    private String UDL;

    /** Field localhost */
    private InetAddress localhost;

    /** Field serverSocket */
    private ServerSocket serverSocket;

    private String host = null;

    private int port = 0;

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

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
        UDL = TEST_UDL;
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

    public void callback(cMsgMessage msg, Object o) {

        String type = msg.getType();
        String subject = msg.getSubject();

        System.out.println("cMsg type : " + type + ", subject : " + subject + ", text : " + msg.getText() + ", int " + msg.getUserInt());
        if (type.matches("get_socket")) {
            System.out.println("Someone asked for our socket data " + msg.toString());

            try {

                cMsgMessage rmsg = msg.response();

                rmsg.setText(localhost.getHostAddress());
                rmsg.setUserInt(serverSocket.getLocalPort());
                rmsg.setSubject(name());
                rmsg.setType("socket_ack");
                System.out.println("Sending response : " + rmsg.toString());

                CMSGPortal.getServer().send(rmsg);

                //cMsgServer.flush(0);
            } catch (cMsgException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

    }

    public boolean maySkipMessages() {
        return false;
    }

    public boolean mustSerializeMessages() {
        return true;
    }

    public int getMaximumCueSize() {
        return 10000;
    }

    public int getSkipSize() {
        return 1;
    }

    public int getMaximumThreads() {
        return 200;
    }

    public int getMessagesPerThread() {
        return 1;
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

                    DataInputStream in = new DataInputStream(incoming
                            .getInputStream());

                    l = in.read();

                    int bytes = in.read(cbuf, 0, l);
                    System.out.println("bytes is : " + bytes);
                    String s = new String(cbuf, 0, bytes);
                    System.out.println("channel name is : " + s);
                    DataChannelImplCMsg c = (DataChannelImplCMsg) channels.get(s);

                    if (c != null) {
                        c.setDataSocket(incoming);
                        c.startInputHelper();
                    }
                } catch (Exception e) {
                    System.err.println("Exception " + e.getMessage());
                    e.printStackTrace();
                    return;
                }
            }

        }

    }

    /**
     * Method startServer ...
     *
     * @throws org.jlab.coda.support.transport.TransportException
     *          when
     */
    public void startServer() throws TransportException {
        // set connected flag

        try {
            localhost = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new TransportException(e.getMessage());
        }

        try {
            serverSocket = new ServerSocket(0, 1, localhost);
        } catch (IOException e) {
            e.printStackTrace();
            throw new TransportException(e.getMessage());
        }
        try {
            System.out.println("subscribe " + name());
            CMSGPortal.getServer().subscribe(name(), "get_socket", this, null);

        } catch (cMsgException e) {
            e.printStackTrace();
            throw new TransportException(e.getMessage());
        }

        Thread acceptHelperThread = new Thread(EMUComponentImpl.THREAD_GROUP, new AcceptHelper(), name() + " accept");
        acceptHelperThread.start();
        connected = true;
    }

    /** Method connect ... */
    public void connect() {
        try {
            System.out.println("Connect called !!");

            cMsgMessage msg = new cMsgMessage();
            msg.setSubject(name());
            msg.setType("get_socket");
            msg.setText("no text");
            System.out.println("Message sent is : " + msg.toString());

            cMsgMessage reply = CMSGPortal.getServer().sendAndGet(msg, 10000);
            host = new String(reply.getText());
            System.out.println("cMsg response : " + reply.toString());
            System.out.println("cMsg reply text : " + reply.getText() + ", int " + reply.getUserInt());

            host = new String(reply.getText());
            port = reply.getUserInt();
            System.out.println("host : " + host + ", " + port + " set in " + name() + " " + this);
        } catch (cMsgException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (TimeoutException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    /**
     * Method createChannel ...
     *
     * @param name of type String
     * @return DataChannel
     */
    public DataChannel createChannel(String name) {
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

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}