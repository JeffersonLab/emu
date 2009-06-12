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
import org.jlab.coda.cMsg.cMsgSubscriptionHandle;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.support.codaComponent.CODAState;
import org.jlab.coda.support.codaComponent.CODATransition;
import org.jlab.coda.support.codaComponent.RunControl;
import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.support.messaging.CMSGPortal;

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
public class DataTransportImplCMsg extends DataTransportCore implements DataTransport, cMsgCallbackInterface {

    /** Field localhost */
    private InetAddress localhost;

    /** Field serverSocket */
    private ServerSocket serverSocket;
    private Thread acceptHelperThread;

    /** Field subscription */
    cMsgSubscriptionHandle sub;

    /** Field host */
    private String host = null;

    /** Field port */
    private int port = 0;

    /**
     * Constructor TransportImplSO creates a new TransportImplSO instance.
     *
     * @param pname  of type String
     * @param attrib of type Map
     *
     * @throws org.jlab.coda.support.configurer.DataNotFoundException
     *          when
     */
    public DataTransportImplCMsg(String pname, Map<String, String> attrib) throws DataNotFoundException {
        super(pname, attrib);
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
                System.out.println("callback DataTransportImplCMsg : " + myInstance);
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
            while (!serverSocket.isClosed()) {
                try {
                    Socket incoming = serverSocket.accept();
                    incoming.setTcpNoDelay(true);

                    DataInputStream in = new DataInputStream(incoming.getInputStream());
                    int magic = in.readInt();
                    if (magic != 0xC0DA2008) {
                        Logger.warn("Invalid connection, got " + magic + " expected " + 0xC0DA2008);
                    }
                    int l = in.readByte();
                    byte[] cbuf = new byte[l + 1];
                    in.readFully(cbuf, 0, l);

                    String cname = new String(cbuf, 0, l);
                    DataChannelImplCMsg c = (DataChannelImplCMsg) channels().get(cname);
                    System.out.println("channel " + cname);
                    System.out.println("channels " + channels() + " " + DataTransportImplCMsg.this);
                    if (c != null) {
                        c.setDataSocket(incoming);
                        c.startInputHelper();
                    } else {
                        throw new DataTransportException("Transport " + name() + " has no channel named " + cname);
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
     * @throws DataTransportException problem with transport
     */
    private void startServer() throws DataTransportException {
        // set connected flag

        try {
            localhost = InetAddress.getLocalHost();

            serverSocket = new ServerSocket(0, 1, localhost);

            System.out.println("subscribe " + name());
            sub = CMSGPortal.getServer().subscribe(name(), "get_socket", this, null);

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataTransportException(e.getMessage(), e);
        }

        acceptHelperThread = new Thread(Emu.THREAD_GROUP, new AcceptHelper(), name() + " accept");
        acceptHelperThread.start();
        setConnected(true);
    }

    /**
     * Method connect ...
     *
     * @throws DataTransportException problem with transport
     */
    private void connect() throws DataTransportException {
        try {
            cMsgMessage msg = new cMsgMessage();
            msg.setSubject(name());
            msg.setType("get_socket");
            msg.setText("no text");

            cMsgMessage reply = CMSGPortal.getServer().sendAndGet(msg, 10000);

            host = reply.getText();
            port = reply.getUserInt();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataTransportException(e.getMessage(), e);
        }
    }

    /**
     * Method createChannel ...
     *
     * @param name    of type String
     * @param isInput set if this is an input channel
     *
     * @return DataChannel
     */
    public DataChannel createChannel(String name, boolean isInput) throws DataTransportException {
        System.out.println("create channel " + name);
        DataChannel c = new DataChannelImplCMsg(name() + ":" + name, this, isInput);
        channels().put(c.getName(), c);
        System.out.println("put channel " + c.getName());
        System.out.println("channels " + channels() + " " + this);
        return c;
    }

    /**
     * Method execute ...
     *
     * @param cmd of type Command
     *
     * @see org.jlab.coda.emu.EmuModule#execute(org.jlab.coda.support.control.Command)
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    public void execute(Command cmd) {
        System.out.println("DataTransportImplCMsg execute : " + cmd);

        if (cmd.equals(CODATransition.prestart)) {

            try {
                if (server) startServer();
                else connect();
            } catch (DataTransportException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            state = cmd.success();
            return;
        }

        if ((cmd.equals(CODATransition.end)) || (cmd.equals(RunControl.reset))) {
            try {
                Logger.debug("CMsg Unsubscribe : " + name() + " " + myInstance);
                CMSGPortal.getServer().unsubscribe(sub);

                serverSocket.close();

                acceptHelperThread.interrupt();
            } catch (Exception e) {
                // ignore
            }
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