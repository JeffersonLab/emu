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
 * Class <b>TransportImplCmsgSock </b>
 * </pre>
 * Implement communication using the cMsg protocol.
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class DataTransportImplCmsgSock extends DataTransportCore implements DataTransport, cMsgCallbackInterface {

    /** Field localhost */
    private InetAddress localhost;

    /** What does this serverSocket do? */
    private ServerSocket serverSocket;

    /**  */
    private Thread acceptHelperThread;

    /** Field subscription */
    cMsgSubscriptionHandle sub;

    /** Field host */
    private String host = null;

    /** Field port */
    private int port = 0;

    /**
     * Constructor.
     *
     * @param pname  of type String
     * @param attrib of type Map
     *
     * @throws DataNotFoundException
     *          when
     */
    public DataTransportImplCmsgSock(String pname, Map<String, String> attrib) throws DataNotFoundException {
        // pname is the "name" entry in the attrib map
        super(pname, attrib);
//System.out.println("^^^^^ CREATED A BLOODY CMSG TRANSPORT OBJECT ^^^^^");
//System.out.println("^^^^^ NOW WE HAVE A DIFFERENT CMSG TRANSPORT OBJECT ^^^^^");
    }

    /**
     * Callback method definition. This completes the implementation of the cMsgCallbackInterface
     * interface - the rest having been done in the extended DataTransportCore. Used to send the
     * server socket's host & port back to the client.
     *
     * @param msg message received from domain server
     * @param o object passed as an argument which was set when the
     *                   client orginally subscribed to a subject and type of
     *                   message.
     */
    public void callback(cMsgMessage msg, Object o) {

        String type = msg.getType();

        // Client sendAndGet's msg to get the server socket host & port.
        // Subject must be the name of this transport object and type must be "get_socket".
System.out.println("    DataTransportImplCmsgSock.callback : got msg of (sub,typ) " + msg.getSubject() + ", " + msg.getType());

        if (type.matches("get_socket")) {
            try {
System.out.println("    DataTransportImplCmsgSock.callback : myinstance = " + myInstance);
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
     * This thread listens on the serverSocket for incoming connections
     * It then looks for a channel with the name read from the socket and
     * associates the connection with that channel.
     */
    public class AcceptHelper extends Thread {

        public AcceptHelper(ThreadGroup group, String name) {
            super(group, name);
        }


        /** Method run ... */
        public void run() {
System.out.println("    DataTransportImplCmsgSock.AcceptHelper.run : for " + this.getName());

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

                    // check for port scanners or other illegitimate connections
                    int magic = in.readInt();
                    if (magic != 0xC0DA2008) {
                        Logger.warn("Invalid connection, got " + magic + " expected " + 0xC0DA2008);
//System.out.println("Invalid connection, got " + magic + " expected " + 0xC0DA2008);
                        in.close();
                        incoming.close();
                        continue;
                    }

                    // length of name
                    int l = in.readByte();
System.out.println("    DataTransportImplCmsgSock.AcceptHelper.run : name length = " + l);

                    // name
                    byte[] cbuf = new byte[l];
                    in.readFully(cbuf, 0, l);
                    String cname = new String(cbuf, 0, l, "US-ASCII");
System.out.println("    DataTransportImplCmsgSock.AcceptHelper.run : channel name = " + cname);

                    // retrieve already-created channel for connecting client
                    DataChannelImplCmsgSock c = (DataChannelImplCmsgSock) channels().get(cname);
System.out.println("    DataTransportImplCmsgSock.AcceptHelper.run : retrieved channel object = " + c);
//System.out.println("    DataTransportImplCmsgSock.AcceptHelper.run : channels " + channels() + " " + DataTransportImplCmsgSock.this);
                    if (c != null) {
                        c.setDataSocket(incoming);
                        c.startInputHelper();
                    } else {
                        throw new DataTransportException("AcceptHelper: Transport " + name() + " has no channel named " + cname);
                    }
                } catch (Exception e) {
                    System.err.println("Exception " + e.getMessage());
                    e.printStackTrace();
                    CODAState.ERROR.getCauses().add(e);
                    state = CODAState.ERROR;
                    return;
                }
            }
System.out.println("    DataTransportImplCmsgSock.AcceptHelper.run : end");

        }

    }

    /**
     * This method only gets called if this transport object is a "server"
     * (sends data).
     *
     * @throws DataTransportException if problem with transport
     */
    private void startServer() throws DataTransportException {
System.out.println("    DataTransportImplCmsgSock.startServer : IN");

        try {
            localhost = InetAddress.getLocalHost();

            // Why are we creating a server socket here?
            // @param 1 local TCP port
            //          0 is a special port number. It tells Java to pick an available port.
            //          You can then find out what port it's picked with the getLocalPort() method.
            //          This is useful if the client and the server have already established a
            //          separate channel of communication over which the chosen port number can
            //          be communicated (in this case a cMsg sendAndGet).
            // @param 2 listen backlog
            // @param 3 local InetAddress the server will bind to
            serverSocket = new ServerSocket(0, 1, localhost);

System.out.println("    DataTransportImplCmsgSock.startServer : subscribe to (sub,typ) " + name() + ", get_socket");
            sub = CMSGPortal.getServer().subscribe(name(), "get_socket", this, null);
            //sub = CMSGPortal.getServer().subscribe("*", "*", this, null);

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataTransportException(e);
        }

        acceptHelperThread = new AcceptHelper(Emu.THREAD_GROUP, name() + " accept");
        acceptHelperThread.start();

        // set connected flag
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
            //msg.setText("no text");
System.out.println("    DataTransportImplCmsgSock.connect : sendAndGet msg to (sub,typ) " + name() + ", get_socket");

            cMsgMessage reply = CMSGPortal.getServer().sendAndGet(msg, 10000);

            host = reply.getText();
            port = reply.getUserInt();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataTransportException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap, boolean isInput)
            throws DataTransportException {
System.out.println("    DataTransportImplCmsgSock.createChannel : create data channel from " + name);
        DataChannel c = new DataChannelImplCmsgSock(name() + ":" + name, this, isInput);
        channels().put(c.getName(), c);
System.out.println("    DataTransportImplCmsgSock.createChannel : put channel " + c.getName());
//System.out.println("    DataTransportImplCmsgSock.createChannel : channels " + channels() + " " + this);
        return c;
    }

    /** {@inheritDoc} */
    public void execute(Command cmd) {
System.out.println("    DataTransportImplCmsgSock.execute : " + cmd);

        if (cmd.equals(CODATransition.PRESTART)) {

            try {
                if (server) {
                    System.out.println("    DataTransportImplCmsgSock.execute PRE : call startServer for " + name());
                    startServer();
                }
                else {
                    System.out.println("    DataTransportImplCmsgSock.execute PRE : call connect to " + name());
                    connect();
                }
            } catch (DataTransportException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            state = cmd.success();
            return;
        }
        else if ((cmd.equals(CODATransition.END)) || (cmd.equals(RunControl.RESET))) {
            try {
                Logger.debug("    DataTransportImplCmsgSock.execute END/RESET: cmsg unsubscribe : " + name() + " " + myInstance);
                CMSGPortal.getServer().unsubscribe(sub);

                serverSocket.close();

                acceptHelperThread.interrupt();
            } catch (Exception e) {
                // ignore
            }
            state = cmd.success();
            return;
        }

        // Do nothing for DOWNLOAD, GO, PAUSE

        // We don't implement other commands so assume success.
        if (state != CODAState.ERROR) state = cmd.success();

    }

    /**
     * Method getHost returns the host of this TransportImplCmsgSock object.
     *
     * @return the host (type String) of this TransportImplCmsgSock object.
     */
    public String getHost() {
        return host;
    }

    /**
     * Method setHost sets the host of this TransportImplCmsgSock object.
     *
     * @param host the host of this TransportImplCmsgSock object.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Method getPort returns the port of this TransportImplCmsgSock object.
     *
     * @return the port (type int) of this TransportImplCmsgSock object.
     */
    public int getPort() {
        return port;
    }

    /**
     * Method setPort sets the port of this TransportImplCmsgSock object.
     *
     * @param port the port of this TransportImplCmsgSock object.
     */
    public void setPort(int port) {
        this.port = port;
    }
}