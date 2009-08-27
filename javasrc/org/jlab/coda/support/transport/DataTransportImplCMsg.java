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
 * Class <b>TransportImplCMsg </b>
 * </pre>
 * Implement communication using the cMsg protocol.
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class DataTransportImplCMsg extends DataTransportCore implements DataTransport, cMsgCallbackInterface {

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
    public DataTransportImplCMsg(String pname, Map<String, String> attrib) throws DataNotFoundException {
        // pname is the "name" entry in the attrib map
        super(pname, attrib);
System.out.println("^^^^^ CREATED A BLOODY CMSG TRANSPORT OBJECT ^^^^^");
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
     * This thread listens on the serverSocket for incoming connections
     * It then looks for a channel with the name read from the socket and
     * associates the connection with that channel.
     */
    public class InnerClass1 extends Thread {

        public InnerClass1(ThreadGroup group, String name) {
            super(group, name);
            // set class loader here
            //setContextClassLoader(ClassLoader.getSystemClassLoader());
//System.out.println("set the context class loader for ACCEPTHELPER class");       
        }


        /** Method run ... */
        public void run() {

System.out.println("AcceptHelper IN !!!");

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
    public class InnerClass3 extends Thread {

        public InnerClass3(ThreadGroup group, String name) {
            super(group, name);
            // set class loader here
            setContextClassLoader(ClassLoader.getSystemClassLoader());
System.out.println("set the context class loader for ACCEPTHELPER class");
        }


        /** Method run ... */
        public void run() {
System.out.println("AcceptHelper 1 for " + this.getName());

            try {
                serverSocket.setReceiveBufferSize(100000);
            } catch (SocketException e1) {
                e1.printStackTrace();
            }
System.out.println("AcceptHelper 2");

            while (!serverSocket.isClosed()) {
                try {
                    Socket incoming = serverSocket.accept();
                    incoming.setTcpNoDelay(true);
                    DataInputStream in = new DataInputStream(incoming.getInputStream());

                    // check for port scanners or other illegitimate connections
                    int magic = in.readInt();
                    if (magic != 0xC0DA2008) {
                        Logger.warn("Invalid connection, got " + magic + " expected " + 0xC0DA2008);
                        in.close();
                        incoming.close();
                        continue;
                    }

                    // length of name
                    int l = in.readByte();

                    // name
                    byte[] cbuf = new byte[l + 1];   // bug bug: why add one to array size ?
                    in.readFully(cbuf, 0, l);
                    String cname = new String(cbuf, 0, l);

                    // retrieve already-created channel for connecting client
//                    DataChannelImplCMsg c = (DataChannelImplCMsg) channels().get(cname);
//System.out.println("channel " + cname);
//System.out.println("channels " + channels() + " " + DataTransportImplCMsg.this);
//                    if (c != null) {
//                        c.setDataSocket(incoming);
//                        c.startInputHelper();
//                    } else {
//                        throw new DataTransportException("Transport " + name() + " has no channel named " + cname);
//                    }
                } catch (Exception e) {
                    System.err.println("Exception " + e.getMessage());
                    e.printStackTrace();
                    CODAState.ERROR.getCauses().add(e);
                    state = CODAState.ERROR;
                    return;
                }
            }
System.out.println("AcceptHelper end");

        }

    }

    /**
     * This method only gets called if this transport object is a "server"
     * (sends data).
     *
     * @throws DataTransportException if problem with transport
     */
    private void startServer() throws DataTransportException {

System.out.println("startServer 1");
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
System.out.println("startServer 2");
            serverSocket = new ServerSocket(0, 1, localhost);

System.out.println("startServer subscribe " + name());
            sub = CMSGPortal.getServer().subscribe(name(), "get_socket", this, null);
System.out.println("startServer 3");

        } catch (Exception e) {
            e.printStackTrace();
System.out.println("startServer 3.5");
            throw new DataTransportException(e);
        }

System.out.println("startServer 4");
        acceptHelperThread = new InnerClass1(Emu.THREAD_GROUP, name() + " accept");
System.out.println("startServer 5");
        acceptHelperThread.start();
System.out.println("startServer 6");

        // set connected flag
        setConnected(true);
System.out.println("startServer end");
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

        if (cmd.equals(CODATransition.PRESTART)) {

            try {
System.out.println("DataTransportImplCMsg execute 1");
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

        if ((cmd.equals(CODATransition.END)) || (cmd.equals(RunControl.RESET))) {
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