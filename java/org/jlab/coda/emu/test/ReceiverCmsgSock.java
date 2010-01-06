/*
 * Copyright (c) 2009, Jefferson Science Associates
 * Sep. 10, 2009
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.test;

import org.jlab.coda.cMsg.*;

import java.net.*;
import java.io.*;

/**
 * This class is designed to receive output from an EMU.
 */
public class ReceiverCmsgSock {

    private String  subject = "BitBucket_SOCKET";
    private String  type = "get_socket";
    private String  name = "BitBucket";
    private String  channelName = "BitBucket_SOCKET"; // channelName is subject
    private String  description = "place to dump EMU data";
    private String  UDL;

    private int     delay, count = 5000, timeout = 3000; // 3 second default timeout
    private boolean debug;



    /** What does this serverSocket do? */
    private ServerSocket serverSocket;

    private Thread acceptHelperThread;

    private InetAddress localhost;

    private cMsg coda;

    /** Field subscription */
    private cMsgSubscriptionHandle sub;

    /** Field host */
    private String host;

    /** Field port */
    private int port;

    /**
      * Field THREAD_GROUP, this object attempts to start all of it's threads in one thread group.
      * the thread group is stored in THREAD_GROUP.
      */
    private ThreadGroup THREAD_GROUP;

    private Socket incoming;
    private DataInputStream in;


    /** Constructor. */
    ReceiverCmsgSock(String[] args) {
        decodeCommandLine(args);
    }


    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private void decodeCommandLine(String[] args) {

        // loop over all args
        for (int i = 0; i < args.length; i++) {

            if (args[i].equalsIgnoreCase("-h")) {
                usage();
                System.exit(-1);
            }
            else if (args[i].equalsIgnoreCase("-n")) {
                name = args[i + 1];
                i++;
            }
            else if (args[i].equalsIgnoreCase("-u")) {
                UDL= args[i + 1];
                i++;
            }
            // s - stands for sender
            else if (args[i].equalsIgnoreCase("-s")) {
                subject = args[i + 1];
                i++;
            }
            else if (args[i].equalsIgnoreCase("-t")) {
                type= args[i + 1];
                i++;
            }
            else if (args[i].equalsIgnoreCase("-c")) {
                count = Integer.parseInt(args[i + 1]);
                if (count < 1)
                    System.exit(-1);
                i++;
            }
            else if (args[i].equalsIgnoreCase("-to")) {
                timeout = Integer.parseInt(args[i + 1]);
                i++;
            }
            else if (args[i].equalsIgnoreCase("-delay")) {
                delay = Integer.parseInt(args[i + 1]);
                i++;
            }
            else if (args[i].equalsIgnoreCase("-debug")) {
                debug = true;
            }
            else {
                usage();
                System.exit(-1);
            }
        }

        return;
    }


    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage:\n\n" +
            "   java ReceiverCmsg\n" +
            "        [-n <name>]          set client name\n"+
            "        [-u <UDL>]           set UDL to connect to cMsg\n" +
            "        [-s <sender>]        set sender of EMU data\n" +
            "        [-t <type>]          set type of subscription/sent messages\n" +
            "        [-c <count>]         set # of messages to get before printing output\n" +
            "        [-to <time>]         set timeout\n" +
            "        [-delay <time>]      set time in millisec between sending of each message\n" +
            "        [-debug]             turn on printout\n" +
            "        [-h]                 print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
        try {
            ReceiverCmsgSock receiver = new ReceiverCmsgSock(args);
            receiver.run();
        }
        catch (cMsgException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    /**
     * Method to convert a double to a string with a specified number of decimal places.
     *
     * @param d double to convert to a string
     * @param places number of decimal places
     * @return string representation of the double
     */
    private static String doubleToString(double d, int places) {
        if (places < 0) places = 0;

        double factor = Math.pow(10,places);
        String s = "" + (double) (Math.round(d * factor)) / factor;

        if (places == 0) {
            return s.substring(0, s.length()-2);
        }

        while (s.length() - s.indexOf(".") < places+1) {
            s += "0";
        }

        return s;
    }

    /**
     * Converts 4 bytes of a byte array into an integer.
     *
     * @param b byte array
     * @param off offset into the byte array (0 = start at first element)
     * @return integer value
     */
    public static final int bytesToInt(byte[] b, int off) {
        return (((b[off]  &0xff) << 24) |
                ((b[off+1]&0xff) << 16) |
                ((b[off+2]&0xff) <<  8) |
                 (b[off+3]&0xff));
    }


    /**
      * Copies an integer value into 4 bytes of a byte array.
      * @param intVal integer value
      * @param b byte array
      * @param off offset into the byte array
      */
     public static final void intToBytes(int intVal, byte[] b, int off) {
       b[off]   = (byte) ((intVal & 0xff000000) >>> 24);
       b[off+1] = (byte) ((intVal & 0x00ff0000) >>> 16);
       b[off+2] = (byte) ((intVal & 0x0000ff00) >>>  8);
       b[off+3] = (byte)  (intVal & 0x000000ff);
     }



    /**
     * Method to wait on string from keyboard.
     * @param s prompt string to print
     * @return string typed in keyboard
     */
    public String inputStr(String s) {
        String aLine = "";
        BufferedReader input =  new BufferedReader(new InputStreamReader(System.in));
        System.out.print(s);
        try {
            aLine = input.readLine();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return aLine;
    }


    /**
     * This class defines the callback to be run when a message matching
     * our subscription arrives.
     */
    class myCallback extends cMsgCallbackAdapter {

        public void callback(cMsgMessage msg, Object userObject) {
            String type = msg.getType();

            // Client sendAndGet's msg to get the server socket host & port.
            // Subject must be the name of this transport object and type must be "get_socket".
System.out.println("callback: got msg of (sub,typ) " + msg.getSubject() + ", " + msg.getType());

            if (type.matches("get_socket")) {
                try {
System.out.println("callback DataTransportImplCmsgSock");
                    cMsgMessage rmsg = msg.response();

                    rmsg.setText(localhost.getHostAddress());
                    rmsg.setUserInt(serverSocket.getLocalPort());
                    rmsg.setSubject(name);
                    rmsg.setType("socket_ack");

                    coda.send(rmsg);
                    coda.flush(0);

                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }
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
    class AcceptHelper extends Thread {

        public AcceptHelper(ThreadGroup group, String name) {
            super(group, name);
        }

        /** Method run ... */
        public void run() {

            try {
                serverSocket.setReceiveBufferSize(100000);
            } catch (SocketException e1) {
                e1.printStackTrace();
            }

            while (!serverSocket.isClosed()) {
                try {
                    incoming = serverSocket.accept();
                    incoming.setTcpNoDelay(true);
                    in = new DataInputStream(incoming.getInputStream());

                    // check for port scanners or other illegitimate connections
                    int magic = in.readInt();
                    if (magic != 0xC0DA2008) {
System.out.println("AcceptHelper: invalid connection, got " + magic + " expected " + 0xC0DA2008);
                        in.close();
                        incoming.close();
                        continue;
                    }

                    // length of name
                    int l = in.readByte();
System.out.println("AcceptHelper: name length = " + l);

                    // name
                    byte[] cbuf = new byte[l];
                    in.readFully(cbuf, 0, l);
                    String cname = new String(cbuf, 0, l, "US-ASCII");
System.out.println("AcceptHelper: channel name = " + cname);

//                    // retrieve already-created channel for connecting client
//                    DataChannelImplCmsgSock c = (DataChannelImplCmsgSock) channels().get(cname);
//System.out.println("AcceptHelper: retrieved channel object = " + c);
//                    if (c != null) {
//                        c.setDataSocket(incoming);
//                        c.startInputHelper();
//                    } else {
//                        throw new DataTransportException("AcceptHelper: Transport " + name() + " has no channel named " + cname);
//                    }
                } catch (Exception e) {
                    System.err.println("Exception " + e.getMessage());
                    e.printStackTrace();
                    return;
                }
            }
System.out.println("AcceptHelper: end");

        }

    }



    class receiveDataThread extends Thread {

        public void run() {

System.out.println("receive thread started");
            int[] data = new int[4];
            int num = 5;
            long counter = 0;
            final int ROCID = 1234;

            int len;

            try {
                while (true) {
                    len = in.readInt();
                    byte[] buf = new byte[len+1];
                    intToBytes(len, buf, 0);
                    in.readFully(buf, 4, len);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }



            return;
        }

    }



    /**
     * This method only gets called if this transport object is a "server"
     * (sends data).
     *
     * @throws IOException if problem with transport
     * @throws cMsgException if problem with cMsg subscription
     */
    private void startServer() throws cMsgException {
System.out.println("startServer : IN");

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
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new cMsgException(e);
        }
        myCallback cb = new myCallback();

System.out.println("startServer : subscribe to (sub,typ) " + channelName + ", get_socket");
        sub = coda.subscribe(channelName, "get_socket", cb, null);

        // start helper thread
        acceptHelperThread = new AcceptHelper(THREAD_GROUP, channelName + " accept");
        acceptHelperThread.start();

        // set connected flag
        // setConnected(true);
    }


    /**
     * This method is executed as a thread.
     */
    public void run() throws cMsgException {

        if (debug) {
            System.out.println("Running ReceiverCmsg as EMU's bitbucket\n");
        }

        // Get the UDL for connection to the cMsg server. If none given,
        // cMSGPortal defaults to using UDL = "cMsg:cMsg://localhost/cMsg/test".
        if (UDL == null) UDL = System.getProperty("cmsgUDL");
        if (UDL == null) UDL = "cMsg://localhost/cMsg/test";

        // connect to cMsg server
        coda = new cMsg(UDL, name, description);
        coda.connect();

        // enable message reception
        coda.start();

        startServer();

        inputStr("Enter when time to GO");


    }

}
