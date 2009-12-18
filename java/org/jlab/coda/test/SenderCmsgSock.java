/*
 * Copyright (c) 2009, Jefferson Science Associates
 * Sep. 1, 2009
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
import org.jlab.coda.jevio.*;
import org.jlab.coda.support.data.Evio;

import java.util.concurrent.TimeoutException;
import java.net.Socket;
import java.io.*;
import java.nio.ByteBuffer;

/**
 * This class is designed to send data to an EMU.
 */
public class SenderCmsgSock {

    private String  subject = "SingleEmu_SOCKET";
    private String  type = "get_socket";
    private String  name = "ROC1";
    private String  channelName = "SingleEmu_SOCKET"; // channelName is subject
    private String  description = "place to send data to EMU";
    private String  UDL;

    private int     delay, timeout = 1000; // 1 second default timeout
    private boolean debug;
    private boolean stopSending;

    private Socket socket;
    private DataOutputStream out;


    /** Constructor. */
    SenderCmsgSock(String[] args) {
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
            "   java SenderCmsg\n" +
            "        [-n <name>]          set client name\n"+
            "        [-u <UDL>]           set UDL to connect to cMsg\n" +
            "        [-s <sender>]        set sender of EMU data\n" +
            "        [-t <type>]          set type of subscription/sent messages\n" +
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
            SenderCmsgSock sender = new SenderCmsgSock(args);
            sender.run();
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


    class sendDataThread extends Thread {

        public void run() {

System.out.println("Send thread started");
//            long counter = 0;

            // in the arg order:
            int rocID = 123;
            int eventID = 10000;
            int dataBankTag = 666; // starting data bank tag
            int dataBankNum = 777; // starting data bank num
            int eventNumber = 1;
            int numEventsInPayloadBank = 1; // number of physics events in payload bank
            int timestamp = 1000;
            int recordId = 1;
            int numPayloadBanks = 1;

            EvioEvent ev;
            ByteBuffer bbuf = ByteBuffer.allocate(1000);


            StringWriter sw = new StringWriter(2048);
//            PrintWriter  wr = new PrintWriter(sw);
            
//            long start_time = System.currentTimeMillis();

            try {
                // send transport records from 3 ROCs
                for (int ix = 0; ix < 3; ix++) {
                    // send event over network
 System.out.println("Send roc record " + rocID + " over network");
                    // turn event into byte array
                    ev = Evio.createDataTransportRecord(rocID, eventID,
                                                        dataBankTag, dataBankNum,
                                                        eventNumber, numEventsInPayloadBank,
                                                        timestamp, recordId, numPayloadBanks);

                    ev.write(bbuf);
                    out.write(bbuf.array());
                    out.flush();

                    // send from next roc
                    rocID++;

                    if (stopSending) {
                        return;
                    }

                    Thread.sleep(1000);

//                    if (counter++ % 10 == 0) {
//                        long now = System.currentTimeMillis();
//                        // format little ints neatly
//                        wr.printf("%2.1e  Hz\n", 10L/(now - start_time));
//                        wr.flush();
//                        start_time = now;
//                    }
                }
            }
            catch (EvioException e) {
                e.printStackTrace();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }

            return;
        }

    }


    /**
     * This method is executed as a thread.
     */
    public void run() throws cMsgException {

        if (debug) {
            System.out.println("Running SenderCmsg as EMU's ROC1\n");
        }

        // Get the UDL for connection to the cMsg server. If none given,
        // cMSGPortal defaults to using UDL = "cMsg:cMsg://localhost/cMsg/test".
        if (UDL == null) UDL = System.getProperty("cmsgUDL");
        if (UDL == null) UDL = "cMsg://localhost/cMsg/test";

        // connect to cMsg server
        cMsg coda = new cMsg(UDL, name, description);
        coda.connect();

        // enable message reception
        coda.start();

        // create a message to send to a responder (for sendAndGet only)
        cMsgMessage msg = null;
        cMsgMessage sendMsg = new cMsgMessage();
        sendMsg.setSubject(channelName);
        sendMsg.setType(type);

        String hostAddress, senderName, msgType;
        int hostPort;

        while (true) {
            // do some gets
            try {
                System.out.println("Sending msg to (sub,typ) = " + channelName + ", " + type);
                msg = coda.sendAndGet(sendMsg, timeout);
            }
            catch (TimeoutException e) {
                System.out.println("Timeout in sendAndGet");
                continue;
            }

            // got a message!
            if (msg != null) {
                hostAddress = msg.getText();
                hostPort    = msg.getUserInt();
                senderName  = msg.getSubject();
                msgType     = msg.getType();  // type = "socket_ack"
                System.out.println("RESPONSE:");
                System.out.println(" addr   = " + hostAddress);
                System.out.println(" port   = " + hostPort);
                System.out.println(" sender = " + senderName);
                System.out.println(" type   = " + msgType);
                break;
            }

            // delay between messages sent
            if (delay != 0) {
                try { Thread.sleep(delay); }
                catch (InterruptedException e) { }
            }

            if (!coda.isConnected()) {
                System.out.println("No longer connected to domain server, quitting");
                System.exit(-1);
            }
        }

        inputStr("Enter to GO");
        stopSending = false;

        // If we're here we got a response to our sendAndGet.
        // Send hand-shaky stuff to EMU.
        try {
            socket = new Socket(hostAddress, hostPort);
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65535));
            // write magic int first
System.out.println("Write magic int (0xC0DA2008)");
            out.writeInt(0xC0DA2008);
            // write name
            String fullName = channelName + ":" + name;
System.out.println("Write name length = " + fullName.length());
            out.writeByte(fullName.length());
            try {
System.out.println("Write name bytes = " + fullName);
                out.write(fullName.getBytes("US-ASCII"));
            }
            catch (UnsupportedEncodingException e) {/* never happen */}
            out.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        sendDataThread sender = new sendDataThread();
        sender.start();

        inputStr("Enter to QUIT");
        stopSending = true;

        try { Thread.sleep(10000); }
        catch (InterruptedException e) { }

    }

}
