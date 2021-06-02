/*
 * Copyright (c) 2017, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.test;


import org.jlab.coda.cMsg.*;
import org.jlab.coda.cMsg.common.cMsgMessageFull;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.messaging.RCConstants;

import java.io.*;
import java.net.*;
import java.util.Date;
import java.util.EnumSet;
import java.util.Random;

import static org.jlab.coda.emu.support.codaComponent.CODAState.ACTIVE;
import static org.jlab.coda.emu.support.codaComponent.CODATransition.*;
import static org.jlab.coda.emu.support.codaComponent.CODATransition.RESET;

/**
 * This class is designed to find bugs in the network communication/protocol
 * when talking to rc server (agent). To use this class, first run the RCServerDomain.RCServer
 * in the cMsg package directly. In other words:
 *    java org.jlab.coda.cMsg.RCServerDomain.RCServer
 * This will print out it's UDP and TCP ports and IP addresses.
 * Use that data in this program to connect and send messages.
 *
 * @author timmer
 * @since Dec 5, 2017
 */
public class RcProtocolTest {

    private int tcpPort;
    private int udpPort;
    private String ipAddress;
    private String name = "DCBCAL";
    // Output TCP data stream from this client to the RC server
    private DataOutputStream domainOut;
    // Packet to send over UDP to RC server to implement
    private DatagramPacket sendUdpPacket;
    // Socket over which to end messages to the RC server over UDP
    private DatagramSocket udpSocket;
    // Socket over which to send messages to the RC server over TCP
    private Socket tcpSocket;

    private cMsgMessage reportMsg;



    /**
     * Constructor.
     * @param args program args
     */
    RcProtocolTest(String[] args) {
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
            else if (args[i].equalsIgnoreCase("-tcp")) {
                tcpPort = Integer.parseInt(args[i + 1]);
                i++;
            }
            else if (args[i].equalsIgnoreCase("-udp")) {
                udpPort = Integer.parseInt(args[i + 1]);
                i++;
            }
            else if (args[i].equalsIgnoreCase("-ip")) {
                ipAddress = args[i + 1];
                i++;
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
            "   java RcProtocolTest\n" +
            "        [-tcp <port>]   TCP port to connect & send msgs to\n" +
            "        [-udp <port>]   UDP port to connect & send msgs to\n" +
            "        [-ip  <addr>]   IP address to use\n" +
            "        [-h]            help\n");
    }


    /**
     * Run as a stand-alone application.
     * @param args args
     */
    public static void main(String[] args) {
        try {
            RcProtocolTest receiver = new RcProtocolTest(args);
            receiver.run();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    /** This method is executed as a thread. */
    public void run() {

        // RunControl server's net addresses obtained from multicast response
        InetAddress rcServerAddress;

        try {
            rcServerAddress = InetAddress.getByName(ipAddress);
            System.out.println("RC protocol test: try making tcp connection to RC test server (host = " + rcServerAddress.getHostName() + ", " +
                                       rcServerAddress.getHostAddress() + "; port = " + tcpPort + ")");
            tcpSocket = new Socket();
            // don't waste time if a connection cannot be made, timeout = 2 seconds
            tcpSocket.connect(new InetSocketAddress(rcServerAddress, tcpPort), 2000);
            tcpSocket.setTcpNoDelay(true);
            tcpSocket.setSendBufferSize(cMsgNetworkConstants.bigBufferSize);
            domainOut = new DataOutputStream(new BufferedOutputStream(tcpSocket.getOutputStream(),
                                                                      cMsgNetworkConstants.bigBufferSize));
            System.out.println("RC protocol test: MADE tcp connection to host " + rcServerAddress + ", port " + tcpPort);


            // Create a UDP "connection". This means security check is done only once
            // and communication with any other host/port is not allowed.
            // create socket to receive at anonymous port & all interfaces
            udpSocket = new DatagramSocket();
            udpSocket.setReceiveBufferSize(cMsgNetworkConstants.bigBufferSize);
System.out.println("RC protocol test: make udp connection to RC test server");
            udpSocket.connect(rcServerAddress, udpPort);
System.out.println("RC protocol test: MADE udp connection to RC test server");
            sendUdpPacket = new DatagramPacket(new byte[0], 0, rcServerAddress, udpPort);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        reportMsg = new cMsgMessage();
        reportMsg.setSubject(name);
        reportMsg.setType(RCConstants.reportStatus);
        //reportMsg.setReliableSend(false);

        int loop = 1000000;
        try {
            while (loop-- > 0) {
                replyToRunControl();
                sendStatusMessage();
                //try {Thread.sleep(6000);}
                //catch (InterruptedException e) {}
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void replyToRunControl() {

        Random randomData = new Random(System.currentTimeMillis());

        String[] states = new String[] {"BOOTED", "CONFIGURED", "DOWNLOADED",
                                        "PAUSED", "ACTIVE", "CONFIGURING",
                                        "DOWNLOADING", "PRESTARTING", "ACTIVATING",
                                        "ENDING"};

        String val = states[randomData.nextInt(10)];

        // If received msg is sendAndGet ...
        cMsgMessageFull msg = new cMsgMessageFull();
        msg.setSubject(name);
        msg.setType(RCConstants.getStateResponse);

        msg.setSysMsgId(33);
        msg.setSenderToken(25);
        msg.setGetResponse(true);

        //msg.setReliableSend(false);
        msg.setText(val);

        try {
            send(msg);
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }
    }


    /**
     * Method to send a message to the domain server for further distribution.
     *
     * @param message {@inheritDoc}
     * @throws cMsgException if there are communication problems with the server;
     *                       subject and/or type is null
     */
    public void send(final cMsgMessage message) throws cMsgException {

        if (!message.getReliableSend()) {
            udpSend(message);
            return;
        }

        String subject = message.getSubject();
        String type    = message.getType();

        // check message fields first
        if (subject == null || type == null) {
            throw new cMsgException("message subject and/or type is null");
        }

        // check for null text
        String text = message.getText();
        int textLen = 0;
        if (text != null) {
            textLen = text.length();
        }

        // Payload stuff. Do NOT keep track of sender history.
        String payloadTxt = message.getPayloadText();
        int payloadLen = 0;
        if (payloadTxt != null) {
            payloadLen = payloadTxt.length();
        }

        int msgType = cMsgConstants.msgSubscribeResponse;
        if (message.isGetResponse()) {
            msgType = cMsgConstants.msgGetResponse;
        }

        int binaryLength = message.getByteArrayLength();

         try {

            // length not including first int
            int totalLength = (4 * 15) + name.length() + subject.length() +
                    type.length() + payloadLen + textLen + binaryLength;

            // total length of msg (not including this int) is 1st item
            domainOut.writeInt(totalLength);
            domainOut.writeInt(msgType);
            domainOut.writeInt(cMsgConstants.version);
            domainOut.writeInt(message.getUserInt());
            domainOut.writeInt(message.getInfo());
            domainOut.writeInt(message.getSenderToken());

            long now = new Date().getTime();
            // send the time in milliseconds as 2, 32 bit integers
            domainOut.writeInt((int) (now >>> 32)); // higher 32 bits
            domainOut.writeInt((int) (now & 0x00000000FFFFFFFFL)); // lower 32 bits
            domainOut.writeInt((int) (message.getUserTime().getTime() >>> 32));
            domainOut.writeInt((int) (message.getUserTime().getTime() & 0x00000000FFFFFFFFL));

            domainOut.writeInt(name.length());
            domainOut.writeInt(subject.length());
            domainOut.writeInt(type.length());
            domainOut.writeInt(payloadLen);
            domainOut.writeInt(textLen);
            domainOut.writeInt(binaryLength);

            // write strings & byte array
            try {
                domainOut.write(name.getBytes("US-ASCII"));
                domainOut.write(subject.getBytes("US-ASCII"));
                domainOut.write(type.getBytes("US-ASCII"));
                if (payloadLen > 0) {
                    domainOut.write(payloadTxt.getBytes("US-ASCII"));
                }
                if (textLen > 0) {
                    domainOut.write(text.getBytes("US-ASCII"));
                }
                if (binaryLength > 0) {
                    domainOut.write(message.getByteArray(),
                                    message.getByteArrayOffset(),
                                    binaryLength);
                }
            }
            catch (UnsupportedEncodingException e) {
            }

            domainOut.flush();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

//-----------------------------------------------------------------------------


    /**
     * Method to send a message to the domain server over UDP for further distribution.
     *
     * @param message message to send
     * @throws cMsgException if there are communication problems with the server;
     *                       subject and/or type is null; message is too big for
     *                       UDP packet size if doing UDP send
     */
    private void udpSend(cMsgMessage message) throws cMsgException {

        String subject = message.getSubject();
        String type    = message.getType();

        // check message fields first
        if (subject == null || type == null) {
            throw new cMsgException("message subject and/or type is null");
        }

        // check for null text
        String text = message.getText();
        int textLen = 0;
        if (text != null) {
            textLen = text.length();
        }

        // Payload stuff. Do NOT keep track of sender history.
        String payloadTxt = message.getPayloadText();
        int payloadLen = 0;
        if (payloadTxt != null) {
            payloadLen = payloadTxt.length();
        }

        int msgType = cMsgConstants.msgSubscribeResponse;
        if (message.isGetResponse()) {
//System.out.println("sending get-response with UDP");
            msgType = cMsgConstants.msgGetResponse;
        }

        int binaryLength = message.getByteArrayLength();

        // total length of msg (not including first int which is this size)
        int totalLength = (4 * 15) + name.length() + subject.length() +
                type.length() + payloadLen + textLen + binaryLength;

        if (totalLength > 8192) {
            throw new cMsgException("Too big a message for UDP to send");
        }

        // create byte array for multicast
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
        DataOutputStream out = new DataOutputStream(baos);

        try {
            out.writeInt(cMsgNetworkConstants.magicNumbers[0]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[1]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[2]);

            out.writeInt(totalLength); // total length of msg (not including this int)
            out.writeInt(msgType);
            out.writeInt(cMsgConstants.version);
            out.writeInt(message.getUserInt());
            out.writeInt(message.getInfo());
            out.writeInt(message.getSenderToken());

            long now = new Date().getTime();
            // send the time in milliseconds as 2, 32 bit integers
            out.writeInt((int) (now >>> 32)); // higher 32 bits
            out.writeInt((int) (now & 0x00000000FFFFFFFFL)); // lower 32 bits
            out.writeInt((int) (message.getUserTime().getTime() >>> 32));
            out.writeInt((int) (message.getUserTime().getTime() & 0x00000000FFFFFFFFL));

            out.writeInt(name.length());
            out.writeInt(subject.length());
            out.writeInt(type.length());
            out.writeInt(payloadLen);
            out.writeInt(textLen);
            out.writeInt(binaryLength);

            // write strings & byte array
            try {
                out.write(name.getBytes("US-ASCII"));
                out.write(subject.getBytes("US-ASCII"));
                out.write(type.getBytes("US-ASCII"));
                if (payloadLen > 0) {
                    out.write(payloadTxt.getBytes("US-ASCII"));
                }
                if (textLen > 0) {
                    out.write(text.getBytes("US-ASCII"));
                }
                if (binaryLength > 0) {
                    out.write(message.getByteArray(),
                              message.getByteArrayOffset(),
                              binaryLength);
                }
            }
            catch (UnsupportedEncodingException e) {
            }
            out.flush();
            out.close();

            // send message packet from the byte array
            byte[] buf = baos.toByteArray();

            // setData is synchronized on the packet.
            sendUdpPacket.setData(buf, 0, buf.length);
            // send in synchronized internally on the packet object.
            // Because we only use one packet object for this client,
            // all udp sends are synchronized.
            udpSocket.send(sendUdpPacket);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    /** Send a cMsg message with the status of this EMU to test server. */
    void sendStatusMessage() {

        Random randomData = new Random(System.currentTimeMillis());

        long eventCount   = randomData.nextLong();
        long wordCount    = randomData.nextLong();
        float eventRate   = randomData.nextFloat();
        float dataRate    = randomData.nextFloat();

        int maxEvSize     = randomData.nextInt(3000000);
        int minEvSize     = randomData.nextInt(10000);
        int avgEvSize     = randomData.nextInt(1000000);
        int chunk_X_EtBuf = randomData.nextInt(15000000);
        int[] timeToBuild = null;

        int[] inChanLevels     = new int[] {randomData.nextInt(100), randomData.nextInt(100),
                                            randomData.nextInt(100), randomData.nextInt(100),
                                            randomData.nextInt(100), randomData.nextInt(100),
                                            randomData.nextInt(100), randomData.nextInt(100)};

        int[] outChanLevels    = new int[] {randomData.nextInt(100), randomData.nextInt(100)};

        String[] inChanNames   = new String[] {"ROC10", "ROC11", "ROC12", "ROC13",
                                               "ROC14", "ROC15", "ROC16", "ROC17"};

        String[] outChanNames  = new String[] {"SEB1, SEB2"};

        String[] states = new String[] {"BOOTED", "CONFIGURED", "DOWNLOADED",
                                        "PAUSED", "ACTIVE", "CONFIGURING",
                                        "DOWNLOADING", "PRESTARTING", "ACTIVATING",
                                        "ENDING"};
        String state = states[randomData.nextInt(10)];

        try {
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.state, state.toLowerCase()));
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.codaClass, CODAClass.DC.name()));
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.objectType, "coda3"));

            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventCount, (int)eventCount));
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventCount64, eventCount));
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.numberOfLongs, wordCount));
            // in Hz
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.eventRate, eventRate));
            // in kBytes/sec
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.dataRate, (double)dataRate));
            // in bytes
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.maxEventSize, maxEvSize));
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.minEventSize, minEvSize));
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.avgEventSize, avgEvSize));
            reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.chunk_X_EtBuf, chunk_X_EtBuf));

            // in/output channel ring levels (0-100)
            if (inChanLevels != null && inChanLevels.length > 0) {
                reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.inputChanLevels,
                                                             inChanLevels));
                reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.inputChanNames,
                                                             inChanNames));
            }
            else {
                reportMsg.removePayloadItem(RCConstants.inputChanLevels);
                reportMsg.removePayloadItem(RCConstants.inputChanNames);
            }

            if (outChanLevels != null && outChanLevels.length > 0) {
                reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.outputChanLevels,
                                                             outChanLevels));
                reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.outputChanNames,
                                                             outChanNames));
            }
            else {
                reportMsg.removePayloadItem(RCConstants.outputChanLevels);
                reportMsg.removePayloadItem(RCConstants.outputChanNames);
            }

            // histogram in nanoseconds
            if (timeToBuild != null && timeToBuild.length > 0) {
                reportMsg.addPayloadItem(new cMsgPayloadItem(RCConstants.timeToBuild, timeToBuild));
            }
            else {
                reportMsg.removePayloadItem(RCConstants.timeToBuild);
            }

//            // ADD A BINARY ARRAY which is not a part of the regular status reporting message
//            byte[] binary = new byte[] {(byte)0, (byte)1, (byte)2, (byte)3, (byte)4};
//            reportMsg.setByteArray(binary);

//            System.out.println("Emu " + name + ": try sending STATUS REPORTING Msg:");
//            System.out.println("   " + RCConstants.state + " = " + state);
//            System.out.println("   " + RCConstants.codaClass + " = " + CODAClass.DC.name());
//            System.out.println("   " + RCConstants.eventCount + " = " + eventCount);
//            System.out.println("   " + RCConstants.eventRate + " = " + eventRate);
//            System.out.println("   " + RCConstants.numberOfLongs + " = " + wordCount);
//            System.out.println("   " + RCConstants.dataRate + " = " + dataRate);
//            System.out.println("   " + RCConstants.timeToBuild + " = " + timeToBuild);
//            System.out.println("   " + RCConstants.maxEventSize + " = " + maxEvSize);
//            System.out.println("   " + RCConstants.minEventSize + " = " + minEvSize);
//            System.out.println("   " + RCConstants.avgEventSize + " = " + avgEvSize);

            // Send msg
            send(reportMsg);
        }
        catch (cMsgException e) {
            e.printStackTrace();
        }
    }


}


