package org.jlab.coda.emu.test;

import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.jevio.EventParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * This class is designed to receive output from an EMU.
 * User: timmer
 * Date: Dec 4, 2009
 */
public class ReceiverCmsg {

    private String  subject = "BitBucket";
    private String  type = "data";
    private String  name = "BitBucket";
    private String  channelName = "BitBucket_SOCKET"; // channelName is subject
    private String  description = "place to dump EMU data";
    private String  UDL;

    private int     delay, count = 5000, timeout = 3000; // 3 second default timeout
    private boolean debug;

    private cMsg coda;
    private cMsgSubscriptionHandle sub;
    EventParser parser;


    /** Constructor. */
    ReceiverCmsg(String[] args) throws cMsgException {
        decodeCommandLine(args);
        parser = new EventParser();
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
            ReceiverCmsg receiver = new ReceiverCmsg(args);
            receiver.run();
        }
        catch (cMsgException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    /**
     * This class defines the callback to be run when a message matching our subscription arrives.
     */
    class myCallback extends cMsgCallbackAdapter {

        public void callback(cMsgMessage msg, Object userObject) {
            byte[] data = msg.getByteArray();
            if (data == null) {
                System.out.println("ReceiverCmsg: got bogus message");
                return;
            }

            count++;

//            try {
//                ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
//
//                if (msg.getByteArrayEndian() == cMsgConstants.endianLittle) {
////System.out.println("NEED TO SWAP DUDE !!!");
//                    byteOrder = ByteOrder.LITTLE_ENDIAN;
//                }
//                else {
////System.out.println("ENDIAN IS BIG DUDE !!!");
//                }
//
//                EvioBank bank = parser.parseEvent(data, byteOrder);
//                StringWriter sw = new StringWriter(1000);
//                XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw);
//                bank.toXML(xmlWriter);
//                System.out.println("Receiving msg:\n" + sw.toString());
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
        }
        
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

        myCallback cb = new myCallback();
        sub = coda.subscribe(subject, type, cb, null);

        // enable message reception
        coda.start();

        // inputStr("Enter when time to GO");

        // variables to track incoming message rate
        double freq, freqAvg;
        long   totalT=0, totalC=0, period = 2000; // millisec

        while (true) {
            count = 0;

            // wait
            try { Thread.sleep(period); }
            catch (InterruptedException e) {}

            freq    = (double)count/period*1000;
            totalT += period;
            totalC += count;
            freqAvg = (double)totalC/totalT*1000;

            if (debug) {
                System.out.println("count = " + count + ", total = " + totalC);
                System.out.println("freq  = " + EmuUtilities.doubleToString(freq, 1) + " Hz, Avg = " +
                                                EmuUtilities.doubleToString(freqAvg, 1) + " Hz");
            }

            if (!coda.isConnected()) {
                // if still not connected, quit
                System.out.println("No longer connected to domain server, quitting");
                System.exit(-1);
            }
        }

    }


}
