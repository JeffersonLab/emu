package org.jlab.coda.emu.test;

import org.jlab.coda.cMsg.cMsg;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.cMsg.cMsgConstants;
import org.jlab.coda.jevio.EvioEvent;
import org.jlab.coda.jevio.EvioException;
import org.jlab.coda.emu.support.data.Evio;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: timmer
 * Date: Mar 4, 2010
 * Time: 2:30:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestEBs {

    private int ebCount = 2;

    private String[] subjects  = {"EB1", "EB2", "EB3", "EB4", "EB5", "EB6"};
    private String[] types = {"a", "b", "c", "d", "e", "f"};
    private String   name = "EBs";
    private String   description = "place to send data to EMU";
    private String   UDL;

    private int     delay = 1000; // 1 second default timeout
    private boolean debug;
    private boolean stopSending;
    private cMsg coda;


    /** Constructor. */
    TestEBs(String[] args) {
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
            "        [-delay <time>]      set time in millisec between sending of each message\n" +
            "        [-debug]             turn on printout\n" +
            "        [-h]                 print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
        try {
            TestEBs sender = new TestEBs(args);
            sender.run();
        }
        catch (cMsgException e) {
            e.printStackTrace();
            System.exit(-1);
        }
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

            // in the arg order:
            int rocNum;
            int startingRocID = 11;
            int eventID       = 15;
            int eventNumber   = 1;
            int timestamp     = 1001;
            int startingRecordId = 1;
            boolean isSingleEventMode = false;
            // when generating physics events, use these parameters:
            int ebId;
            int numEvents = 2; // number of "actual" events in each physics event
            int numRocs = 2;   // # of ROCs in each physics event
            int status = 0;

            if (isSingleEventMode) {
                numEvents = 1;
            }

            int recordId;
            EvioEvent ev;
            ByteBuffer bbuf = ByteBuffer.allocate(2048);
            cMsgMessage msg = new cMsgMessage();

            StringWriter sw = new StringWriter(2048);
            PrintWriter  wr = new PrintWriter(sw, true);
            long start_time = System.currentTimeMillis();


            try {
                // starting values
                recordId = startingRecordId;

                while (true) {
                    // send transport records from some ROCs
                    ebId = 7;
                    rocNum = startingRocID;
                    
                    if (true) {

                        for (int ix = 0; ix < ebCount; ix++) {
                            // send event over network
    //System.out.println("Send roc record " + startingRocID + " over network");
                            // turn event into byte array

                            ev = Evio.createDataTransportRecordFromEB(ebId,         eventID,
                                                                      numRocs,      rocNum,
                                                                      eventNumber,  numEvents,
                                                                      timestamp,    recordId,
                                                                      status,       isSingleEventMode);

                            //sw.getBuffer().delete(0, sw.getBuffer().capacity());
                            bbuf.clear();
                            ev.write(bbuf);
                            bbuf.flip();

                            msg.setByteArray(bbuf.array(),0,bbuf.limit());
                            msg.setByteArrayEndian(cMsgConstants.endianBig);
                            msg.setSubject(subjects[ix]);
                            msg.setType(types[ix]);

                            coda.send(msg);

    //                        for (int j=0; j<bbuf.asIntBuffer().limit(); j++) {
    //                            System.out.println(bbuf.asIntBuffer().get(j));
    //                        }


    //                        try {
    //                            StringWriter sw2 = new StringWriter(1000);
    //                            XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
    //                            ev.toXML(xmlWriter);
    //                            System.out.println("\nSending msg:\n" + sw2.toString());
    //
    //                            System.out.println("Sending msg (bin):");
    //                            while (bbuf.hasRemaining()) {
    //                                wr.printf("%#010x\n", bbuf.getInt());
    //                            }
    //                            System.out.println(sw.toString() + "\n\n");
    //                        }
    //                        catch (XMLStreamException e) {
    //                            e.printStackTrace();
    //                        }

                            // send from next eb (group of rocs)
                            ebId++;
                            rocNum += numRocs;

                            if (stopSending) {
                                return;
                            }

                        }

                        timestamp   += numEvents;
                        eventNumber += numEvents;
                        recordId++;
                    }

                    Thread.sleep(delay);

                    long now = System.currentTimeMillis();
                    long deltaT = now - start_time;
                    if (deltaT > 2000) {
                        wr.printf("%d  Hz\n", 3L/deltaT);
                        System.out.println(sw.toString());
                        start_time = now;
                    }

                } // while

            }
            catch (EvioException e) {
                e.printStackTrace();
            }
            catch (cMsgException e) {
                e.printStackTrace();
            }
            catch (InterruptedException e) {
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
        coda = new cMsg(UDL, name, description);
        coda.connect();

        // enable message reception
        coda.start();

        inputStr("Enter to GO");
        stopSending = false;

        sendDataThread sender = new sendDataThread();
        sender.start();

        inputStr("Enter to QUIT");
        stopSending = true;

        try { Thread.sleep(10000); }
        catch (InterruptedException e) { }

    }




}
