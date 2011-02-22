package org.jlab.coda.emu.test;

import org.jlab.coda.cMsg.cMsg;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.cMsg.cMsgConstants;
import org.jlab.coda.jevio.*;
import org.jlab.coda.emu.support.data.Evio;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.nio.ByteBuffer;

/**
 * Class designed to simulate ROCs sending data to EMU.
 * Date: Feb 11, 2010
 * @author timmer
 */
public class TestROCs {

    private int rocCount = 4;

    private String[] subjects  = {"ROC1", "ROC2", "ROC3", "ROC4", "ROC5", "ROC6"};
    private String[] types = {"a", "b", "c", "d", "e", "f"};
    private String   name = "ROCs";
    private String   description = "place to send data to EMU";
    private String   UDL;

    private int     delay = 2000; // 1 second default timeout
    private boolean debug;
    private boolean stopSending;
    private cMsg coda;


    /** Constructor. */
    TestROCs(String[] args) {
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
            TestROCs sender = new TestROCs(args);
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
        EventWriter evWriter = null;

        public void run() {

//System.out.println("Send thread started");

            // in the arg order:
            int status        = 0;
            int startingRocID = 1;
            int eventID       = 15;
            int dataBankTag   = 111; // starting data bank tag
            int eventNumber   = 1;
            int numEventsInPayloadBank = 1; // number of events in first payload bank (incremented for each additional bank)
            int timestamp        = 1001;
            int startingRecordId = 1;
            int numPayloadBanks  = 2;
            boolean isSingleEventMode = false;

            if (isSingleEventMode) {
                numEventsInPayloadBank = 1;    
            }

            int rocNum, recordId;
            EvioEvent ev;
            ByteBuffer bbuf = ByteBuffer.allocate(2048);
            cMsgMessage msg = new cMsgMessage();

            StringWriter sw = new StringWriter(2048);
            PrintWriter wr = new PrintWriter(sw, true);
            long start_time = System.currentTimeMillis();

            try {
                // starting values
                recordId = startingRecordId;

                while (true) {
                    // send transport records from some ROCs

                    // init
                    rocNum = startingRocID;

                    for (int ix = 0; ix < rocCount; ix++) {
                        // send event over network
//System.out.println("Send roc record " + startingRocID + " over network");
                        // turn event into byte array
                        ev = Evio.createDataTransportRecord(rocNum,      eventID,
                                                            dataBankTag, status,
                                                            eventNumber, numEventsInPayloadBank,
                                                            timestamp,   recordId,
                                                            numPayloadBanks, isSingleEventMode);

                        bbuf.clear();
                        try {
                            evWriter = new EventWriter(bbuf, 128000, 10, null, null);
                            evWriter.writeEvent(ev);
                            evWriter.close();
                        }
                        catch (EvioException e) {
                            /* never happen */
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                        bbuf.flip();

                        msg.setByteArray(bbuf.array(),0,bbuf.limit());
                        msg.setByteArrayEndian(cMsgConstants.endianBig);
                        msg.setSubject(subjects[ix]);
                        msg.setType(types[ix]);

//                        for (int j=0; j<bbuf.asIntBuffer().limit(); j++) {
//                            System.out.println(bbuf.asIntBuffer().get(j));
//                        }

                        coda.send(msg);

                        try {
                            StringWriter sw2 = new StringWriter(1000);
                            XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
                            ev.toXML(xmlWriter);
                            System.out.println("\nSending msg:\n" + sw2.toString());

//                            System.out.println("Sending msg (bin):");
//                            while (bbuf.hasRemaining()) {
//                                wr.printf("%#010x\n", bbuf.getInt());
//                            }
//                            System.out.println(sw.toString() + "\n\n");
                        }
                        catch (XMLStreamException e) {
                            e.printStackTrace();
                        }

                        // send from next roc
                        rocNum++;

                        if (stopSending) {
                            return;
                        }

                    }

                    timestamp   += numEventsInPayloadBank*numPayloadBanks;
                    eventNumber += numEventsInPayloadBank*numPayloadBanks;
                    recordId++;

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
