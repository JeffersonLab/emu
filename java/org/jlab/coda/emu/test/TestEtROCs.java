package org.jlab.coda.emu.test;

import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.jevio.EventWriter;
import org.jlab.coda.jevio.EvioEvent;
import org.jlab.coda.jevio.EvioException;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.nio.ByteBuffer;

/**
 * Class designed to simulate ROCs sending data to EMU through ET system.
 * Date: Apr 1, 2011
 * @author timmer
 */
public class TestEtROCs {

    private int rocCount = 1;

    private String[] names  = {"Roc1", "Roc2", "Roc3", "Roc4", "Roc5", "Roc6"};

    private int     delay = 2000; // 1 second default timeout
    private boolean debug;
    private boolean stopSending;


    /** Constructor. */
    TestEtROCs(String[] args) {
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
//            else if (args[i].equalsIgnoreCase("-n")) {
//                name = args[i + 1];
//                i++;
//            }
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
            "        [-delay <time>]      set time in millisec between sending of each message\n" +
            "        [-debug]             turn on printout\n" +
            "        [-h]                 print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
        try {
            TestEtROCs sender = new TestEtROCs(args);
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

        // Number of events to ask for in an array
        int chunk = 300;
        // ET system connected to
        EtSystem etSystem;
        // ET station attached to
        EtStation station;
        // Name of ET station attached to
        String stationName;
        // Position of ET station attached to
        int stationPosition = 1;
        // Attachment to ET station
        EtAttachment attachment;
        // Configuration of ET station being created and attached to
        EtStationConfig stationConfig;
        // Array of events obtained from ET system
        EtEvent[] events;


        /**
         * Get the ET sytem object.
         * @return the ET system object.
         */
        private void openEtSystem() {
            try {
                etSystem.open();
                station = etSystem.stationNameToObject("GRAND_CENTRAL");
                System.out.println("Found station = " + stationName);

                // attach to station
                attachment = etSystem.attach(station);
                System.out.println("attached to station " + stationName);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }



        public void run() {

//System.out.println("Send thread started");



            // ID number of this ROC obtained from config file
            int rocId;
            // Keep track of the number of records built in this ROC. Reset at prestart
            int rocRecordId = 0;
            // Type of trigger sent from trigger supervisor
            int triggerType = 13;
            // Is this ROC in single event mode?
            boolean isSingleEventMode = false;
            // Number of events in each ROC raw record
            int eventBlockSize = 10;
            // Number of ROC Raw records in each data transport record
            int numPayloadBanks = 1;
            // The id of the detector which produced the data in block banks of the ROC raw records
            int detectorId = 23;
            // For keeping stats
            long eventNumber=1, eventCountTotal=0, wordCountTotal=0;

            EvioEvent ev = null;
            int timestamp=1001, status=0;

            int startingRocID = 1;

            // number of events in transport record
            int numEvents = 1;
            int startingRecordId = 1;

            if (isSingleEventMode) {
                numEvents = 1;
            }

            ByteBuffer bbuf = ByteBuffer.allocate(2048);

            StringWriter sw = new StringWriter(2048);
            PrintWriter wr = new PrintWriter(sw, true);
            long start_time = System.currentTimeMillis();


            openEtSystem();

            try {
                // starting values
                rocRecordId = startingRecordId;

                while (true) {
                    // send transport records from some ROCs

                    // init
                    rocId = startingRocID;

                    for (int ix = 0; ix < rocCount; ix++) {
                        // send event over network
//System.out.println("Send roc record " + startingRocID + " over network");
                        // turn event into byte array
                        try {
                            // turn event into byte array
                            ev = Evio.createDataTransportRecord(rocId, triggerType,
                                                                detectorId, status,
                                                                (int)eventNumber, eventBlockSize,
                                                                timestamp, rocRecordId,
                                                                numPayloadBanks, isSingleEventMode);

                            // stats
                            numEvents = eventBlockSize * numPayloadBanks;
                            rocRecordId++;
                            timestamp       += numEvents;
                            eventNumber     += numEvents;
                            eventCountTotal += numEvents;
                            wordCountTotal  += ev.getHeader().getLength() + 1;
                        }
                        catch (EvioException e) {
                            System.out.println("MAJOR ERROR generating events");
                            e.printStackTrace();
                        }

                        try {
                            // read in new event in chunks
                            events = etSystem.newEvents(attachment, Mode.SLEEP, 0, chunk,
                                                        (int)etSystem.getEventSize());
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

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

//                        msg.setByteArray(bbuf.array(),0,bbuf.limit());
//                        msg.setByteArrayEndian(cMsgConstants.endianBig);
//                        msg.setSubject(subjects[ix]);
//                        msg.setType(types[ix]);
//
//                        for (int j=0; j<bbuf.asIntBuffer().limit(); j++) {
//                            System.out.println(bbuf.asIntBuffer().get(j));
//                        }

//                        coda.send(msg);

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
                        rocId++;

                        if (stopSending) {
                            return;
                        }

                    }

                    rocRecordId++;

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
//            catch (EvioException e) {
//                e.printStackTrace();
//            }
//            catch (cMsgException e) {
//                e.printStackTrace();
//            }
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
