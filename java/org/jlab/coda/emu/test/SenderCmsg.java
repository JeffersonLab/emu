package org.jlab.coda.emu.test;

import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsg;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.cMsg.cMsgConstants;
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.jevio.*;
//import org.jlab.coda.emu.support.data.Evio;

//import javax.xml.stream.XMLStreamWriter;
//import javax.xml.stream.XMLOutputFactory;
//import javax.xml.stream.XMLStreamException;
//import java.net.Socket;
import java.io.*;
import java.nio.ByteBuffer;
//import java.util.concurrent.TimeoutException;

/**
 * This class is designed to send data to an EMU.
 * User: timmer
 * Date: Dec 4, 2009
 * Time: 10:39:19 AM
 * To change this template use File | Settings | File Templates.
 */
public class SenderCmsg {

    private String  subject  = "ROC1";
    private String  subject1 = "ROC1";
    private String  subject2 = "ROC2";
    private String  type = "data";
    private String  name = "ROC1";
    private String  channelName = "SingleEmu_SOCKET"; // channelName is subject
    private String  description = "place to send data to EMU";
    private String  UDL;

    private int     delay = 1000; // 1 second default timeout
    private boolean debug;
    private boolean stopSending;
    private cMsg coda;


    /** Constructor. */
    SenderCmsg(String[] args) {
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
            "        [-s <subject>]       set subject of data messages\n" +
            "        [-t <type>]          set type of data messages\n" +
            "        [-delay <time>]      set time in millisec between sending of each message\n" +
            "        [-debug]             turn on printout\n" +
            "        [-h]                 print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
        try {
            SenderCmsg sender = new SenderCmsg(args);
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


    class sendDataThread extends Thread {

        public void run() {

System.out.println("Send thread started");

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

            int rocNum;
            EvioEvent ev;
            ByteBuffer bbuf = ByteBuffer.allocate(1000);
            cMsgMessage msg = new cMsgMessage();
            msg.setSubject(subject);
            msg.setType(type);

            int counter = 0;
            StringWriter sw = new StringWriter(2048);
            PrintWriter  wr = new PrintWriter(sw, true);
            long start_time = System.currentTimeMillis();

            try {

                while (true) {
                    // send transport records from 3 ROCs
                    rocNum = rocID;

                    for (int ix = 0; ix < 2; ix++) {
                        // send event over network
//System.out.println("Send roc record " + rocID + " over network");
                        // turn event into byte array
//                        ev = Evio.createDataTransportRecord(rocNum, eventID,
//                                                            dataBankTag, dataBankNum,
//                                                            eventNumber, numEventsInPayloadBank,
//                                                            timestamp, recordId, numPayloadBanks);


                        // traditional bank of banks
                        EventBuilder eventBuilder = new EventBuilder(0x1234, DataType.BANK, 1);
                        EvioEvent event = eventBuilder.getEvent();

                        // add a bank of ints
                        EvioBank bank = new EvioBank(0x22, DataType.UINT32, 0); // tag, type, num
                        int[] iarray = {counter++};
                        eventBuilder.appendIntData(bank, iarray);
                        eventBuilder.addChild(event, bank);

//                        // add a bank of segments
//                        EvioBank bank2 = new EvioBank(0x33, DataType.SEGMENT, 0);
//
//                        EvioSegment segment1 = new EvioSegment(0x34, DataType.SHORT16);
//                        short[] sarray = {4,5,6};
//                        eventBuilder.appendShortData(segment1, sarray);
//                        eventBuilder.addChild(bank2, segment1);
//                        eventBuilder.addChild(event, bank2);
//
//                        // add a bank of tag segments
//                        EvioBank bank3 = new EvioBank(0x45, DataType.TAGSEGMENT, 0);
//
//                        EvioTagSegment tagsegment1 = new EvioTagSegment(0x46, DataType.DOUBLE64);
//                        double[] darray = {7,8,9};
//                        eventBuilder.appendDoubleData(tagsegment1, darray);
//                        eventBuilder.addChild(bank3, tagsegment1);
//                        eventBuilder.addChild(event, bank3);
//

                        sw.getBuffer().delete(0, sw.getBuffer().capacity());
                        bbuf.clear();
                        event.write(bbuf);
                        bbuf.flip();
                        
//                        System.out.println("Sending array of length " + bbuf.limit());
                        msg.setByteArrayNoCopy(bbuf.array(),0,bbuf.limit());
                        msg.setByteArrayEndian(cMsgConstants.endianBig);

                        if (ix == 0) {
                            msg.setSubject(subject1);
                            msg.setType("a");
                        }
                        else {
                            msg.setSubject(subject2);
                            msg.setType("b");
                        }
                        coda.send(msg);

//                        StringWriter sw2 = new StringWriter(1000);
//                        XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
//                        event.toXML(xmlWriter);
//                        System.out.println("\nSending msg:\n" + sw2.toString());
                        
//                        System.out.println("Sending msg (bin):");
//                        while (bbuf.hasRemaining()) {
//                            wr.printf("%#010x\n", bbuf.getInt());
//                        }
//                        System.out.println(sw.toString() + "\n\n");
                    
                        // send from next roc
                        rocNum++;

                        if (stopSending) {
                            return;
                        }

                    }

                    Thread.sleep(delay);

                    long now = System.currentTimeMillis();
                    long deltaT = now - start_time;
                    if (deltaT > 2000) {
                        wr.printf("%d  Hz\n", 3L/deltaT);
                        System.out.println(sw.toString());
                        start_time = now;
                    }

                    eventID++;
                    timestamp += numEventsInPayloadBank;
                    eventNumber += numEventsInPayloadBank;
                }

            }
//            catch (XMLStreamException e) {
//                e.printStackTrace();
//            }
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
System.out.println("Using udl = " + UDL);

        // connect to cMsg server
        coda = new cMsg(UDL, name, description);
        coda.connect();

        // enable message reception
        coda.start();

        EmuUtilities.inputStr("Enter to GO");
        stopSending = false;

        sendDataThread sender = new sendDataThread();
        sender.start();

        EmuUtilities.inputStr("Enter to QUIT");
        stopSending = true;

        try { Thread.sleep(10000); }
        catch (InterruptedException e) { }

    }


}
