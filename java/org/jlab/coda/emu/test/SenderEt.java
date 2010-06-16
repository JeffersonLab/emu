package org.jlab.coda.emu.test;

import org.jlab.coda.jevio.*;
import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.et.exception.EtException;
import org.jlab.coda.et.exception.EtTooManyException;
import org.jlab.coda.emu.support.data.Evio;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: timmer
 * Date: Jun 11, 2010
 * Time: 2:45:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class SenderEt {

    private String  name = "ROC1";
    private String  channelName = "SingleEmu_SOCKET"; // channelName is subject
    private String  etName = "/tmp/emuIn";

    private int     delay = 1000; // 1 second default timeout
    private boolean debug;
    private boolean stopSending;

    // create ET system object with verbose debugging output
    private EtSystem sys;
    // get GRAND_CENTRAL station object
    private EtStation gc;
    // attach to grandcentral
    private EtAttachment att;


    /** Constructor. */
    SenderEt(String[] args) {
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
            else if (args[i].equalsIgnoreCase("-f")) {
                etName= args[i + 1];
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
                "        [-n <name>]          client name\n"+
                "        [-f <et filename>]   et system filename\n"+
                "        [-delay <time>]      set time in millisec between sending of each message\n" +
                "        [-debug]             turn on printout\n" +
                "        [-h]                 print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
        try {
            SenderEt sender = new SenderEt(args);
            sender.run();
        }
        catch (Exception e) {
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

            // in the arg order:
            int rocID = 1;
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
            ByteBuffer bbuf;
            int[] control = new int[EtConstants.stationSelectInts];
            Arrays.fill(control, -1);

            int counter = 0;
            StringWriter sw = new StringWriter(2048);
            PrintWriter wr = new PrintWriter(sw, true);
            long start_time = System.currentTimeMillis();

            try {

                while (true) {
                    // send transport records from 3 ROCs
                    rocNum = rocID;

                    EtEvent[] evs = sys.newEvents(att, Mode.SLEEP, 0, 3, 1024);
                    if (evs.length < 3) {
                        sys.dumpEvents(att, evs);
                        try { Thread.sleep(100); }
                        catch (InterruptedException e) { }
                        continue;
                    }

                    for (int ix = 0; ix < 3; ix++) {
                        // send event over network
//System.out.println("Send roc record " + rocID + " over network");
                        // turn event into byte array
                        ev = Evio.createDataTransportRecord(rocNum, eventID,
                                                            dataBankTag, dataBankNum,
                                                            eventNumber, numEventsInPayloadBank,
                                                            timestamp, recordId, numPayloadBanks,
                                                            false);

//
//                        // traditional bank of banks
//                        EventBuilder eventBuilder = new EventBuilder(0x1234, DataType.BANK, 1);
//                        EvioEvent event = eventBuilder.getEvent();
//
//                        // add a bank of ints
//                        EvioBank bank = new EvioBank(0x22, DataType.INT32, 0); // tag, type, num
//                        int[] iarray = {counter++};
//                        eventBuilder.appendIntData(bank, iarray);
//                        eventBuilder.addChild(event, bank);

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
//                        sw.getBuffer().delete(0, sw.getBuffer().capacity());

                        bbuf = evs[ix].getDataBuffer();
                        evs[ix].setLength(bbuf.limit());
                        control[0] = rocNum;
                        evs[ix].setControl(control);
                        ev.write(bbuf);

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

                    sys.putEvents(att, evs);

                    counter += 3;

                    Thread.sleep(delay);

                    long now = System.currentTimeMillis();
                    long deltaT = now - start_time;
                    if (deltaT > 2000) {
                        wr.printf("%d  Hz\n", counter/deltaT);
                        System.out.println(sw.toString());
                        start_time = now;
                        counter = 0;
                    }

                    eventID++;
                    timestamp   += numEventsInPayloadBank;
                    eventNumber += numEventsInPayloadBank;
                }

            }
//            catch (XMLStreamException e) {
//                e.printStackTrace();
//            }
            catch (EvioException e) {
                e.printStackTrace();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            return;
        }

    }


    /**
     * This method is executed as a thread.
     */
    public void run() {

        if (debug) {
            System.out.println("Running SenderEt as EMU's ROC1,2,3\n");
        }

        try {
            // make a direct connection to ET system's tcp server
            EtSystemOpenConfig config = new EtSystemOpenConfig(etName, "localhost", 12347);

            // create ET system object with verbose debugging output
            sys = new EtSystem(config, EtConstants.debugInfo);

            sys.open();

            // get GRAND_CENTRAL station object
            gc = sys.stationNameToObject("GRAND_CENTRAL");

            // attach to grandcentral
            att = sys.attach(gc);
        }
        catch (Exception e) {
            e.printStackTrace();
            return;
        }

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



