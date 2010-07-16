package org.jlab.coda.emu.test;

import org.jlab.coda.jevio.*;
import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.et.exception.EtException;
import org.jlab.coda.et.exception.EtTooManyException;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.emu.support.data.EventType;
import org.jlab.coda.emu.support.data.PayloadBank;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

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

    private int     delay = 0; // 1 second default timeout
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


    /**
     * This method is executed as a thread.
     */
    public void runMT() {

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

        // latch to start things off at the same time
        CountDownLatch latch = new CountDownLatch(1);


        stopSending = false;

        // ROC 1
        sendDataThreadMT sender = new sendDataThreadMT(1, true, false, latch);
        sender.start();
        // ROC 2
        sender = new sendDataThreadMT(2, true, false, latch);
        sender.start();
        // ROC 3
        sender = new sendDataThreadMT(3, true, false, latch);
        sender.start();

        inputStr("Enter to GO");
        latch.countDown();
        
        inputStr("Enter to QUIT");
        stopSending = true;

        try { Thread.sleep(10000); }
        catch (InterruptedException e) { }

    }


    class sendDataThreadMT extends Thread {

        int rocNum;
        boolean doStats;
        boolean doUserEvents;
        CountDownLatch latch;

        sendDataThreadMT(int rocNum, boolean doStats, boolean doUserEvents, CountDownLatch latch) {
            this.latch = latch;
            this.rocNum = rocNum;
            this.doStats = doStats;
            this.doUserEvents = doUserEvents;
        }

        public void run() {

            // one ET connection per thread
//            EtSystem mySys;
//            EtAttachment myAtt;
//            EtStation myGc;
//
//            try {
//                // make a direct connection to ET system's tcp server
//                EtSystemOpenConfig config = new EtSystemOpenConfig(etName, "localhost", 12347);
//
//                // create ET system object with verbose debugging output
//                mySys = new EtSystem(config, EtConstants.debugInfo);
//
//                mySys.open();
//
//                // get GRAND_CENTRAL station object
//                myGc = mySys.stationNameToObject("GRAND_CENTRAL");
//
//                // attach to grandcentral
//                myAtt = mySys.attach(myGc);
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//                return;
//            }

            // in the arg order:
            int eventID = 10000;
            int dataBankTag = 666; // starting data bank tag
            int dataBankNum = 777; // starting data bank num
            int eventNumber = 1;
            int numEventsInPayloadBank = 1; // number of physics events in payload bank
            int timestamp = 1000;
            int recordId = 1;
            int numPayloadBanks = 1;

            EvioEvent ev;
            ByteBuffer bbuf;
            int[] control = new int[EtConstants.stationSelectInts];
            Arrays.fill(control, -1);

            int chunk = 100, ignore = 3;
            long counter = 0L;
            long start_time = System.currentTimeMillis();
            long totalT=0L, totalC=0L;


            EvioBank bank;
            EventBuilder builder = new EventBuilder(0, DataType.BANK, 0); // this event not used, just need a builder

            // create user event to be inserted to test EMU building algorithm
            int dtrTag = Evio.createCodaTag(EventType.USER.getValue(), 1);  // Tag(user value, source id)
            EvioEvent dtrEvent = new PayloadBank(dtrTag, DataType.BANK, 1);
            builder.setEvent(dtrEvent);

            try {
                // add bank with full recordId
                bank = new EvioBank(Evio.RECORD_ID_BANK, DataType.INT32, 1);
                bank.appendIntData(new int[] {1});
                builder.addChild(dtrEvent, bank);
                bank = new EvioBank(123, DataType.INT32, 456);
                bank.appendIntData(new int[] {123});
                builder.addChild(dtrEvent, bank);
                dtrEvent.setAllHeaderLengths();  // TODO: necessary?

            } catch (EvioException e) {/* never happen */}


            try {

                int loops = 0;

                latch.await();

                while (true) {
                    EtEvent[] evs = sys.newEvents(att, Mode.SLEEP, 0, chunk, 1024, rocNum);
                    if (evs.length < chunk) {
                        sys.dumpEvents(att, evs);
                        continue;
                    }

                    // stations will filter events on this number
                    control[0] = rocNum;

                    // put user event(s) into the event flow from this "ROC"
                    if (doUserEvents) {
                        if (loops++ % 1000 == 0) {
                            EtEvent[] userEvs = sys.newEvents(att, Mode.SLEEP, 0, 1, 1024);
                            for (EtEvent userEv : userEvs) {
                                bbuf = userEv.getDataBuffer();
                                userEv.setLength(bbuf.limit());
                                userEv.setControl(control);
                                dtrEvent.write(bbuf);
                            }
                            System.out.println("Putting " + userEvs.length + " user events in");
                            sys.putEvents(att, userEvs);
                        }
                    }


                    for (int j = 0; j < chunk; j++) {
                        // turn event into byte array
                        ev = Evio.createDataTransportRecord(rocNum, eventID,
                                                            dataBankTag, dataBankNum,
                                                            eventNumber, numEventsInPayloadBank,
                                                            timestamp, recordId, numPayloadBanks,
                                                            false);
                        bbuf = evs[j].getDataBuffer();
                        evs[j].setLength(bbuf.limit());
                        evs[j].setControl(control);
                        ev.write(bbuf);

                        if (stopSending) {
                            return;
                        }

                        recordId++;
                        eventID++;
                        timestamp   += numEventsInPayloadBank;
                        eventNumber += numEventsInPayloadBank;
                    }

                    sys.putEvents(att, evs);

                    counter += evs.length;

                    Thread.sleep(delay);

                    // only need stats from one of these threads
                    if (doStats) {
                        long now = System.currentTimeMillis();
                        long deltaT = now - start_time;
                        if (deltaT > 2000) {
                            String s;
                            if (ignore < 1) {
                                totalC += counter;
                                totalT += deltaT;
                                s = String.format("%d:  %d Hz, avg %d Hz", rocNum, counter*1000/deltaT, totalC*1000/totalT);
                            }
                            // don't count the first few rates (slow) as part of the average
                            else {
                                s = String.format("%d Hz", counter*1000/deltaT);
                                ignore--;
                            }
                            System.out.println(s);
                            start_time = now;
                            counter = 0;
                        }
                    }
                }

            }
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

            int index, chunk = 300, ignore = 3;
            long counter = 0L;
            long start_time = System.currentTimeMillis();
            long totalT=0L, totalC=0L;


            EvioBank bank;
            EventBuilder builder = new EventBuilder(0, DataType.BANK, 0); // this event not used, just need a builder

            // create user event to be inserted to test EMU building algorithm
            int dtrTag = Evio.createCodaTag(EventType.USER.getValue(), 1);  // Tag(user value, source id)
            EvioEvent dtrEvent = new PayloadBank(dtrTag, DataType.BANK, 1);
            builder.setEvent(dtrEvent);

            try {
                // add bank with full recordId
                bank = new EvioBank(Evio.RECORD_ID_BANK, DataType.INT32, 1);
                bank.appendIntData(new int[] {1});
                builder.addChild(dtrEvent, bank);
                bank = new EvioBank(123, DataType.INT32, 456);
                bank.appendIntData(new int[] {123});
                builder.addChild(dtrEvent, bank);
                dtrEvent.setAllHeaderLengths();  // TODO: necessary?

            } catch (EvioException e) {/* never happen */}

           int loops = 0;

            try {

                while (true) {
                    EtEvent[] evs = sys.newEvents(att, Mode.SLEEP, 0, chunk, 1024);
                    if (evs.length < chunk) {
                        sys.dumpEvents(att, evs);
                        continue;
                    }

//                    if (loops++ % 1000 == 0) {
//                        EtEvent[] userEvs = sys.newEvents(att, Mode.SLEEP, 0, 5, 1024);
//                        for (EtEvent userEv : userEvs) {
//                            bbuf = userEv.getDataBuffer();
//                            userEv.setLength(bbuf.limit());
//                            control[0] = 1;
//                            userEv.setControl(control);
//                            dtrEvent.write(bbuf);
//                        }
//                        System.out.println("Putting " + userEvs.length + " user events in");
//                        sys.putEvents(att, userEvs);
//                    }

                    for (int j = 0; j < chunk; j += 3) {
                        // send transport records from 3 ROCs
                        rocNum = rocID;

                        for (int i = 0; i < 3; i++) {
                            // turn event into byte array
                            ev = Evio.createDataTransportRecord(rocNum, eventID,
                                                                dataBankTag, dataBankNum,
                                                                eventNumber, numEventsInPayloadBank,
                                                                timestamp, recordId, numPayloadBanks,
                                                                false);
                            index = j+i;
                            bbuf = evs[index].getDataBuffer();
                            evs[index].setLength(bbuf.limit());
                            control[0] = rocNum;
                            evs[index].setControl(control);
                            ev.write(bbuf);

                             //send from next roc
                            rocNum++;

                            if (stopSending) {
                                return;
                            }
                        }
                        recordId++;
                        eventID++;
                        timestamp   += numEventsInPayloadBank;
                        eventNumber += numEventsInPayloadBank;
                    }

                    sys.putEvents(att, evs);

                    counter += evs.length;

                    Thread.sleep(delay);

                    long now = System.currentTimeMillis();
                    long deltaT = now - start_time;
                    if (deltaT > 2000) {
                        String s;
                        if (ignore < 1) {
                            totalC += counter;
                            totalT += deltaT;
                            s = String.format("%d Hz, avg %d Hz", counter*1000/3/deltaT, totalC*1000/3/totalT);
                        }
                        // don't count the first few rates (slow) as part of the average
                        else {
                            s = String.format("%d Hz", counter*1000/3/deltaT);
                            ignore--;
                        }
                        System.out.println(s);
                        start_time = now;
                        counter = 0;
                    }
                }

            }
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


}



