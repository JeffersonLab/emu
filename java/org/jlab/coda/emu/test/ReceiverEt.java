package org.jlab.coda.emu.test;

import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.et.exception.*;
import org.jlab.coda.jevio.ByteParser;
import org.jlab.coda.jevio.EvioEvent;
import org.jlab.coda.jevio.EvioException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by IntelliJ IDEA.
 * User: timmer
 * Date: Jun 16, 2010
 * Time: 3:11:18 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReceiverEt {
    private String  name = "Looker";
    private String  etName = "/tmp/emuOut";

    private int     delay = 1000; // 1 second default timeout
    private boolean debug;

    // create ET system object with verbose debugging output
    private EtSystem sys;
    // get GRAND_CENTRAL station object
    private EtStation station;
    // attach to grandcentral
    private EtAttachment att;


    /** Constructor. */
    ReceiverEt(String[] args) {
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
                "   java ReceiverEt\n" +
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
            ReceiverEt receiver = new ReceiverEt(args);
            receiver.run();
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


    class receiveDataThread extends Thread {

        public void run() {

            if (debug) {
                System.out.println("Running ReceiverEt as EMU's bitbucket\n");
            }

            // inputStr("Enter when time to GO");

            // array of events
            EtEvent[] mevs;

            int num, chunk = 300, count = 0;
            long t1, t2, totalT = 0, totalCount = 0;
            double rate, avgRate;

            // initialize
            t1 = System.currentTimeMillis();

            try {

                for (int i = 0; i < 50; i++) {
                    while (count < 300000L) {
                        // get events from ET system
                        mevs = sys.getEvents(att, Mode.SLEEP, null, 0, chunk);

                        // keep track of time
                        if (count == 0) t1 = System.currentTimeMillis();

                        // example of reading & printing event data
//                        if (true) {

//                            for (EtEvent mev : mevs) {
                                // get event's data buffer
//                                ByteBuffer buf = mev.getDataBuffer();
//System.out.println("event's data buffer is " + buf.order() + ", limit = " + buf.limit() +
//", capacity = " + buf.capacity());
//System.out.println("swap = " + mev.needToSwap());
//                                if (mev.needToSwap()) {
//                                    buf.order(ByteOrder.LITTLE_ENDIAN);
//                                }
                                // buf.limit() is set to the length of the actual data (not buffer capacity)
//                                ByteParser parser = new ByteParser();
//                                try {
//                                    EvioEvent ev = parser.parseEvent(buf);
//                                    System.out.println("Event = \n"+ev.toXML());
//                                }
//                                catch (EvioException e) {
//                                    System.out.println("Event NOT in evio foramt");
//                                }

    //                            System.out.println("buffer cap = " + buf.capacity() + ", lim = " + buf.limit() +
    //                            ", pos = " + buf.position());
    //                            num = mev.getDataBuffer().getInt(0);
    //                            System.out.println("data byte order = " + mev.getByteOrder());

    //                            if (mev.needToSwap()) {
    //                                System.out.println("    data swap = " + Integer.reverseBytes(num));
    //                            }
    //                            else {
    //                                System.out.println("    data = " + num);
    //                            }

    //                            int[] con = mev.getControl();
    //                            for (int j : con) {
    //                                System.out.print(j + " ");
    //                            }
    //
    //                            System.out.println("pri = " + mev.getPriority());
//                            }
//                        }
//
                        // put events back into ET system
                        sys.putEvents(att, mevs);
                        //sys.dumpEvents(att, mevs);
                        count += mevs.length;
                    }

                    // calculate the event rate
                    t2 = System.currentTimeMillis();
                    rate = 1000.0 * ((double) count) / (t2 - t1);
                    totalCount += count;
                    totalT += t2 - t1;
                    avgRate = 1000.0 * ((double) totalCount) / totalT;
                    System.out.println("rate = " + String.format("%.3g", rate) +
                                       " Hz,   avg = " + String.format("%.3g", avgRate));
                    count = 0;
                }

            }
            catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            catch (EtException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            catch (EtDeadException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            catch (EtEmptyException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            catch (EtBusyException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            catch (EtTimeoutException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            catch (EtWakeUpException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

    }


    /**
     * This method is executed as a thread.
     */
    public void run() {

        if (debug) {
            System.out.println("Running ReceiverEt\n");
        }

        try {
            // make a direct connection to ET system's tcp server
            EtSystemOpenConfig config = new EtSystemOpenConfig(etName, "localhost", 12349);

            // create ET system object with verbose debugging output
            sys = new EtSystem(config, EtConstants.debugInfo);

            sys.open();

            // get/create station object
            try {
                station = sys.createStation(new EtStationConfig(), "getMeEvents");
            }
            catch (EtExistsException e) {
                station = sys.stationNameToObject("getMeEvents");
            }

            // attach to station
            att = sys.attach(station);
        }
        catch (Exception e) {
            e.printStackTrace();
            return;
        }

        inputStr("Enter to GO");

        receiveDataThread receiver = new receiveDataThread();
        receiver.start();

        inputStr("Enter to QUIT");

        try { Thread.sleep(10000); }
        catch (InterruptedException e) { }

    }

}
