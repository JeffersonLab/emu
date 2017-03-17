/*
 * Copyright (c) 2014, Jefferson Science Associates
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
import org.jlab.coda.emu.EmuUtilities;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * This class is designed to receive output from fake Roc EMUs and
 * is used to synchronize their output so they all produce the same
 * amount of events and therefore the run can END properly.
 * Run this before PRESTART.
 * @author timmer
 * (Sep 24, 2014)
 */
public class RocSynchronizer {

    // Subscribe the this subject and type to get messages from fake ROCs
    private String subjectIn = "syncFromRoc";
    private cMsgSubscriptionHandle sub;

    // Message to send to fake ROCs
    private cMsgMessage message;

    // Members for connection to platform's cMsg name server
    private cMsg   coda;
    private String UDL;
    private String name = "Synchronizer";
    private String description = "synchronize fake ROCs";

    // Members for statistics
    private int     count;
    private boolean debug;

    // List of expected fake ROCs we need to communicate with
    private ArrayList<String> expectedRocs = new ArrayList<>(10);
    // List of actually responding fake ROCs
    private ArrayList<String> respondingRocs = new ArrayList<>(10);
    // List of actually state of responding fake ROCs
    private ArrayList<Integer> respondingStates = new ArrayList<>(10);


    /** Constructor. */
    RocSynchronizer(String[] args) throws cMsgException {
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
            else if (args[i].equalsIgnoreCase("-u")) {
                UDL= args[i + 1];
                i++;
            }
            // s - stands for sender
            else if (args[i].equalsIgnoreCase("-r")) {
                expectedRocs.add(args[i + 1]);
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
            "   java RocSynchronizer\n" +
            "        [-u <UDL>]    set UDL to connect to cMsg\n" +
            "        [-r <roc>]    fake ROC to sync (use this multiple times)\n" +
            "        [-debug]      turn on printout\n" +
            "        [-h]          print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
        try {
            RocSynchronizer receiver = new RocSynchronizer(args);
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
            // We expect one message from each ROC
            // after each round of producing events.

            // Type = emu name
            respondingRocs.add(msg.getType());
            // Int = 1 (got End cmd) or 0 (no End received)
            respondingStates.add(msg.getUserInt());
//System.out.println("Got type = " + msg.getType() + " & user int = " + msg.getUserInt());

            // If we got the expected number of responses, see if
            // we got them from the expected ROCs.
            if (respondingRocs.size() == expectedRocs.size()) {
//                for (String roc : respondingRocs) {
//                    if (!expectedRocs.contains(roc)) {
//                        System.out.println("Not expecting ROC named " + roc);
//                        System.exit(-1);
//                    }
//                }

//System.out.println("All ROCs reporting");
                // If we're here we got the proper responses

                // If all ROCs have received the END command,
                // state = "gotEnd", then tell ROCs to finish up.
                if (!respondingStates.contains(0)) {
System.out.println("All ROCs got end cmd");
                    // Tell ROCs to stop sending events
                    message.setUserInt(1);
                }
                else {
                    // Tell ROCs to send next batch of events
                    message.setUserInt(0);
                }

                try {
                    // This message will reach all fake ROCs
                    coda.send(message);
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }

                respondingRocs.clear();
                respondingStates.clear();
                count++;
            }
        }
    }


    /**
     * This method is executed as a thread.
     */
    public void run() throws cMsgException {

        if (debug) {
            System.out.println("Running RocSynchronzier\n");
        }

        // Get the UDL for connection to the cMsg server. If none given,
        if (UDL == null) UDL = "cMsg://localhost/cMsg/RocSync";

        // connect to cMsg server
        coda = new cMsg(UDL, name, description);
        coda.connect();
System.out.println("  connected");

        message = new cMsgMessage();
        message.setSubject("sync");
        message.setType("ROC");
        message.setHistoryLengthMax(0);

        myCallback cb = new myCallback();
        sub = coda.subscribe(subjectIn, "*", cb, null);

        // enable message reception
        coda.start();

        // variables to track incoming message rate
        double freq, freqAvg;
        long   t1, t2, deltaT, totalT=0, totalC=0, period = 3000; // millisec

        while (true) {
            count = 0;

            t1 = System.currentTimeMillis();

            // wait
            try { Thread.sleep(period); }
            catch (InterruptedException e) {}

            t2 = System.currentTimeMillis();

            deltaT  = t2 - t1; // millisec
            freq    = (double)count/deltaT*1000;
            totalT += deltaT;
            totalC += count;
            freqAvg = (double)totalC/totalT*1000;

            if (debug) {
                System.out.println("count = " + count + ", total = " + totalC);
                System.out.println("freq  = " + EmuUtilities.doubleToString(freq, 1) + " Hz, Avg = " +
                                                EmuUtilities.doubleToString(freqAvg, 1) + " Hz");
            }

            if (!coda.isConnected()) {
                // if still not connected, quit
                System.out.println("No longer connected to server, quitting");
                System.exit(-1);
            }
        }

    }


}
