/*
 * Copyright (c) 2015, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.modules;

import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;

import java.util.ArrayList;
import java.util.Map;

/**
 * This class is used when running simulated, emu-based ROCS.
 * This must be the master ROC in order for synchronization to work properly.
 * All simulated ROCs need to be the same priority level so that they all
 * received the RC commands - END in particular - at the same time.
 * This module takes no input and has no output. Instead it provides
 * synchronization through cMsg commands handled by the RC platform's
 * cMsg server.
 *
 * @author timmer
 * 1/29/2015
 */
public class TsSimulation extends ModuleAdapter {

    /** Subscribe the this subject to get messages from fake ROCs. */
    private String subjectIn = "syncFromRoc";

    /** Handle to subscription to get messages from fake ROCs. */
    private cMsgSubscriptionHandle sub;

    /** Message to send to fake ROCs. */
    private cMsgMessage message;

    /** cMsg name server. */
    private cMsg coda;

    /** Number of incoming ROC messages per statistics interval (3 sec). */
    private int count;

    /** Debug printout on/off. */
    private boolean debug = true;

    /** List of expected fake ROCs we need to communicate with. */
    private ArrayList<String> expectedRocs = new ArrayList<String>(10);

    /** List of responding fake ROCs. */
    private ArrayList<String> respondingRocs = new ArrayList<String>(10);

    /** List of state of responding fake ROCs. */
    private ArrayList<Integer> respondingStates = new ArrayList<Integer>(10);

    /** Instantiate a callback to use in subscription. */
    private myCallback callback = new myCallback();

    /** Callback to use in subscription. */
    private PrintRateThread thread;


    /**
     * This class defines the callback to be run when a message matching our subscription arrives.
     */
    private class myCallback extends cMsgCallbackAdapter {

        public void callback(cMsgMessage msg, Object userObject) {
            // We expect one message from each ROC
            // after each round of producing events.

            // Type = emu name
            respondingRocs.add(msg.getType());
            // Int = 1 (got End cmd) or 0 (no End received)
            respondingStates.add(msg.getUserInt());
System.out.println("  TS mod: got type = " + msg.getType() + " & user int = " + msg.getUserInt());

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
System.out.println("  TS mod: all ROCs got end cmd");
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


    // ---------------------------------------------------


    /** Constructor. */
    public TsSimulation(String name, Emu emu) {
        super(name, emu);

        // Find out which Rocs need syncs from command line args
        // (-Dr1=Roc1 -Dr2=Roc2, etc).
        int rocId = 1;
        String rocName;
        String emuArg = "r" + rocId++;
        while ((rocName = System.getProperty(emuArg)) != null) {
System.out.println("  TS mod: adding roc " + rocName);
            expectedRocs.add(rocName);
            emuArg = "r" + rocId++;
        }

        // Prepare sync message to send to all fake ROCs
        message = new cMsgMessage();
        message.setSubject("sync");
        message.setType("ROC");
        try {  message.setHistoryLengthMax(0); }
        catch (cMsgException e) {/* never happen */}
    }


    /** {@inheritDoc} */
    public void go() {
        state = CODAState.ACTIVE;
        paused = false;

        // Start thread to print out msg rate info
        thread = new PrintRateThread();
        thread.start();
    }


    /** {@inheritDoc} */
    public void prestart() {
        if (debug) System.out.println("  TS: prestart");

        // Get platform's cMsg server
        coda = emu.getCmsgPortal().getCmsgServer();
        try {
System.out.println("  TS mod: subscribe to sub = " + subjectIn + ", typ = *");
            // Subscribe to get ROCs' sync-related messages
            sub = coda.subscribe(subjectIn, "*", callback, null);
        }
        catch (cMsgException e) {}

        state = CODAState.PAUSED;
        paused = false;
    }


    /** {@inheritDoc} */
    public void reset() {
        if (debug) System.out.println("  TS mod: reset()");

        // Unsubscribe
        try { coda.unsubscribe(sub); }
        catch (cMsgException e) {}

        // Stop thread
        thread.stop();

        state = CODAState.CONFIGURED;
        paused = false;
    }


    /** {@inheritDoc} */
    public void end() throws CmdExecException {
        if (debug) System.out.println("  TS mod: end()");

        // Unsubscribe
        try { coda.unsubscribe(sub); }
        catch (cMsgException e) {}

        // Stop thread
        thread.stop();

        state = CODAState.DOWNLOADED;
        paused = false;
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
     * This thread prints out rate of msgs from ROCs.
     */
    private class PrintRateThread extends Thread {

        /** This method is executed as a thread. */
        @Override
        public void run() {
            if (debug) System.out.println("  TS mod: start statistics thread");

            // variables to track incoming message rate
            double freq, freqAvg;
            long t1, t2, deltaT, totalT = 0, totalC = 0, period = 3000; // millisec

            while (state == CODAState.ACTIVE || paused) {
                count = 0;

                t1 = System.currentTimeMillis();

                // wait
                try {
                    Thread.sleep(period);
                }
                catch (InterruptedException e) {
                }

                t2 = System.currentTimeMillis();

                deltaT = t2 - t1; // millisec
                freq = (double) count / deltaT * 1000;
                totalT += deltaT;
                totalC += count;
                freqAvg = (double) totalC / totalT * 1000;

                if (debug) {
                    System.out.println("\nTS: count = " + count + ", total = " + totalC);
                    System.out.println("TS: freq  = " + doubleToString(freq, 1) + " Hz, Avg = " +
                                               doubleToString(freqAvg, 1) + " Hz");
                }

                if (!coda.isConnected()) {
                    // if still not connected, quit
                    System.exit(-1);
                }
            }
        }

    }


}
