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
 * receive the RC commands - END in particular - at the same time.
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
    private ArrayList<String> expectedRocs = new ArrayList<>(10);

    /** List of responding fake ROCs. */
    private ArrayList<String> respondingRocs = new ArrayList<>(10);

    /** List of state of responding fake ROCs. */
    private ArrayList<Integer> respondingStates = new ArrayList<>(10);

    /** Instantiate a callback to use in subscription. */
    private myCallback callback = new myCallback();


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

System.out.println("All ROCs reporting");
                // If we're here we got the proper responses

                // If all ROCs have received the END command,
                // state = "gotEnd", then tell ROCs to finish up.
                if (!respondingStates.contains(0)) {
System.out.println("  TS mod: all ROCs got end cmd, send 1 to tell ROCS to stop");
                    // Tell ROCs to stop sending events
                    message.setUserInt(1);
                }
                else {
                    // Tell ROCs to send next batch of events
System.out.println("  TS mod: all ROCs, send next batch (0)");
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


    /**
     * Constructor.
     * @param name name of module
     * @param attributeMap map containing attributes of module
     * @param emu Emu this module belongs to.
     */
    public TsSimulation(String name, Map<String, String> attributeMap, Emu emu) {
        super(name, attributeMap, emu);

        // Find out which Rocs need syncs
        int rocCount = 2;
        String rocName = "r1";
        while (true) {
            String roc = attributeMap.get(rocName);
            if (roc == null) break;
            System.out.println("  TS mod: adding roc " + rocName);
            expectedRocs.add(rocName);
            rocName = "r" + rocCount++;
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
        moduleState = CODAState.ACTIVE;
        paused = false;
    }


    /** {@inheritDoc} */
    public void prestart() {
        if (debug) System.out.println("  TS: prestart");

        // Get platform's cMsg server
        coda = emu.getCmsgPortal().getCmsgServer();

        try {
            // Unsubscribe from any previous subscription
            if (sub != null) coda.unsubscribe(sub);
        }
        catch (cMsgException e) {/* ignore any error */}

        try {
//System.out.println("  TS mod: subscribe to sub = " + subjectIn + ", typ = *");
            // Subscribe to get ROCs' sync-related messages
            sub = coda.subscribe(subjectIn, "*", callback, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        moduleState = CODAState.PAUSED;
        paused = false;
    }


    /** {@inheritDoc} */
    public void reset() {
        if (debug) System.out.println("  TS mod: reset()");

        // Unsubscribe
        try { if (coda != null && sub != null) coda.unsubscribe(sub); }
        catch (cMsgException e) {}

        moduleState = CODAState.CONFIGURED;
        paused = false;
    }


    /** {@inheritDoc} */
    public void end() throws CmdExecException {
        if (debug) System.out.println("  TS mod: end()");

        moduleState = CODAState.DOWNLOADED;
        paused = false;
    }

}
