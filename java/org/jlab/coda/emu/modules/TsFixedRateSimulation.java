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
import java.util.concurrent.CountDownLatch;

/**
 * This class is used when running simulated, emu-based ROCS.
 * This must be the master ROC in order for synchronization to work properly.
 * All simulated ROCs need to be the same priority level so that they all
 * receive the RC commands - END in particular - at the same time.
 * This module takes no input and has no output. Instead it provides
 * synchronization through cMsg commands handled by the RC platform's
 * cMsg server.<p>
 *
 * It differs from TsSimulation in that it establishes a fixed data rate to
 * the EB accepting inputs from a particular group of ROCs.
 *
 * @author timmer
 * 1/29/2015
 */
public class TsFixedRateSimulation extends ModuleAdapter {

    /** Subscribe the this subject to get sync messages from fake ROCs. */
    private String syncSubjectIn = "syncFromRoc";

    /** Subscribe the this subject to get init messages from fake ROCs. */
    private String initSubjectIn = "initFromRoc";

    /** Handle to subscription to get sync messages from fake ROCs. */
    private cMsgSubscriptionHandle syncSub;

    /** Handle to subscription to get init messages from fake ROCs. */
    private cMsgSubscriptionHandle initSub;

    /** Synchronization message to send to fake ROCs. */
    private cMsgMessage syncMessage;

    /** Initialization message to send to fake ROCs. */
    private cMsgMessage initMessage;

    /** cMsg name server. */
    private cMsg coda;

    /** Number of incoming ROC messages per statistics interval (3 sec). */
    private int count;

    /** Debug printout on/off. */
    private boolean debug = true;

    /** List of expected fake ROCs we need to communicate with. */
    private ArrayList<String> expectedRocs = new ArrayList<>(20);

    /** List of responding fake ROCs to initialization. */
    private ArrayList<String> initRespondingRocs = new ArrayList<>(20);

    /** List of the size of events of responding fake ROCs. */
    private ArrayList<Integer> eventSizes = new ArrayList<>(20);

    /** List of responding fake ROCs to sync. */
    private ArrayList<String> syncRespondingRocs = new ArrayList<>(20);

    /** List of state of responding fake ROCs to sync. */
    private ArrayList<Integer> syncRespondingStates = new ArrayList<>(20);

    /** Instantiate a sync callback to use in subscription. */
    private SyncCallback syncCallback = new SyncCallback();

    /** Instantiate an init callback to use in subscription. */
    private InitCallback initCallback = new InitCallback();




    /** Desired total data rate to ER in bytes/sec. */
    private long desiredTotalDataRate;

    /** Of responding ROCs, the largest of the sizes of an entangled block of events in bytes. */
    private int largestEntangledEventByteSize;

    /** Of responding ROCs, the average of the sizes of an entangled block of events in bytes. */
    private int avgEntangledEventByteSize;

    /** Total number of ROCs for hallD (actually 63, but I want round #s). */
    private int rocCount = 63;

    /** Number of 4MB buffers to send per sec from each ROC to reach desired total data rate to ER. */
    private double desiredBuffersPerSec;

    /** Don't let prestart finish until all ROCs report their event size. */
    private final CountDownLatch initLatch = new CountDownLatch(1);



    /**
     * This class defines the callback to be run when a message matching our subscription arrives.
     */
    private class SyncCallback extends cMsgCallbackAdapter {

        public void callback(cMsgMessage msg, Object userObject) {
            // We expect one message from each ROC
            // after each round of producing events.

            // Type = emu name
            syncRespondingRocs.add(msg.getType());
            // Int = 1 (got End cmd) or 0 (no End received)
            syncRespondingStates.add(msg.getUserInt());
//System.out.println("  TS mod: got type = " + msg.getType() + " & user int = " + msg.getUserInt());

            // If we got the expected number of responses, see if
            // we got them from the expected ROCs.
            if (syncRespondingRocs.size() == expectedRocs.size()) {
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
                if (!syncRespondingStates.contains(0)) {
System.out.println("  TS mod: all ROCs got end cmd");
                    // Tell ROCs to stop sending buffers
                    syncMessage.setUserInt(1);
                }
                else {
                    // Tell ROCs to send next batch of buffers
                    syncMessage.setUserInt(0);
                }

                try {
                    // This message will reach all fake ROCs
                    coda.send(syncMessage);
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }

                syncRespondingRocs.clear();
                syncRespondingStates.clear();
                count++;
            }
        }
    }


    /**
     * This class defines the callback to be run when initializing things before a run.
     */
    private class InitCallback extends cMsgCallbackAdapter {

        public void callback(cMsgMessage msg, Object userObject) {
            // We expect one message from each ROC for initialization at prestart

            // Type = emu name
            initRespondingRocs.add(msg.getType());
            // Total size of each generated event in bytes
            eventSizes.add(msg.getUserInt());
System.out.println("  TS mod: got roc = " + msg.getType() + ", event size = " + msg.getUserInt());

            // If we got the expected number of responses ...
            if (initRespondingRocs.size() == expectedRocs.size()) {
                int totalEventSize = 0;
                largestEntangledEventByteSize = -1;

                for (int size : eventSizes) {
                    if (size > largestEntangledEventByteSize) {
                        largestEntangledEventByteSize = size;
                    }
                    totalEventSize += size;
                }

                avgEntangledEventByteSize = totalEventSize / expectedRocs.size();
// TODO: Assumes 4MB buffers!!!
                // How many events can be fit in a 4MB buffer?
                // Take block header and footer into account.
                int eventsPerBuffer = (4000000 - 64) / largestEntangledEventByteSize;

                // Number of (~4MB) buffers to send per sec from each ROC to reach target total data rate:
                //
                // Total Bytes /sec = (bufs/sec/roc)(#rocs)(bytes/buf) =
                //                  = (bufs/sec/roc)(#rocs)(evSize)(events/buf)
                // bufs/sec/roc     = (total bytes/sec) / ((#rocs)(evSize)(events/buf))
                //
                //
                // bytes/sec/roc = (bufs/sec/roc)(bytes/buf) = (bufs/sec/roc)(events/buf)(bytes/event)

                desiredBuffersPerSec = (double)desiredTotalDataRate / (rocCount * avgEntangledEventByteSize * eventsPerBuffer);
                double bytesPerSecPerRoc = desiredBuffersPerSec * eventsPerBuffer * avgEntangledEventByteSize;

System.out.println("All ROCs reporting event sizes, largest ev size = " + largestEntangledEventByteSize +
                   ", avg size = " + avgEntangledEventByteSize +
                   ", events/buf = " + eventsPerBuffer + ", bufs/sec = " + desiredBuffersPerSec +
                   ", *** Avg Data Rate / Roc = " + bytesPerSecPerRoc + " ***");


                // Send back how many events to write in each 4MB buffer.
                initMessage.setUserInt(eventsPerBuffer);
                try {
                    cMsgPayloadItem item = new cMsgPayloadItem("bufsPerSec", desiredBuffersPerSec);
                    initMessage.addPayloadItem(item);
                }
                catch (cMsgException e) {}

                try {
                    // This message will reach all fake ROCs
                    coda.send(initMessage);
                }
                catch (cMsgException e) {
                    e.printStackTrace();
                }

                // Allow GO to finish
                initLatch.countDown();
            }
        }
    }



    // ---------------------------------------------------


    /** Constructor. */
    public TsFixedRateSimulation(String name, Map<String, String> attributeMap, Emu emu) {

        super(name, attributeMap, emu);

        // Desired total data rate to ER.
        desiredTotalDataRate = 2400000000L;  // 2.1 GB/s

        try { desiredTotalDataRate = Long.parseLong(attributeMap.get("dataRate")); }
        catch (NumberFormatException e) {}

        if (desiredTotalDataRate < 0) desiredTotalDataRate = 500000000; // 500 MB/s
        else if (desiredTotalDataRate > 3000000000L) desiredTotalDataRate = 3000000000L;  // 3 GB/s

        // Find out which Rocs need syncs
        rocCount = 0;
        int counter = 2;
        String rocName = "r1";
        while (true) {
            String roc = attributeMap.get(rocName);
            if (roc == null) break;
            System.out.println("  TS mod: adding roc " + rocName);
            expectedRocs.add(rocName);
            rocName = "r" + counter++;
            // Total number of ROCs sending data in this configuration
            rocCount++;
        }


        // Prepare sync message to send to all fake ROCs
        syncMessage = new cMsgMessage();
        syncMessage.setSubject("sync");
        syncMessage.setType("ROC");
        try {  syncMessage.setHistoryLengthMax(0); }
        catch (cMsgException e) {/* never happen */}

        // Prepare init message to send to all fake ROCs
        initMessage = new cMsgMessage();
        initMessage.setSubject("init");
        initMessage.setType("ROC");
        try {  initMessage.setHistoryLengthMax(0); }
        catch (cMsgException e) {/* never happen */}
    }


    /** {@inheritDoc} */
    public void go() {
        // Since TS gets prestarted AFTER the ROC, need to wait in GO
        // to get init msg sent by ROC in its PRESTART.
        try {
            initLatch.await();
        }
        catch (InterruptedException e) {}

        moduleState = CODAState.ACTIVE;
        paused = false;
    }


    /** {@inheritDoc} */
    public void prestart() {
        debug = true;
        if (debug) System.out.println("  TS mod: prestart");

        // Get platform's cMsg server
        coda = emu.getCmsgPortal().getCmsgServer();

        try {
            if (initSub != null) coda.unsubscribe(initSub);
        }
        catch (cMsgException e) {/* ignore any error */}

        try {
            // Unsubscribe from any previous subscription
            if (syncSub != null) coda.unsubscribe(syncSub);
        }
        catch (cMsgException e) {/* ignore any error */}

        try {
System.out.println("  TS mod: subscribe to sub = " + initSubjectIn + ", typ = *");
            // Subscribe to get ROCs' sync-related messages
            initSub = coda.subscribe(initSubjectIn, "*", initCallback, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
System.out.println("  TS mod: subscribe to sub = " + syncSubjectIn + ", typ = *");
            // Subscribe to get ROCs' sync-related messages
            syncSub = coda.subscribe(syncSubjectIn, "*", syncCallback, null);
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
        try { if (coda != null && syncSub != null) coda.unsubscribe(syncSub); }
        catch (cMsgException e) {}

        // Unsubscribe
        try { if (coda != null && initSub != null) coda.unsubscribe(initSub); }
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
