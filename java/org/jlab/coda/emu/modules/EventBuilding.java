/*
 * Copyright (c) 2010, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.modules;

import org.jlab.coda.emu.*;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <pre><code>
 * Input Channels
 * (evio bank Qs):       IC1      IC2 ...  ICN
 *                        |        |        |
 *                        V        V        V
 * QFiller Threads:      QF1      QF2      QFN
 *  Grab evio bank        |        |        |
 *  & check for           |        |        |
 *  good event type       |        |        |
 *                        V        V        V
 * Payload Bank Qs:      PBQ1     PBQ2     PBQN
 *   1 for each           | \ \   /      /       _
 *  input channel         |  \ \/      /       /
 *                        |   \/\    /        /
 *                        |  / \  \/         <   Crossbar of
 *                        | /   \/  \         \  Connections
 *                        |/   / \    \        \
 *                        V  /    \     \       \
 *                        |/       \      \      -
 *                        V        V       V
 *  BuildingThreads:     BT1      BT2      BTM
 *  Grab 1 payload        |        |        |
 *  bank from each        |        |        |
 *  payload Bank Q,       |        |        |
 *  build event, &        |        |        |
 *  place (IN ORDER)      |        |        |
 *   in module's           \       |       /
 *  output channels         \      |      /
 *                           V     V     V
 * Output Channel(s):          OC1 - OCZ
 *
 *
 *  M != N in general,= 3 by default
 *  DTR = Data Transport Record
 * </code></pre><p>
 *
 * This class is the event building module. It is a multithreaded module which has 1
 * QFiller thread per input channel. Each of these threads exists for the sole purpose
 * of taking Evio banks off of 1 input channel, seeing if it is in the proper format
 * (ROC Raw, Physics, Control, User) and placing those banks into 1 payload bank queue.
 * At that point the BuildingThreads - of which there may be any number - take turns
 * at grabbing one bank from each payload bank queue (and therefore input channel),
 * building them into a single event, and placing it (in order) into one of
 * the output channels (by round robin if more than one or on all output channels
 * if wrapping a control event).<p>
 *
 * NOTE: QFiller threads ignore any banks that are not in the proper format. BuildingThread
 * objects immediately pass along any User events to their output queues. Any Control events
 * they find must appear on each payload queue in the same position. If not, an exception
 * is thrown. If so, the Control event is passed along to all output queues.
 * Finally, the Building threads place any User events in the first output
 * channel. Control & User events are not part of the round-robin output to each channel in turn.
 * If no output channels are defined in the config file, this module discards all events.
 */
public class EventBuilding extends ModuleAdapter {


    /** Keep track of the number of records built in this event builder. Reset at prestart. */
    private volatile int ebRecordId;

    /**
     * There is one waiting list per output channel -
     * each of which stores built events until their turn to go over the
     * output channel has arrived.
     */
    private PriorityBlockingQueue<PayloadBank> waitingLists[];

    /** Container for queues used to hold QueueItems taken from Data Transport channels. */
    private LinkedList<PayloadQueue<PayloadBank>> payloadQueues =
            new LinkedList<PayloadQueue<PayloadBank>>();

    /** Each payloadBankQueue has this max size. */
    private final int payloadBankQueueSize = 500;

    /** The number of BuildingThread objects. */
    private int buildingThreadCount;

    /** Container for threads used to build events. */
    private LinkedList<BuildingThread> buildingThreadList = new LinkedList<BuildingThread>();

    /**
     * Array of threads used to take Evio data from
     * input channels, dissect them, and place resulting payload
     * banks onto payload queues.
     */
    private Thread qFillers[];

    /** Lock to ensure that a BuildingThread grabs the same positioned event from each Q.  */
    private ReentrantLock getLock = new ReentrantLock();

    /** END event detected by one of the building threads. */
    private volatile boolean haveEndEvent;

    /** Maximum time in milliseconds to wait when commanded to END but no END event received. */
    private long endingTimeLimit = 30000;

    /** The number of the experimental run. */
    private int runNumber;

    /** The number of the experimental run's configuration. */
    private int runTypeId;

    /** The number of the event to be assigned to that which is built next. */
    private long eventNumber;

    /** The eventNumber value when the last sync event arrived. */
    private long eventNumberAtLastSync;


    // ---------------------------------------------------

    /** If <code>true</code>, then print sizes of various queues for debugging. */
    private boolean printQSizes;

    /**
     * If <code>true</code>, then each event building thread can put its built event
     * onto a waiting list if it is not next in line for the Q. That allows it
     * to continue building events instead of waiting for another thread to
     * build the event that is next in line.
     */
    private boolean useOutputWaitingList = false;

    /** If <code>true</code>, get debug print out. */
    private boolean debug = false;

    // ---------------------------------------------------
    // Configuration parameters
    // ---------------------------------------------------

    /** If <code>true</code>, check timestamps for consistency. */
    private boolean checkTimestamps;

    /**
     * The maximum difference in ticks for timestamps for a single event before
     * an error condition is flagged. Only used if {@link #checkTimestamps} is
     * <code>true</code>.
     */
    private int timestampSlop;

    /**
     * If true, swap data if necessary when building events.
     * Assume data is all 32 bit integers.
     */
    private boolean swapData;

    /** If true, include run number & type in built trigger bank. */
    private boolean includeRunData;

    /** If true, do not include empty roc-specific segments in trigger bank. */
    private boolean sparsify;

    // ---------------------------------------------------


    /** Number of output channels. */
    private int outputChannelCount;

    /** Index to help cycle through output channels sequentially. */
    private int outputChannelIndex;

    /**
     * Array of locks - one for each output channel -
     * so building threads can synchronize their output.
     */
    private Object locks[];

    /**
     * Array of input orders - one for each output channel.
     * Keeps track of a built event's output order for a
     * particular output channel.
     */
    private int[] inputOrders;

    /**
     * Array of output orders - one for each output channel.
     * Keeps track of which built event is next to be output
     * on a particular output channel.
     */
    private int[] outputOrders;



    /**
     * Constructor creates a new EventBuilding instance.
     *
     * @param name         name of module
     * @param attributeMap map containing attributes of module
     * @param emu          emu which created this module
     */
    public EventBuilding(String name, Map<String, String> attributeMap, Emu emu) {

        super(name, attributeMap, emu);

        // default to 3 event building threads
        buildingThreadCount = 3;
        try {
            buildingThreadCount = Integer.parseInt(attributeMap.get("threads"));
            if (buildingThreadCount < 1)  buildingThreadCount = 1;
            if (buildingThreadCount > 10) buildingThreadCount = 10;
        }
        catch (NumberFormatException e) {}
System.out.println("EventBuilding constr: " + buildingThreadCount +
                           " number of event building threads");

        // Does this module accurately represent the whole EMU's stats?
        String str = attributeMap.get("statistics");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                representStatistics = true;
            }
        }

        // default is to swap data if necessary -
        // assume 32 bit ints
        swapData = true;
        str = attributeMap.get("swap");
        if (str != null) {
            if (str.equalsIgnoreCase("false") ||
                str.equalsIgnoreCase("off")   ||
                str.equalsIgnoreCase("no"))   {
                swapData = false;
            }
        }

        // default is NOT to include run number & type in built trigger bank
        str = attributeMap.get("runData");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("in")   ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                includeRunData = true;
            }
        }

        // default is NOT to sparsify roc-specific segments in trigger bank
        sparsify = false;
        str = attributeMap.get("sparsify");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                sparsify = true;
            }
        }

        // default is to check timestamp consistency
        checkTimestamps = true;
        str = attributeMap.get("tsCheck");
        if (str != null) {
            if (str.equalsIgnoreCase("false") ||
                str.equalsIgnoreCase("off")   ||
                str.equalsIgnoreCase("no"))   {
                checkTimestamps = false;
            }
        }

        // default to 2 clock ticks
        timestampSlop = 2;
        try {
            timestampSlop = Integer.parseInt(attributeMap.get("tsSlop"));
        }
        catch (NumberFormatException e) {}
    }


    /**
     * This class takes items from a queue (an input channel, eg. ROC),
     * and dumps them. Used for testing purposes only, in place of QFiller threads.
     */
    private class QfillerDump extends Thread {

        BlockingQueue<QueueItem> channelQ;

        QfillerDump(PayloadQueue<PayloadBank> payloadBankQ, BlockingQueue<QueueItem> channelQ) {
            this.channelQ = channelQ;
        }

        @Override
        public void run() {
            while (state == CODAState.ACTIVE || paused) {
                try {
                    while (state == CODAState.ACTIVE || paused) {
                        // block waiting for the next data from ROC.
                        channelQ.take();  // blocks, throws InterruptedException
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }


    /**
     * This class takes QueueItems from a queue (an input channel, eg. ROC),
     * and places the them in a payload bank queue associated with that channel.
     * All other types of events are ignored.
     * Nothing in this class depends on single event mode status.
     */
    private class Qfiller extends Thread {

        BlockingQueue<QueueItem> channelQ;
        PayloadQueue<PayloadBank> payloadBankQ;

        Qfiller(PayloadQueue<PayloadBank> payloadBankQ, BlockingQueue<QueueItem> channelQ) {
            this.channelQ = channelQ;
            this.payloadBankQ = payloadBankQ;
        }

        @Override
        public void run() {
            QueueItem qItem;
            PayloadBank pBank;

            while (state == CODAState.ACTIVE || paused) {
                try {
                    while (state == CODAState.ACTIVE || paused) {
                        // Block waiting for the next bank from ROC
                        pBank = (PayloadBank) channelQ.take();   // blocks, throws InterruptedException
                        // Check this bank's format. If bad, ignore it
                        Evio.checkPayload(pBank, payloadBankQ);
                    }
                } catch (EmuException e) {
                    // EmuException from Evio.checkPayloadBank() if
                    // Roc raw or physics banks are in the wrong format
if (debug) System.out.println("Qfiller: Roc raw or physics event in wrong format");
                    errorMsg.compareAndSet(null, "Roc raw or physics banks are in the wrong format");
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();
                    return;
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }


    /**
     * This method is called by a build thread and is used to place
     * a bank onto the queue of an output channel. If the event is
     * not next in line for the Q, it can be put in a waiting list.
     *
     * @param bankOut the built/control/user event to place on output channel queue
     * @throws InterruptedException if wait, put, or take interrupted
     */
    private void bankToOutputChannel(PayloadBank bankOut)
                    throws InterruptedException {

        // Have output channels?
        if (outputChannelCount < 1) {
            return;
        }

        PayloadBank bank;
        EventOrder evOrder;
        EventOrder eo = (EventOrder)bankOut.getAttachment();

        synchronized (eo.lock) {
            if (!useOutputWaitingList) {
                // Is the bank we grabbed next to be output? If not, wait.
                while (eo.inputOrder != outputOrders[eo.index]) {
                    eo.lock.wait();
                }
                // Place bank on output channel
//System.out.println("Put bank on output channel");
                eo.outputChannel.getQueue().put(bankOut);
                outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
                eo.lock.notifyAll();
            }
            // else if we're using waiting lists
            else {
                // Is the bank we grabbed next to be output?
                // If not, put in waiting list and return.
                if (eo.inputOrder != outputOrders[eo.index]) {
                    bankOut.setAttachment(eo);
                    waitingLists[eo.index].add(bankOut);

                    // If the waiting list gets too big, just wait here
                    if (waitingLists[eo.index].size() > 9) {
                        eo.lock.wait();
                    }
if (debug) {
    System.out.println("out of order = " + eo.inputOrder);
    System.out.println("waiting list = ");
    for (PayloadBank bk : waitingLists[eo.index]) {
        System.out.println("" + ((EventOrder)bk.getAttachment()).inputOrder);
    }
}
                    return;
                }

                // Place bank on output channel
                eo.outputChannel.getQueue().put(bankOut);
                outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
//if (debug) System.out.println("placing = " + eo.inputOrder);

                // Take a look on the waiting list without removing ...
                bank = waitingLists[eo.index].peek();
                while (bank != null) {
                    evOrder = (EventOrder) bank.getAttachment();
                    // If it's not next to be output, skip this waiting list
                    if (evOrder.inputOrder != outputOrders[eo.index]) {
                        break;
                    }
                    // Remove from waiting list permanently
                    bank = waitingLists[eo.index].take();
                    // Place bank on output channel
                    eo.outputChannel.getQueue().put(bank);
                    outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
                    bank = waitingLists[eo.index].peek();
//if (debug) System.out.println("placing = " + evOrder.inputOrder);
                }
                eo.lock.notifyAll();
            }
        }

if (debug && printQSizes) {
    int size = eo.outputChannel.getQueue().size();
    if (size > 400 && size % 100 == 0) System.out.println("output chan: " + size);
}

    }


    /**
     * This method is called by a build thread and is used to place
     * a list of banks onto the queue of an output channel. If the events
     * are not next in line for the Q, they can be put in a waiting list.
     *
     * @param banksOut a list of the built/control/user events to place on output channel queue
     * @throws InterruptedException if wait, put, or take interrupted
     */
    private void bankToOutputChannel(List<PayloadBank> banksOut)
                    throws InterruptedException {

        // Have output channels? Have output banks?
        if (outputChannelCount < 1 || banksOut.size() < 1) {
            return;
        }

        PayloadBank bank;
        EventOrder evOrder;
        EventOrder eo = (EventOrder)banksOut.get(0).getAttachment();

        synchronized (eo.lock) {
            if (!useOutputWaitingList) {
                // Is the bank we grabbed next to be output? If not, wait.
                while (eo.inputOrder != outputOrders[eo.index]) {
                    eo.lock.wait();
                }
                // Place banks on output channel
//System.out.println("Put banks on output channel");
                for (PayloadBank bBank : banksOut) {
                    eo.outputChannel.getQueue().put(bBank);
                }
                outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
                eo.lock.notifyAll();
            }
            // else if we're using waiting lists
            else {
                // Is the bank we grabbed next to be output?
                // If not, put in waiting list and return.
                if (eo.inputOrder != outputOrders[eo.index]) {
                    for (PayloadBank bBank : banksOut) {
                        bBank.setAttachment(eo);
                        waitingLists[eo.index].add(bBank);
                    }

                    // If the waiting list gets too big, just wait here
                    if (waitingLists[eo.index].size() > 9) {
                        eo.lock.wait();
                    }
                    return;
                }

                // Place banks on output channel
                for (PayloadBank bBank : banksOut) {
                    eo.outputChannel.getQueue().put(bBank);
                }
                outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;

                // Take a look on the waiting list without removing ...
                bank = waitingLists[eo.index].peek();
                while (bank != null) {
                    evOrder = (EventOrder) bank.getAttachment();
                    // If it's not next to be output, skip this waiting list
                    if (evOrder.inputOrder != outputOrders[eo.index]) {
                        break;
                    }
                    // Remove from waiting list permanently
                    bank = waitingLists[eo.index].take();
                    // Place banks on output channel
                    eo.outputChannel.getQueue().put(bank);
                    outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
                    bank = waitingLists[eo.index].peek();
                }
                eo.lock.notifyAll();
            }
        }

    }


    /**
     * This thread is started by the GO transition and runs while the state of the module is ACTIVE.
     * <p/>
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * selects an output by taking the next one from a simple iterator. The thread then pulls
     * one DataBank off each input DataChannel and stores them in an ArrayList.
     * <p/>
     * An empty DataBank big enough to store all of the banks pulled off the inputs is created.
     * The incoming banks from the ArrayList are built into a new bank.
     * The count of outgoing banks and the count of data words are incremented.
     * If the Module has an output, the bank of banks is put on the output DataChannel.
     */
    class BuildingThread extends Thread {

        BuildingThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        BuildingThread() {
            super();
        }

        @Override
        public void run() {

            boolean runChecks = true;

            // initialize
            int totalNumberEvents=1;
            long firstEventNumber=1;
            boolean nonFatalError;
            boolean haveControlEvents;
            boolean havePhysicsEvents;
            boolean gotFirstBuildEvent;
            EventType eventType;
            EvioEvent combinedTrigger;
            PayloadBank physicsEvent;
            PayloadBank[] buildingBanks = new PayloadBank[inputChannels.size()];
            EventOrder[] controlEventOrders = new EventOrder[outputChannelCount];
            EventBuilder builder = new EventBuilder(0, DataType.BANK, 0); // this event not used, just need a builder
            LinkedList<PayloadBank> userEventList = new LinkedList<PayloadBank>();

            int myInputOrder = -1;
            int myOutputChannelIndex = 0;
            Object myOutputLock = null;
            DataChannel myOutputChannel = null;

            int endEventCount;
            int controlEventCount;


            while (state == CODAState.ACTIVE || paused) {

                try {
                    nonFatalError = false;

                    // The payload bank queues are filled by the QFiller thread.

                    // Here we have what we need to build:
                    // ROC raw events from all ROCs (or partially built events from
                    // each contributing EB) each with sequential record IDs.
                    // However, there are also user and control events on queues.

                    // Put null into buildingBanks array elements
                    Arrays.fill(buildingBanks, null);

                    // Set variables/flags
                    haveControlEvents  = false;
                    gotFirstBuildEvent = false;
                    endEventCount      = 0;
                    controlEventCount  = 0;

                    // Fill array with actual banks
                    try {
                        // grab lock so we get the very next bank from each channel
                        getLock.lock();

                        // Grab one non-user bank from each channel.
                        // This algorithm retains the proper order of any user events.
                        for (int i=0; i < payloadQueues.size(); i++) {

                            // Loop until we get event which is NOT a user event
                            while (true) {

                                // will BLOCK here waiting for payload bank if none available
                                buildingBanks[i] = payloadQueues.get(i).take();

                                eventType = buildingBanks[i].getEventType();

                                // If event needs to be built ...
                                if (!eventType.isControl() && !eventType.isUser()) {
                                    // One-time init stuff for a group of
                                    // records that will be built together.
                                    if (!gotFirstBuildEvent) {
                                        // Set flag
                                        gotFirstBuildEvent = true;

                                        // Find the total # of events
                                        totalNumberEvents = buildingBanks[i].getHeader().getNumber();

                                        // Store first event number
                                        firstEventNumber = eventNumber;

                                        // Calculate event number for next time through
                                        eventNumber += totalNumberEvents;

                                        if (outputChannelCount > 0) {
                                            // We can already figure out which output channel it should go to.
                                            // We simply need to cycle through all the output channels.
                                            myOutputChannel = outputChannels.get(outputChannelIndex);
                                            myOutputChannelIndex = outputChannelIndex;
                                            outputChannelIndex = ++outputChannelIndex % outputChannelCount;

                                            // Order in which this will be placed into its output channel.
                                            myInputOrder = inputOrders[myOutputChannelIndex];
                                            myOutputLock = locks[myOutputChannelIndex];

                                            // Keep track of the next slot in this output channel.
                                            inputOrders[myOutputChannelIndex] =
                                                    ++inputOrders[myOutputChannelIndex] % Integer.MAX_VALUE;
                                        }
                                    }

                                    // Go to next input channel
                                    break;
                                }

                                // Check if this is a user event.
                                // If so, store it in a list and get another.
                                if (eventType.isUser()) {
if (debug) System.out.println("BuildingThread: Got user event");
                                    EventOrder eo = null;

                                    // User events are thrown away if no output channels
                                    // since this event builder does nothing with them.
                                    if (outputChannelCount < 1) {
                                        continue;
                                    }

                                    // User events go into 1 - the first - channel
                                    myOutputChannel = outputChannels.get(0);
                                    myOutputChannelIndex = 0;

                                    // Order in which this will be placed into its output channel.
                                    myInputOrder = inputOrders[myOutputChannelIndex];
                                    myOutputLock = locks[myOutputChannelIndex];

                                    // Keep track of the next slot in this output channel.
                                    inputOrders[myOutputChannelIndex] =
                                            ++inputOrders[myOutputChannelIndex] % Integer.MAX_VALUE;

                                    eo = new EventOrder();
                                    eo.index = myOutputChannelIndex;
                                    eo.outputChannel = myOutputChannel;
                                    eo.lock = myOutputLock;
                                    eo.inputOrder = myInputOrder;

                                    // Store its output order info
                                    buildingBanks[i].setAttachment(eo);
                                    // Stick it in a list
                                    userEventList.add(buildingBanks[i]);
                                    // Since we got a user event, try again from the
                                    // same input channel until we get one that isn't.
                                    continue;
                                }

                                // If we're here, we've got a CONTROL event. Count them.
                                haveControlEvents = true;
                                controlEventCount++;

                                // How many are END events?
                                if (buildingBanks[i].getControlType().isEnd()) endEventCount++;

                                // We want one EventOrder object for each output channel
                                // since we want one control event placed on each.
                                if (!gotFirstBuildEvent) {
                                    // Set flag
                                    gotFirstBuildEvent = true;

                                    // Loop through the output channels and get
                                    // them ready to accept a control event.
                                    for (int j=0; j < outputChannelCount; j++) {
                                        // Output channel it should go to.
                                        myOutputChannel = outputChannels.get(outputChannelIndex);
                                        myOutputChannelIndex = outputChannelIndex;
                                        outputChannelIndex = ++outputChannelIndex % outputChannelCount;

                                        // Order in which this will be placed into its output channel.
                                        myInputOrder = inputOrders[myOutputChannelIndex];
                                        myOutputLock = locks[myOutputChannelIndex];

                                        // Keep track of the next slot in this output channel.
                                        inputOrders[myOutputChannelIndex] =
                                                ++inputOrders[myOutputChannelIndex] % Integer.MAX_VALUE;

                                        EventOrder eo = new EventOrder();
                                        eo.index = myOutputChannelIndex;
                                        eo.outputChannel = myOutputChannel;
                                        eo.lock = myOutputLock;
                                        eo.inputOrder = myInputOrder;

                                        // Store control event output order info in array
                                        controlEventOrders[j] = eo;
                                    }
                                }

                                // Go to next input channel
                                break;
                            }
                        }

                        // Do some initial CONTROL events checks here, more later
                        if (haveControlEvents) {
                            // Do a check on END events before we release the mutex
                            if (endEventCount > 0) {
                                // If there is at least one end event, then we need to
                                // end everything. If not all channels have an END event,
                                // see if we can find them. Then clear all input channels
                                // as there should be nothing coming after an END event.
                                //
                                // The clearing is done so other building threads have nothing
                                // to build when we release the mutex - even if we have
                                // a mismatch. Avoids unnecessary generation of errors.
                                //
                                // If all channels have an END, we can end normally
                                // with a warning about the mismatch in number of events.
                                // If some do NOT have an END, then stop with major error.


                                // If not all banks are END events
                                if (endEventCount != buildingBanks.length) {

                                    int finalEndEventCount = endEventCount;

                                    // Look through Q's to see if we can find the rest ...
                                    for (int i=0; i < payloadQueues.size(); i++) {
                                        PayloadBank pBank;
                                        EventType   eType = buildingBanks[i].getEventType();
                                        ControlType cType = buildingBanks[i].getControlType();

                                        if (cType != null)  {
                                            System.out.println("got " + cType + " event from " + buildingBanks[i].getSourceName());
                                        }
                                        else {
                                            System.out.println("got " + eType + " event from " + buildingBanks[i].getSourceName());
                                        }

                                        // If this channel doesn't have an END, try finding it somewhere in Q
                                        if (cType != ControlType.END) {
                                            int offset = 0;
                                            // Loop through all events on this channel
                                            while ( (pBank = payloadQueues.get(i).poll()) != null) {
                                                offset++;
                                                if (pBank.getControlType() == ControlType.END) {
System.out.println("got END from " + buildingBanks[i].getSourceName() +
                   ", back " + offset + " places in Q");
                                                    finalEndEventCount++;
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    // If we still can't find all ENDs, throw exception - major error
                                    if (finalEndEventCount!= buildingBanks.length) {
                                        throw new EmuException("only " + finalEndEventCount + " ENDs for " +
                                                buildingBanks.length + " channels");
                                    }

                                    // If we're here, we've found all ENDs, continue on with warning ...
                                    nonFatalError = true;
if (true) System.out.println("Have all ENDs, but differing # of physics events in channels");
                                }

                                // Clear all channels' Q's
                                for (int i=0; i < payloadQueues.size(); i++) {
                                    payloadQueues.get(i).clear();
                                }
                            }

                            // If no ENDs, do a quick check on the # of CONTROL events
                            else if (controlEventCount !=  buildingBanks.length) {
                                throw new EmuException("have " + controlEventCount + " control events, but " +
                                        buildingBanks.length + " in channels");
                            }

                            // Prestart creates & clears payloadQueues below in execute()
                        }
                    }
                    finally {
                        getLock.unlock();
                    }

                    // store all channel & order info here
                    EventOrder evOrder = new EventOrder();
                    evOrder.index = myOutputChannelIndex;
                    evOrder.outputChannel = myOutputChannel;
                    evOrder.lock = myOutputLock;
                    evOrder.inputOrder = myInputOrder;

                    // If we have any user events, stick those on the Q first.
                    // Source may be any of the inputs.
                    if (userEventList.size() > 0) {
                        // Send each user event to all output channels
                        for (PayloadBank pBank : userEventList) {
                            EventOrder[] userEventOrders = (EventOrder[]) pBank.getAttachment();
                            pBank.setAttachment(userEventOrders[0]);
                            bankToOutputChannel(pBank);
                            for (int j=1; j < outputChannelCount; j++) {
                                // Copy user event
                                PayloadBank bb = new PayloadBank(pBank);
                                bb.setAttachment(userEventOrders[j]);
                                // Write to other output Q's
                                bankToOutputChannel(bb);
                            }
                        }
                        userEventList.clear();
                    }

                    // Check endianness & source IDs
                    if (runChecks) {
                        for (int i=0; i < payloadQueues.size(); i++) {
                            // Do NOT do any endian checking since we swap little
                            // endian data while building the physics event

                            // Check the source ID of this bank to see if it matches
                            // what should be coming over this channel.
                            if (buildingBanks[i].getSourceId() != payloadQueues.get(i).getSourceId()) {
if (debug) System.out.println("bank tag = " + buildingBanks[i].getSourceId());
if (debug) System.out.println("queue source id = " + payloadQueues.get(i).getSourceId());
                                nonFatalError = true;
                            }
                        }
                    }

if (debug && nonFatalError) System.out.println("\nERROR 1\n");

                    // If we have all control events ...
                    if (haveControlEvents) {
                        // Throw exception if inconsistent
                        Evio.gotConsistentControlEvents(buildingBanks, runNumber, runTypeId);

if (true) System.out.println("Have consistent CONTROL event(s)");

                        // Put 1 event on each output Q.
                        if (outputChannelCount > 0) {
                            // Take one of the control events and update
                            // it with the latest event builder data.
                            Evio.updateControlEvent(buildingBanks[0].getEvent(), runNumber,
                                                    runTypeId, (int)eventCountTotal,
                                                    (int)(eventNumber - eventNumberAtLastSync));

                            // We must copy the newly-updated control event
                            // and make sure one is placed on each output channel.
                            buildingBanks[0].setAttachment(controlEventOrders[0]);
                            bankToOutputChannel(buildingBanks[0]);
                            for (int j=1; j < outputChannelCount; j++) {
                                // Copy first control event
                                PayloadBank bb = new PayloadBank(buildingBanks[0]);
                                bb.setAttachment(controlEventOrders[j]);
                                // Write to other output Q's
                                bankToOutputChannel(bb);
                            }
                        }

                        // If this is a sync event, keep track of the next event # to be sent
                        if (Evio.isSyncEvent(buildingBanks[0].getEvent())) {
                            eventNumberAtLastSync = eventNumber;
                        }

                        // If it is an END event, interrupt other build threads
                        // then quit this one.
                        if (buildingBanks[0].getControlType() == ControlType.END) {
if (true) System.out.println("Found END event in build thread");
                            haveEndEvent = true;
                            endBuildAndQFillerThreads(this, false);
                            if (endCallback != null) endCallback.endWait();
                            return;
                        }

                        continue;
                    }

                    // At this point there are only physics or ROC raw events, which do we have?
                    havePhysicsEvents = buildingBanks[0].getEventType().isAnyPhysics();

                    // Check for identical syncs, uniqueness of ROC ids,
                    // single-event-mode, identical (physics or ROC raw) event types,
                    // and the same # of events in each bank
                    nonFatalError |= Evio.checkConsistency(buildingBanks);

                    // Are events in single event mode?
                    boolean eventsInSEM = buildingBanks[0].isSingleEventMode();

if (debug && nonFatalError) System.out.println("\nERROR 2\n");

                    //--------------------------------------------------------------------
                    // Build trigger bank, number of ROCs given by number of buildingBanks
                    //--------------------------------------------------------------------
                    // The tag will be finally set when this trigger bank is fully created
if (debug && havePhysicsEvents)
    System.out.println("BuildingThread: create combined trig w/ num (# Rocs) = " + buildingBanks.length);
                    combinedTrigger = new EvioEvent(CODATag.BUILT_TRIGGER_BANK.getValue(),
                                                    DataType.SEGMENT,
                                                    buildingBanks.length);
                    builder.setEvent(combinedTrigger);

                    // If building with Physics events ...
                    if (havePhysicsEvents) {
                        //-----------------------------------------------------------------------------------
                        // The actual number of rocs will replace num in combinedTrigger definition above
                        //-----------------------------------------------------------------------------------
                        // Combine the trigger banks of input events into one (same if single event mode)
if (debug) System.out.println("BuildingThread: create trig bank from built banks, sparsify = " + sparsify);
                        nonFatalError |= Evio.makeTriggerBankFromPhysics(buildingBanks, builder, id,
                                                                    runNumber, runTypeId, includeRunData,
                                                                    eventsInSEM, sparsify,
                                                                    checkTimestamps, timestampSlop);
                    }
                    // else if building with ROC raw records ...
                    else {
                        // If in single event mode, build trigger bank differently
                        if (eventsInSEM) {
                            // Create a trigger bank from data in Data Block banks
//if (debug) System.out.println("BuildingThread: create trigger bank in SEM");
                            nonFatalError |= Evio.makeTriggerBankFromSemRocRaw(buildingBanks, builder,
                                                                               id, firstEventNumber,
                                                                               runNumber, runTypeId,
                                                                               includeRunData,
                                                                               checkTimestamps,
                                                                               timestampSlop);
                        }
                        else {
                            // Combine the trigger banks of input events into one
if (debug) System.out.println("BuildingThread: create trigger bank from Rocs, sparsify = " + sparsify);
                            nonFatalError |= Evio.makeTriggerBankFromRocRaw(buildingBanks, builder,
                                                                            id, firstEventNumber,
                                                                            runNumber, runTypeId,
                                                                            includeRunData, sparsify,
                                                                            checkTimestamps,
                                                                            timestampSlop);
                        }
                    }

if (debug && nonFatalError) System.out.println("\nERROR 3\n");
                    // Print out trigger bank
//                    printEvent(combinedTrigger, "combined trigger");

                    // Check payload banks for non-fatal errors when
                    // extracting them onto the payload queues.
                    for (PayloadBank pBank : buildingBanks)  {
                        nonFatalError |= pBank.hasNonFatalBuildingError();
                    }

if (debug && nonFatalError) System.out.println("\nERROR 4\n");

                    // Create a physics event from payload banks and combined trigger bank
                    // First create the tag:
                    //   -if I'm a data concentrator or DC, the tag has 4 status bits and the ebId
                    //   -if I'm a primary event builder or PEB, the tag is 0xFF50 (or 0xFF51 if SEM)
                    //   -if I'm a secondary event builder or SEB, the tag is 0xFF70 (or 0xFF71 if SEM)
                    int tag;
                    CODAClass myClass = emu.getCodaClass();
                    switch (myClass) {
                        case SEB:
                            if (eventsInSEM) {
                                tag = CODATag.BUILT_BY_SEB_IN_SEM.getValue();
                            }
                            else {
                                tag = CODATag.BUILT_BY_SEB.getValue();
                            }
                            break;
                        case PEB:
                            if (eventsInSEM) {
                                tag = CODATag.BUILT_BY_PEB_IN_SEM.getValue();
                            }
                            else {
                                tag = CODATag.BUILT_BY_PEB.getValue();
                            }
                            break;
                        //case DC:
                        default:
                            tag = Evio.createCodaTag(buildingBanks[0].isSync(),
                                                 buildingBanks[0].hasError() || nonFatalError,
                                                 buildingBanks[0].getByteOrder() == ByteOrder.BIG_ENDIAN,
                                                 buildingBanks[0].isSingleEventMode(),
                                                 id);
//if (debug) System.out.println("tag = " + tag + ", is sync = " + buildingBanks[0].isSync() +
//                   ", has error = " + (buildingBanks[0].hasError() || nonFatalError) +
//                   ", is big endian = " + buildingBanks[0].getByteOrder() == ByteOrder.BIG_ENDIAN +
//                   ", is single mode = " + buildingBanks[0].isSingleEventMode());
                    }


                    physicsEvent = new PayloadBank(tag, DataType.BANK, totalNumberEvents);
                    builder.setEvent(physicsEvent.getEvent());
                    if (havePhysicsEvents) {
//if (debug) System.out.println("BuildingThread: build physics event with physics banks");
                        Evio.buildPhysicsEventWithPhysics(combinedTrigger, buildingBanks, builder);
                    }
                    else {
//if (debug) System.out.println("BuildingThread: build physics event with ROC raw banks");
                        Evio.buildPhysicsEventWithRocRaw(combinedTrigger, buildingBanks,
                                                         builder, eventsInSEM);
                    }

                    // setting header lengths done in Evio.buildPhysicsEventWith* methods
//                    physicsEvent.setAllHeaderLengths();

                    physicsEvent.setAttachment(evOrder); // store its input order info
                    physicsEvent.setEventType(EventType.PHYSICS);
                    physicsEvent.setEventCount(totalNumberEvents);
                    physicsEvent.setFirstEventNumber(firstEventNumber);

                    // Put it in the correct output channel.
                    //
                    // But wait! One more thing.
                    // We must check for the sync bits being set. If they are set,
                    // generate a SYNC event and write both the SYNC & PHYSICS events
                    // at the same time. Actually the physics must go first because
                    // it's attachment is needed.
                    if (!buildingBanks[0].isSync()) {
                        bankToOutputChannel(physicsEvent);
                    }
                    else {
                        try {
                            EvioEvent controlEvent = Evio.createControlEvent(ControlType.SYNC,
                                                                             0, 0, (int)eventCountTotal,
                                                    (int)(eventNumber - eventNumberAtLastSync));
                            PayloadBank controlPBank = new PayloadBank(controlEvent);
                            eventNumberAtLastSync = eventNumber;
                            ArrayList<PayloadBank> list = new ArrayList<PayloadBank>(2);
                            // Don't switch the order of the next 2 statements
                            list.add(physicsEvent);
                            list.add(controlPBank);
                            bankToOutputChannel(list);
                        }
                        catch (EvioException e) {/* never happen */}
                    }


//                    synchronized (EventBuilding.this) {
                        // stats  // TODO: protect since in multithreaded environs
                        eventCountTotal += totalNumberEvents;
                        wordCountTotal  += physicsEvent.getHeader().getLength() + 1;
//                    }
                }
                catch (EmuException e) {
if (debug) System.out.println("MAJOR ERROR building events");
                    // If we haven't yet set the cause of error, do so now & inform run control
                    errorMsg.compareAndSet(null, e.getMessage());

                    // set state
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();

                    e.printStackTrace();
                    return;
                }

                catch (InterruptedException e) {
if (debug) System.out.println("INTERRUPTED thread " + Thread.currentThread().getName());
                    return;
                }
            }
if (debug) System.out.println("Building thread is ending !!!");
        }

    }


//    private void printEvent(PayloadBank bank, String label) {
//        try {
//            StringWriter sw2 = new StringWriter(1000);
//            XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
//            bank.toXML(xmlWriter);
//            System.out.println("\n" + label + "\n" + sw2.toString());
//
//        }
//        catch (XMLStreamException e) {
//            e.printStackTrace();
//        }
//        PayloadBuffer bbuf = PayloadBuffer.allocate(2048);
//        bank.write(bbuf);
//        bbuf.flip();
//        for (int j = 0; j < bbuf.asIntBuffer().limit(); j++) {
//            System.out.println(bbuf.asIntBuffer().get(j));
//        }
//        System.out.println("\n\n\n");
//    }


    /**
     * End all build and QFiller threads because an END cmd or event came through.
     * The build thread calling this method is not interrupted.
     *
     * @param thisThread the build thread calling this method; if null,
     *                   all build & QFiller threads are interrupted
     * @param wait if <code>true</code> check if END event has arrived and
     *             if all the Qs are empty, if not, wait up to 1/2 second.
     */
    private void endBuildAndQFillerThreads(BuildingThread thisThread, boolean wait) {

        if (wait) {
            // Look to see if anything still on the payload bank or input channel Qs
            boolean haveUnprocessedEvents = false;
            long startTime = System.currentTimeMillis();

            for (int i=0; i < payloadQueues.size(); i++) {
                if (payloadQueues.get(i).size() +
                        inputChannels.get(i).getQueue().size() > 0) {
                    haveUnprocessedEvents = true;
                    break;
                }
            }

            // Wait up to endingTimeLimit millisec for events to
            // be processed & END event to arrive, then proceed
            while ((haveUnprocessedEvents || !haveEndEvent) &&
                   (System.currentTimeMillis() - startTime < endingTimeLimit)) {
                try {Thread.sleep(200);}
                catch (InterruptedException e) {}

                haveUnprocessedEvents = false;
                for (int i=0; i < payloadQueues.size(); i++) {
                    if (payloadQueues.get(i).size() +
                            inputChannels.get(i).getQueue().size() > 0) {
                        haveUnprocessedEvents = true;
                        break;
                    }
                }
            }

            if (haveUnprocessedEvents || !haveEndEvent) {
                if (debug) System.out.println("endBuildThreads: will end building/filling threads but no END event or Qs not empty !!!");
                state = CODAState.ERROR;
            }
        }

        // NOTE: EMU has a command executing thread which calls this EB module's execute
        // method which, in turn, calls this method when an END cmd is sent. In this case
        // all build threads will be interrupted in the following code.

        // Interrupt all Building threads except the one calling this method
        for (Thread thd : buildingThreadList) {
            if (thd == thisThread) continue;
            thd.interrupt();
        }

        // Interrupt all QFiller threads too
        if (qFillers != null) {
            for (Thread qf : qFillers) {
                qf.interrupt();
            }
        }
    }


    //---------------------------------------
    // State machine
    //---------------------------------------



    /** {@inheritDoc} */
    public void pause() {
        paused = true;
    }


    /** {@inheritDoc} */
    public void reset() {
        Date theDate = new Date();
        State previousState = state;
        state = CODAState.CONFIGURED;

        if (RateCalculator != null) RateCalculator.interrupt();

        // Build & QFiller threads must be immediately ended
        endBuildAndQFillerThreads(null, false);

        RateCalculator = null;
        qFillers = null;
        buildingThreadList.clear();

        if (inputOrders  != null) Arrays.fill(inputOrders, 0);
        if (outputOrders != null) Arrays.fill(outputOrders, 0);

        paused = false;

        if (previousState.equals(CODAState.ACTIVE)) {
            try {
                // Set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            }
            catch (DataNotFoundException e) {}
        }
    }


    /** {@inheritDoc} */
    public void end() {

        state = CODAState.DOWNLOADED;

        // The order in which these thread are shutdown does(should) not matter.
        // Rocs should already have been shutdown, followed by the input transports,
        // followed by this module (followed by the output transports).
        if (RateCalculator != null) RateCalculator.interrupt();

        // Build & QFiller threads should already be ended by END event
        endBuildAndQFillerThreads(null, true);

        RateCalculator = null;
        qFillers = null;
        buildingThreadList.clear();

        if (inputOrders  != null) Arrays.fill(inputOrders, 0);
        if (outputOrders != null) Arrays.fill(outputOrders, 0);

        paused = false;

        try {
            // Set end-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {

        // Event builder needs inputs
        if (inputChannels.size() < 1) {
            errorMsg.compareAndSet(null, "no input channels to EB");
            state = CODAState.ERROR;
            emu.sendStatusMessage();
            throw new CmdExecException("no input channels to EB");
        }

        // Make sure each input channel is associated with a unique rocId
        for (int i=0; i < inputChannels.size(); i++) {
            for (int j=i+1; j < inputChannels.size(); j++) {
                if (inputChannels.get(i).getID() == inputChannels.get(j).getID()) {
                    errorMsg.compareAndSet(null, "input channels duplicate rocIDs");
                    state = CODAState.ERROR;
                    emu.sendStatusMessage();
                    throw new CmdExecException("input channels duplicate rocIDs");
                }
            }
        }

        state = CODAState.PAUSED;
        paused = true;

        // Make sure we have the correct # of payload bank queues available.
        // Each queue holds payload banks taken from a particular source (ROC).
        int diff = inputChannels.size() - payloadQueues.size();
        boolean add = true;
        if (diff < 0) {
            add  = false;
            diff = -diff;
        }

        for (int i=0; i < diff; i++) {
            // Add more queues
            if (add) {
                // Allow only payloadBankQueueSize items on the q at once
                payloadQueues.add(new PayloadQueue<PayloadBank>(payloadBankQueueSize));
            }
            // Remove excess queues (from head of linked list)
            else {
                payloadQueues.remove();
            }
        }

        int qCount = payloadQueues.size();

        // Clear all payload bank queues, associate each one with source ID, reset record ID
        for (int i=0; i < qCount; i++) {
            payloadQueues.get(i).clear();
            payloadQueues.get(i).setSourceId(inputChannels.get(i).getID());
            payloadQueues.get(i).setRecordId(0);
        }

        // How many output channels do we have?
        outputChannelCount = outputChannels.size();

        // Allocate some arrays based on # of output channels
        waitingLists = null;
        if (outputChannelCount > 0) {
            locks = new Object[outputChannelCount];
            for (int i=0; i < outputChannelCount; i++) {
                locks[i] = new Object();
            }
            inputOrders  = new int[outputChannelCount];
            outputOrders = new int[outputChannelCount];

            waitingLists = new PriorityBlockingQueue[outputChannelCount];
            for (int i=0; i < outputChannelCount; i++) {
                waitingLists[i] = new PriorityBlockingQueue<PayloadBank>(100, comparator);
            }
        }

        // Reset some variables
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;
        runTypeId = emu.getRunTypeId();
        runNumber = emu.getRunNumber();
        ebRecordId = 0;
        eventNumber = 1L;
        eventNumberAtLastSync = eventNumber;

        // Create & start threads
        createThreads();
        startThreads();

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}
    }



    /** {@inheritDoc} */
    public void go() {
        state = CODAState.ACTIVE;
        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }


    /**
     * Method to create thread objects for stats, filling Qs and building events.
     */
    private void createThreads() {
        RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), name+":watcher");

        for (int i=0; i < buildingThreadCount; i++) {
            BuildingThread thd1 = new BuildingThread(emu.getThreadGroup(), new BuildingThread(), name+":builder"+i);
            buildingThreadList.add(thd1);
        }

        // Sanity check
        if (buildingThreadList.size() != buildingThreadCount) {
            System.out.println("Have " + buildingThreadList.size() + " build threads, but want " +
                                buildingThreadCount);
        }

        qFillers = new Thread[payloadQueues.size()];
        for (int i=0; i < payloadQueues.size(); i++) {
            qFillers[i] = new Thread(emu.getThreadGroup(),
                                     new Qfiller(payloadQueues.get(i),
                                                    inputChannels.get(i).getQueue()),
                                     name+":qfiller"+i);
        }
    }

    /**
     * Method to start threads for stats, filling Qs, and building events.
     * It creates these threads if they don't exist yet.
     */
    private void startThreads() {
        if (RateCalculator == null) {
System.out.println("startThreads(): recreating watcher thread");
            RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), name+":watcher");
        }

        if (RateCalculator.getState() == Thread.State.NEW) {
            RateCalculator.start();
        }

        if (buildingThreadList.size() < 1) {
            for (int i=0; i < buildingThreadCount; i++) {
                BuildingThread thd1 = new BuildingThread(emu.getThreadGroup(), new BuildingThread(), name+":builder"+i);
                buildingThreadList.add(thd1);
            }
System.out.println("startThreads(): recreated building threads, # = " +
                               buildingThreadList.size());
        }

        for (BuildingThread thd : buildingThreadList) {
            if (thd.getState() == Thread.State.NEW) {
                thd.start();
            }
        }
System.out.println("startThreads(): started " + buildingThreadList.size() +
                   " building threads");

        if (qFillers == null) {
            qFillers = new Thread[payloadQueues.size()];
            for (int i=0; i < payloadQueues.size(); i++) {
                qFillers[i] = new Thread(emu.getThreadGroup(),
                                         new Qfiller(payloadQueues.get(i),
                                                     inputChannels.get(i).getQueue()),
                                         name+":qfiller"+i);
            }

System.out.println("startThreads(): recreated " + payloadQueues.size() +
                                       " Q-filling threads");
        }
        for (int i=0; i < payloadQueues.size(); i++) {
            if (qFillers[i].getState() == Thread.State.NEW) {
                qFillers[i].start();
            }
        }
System.out.println("startThreads(): started " + payloadQueues.size() +
                   " Q-filling threads");
    }


 }