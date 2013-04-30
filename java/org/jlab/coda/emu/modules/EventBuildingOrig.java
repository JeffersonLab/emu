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

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.EmuStateMachineAdapter;
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;

import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <pre><code>
 * Input Channels
 *   (DTR Qs)  :         IC1      IC2 ...  ICN
 *                        |        |        |
 *                        V        V        V
 * QFiller Threads:      QF1      QF2      QFN
 *  Grab DTR bank         |        |        |
 *  & split into          |        |        |
 *  payload banks         |        |        |
 *                        V        V        V
 * Payload Bank Qs:      PBQ1     PBQ2     PBQN
 *   1 for each           | \ \   /      /       _
 *    channel             |  \ \/      /       /
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
 *  payload Bank Q        |        |        |
 *  & build event.        |        |        |
 *                        V        V        V
 *    Output Qs:         OQ1      OQ2      OQM
 *                        |        |        |
 *                        |        |        |
 *                        \        |        /
 *                          \      |      /
 *                            \    |    /
 *                              \  |  /
 *                                \|/
 *                                 |
 *                                 V
 * QCollector Thread:             QCT
 * take all events,                |
 *  wrap in DTRs, &                |
 *  place (IN ORDER)               |
 *   in module's                   |
 *  output channels                |
 *                                 V
 * Output Channel(s):             OC1...
 *
 *
 *  M != N in general,= 3 by default
 *  DTR = Data Transport Record
 * </code></pre><p>
 *
 * This class is the event building module. It is a multithreaded module which has 1
 * QFiller thread per input channel. Each of these threads exists for the sole purpose
 * of taking Data Transport Records off of 1 input channel, pulling it apart into multiple
 * payload banks and placing those banks into 1 payload bank queue. At that point the
 * BuildingThreads - of which there may be any number - take turns at grabbing one bank
 * from each payload bank queue (and therefore input channel), building it into a single
 * event, and placing it into it output queue. Finally, 1 QCollector thread takes the
 * built events from the output queues of the BuildingThreads, wraps them in Data
 * Transport Record events, and places them in order into the output channels
 * (by round robin if more than one).<p>
 *
 * NOTE: QFiller threads ignore any banks that are not DTRs. BuildingThread objects
 * immediately pass along any User events to their output queues. Any Control events
 * they find must appear on each payload queue in the same position. If not, an exception
 * is thrown. If so, one of the Control events is passed along to its output queue.
 * Finally, the QCollector thread places any User or Control events in the first output
 * channel. These are not part of the round-robin output to each channel in turn.
 * If no output channels are defined in the config file, this module's QCollector thread
 * pulls off all events and discards them.
 */
public class EventBuildingOrig extends EmuStateMachineAdapter implements EmuModule {


    /** Name of this event builder. */
    private final String name;

    /** ID number of this event builder obtained from config file. */
    private int ebId;

    /** Keep track of the number of records built in this event builder. Reset at prestart. */
    private volatile int ebRecordId;

    /** State of the module. */
    private volatile State state = CODAState.BOOTED;

    /** InputChannels is an ArrayList of DataChannel objects that are inputs. */
    private ArrayList<DataChannel> inputChannels = new ArrayList<DataChannel>();

    /** OutputChannels is an ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    /** Container for queues used to hold payload banks taken from Data Transport Records. */
    private LinkedList<PayloadBankQueue<PayloadBank>> payloadBankQueues =
            new LinkedList<PayloadBankQueue<PayloadBank>>();

    /** The number of BuildingThread objects. */
    private int buildingThreadCount;

    /** Container for threads used to build events. */
    private LinkedList<BuildingThread> buildingThreadList = new LinkedList<BuildingThread>();

    /** Map containing attributes of this module given in config file. */
    private Map<String,String> attributeMap;

    /** Last error thrown by the module. */
    private final Throwable lastError = null;

    /** Threads used to take Data Transport Records from input channels, dissect them,
     *  and place resulting payload banks onto payload queues. */
    private Thread qFillers[];

    /** Thread used to collect built events from the buildingThreads, wrap them in a
     * Data Transport Record, and place them in order in an output channel. */
    private Thread qCollector;

    /** Lock to ensure that a BuldingThread grabs the same positioned event from each Q.  */
    private ReentrantLock getLock = new ReentrantLock();

    /** Order in which a buildingThread pulled banks off the payload bank Qs. */
    private int inputOrder;

    /** Next inputOrder due to be placed on the output channel. */
    private int outputOrder;

    /** Used to switch between output channel for physics events. */
    private int outputOrderPhysics;

    /** User hit pause button if <code>true</code>. */
    private boolean paused;

    /** If <code>true</code>, get debug print out. */
    private boolean debug = true;

    /** END event detected by one of the building threads. */
    private volatile boolean haveEndEvent;

    /**
     * If true, swap data if necessary when building event
     * Assume data is all 32 bit integers.
     */
    private boolean swapData;

    /** If true, include run number & type in built trigger bank. */
    private boolean includeRunData;

    /** The number of the experimental run's configuration. */
    private int runType;

    /** The number of the experimental run. */
    private int runNumber;

    /** If <code>true</code>, check timestamps for consistency. */
    private boolean checkTimestamps;

    /**
     * The maximum difference in ticks for timestamps for a single event before
     * an error condition is flagged. Only used if {@link #checkTimestamps} is
     * <code>true</code>.
     */
    private int timestampSlop;

    /** The number of the event to be assigned to that which is built next. */
    private long eventNumber;

    /** The number of the event that this Event Builder last completely built. */
    private long lastEventNumberBuilt;

    // The following members are for keeping statistics

    /** Total number of DataBank objects written to the outputs. */
    private long eventCountTotal;

    /** Sum of the sizes, in 32-bit words, of all DataBank objects written to the outputs. */
    private long wordCountTotal;

    /** Instantaneous event rate in Hz over the last time period of length {@link #statGatheringPeriod}. */
    private float eventRate;

    /** Instantaneous word rate in Hz over the last time period of length {@link #statGatheringPeriod}. */
    private float wordRate;

    /** Targeted time period in milliseconds over which instantaneous rates will be calculated. */
    private static final int statGatheringPeriod = 2000;

    /** Thread to update statistics. */
    private Thread watcher;

    /** Logger used to log messages to debug console. */
    private Logger logger;

    /** Emu this module belongs to. */
    private Emu emu;

    /** If <code>true</code>, then print sizes of various queues for debugging. */
    private boolean printQSizes;


    /**
     * Constructor creates a new EventBuildingOrig instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public EventBuildingOrig(String name, Map<String, String> attributeMap, Emu emu) {
        this.emu = emu;
        this.name = name;
        this.attributeMap = attributeMap;

        logger = emu.getLogger();

        try {
            ebId = Integer.parseInt(attributeMap.get("id"));
        }
        catch (NumberFormatException e) { /* default to 0 */ }

        // default to 3 event building threads
        buildingThreadCount = 3;
        try {
            buildingThreadCount = Integer.parseInt(attributeMap.get("threads"));
        }
        catch (NumberFormatException e) {}

        // default is to swap data if necessary -
        // assume 32 bit ints
        swapData = true;
        String str = attributeMap.get("swap");
        if (str != null) {
            if (str.equalsIgnoreCase("false") ||
                str.equalsIgnoreCase("off")   ||
                str.equalsIgnoreCase("no"))   {
                swapData = false;
            }
        }

        // default is NOT to include run number & type in build trigger bank
        str = attributeMap.get("runData");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("in")   ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                includeRunData = true;
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


    /** {@inheritDoc} */
    public String name() {
        return name;
    }


    /** {@inheritDoc} */
    synchronized public Object[] getStatistics() {
        Object[] stats = new Object[4];

        // nothing going on since we're not active
        if (state != CODAState.ACTIVE) {
            stats[0] = 0L;
            stats[1] = 0L;
            stats[2] = 0F;
            stats[3] = 0F;
        }
        else {
            stats[0] = eventCountTotal;
            stats[1] = wordCountTotal;
            stats[2] = eventRate;
            stats[3] = wordRate;
        }

        return stats;
    }
    

    /** {@inheritDoc} */
    public boolean representsEmuStatistics() {
        String stats = attributeMap.get("statistics");
        return (stats != null && stats.equalsIgnoreCase("on"));
    }


    /**
     * This class defines a thread that makes instantaneous rate calculations
     * once every few seconds. Rates are sent to runcontrol
     * (or stored in local xml config file).
     */
    private class Watcher extends Thread {
        /**
         * Method run is the action loop of the thread. It's created while the module is in the
         * ACTIVE or PAUSED state. It is exited on end of run or reset.
         * It is started by the GO transition.
         */
        public void run() {

            // variables for instantaneous stats
            long deltaT, t1, t2, prevEventCount=0L, prevWordCount=0L;

            while ((state == CODAState.ACTIVE) || paused) {
                try {
                    // In the paused state only wake every two seconds.
                    sleep(2000);

                    t1 = System.currentTimeMillis();

                    while (state == CODAState.ACTIVE) {
                        sleep(statGatheringPeriod);

                        t2 = System.currentTimeMillis();
                        deltaT = t2 - t1;

                        // calculate rates
                        eventRate = (eventCountTotal - prevEventCount)*1000F/deltaT;
                        wordRate  = (wordCountTotal  - prevWordCount)*1000F/deltaT;

                        t1 = t2;
                        prevEventCount = eventCountTotal;
                        prevWordCount  = wordCountTotal;

                        // The following was in the old Watcher thread ...
//                        try {
//                            Configurer.setValue(emu.parameters(), "status/eventCount", Long.toString(eventCountTotal));
//                            Configurer.setValue(emu.parameters(), "status/wordCount", Long.toString(wordCountTotal));
//                        }
//                        catch (DataNotFoundException e) {}
                    }

                } catch (InterruptedException e) {
                    logger.info("EventBuildingOrig watcher thread " + name() + " interrupted");
                }
            }
        }
    }


    /**
      * This class takes payload banks from a queue (an input channel, eg. ROC),
      * and places the them in a payload bank queue associated with that channel.
      * All other types of events are ignored.
      * Nothing in this class depends on single event mode status.
      */
     private class Qfiller extends Thread {

         BlockingQueue<QueueItem> channelQ;
         PayloadBankQueue<PayloadBank> payloadBankQ;

         Qfiller(PayloadBankQueue<PayloadBank> payloadBankQ, BlockingQueue<QueueItem> channelQ) {
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
                         qItem = channelQ.take();  // blocks, throws InterruptedException
                         pBank = qItem.getPayloadBank();
                         // Check this bank's format. If bad, ignore it
                         Evio.checkPayloadBank(pBank, payloadBankQ);
                     }
                 } catch (EmuException e) {
                     // EmuException from Evio.checkPayloadBank() if
                     // Roc raw or physics banks are in the wrong format
 if (debug) System.out.println("Qfiller: Roc raw or physics event in wrong format");
                     state = CODAState.ERROR;
                     return;
                 } catch (InterruptedException e) {
                     return;
                 }
             }
         }
     }


    /**
     * This class takes built events from the output Q of each BuildThread, wraps
     * each in a DataTransportRecord, and places them, <b>in the proper order</b>,
     * into this module's output channels.<p>
     */
    private class QCollector extends Thread {

        public void run() {
            int dtrTag;
            EvioEvent dtrEvent;
            EvioBank bank;
            EventBuilder builder = new EventBuilder(0, DataType.BANK, 0); // this event not used, just need a builder
            EventType eventType;

            // have output channels?
            boolean hasOutputs = !outputChannels.isEmpty();

            int index = 0;
            int size = buildingThreadList.size();
            PayloadBank[] banks = new PayloadBank[size];
            BuildingThread[] threads = new BuildingThread[size];

            for (BuildingThread thread : buildingThreadList) {
                threads[index] = thread;
                banks[index++] = null;
            }

            int counter = 1;

            while (state == CODAState.ACTIVE || paused) {

                try {
                    // try getting a built bank from each of the build threads
                    for (int i=0; i < size ; i++) {
                        if (banks[i] == null) {
                            // don't block, just try next Q if empty
                            banks[i] = threads[i].outputQueue.poll();
                        }
                    }

                    // see if any of the built banks are next to be output
                    for (int i=0; i < size ; i++) {
                        if (banks[i] == null) {
                            // don't chew up too much CPU time
                            if (counter++ % 5000 == 0) {
//                                System.out.println("EBOrig: sleeeeeeeeeep in QCollector 1");
                                Thread.sleep(40);
                            }
                            continue;
                        }

                        if (!hasOutputs) {
                            // Just keep stats and pull stuff off & discard if no outputs.
                            eventType = banks[i].getEventType();
                            if (eventType.isPhysics()) {
                                eventCountTotal += banks[i].getEventCount();              // event count
                                wordCountTotal  += banks[i].getHeader().getLength() + 1;  //  word count
                                lastEventNumberBuilt = banks[i].getFirstEventNumber() + eventCountTotal - 1;
                            }
                            banks[i] = null;
                        }
                        // Is the bank we grabbed next to be output? If so ...
                        else if ((Integer)(banks[i].getAttachment()) == outputOrder) {
                            // wrap event-to-be-sent in Data Transport Record for next EB or ER
                            eventType = banks[i].getEventType();

                            if (eventType.isPhysics()) {
                                outputChannels.get(outputOrderPhysics % outputChannels.size()).
                                        getQueue().put(new QueueItem(banks[i]));
                                // stats
                                eventCountTotal += banks[i].getEventCount();              // event count
                                wordCountTotal  += banks[i].getHeader().getLength() + 1;  //  word count
                                lastEventNumberBuilt = banks[i].getFirstEventNumber() + eventCountTotal - 1;
                                outputOrderPhysics = ++outputOrderPhysics % Integer.MAX_VALUE;
                            }
                            else if (eventType.isControl()) {
                                // Control events must go out over all output output channels
                                // and are not part of the round-robin output.
                                for (DataChannel outChannel : outputChannels) {
                                    // As far as I can tell, the EvioWriter write to an ET
                                    // buffer is thread safe. Guess we'll find out.
                                    outChannel.getQueue().put(new QueueItem(banks[i]));
                                }
                            }
                            else {
                                // User events are not part of the round-robin output,
                                // put on first output channel.
                                outputChannels.get(0).getQueue().put(new QueueItem(banks[i]));
                            }

                            // Get another built bank from this Q
                            banks[i] = null;
                            outputOrder = ++outputOrder % Integer.MAX_VALUE;
                            counter = 1;
                        }
                        else {
                            // don't chew up too much CPU time
                            if (counter++ % 5000 == 0) {
                                Thread.sleep(40);
                            }
                        }
                    }

                } catch (InterruptedException e) {
                    return;
                }
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
     * Each incoming bank from the ArrayList is copied into the new bank.
     * The count of outgoing banks and the count of data words are incremented.
     * If the Module has an output, the bank of banks is put on the output DataChannel.
     */
    class BuildingThread extends Thread {

        BlockingQueue<PayloadBank> outputQueue = new ArrayBlockingQueue<PayloadBank>(2000);

        BuildingThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        BuildingThread() {
            super();
        }

        public void run() {

            boolean runChecks = true;

            // initialize
            int myInputOrder=-1;
            int totalNumberEvents=1;
            long firstEventNumber=1;
            boolean gotBuildEvent;
            boolean nonFatalError;
            boolean haveControlEvents;
            boolean havePhysicsEvents;
            EventType eventType;
            EvioEvent combinedTrigger;
            PayloadBank physicsEvent;
            PayloadBank[] buildingBanks = new PayloadBank[inputChannels.size()];
            EventBuilder builder = new EventBuilder(0, DataType.BANK, 0); // this event not used, just need a builder
            LinkedList<PayloadBank> userEventList = new LinkedList<PayloadBank>();

            while (state == CODAState.ACTIVE || paused) {

                    try {
                        nonFatalError = false;

                        // The payload bank queues are filled by the QFiller thread.

                        // Here we have what we need to build:
                        // ROC raw events from all ROCs, each with sequential record IDs.
                        // However, there are also control events on queues.

                        // Put null into buildingBanks array elements
                        Arrays.fill(buildingBanks, null);

                        // reset flag
                        gotBuildEvent = false;

                        // Fill array with actual banks
                        try {
                            // grab lock so we get the very next bank from each channel
                            getLock.lock();

                            // Grab one non-user bank from each channel.
                            // This algorithm retains the proper order of any user events.
                            for (int i=0; i < payloadBankQueues.size(); i++) {
                                
                                // Loop until we get event which is NOT a user event
                                while (true) {
                                    
                                    // will BLOCK here waiting for payload bank if none available
                                    buildingBanks[i] = payloadBankQueues.get(i).take();

                                    eventType = buildingBanks[i].getEventType();

                                    // Check immediately if it is a user event.
                                    // If it is, stick it in a list and get another.
                                    if (eventType.isUser()) {
                                        myInputOrder = inputOrder;
                                        inputOrder   = ++inputOrder % Integer.MAX_VALUE;
if (debug) System.out.println("BuildingThread: Got user event, order = " + myInputOrder);
                                        // Store its output order
                                        buildingBanks[i].setAttachment(myInputOrder);
                                        // Stick it in a list
                                        userEventList.add(buildingBanks[i]);
                                        // Since we got a user event, try again until we get one that isn't.
                                        continue;
                                    }

                                    // If event to be built, find the beginning event number to use.
                                    if (!gotBuildEvent && (eventType.isROCRaw() || eventType.isPhysics())) {
                                        gotBuildEvent = true;
                                        firstEventNumber = eventNumber;
                                        // Find the total # of events
                                        totalNumberEvents = buildingBanks[i].getHeader().getNumber();
                                        eventNumber += totalNumberEvents;
                                    }

                                    break;
                                }
                            }

                            // record its input order so that it can be used to order the output
                            myInputOrder = inputOrder;
                            inputOrder   = ++inputOrder % Integer.MAX_VALUE;
                        }
                        finally {
                            getLock.unlock();
                        }

                        // If we have any user events, stick those on the Q first.
                        // Source may be any of the inputs.
                        if (userEventList.size() > 0) {
                            for (PayloadBank pbank : userEventList) {
                                outputQueue.put(pbank);
                            }
                            userEventList.clear();
                        }

                        // Check endianness & source IDs
                        if (runChecks) {
                            for (int i=0; i < payloadBankQueues.size(); i++) {
                                // Check endianness
                                if (buildingBanks[i].getByteOrder() != ByteOrder.BIG_ENDIAN) {
if (debug) System.out.println("All events sent to EMU must be BIG endian");
                                    throw new EmuException("events must be BIG endian");
                                }

                                // Check the source ID of this bank to see if it matches
                                // what should be coming over this channel.
                                if (buildingBanks[i].getSourceId() != payloadBankQueues.get(i).getSourceId()) {
if (debug) System.out.println("bank tag = " + buildingBanks[i].getSourceId());
if (debug) System.out.println("queue source id = " + payloadBankQueues.get(i).getSourceId());
                                    nonFatalError = true;
                                }
                            }
                        }

if (debug && nonFatalError) System.out.println("\nERROR 1\n");

                        // All or none must be control events, else throw exception.
                        haveControlEvents = gotValidControlEvents(buildingBanks);

                        // If they are all control events, just store
                        // their input order and put 1 on output Q.
                        if (haveControlEvents) {
if (debug) System.out.println("Have CONTROL event");
                            buildingBanks[0].setAttachment(myInputOrder);
                            outputQueue.put(buildingBanks[0]);

                            // If it is an END event, interrupt other build threads
                            // then quit this one.
                            if (buildingBanks[0].getControlType() == ControlType.END) {
if (debug) System.out.println("Found END event in build thread");
                                haveEndEvent = true;
                                endBuildAndFillerAndCollectorThreads(this, true);
                                return;
                            }

                            continue;
                        }

                        // At this point there are only physics or ROC raw events, which do we have?
                        havePhysicsEvents = buildingBanks[0].getEventType().isPhysics();

                        // Check for identical syncs, uniqueness of ROC ids,
                        // single-event-mode, and identical (physics or ROC raw) event types
                        nonFatalError |= checkConsistency(buildingBanks);

                        // Are events in single event mode?
                        boolean eventsInSEM = buildingBanks[0].isSingleEventMode();

if (debug && nonFatalError) System.out.println("\nERROR 2\n");

                        //--------------------------------------------------------------------
                        // Build trigger bank, number of ROCs given by number of buildingBanks
                        //--------------------------------------------------------------------
                        combinedTrigger = new EvioEvent(CODATag.BUILT_TRIGGER_BANK.getValue(),
                                                        DataType.SEGMENT,
                                                        buildingBanks.length + 2);
                        builder.setEvent(combinedTrigger);

                        // if building with Physics events ...
                        if (havePhysicsEvents) {
                            //-----------------------------------------------------------------------------------
                            // The actual number of rocs + 2 will replace num in combinedTrigger definition above
                            //-----------------------------------------------------------------------------------
                            // combine the trigger banks of input events into one (same if single event mode)
//if (debug) System.out.println("BuildingThread: create trigger bank from built banks");
                            nonFatalError |= Evio.makeTriggerBankFromPhysics(buildingBanks, builder, ebId,
                                                                   runNumber, runType, includeRunData,
                                                                   eventsInSEM, false,
                                                                   checkTimestamps, timestampSlop);
                        }
                        // else if building with ROC raw records ...
                        else {
                            // if in single event mode, build trigger bank differently
                            if (eventsInSEM) {
                                // create a trigger bank from data in Data Block banks
//if (debug) System.out.println("BuildingThread: create trigger bank in SEM");
                                nonFatalError |= Evio.makeTriggerBankFromSemRocRaw(buildingBanks, builder,
                                                                                   ebId, firstEventNumber,
                                                                                   runNumber, runType,
                                                                                   includeRunData,
                                                                                   checkTimestamps,
                                                                                   timestampSlop);
                            }
                            else {
                                // combine the trigger banks of input events into one
//if (debug) System.out.println("BuildingThread: create trigger bank");
                                nonFatalError |= Evio.makeTriggerBankFromRocRaw(buildingBanks, builder,
                                                                                ebId, firstEventNumber,
                                                                                runNumber, runType,
                                                                                includeRunData, false,
                                                                                checkTimestamps,
                                                                                timestampSlop);
                            }
                        }

if (debug && nonFatalError) System.out.println("\nERROR 3\n");

                        // check payload banks for non-fatal errors when extracting them onto the payload queues
                        for (PayloadBank pBank : buildingBanks)  {
                            nonFatalError |= pBank.hasNonFatalBuildingError();
                        }

if (debug && nonFatalError) System.out.println("\nERROR 4\n");

                        // create a physics event from payload banks and combined trigger bank
                        int tag = Evio.createCodaTag(buildingBanks[0].isSync(),
                                                     buildingBanks[0].hasError() || nonFatalError,
                                                     buildingBanks[0].isReserved(),
                                                     buildingBanks[0].isSingleEventMode(),
                                                     ebId);
//if (debug) System.out.println("tag = " + tag + ", is sync = " + buildingBanks[0].isSync() +
//                   ", has error = " + (buildingBanks[0].hasError() || nonFatalError) +
//                   ", is reserved = " + buildingBanks[0].isReserved() +
//                   ", is single mode = " + buildingBanks[0].isSingleEventMode());

                        physicsEvent = new PayloadBank(tag, DataType.BANK, totalNumberEvents);
                        builder.setEvent(physicsEvent);
                        if (havePhysicsEvents) {
//if (debug) System.out.println("BuildingThread: build physics event with physics banks");
                            Evio.buildPhysicsEventWithPhysics(combinedTrigger, buildingBanks, builder);
                        }
                        else {
//if (debug) System.out.println("BuildingThread: build physics event with ROC raw banks");
                            Evio.buildPhysicsEventWithRocRaw(combinedTrigger, buildingBanks,
                                                             builder, swapData);
                        }

                        // setting header lengths done in Evio.buildPhysicsEventWith* methods
//                        physicsEvent.setAllHeaderLengths();

                        physicsEvent.setAttachment(myInputOrder); // store its output order
                        physicsEvent.setEventType(EventType.PHYSICS);
                        physicsEvent.setEventCount(totalNumberEvents);
                        physicsEvent.setFirstEventNumber(firstEventNumber);

                        // Stick it on the local output Q (for this building thread).
                        // That way we don't waste time trying to coordinate between
                        // building threads right here - leave that to the QCollector thread.
                        outputQueue.put(physicsEvent);
if (debug && printQSizes) {
    int size3 = outputQueue.size();
    if (size3 > 400 && size3 % 100 == 0) System.out.println("output Q: " + size3);
}
                    }
                    catch (EmuException e) {
if (debug) System.out.println("MAJOR ERROR building events");
                        emu.getCauses().add(e);
                        state = CODAState.ERROR;
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


    /**
     * Check each payload bank - one from each input channel - for a number of issues:<p>
     * <ol>
     * <li>if there are any sync bits set, all must be sync banks
     * <li>the ROC ids of the banks must be unique
     * <li>if any banks are in single-event-mode, all need to be in that mode
     * <li>at this point all banks are either physics events or ROC raw record, but must be identical types
     * </ol>
     *
     * @param buildingBanks array containing banks that will be built together
     * @return <code>true</code> if non fatal error occurred, else <code>false</code>
     * @throws org.jlab.coda.emu.EmuException if some events are in single event mode and others are not, or
     *                      if some physics and others ROC raw event types
     */
    private boolean checkConsistency(PayloadBank[] buildingBanks) throws EmuException {
        boolean nonFatalError = false;

        // for each ROC raw data record check the sync bit
        int syncBankCount = 0;

        // for each ROC raw data record check the single-event-mode bit
        int singleEventModeBankCount = 0;

        // By the time this method is run, all input banks are either physics or ROC raw.
        // Just make sure they're all identical.
        int physicsEventCount = 0;

        for (int i=0; i < buildingBanks.length; i++) {
            if (buildingBanks[i].isSync()) {
                syncBankCount++;
            }

            if (buildingBanks[i].isSingleEventMode()) {
                singleEventModeBankCount++;
            }

            if (buildingBanks[i].getEventType().isPhysics()) {
                physicsEventCount++;
            }

            for (int j=i+1; j < buildingBanks.length; j++) {
                if ( buildingBanks[i].getSourceId() == buildingBanks[j].getSourceId()  ) {
                    // ROCs have duplicate IDs
                    nonFatalError = true;
                }
            }
        }

        // if one is a sync, all must be syncs
        if (syncBankCount > 0 && syncBankCount != buildingBanks.length) {
            // some banks are sync banks and some are not
            nonFatalError = true;
        }

        // if one is a single-event-mode, all must be
        if (singleEventModeBankCount > 0 && singleEventModeBankCount != buildingBanks.length) {
            // some banks are single-event-mode and some are not, so we cannot build at this point
            throw new EmuException("not all events are in single event mode");
        }

        // all must be physics or all must be ROC raw
        if (physicsEventCount > 0 && physicsEventCount != buildingBanks.length) {
            // some banks are physics and some ROC raw
            throw new EmuException("not all events are physics or not all are ROC raw");
        }

        return nonFatalError;
    }


    /**
     * Check each payload bank - one from each input channel - to see if there are any
     * control events. A valid control event requires all channels to have identical
     * control events. If only some are control events, throw exception as it must
     * be all or none. If none are control events, do nothing as the banks will be built
     * into a single event momentarily.
     *
     * @param buildingBanks array containing events that will be built together
     * @return <code>true</code> if a proper control events found, else <code>false</code>
     * @throws EmuException if events contain mixture of control/data or control types
     */
    private boolean gotValidControlEvents(PayloadBank[] buildingBanks)
            throws EmuException {

        int counter = 0;
        int controlEventCount = 0;
        int numberOfBanks = buildingBanks.length;
        EventType eventType;
        EventType[] types = new EventType[numberOfBanks];

        // Count control events
        for (PayloadBank bank : buildingBanks) {
            // Might be a ROC Raw, Physics, or Control Event
            eventType = bank.getEventType();
            if (eventType.isControl()) {
                controlEventCount++;
            }
            types[counter++] = eventType;
        }

        // If one is a control event, all must be identical control events.
        if (controlEventCount > 0) {
            // All events must be control events
            if (controlEventCount != numberOfBanks) {
if (debug) System.out.println("gotValidControlEvents: got " + controlEventCount +
                              " control events, but have " + numberOfBanks + " banks!");
                throw new EmuException("some channels have control events and some do not");
            }

            // Make sure all are the same type of control event
            eventType = types[0];
            for (int i=1; i < types.length; i++) {
                if (eventType != types[i]) {
                    throw new EmuException("different type control events on each channel");
                }
            }
if (debug) System.out.println("gotValidControlEvents: found control event of type " + eventType.name());

            return true;
        }

        return false;
    }


    /**
     * End all Build, QFiller, and QCollector threads because an END event came through
     * one of them. The build thread calling this method is not interrupted.
     *
     * @param thisThread the build thread calling this method; if null,
     *                   all build & QFiller threads are interrupted
     * @param wait if <code>true</code> check if END event has arrived and
     *             if all the Qs are empty, if not, wait up to 1/2 second.
     */
    private void endBuildAndFillerAndCollectorThreads(BuildingThread thisThread, boolean wait) {

        if (wait) {
            // Look to see if anything still on the payload bank or input channel Qs
            int roundsLeft = 5;
            boolean haveUnprocessedEvents = false;

            for (int i=0; i < payloadBankQueues.size(); i++) {
                // Strictly speaking the input channel Q holds events with
                // multiple payload banks, but that doesn't matter here.
                if (payloadBankQueues.get(i).size() +
                        inputChannels.get(i).getQueue().size() > 0) {
                    haveUnprocessedEvents = true;
                    break;
                }
            }

            for (BuildingThread thd : buildingThreadList) {
                // also check the output Qs of the building threads
                if (thd.outputQueue.size() > 0) {
                    haveUnprocessedEvents = true;
                    break;
                }
            }

            // Wait up to 1/2 sec for events to be processed & END to arrive, then proceed
            while ((haveUnprocessedEvents || !haveEndEvent) && (roundsLeft-- > 0)) {
                try {Thread.sleep(100);}
                catch (InterruptedException e) {}

                haveUnprocessedEvents = false;
                for (int i=0; i < payloadBankQueues.size(); i++) {
                    if (payloadBankQueues.get(i).size() +
                            inputChannels.get(i).getQueue().size() > 0) {
                        haveUnprocessedEvents = true;
                        break;
                    }
                }

                for (BuildingThread thd : buildingThreadList) {
                    if (thd.outputQueue.size() > 0) {
                        haveUnprocessedEvents = true;
                        break;
                    }
                }
            }

            if (haveUnprocessedEvents || !haveEndEvent) {
                if (debug) System.out.println("endBuildThreads: will end building/filling/collecting threads but no END event or Qs not empty !!!");
                state = CODAState.ERROR;
            }
        }

        // Interrupt all Building threads except the one calling this method
        for (Thread thd : buildingThreadList) {
            if (thd == thisThread) continue;
            thd.interrupt();
        }

        // Interrupt all QFiller threads
        if (qFillers != null) {
            for (Thread qf : qFillers) {
                qf.interrupt();
            }
        }

        // Interrupt QCollector thread too
        if (qCollector != null) qCollector.interrupt();
    }


    /** {@inheritDoc} */
    public State state() {
        return state;
    }


    /**
     * Set the state of this object.
     * @param s the state of this Cobject
     */
    public void setState(State s) {
        state = s;
    }


    /**
     * Method getError returns the error of this EventBuildingOrig object.
     * @return the error (type Throwable) of this EventBuildingOrig object.
     */
    public Throwable getError() {
        return lastError;
    }


    /** {@inheritDoc} */
    public void reset() {
        Date theDate = new Date();
        State previousState = state;
        state = CODAState.CONFIGURED;

        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;

        if (watcher  != null) watcher.interrupt();

        // Build & QFiller & QCollector threads must be immediately ended
        endBuildAndFillerAndCollectorThreads(null, false);

        watcher    = null;
        qFillers   = null;
        qCollector = null;
        buildingThreadList.clear();

        inputOrder = 0;
        outputOrder = 0;
        outputOrderPhysics = 0;
        paused = false;

        if (previousState.equals(CODAState.ACTIVE)) {
            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            } catch (DataNotFoundException e) {
                e.printStackTrace();
            }
        }
    }


    public void end() throws CmdExecException {
        state = CODAState.DOWNLOADED;

        // The order in which these thread are shutdown does(should) not matter.
        // Rocs should already have been shutdown, followed by the input transports,
        // followed by this module (followed by the output transports).
        if (watcher  != null) watcher.interrupt();

        // Build & QFiller & QCollector threads should already be ended by END event
        endBuildAndFillerAndCollectorThreads(null, true);

        watcher    = null;
        qFillers   = null;
        qCollector = null;
        buildingThreadList.clear();

        inputOrder = 0;
        outputOrder = 0;
        outputOrderPhysics = 0;
        paused = false;

        try {
            // Set end-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        } catch (DataNotFoundException e) {
            state = CODAState.ERROR;
            throw new CmdExecException("status/run_end_time entry not found in local config file");
        }
    }


    public void prestart() throws CmdExecException {
        // Make sure each input channel is associated with a unique rocId
        for (int i=0; i < inputChannels.size(); i++) {
            for (int j=i+1; j < inputChannels.size(); j++) {
                if (inputChannels.get(i).getID() == inputChannels.get(j).getID()) {
                    state = CODAState.ERROR;
                    throw new CmdExecException("input channels duplicate rocIDs");
                }
            }
        }

        state = CODAState.PAUSED;

        // Make sure we have the correct # of payload bank queues available.
        // Each queue holds payload banks taken from Data Transport Records
        // from a particular source (ROC).
        int diff = inputChannels.size() - payloadBankQueues.size();
        boolean add = true;
        if (diff < 0) {
            add  = false;
            diff = -diff;
        }

        for (int i=0; i < diff; i++) {
            // Add more queues
            if (add) {
                // Allow only 1000 items on the q at once
                payloadBankQueues.add(new PayloadBankQueue<PayloadBank>(1000));
            }
            // Remove excess queues
            else {
                payloadBankQueues.remove();
            }
        }

        int qCount = payloadBankQueues.size();

        // Clear all payload bank queues, associate each one with source ID, and reset record ID
        for (int i=0; i < qCount; i++) {
            payloadBankQueues.get(i).clear();
            payloadBankQueues.get(i).setSourceId(inputChannels.get(i).getID());
            payloadBankQueues.get(i).setRecordId(0);
        }

        // Reset some variables
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;
        runType = emu.getRunType();
        runNumber = emu.getRunNumber();
        ebRecordId = 0;
        eventNumber = 1L;
        lastEventNumberBuilt = 0L;

        // Create threads objects (but don't start them yet)
        watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");
        for (int i=0; i < buildingThreadCount; i++) {
            BuildingThread thd1 = new BuildingThread(emu.getThreadGroup(), new BuildingThread(), name+":builder"+i);
            buildingThreadList.add(thd1);
        }
        qCollector = new Thread(emu.getThreadGroup(), new QCollector(), name+":qcollector");
        qFillers = new Thread[qCount];
        for (int i=0; i < qCount; i++) {
            qFillers[i] = new Thread(emu.getThreadGroup(),
                                     new Qfiller(payloadBankQueues.get(i),
                                                 inputChannels.get(i).getQueue()),
                                     name+":qfiller"+i);
        }

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        } catch (DataNotFoundException e) {
            state = CODAState.ERROR;
            throw new CmdExecException("status/run_start_time entry not found in local config file");
        }
    }

    public void pause() {
        paused = true;
    }


    public void go() throws CmdExecException {
        state = CODAState.ACTIVE;

        // Start up all threads
        if (watcher == null) {
            watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");
        }
        if (watcher.getState() == Thread.State.NEW) {
            watcher.start();
        }

        if (buildingThreadList.size() < 1) {
            for (int i=0; i < buildingThreadCount; i++) {
                BuildingThread thd1 = new BuildingThread(emu.getThreadGroup(), new BuildingThread(), name+":builder"+i);
                buildingThreadList.add(thd1);
            }
        }

        for (BuildingThread thd : buildingThreadList) {
            if (thd.getState() == Thread.State.NEW) {
                thd.start();
            }
        }

        if (qCollector == null) {
            qCollector = new Thread(emu.getThreadGroup(), new QCollector(), name+":qcollector");
        }

        if (qCollector.getState() == Thread.State.NEW) {
            qCollector.start();
        }

        if (qFillers == null) {
            qFillers = new Thread[payloadBankQueues.size()];
            for (int i=0; i < payloadBankQueues.size(); i++) {
                qFillers[i] = new Thread(emu.getThreadGroup(),
                                         new Qfiller(payloadBankQueues.get(i),
                                                     inputChannels.get(i).getQueue()),
                                         name+":qfiller"+i);
            }
        }
        for (int i=0; i < payloadBankQueues.size(); i++) {
            if (qFillers[i].getState() == Thread.State.NEW) {
                qFillers[i].start();
            }
        }

        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        } catch (DataNotFoundException e) {
            state = CODAState.ERROR;
            throw new CmdExecException("status/run_start_time entry not found in local config file");
        }
    }


    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {
        this.inputChannels.addAll(input_channels);
    }

    /** {@inheritDoc} */
    public void addOutputChannels(ArrayList<DataChannel> output_channels) {
        this.outputChannels.addAll(output_channels);
    }

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getInputChannels() {
        return inputChannels;
    }

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getOutputChannels() {
        return outputChannels;
    }

    /** {@inheritDoc} */
    public void clearChannels() {
        inputChannels.clear();
        outputChannels.clear();
    }


}
