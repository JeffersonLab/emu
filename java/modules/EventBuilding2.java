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

package modules;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.data.EventType;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.emu.support.data.PayloadBank;
import org.jlab.coda.emu.support.data.PayloadBankQueue;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;

import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;

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
 *  payload Bank Q,       |        |        |
 *  build event,          |        |        |
 *  wrap in DTRs, &       |        |        |
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
 * of taking Data Transport Records off of 1 input channel, pulling it apart into multiple
 * payload banks and placing those banks into 1 payload bank queue. At that point the
 * BuildingThreads - of which there may be any number - take turns at grabbing one bank
 * from each payload bank queue (and therefore input channel), building them into a single
 * event, wrapping that in a Data Transport Record, and placing it (in order) into one of
 * the output channels (by round robin if more than one).<p>
 *
 * NOTE: QFiller threads ignore any banks that are not DTRs. BuildingThread objects
 * immediately pass along any User events to their output queues. Any Control events
 * they find must appear on each payload queue in the same position. If not, an exception
 * is thrown. If so, one of the Control events is passed along to its output queue.
 * Finally, the Building threads place any User or Control events in the first output
 * channel. These are not part of the round-robin output to each channel in turn.
 * If no output channels are defined in the config file, this module discards all events.
 *
 * TODO: ET buffers have the number of events in them which varies from ROC to ROC.
 */
public class EventBuilding2 implements EmuModule, Runnable {


    /** Name of this event builder. */
    private final String name;

    /** ID number of this event builder obtained from config file. */
    private int ebId;

    /** Keep track of the number of records built in this event builder. Reset at prestart. */
    private volatile int ebRecordId;

    /** State of the module. */
    private volatile State state = CODAState.UNCONFIGURED;

    /** InputChannels is an ArrayList of DataChannel objects that are inputs. */
    private ArrayList<DataChannel> inputChannels = new ArrayList<DataChannel>();

    /** OutputChannels is an ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    private PriorityBlockingQueue<EvioBank> waitingLists[];

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

    /** Lock to ensure that a BuldingThread grabs the same positioned event from each Q.  */
    private ReentrantLock getLock = new ReentrantLock();

    /** User hit pause button if <code>true</code>. */
    private boolean paused;

    /** END event detected by one of the building threads. */
    private volatile boolean haveEndEvent;


    // The following members are for keeping statistics

    /** The number of the experimental run. */
    private long runNumber;

    /** The number of the event to be assigned to that which is built next. */
    private long eventNumber;

    /** The number of the event that this Event Builder last completely built. */
    private long lastEventNumberBuilt;

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

    private Logger logger;

    private Emu emu;

    private boolean printQSizes;

    /** Comparator which tells priority queue how to sort elements. */
    private BankComparator<EvioBank> comparator = new BankComparator<EvioBank>();


    /**
     * Keep some data together and store as event attachment.
     */
    private class EventOrder {
        int index;
        int inputOrder;
        Object lock;
        DataChannel outputChannel;
    }

    /**
     * Class defining comparator which tells priority queue how to sort elements.
     * @param <T> Must be EvioBank in this case
     */
    private class BankComparator<T> implements Comparator<T> {
        public int compare(T o1, T o2) throws ClassCastException {
            EvioBank bank1 = (EvioBank) o1;
            EvioBank bank2 = (EvioBank) o2;
            EventOrder eo1 = (EventOrder) (bank1.getAttachment());
            EventOrder eo2 = (EventOrder) (bank2.getAttachment());

            if (eo1 == null || eo2 == null) {
                return 0;
            }

            return (eo1.inputOrder - eo2.inputOrder);
        }
    }

    /** Number of output channels. */
    private int outputChannelCount;

    /** Index to help cycle through output channels sequentially. */
    private int outputChannelIndex;

    /**
     * Array of locks - one for each output channel -
     * so building threads can synchronize their output.
     */
    private Object locks[];

    /** Array of input orders - one for each output channel. */
    private int[] inputOrders;

    /**
     * Array of output orders - one for each output channel.
     * Keeps track of which input was the last output in which channel.
     */
    private int[] outputOrders;

    private AtomicIntegerArray outputOrders2;


    /**
     * Constructor creates a new EventBuilding2 instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public EventBuilding2(String name, Map<String, String> attributeMap, Emu emu) {
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
//System.out.println("\nSetting #### of threads to " + buildingThreadCount + "\n");
        }
        catch (NumberFormatException e) { /* default to 0 */ }

        // the module sets the type of CODA class it is.
        emu.setCodaClass(CODAClass.CDEB);

// System.out.println("**** HEY, HEY someone created one of ME (modules.EventBuilding2 object) ****");
// System.out.println("**** LOADED NEW CLASS, DUDE!!! (modules.EventBuilding2 object) ****");
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

    public boolean representsEmuStatistics() {
        String stats = attributeMap.get("statistics");
        return (stats != null && stats.equalsIgnoreCase("on"));
    }

    /** Method run is the action loop of the main thread of the module. */
    public void run() {
//        BuildingThread builder1 = new BuildingThread();
//        BuildingThread builder2 = new BuildingThread();
//        BuildingThread builder3 = new BuildingThread();
//        buildingThreadList.add(builder1);
//        buildingThreadList.add(builder2);
//        buildingThreadList.add(builder3);
//        builder1.start();
//        builder2.start();
//        builder3.start();
    }




    /**
     * This class defines a thread that copies the event number and data count into the EMU status
     * once every 1/2 second. This is much more efficient than updating the status
     * every time that the counters are incremented. Not currently used. Can't remember why
     * we needed to write values into xml doc.
     */
    private class WatcherOld extends Thread {
        /**
         * Method run is the action loop of the thread. It executes while the module is in the
         * state ACTIVE or PAUSED. It is exited on end of run or reset.
         * It is started by the GO transition.
         */
        public void run() {
            while ((state == CODAState.ACTIVE) || (state == CODAState.PAUSED)) {
                try {
                    // In the paused state only wake every two seconds.
                    sleep(2000);

                    // synchronized to prevent problems if multiple watchers are running
                    synchronized (this) {
                        while (state == CODAState.ACTIVE) {
                            sleep(500);
                            Configurer.setValue(emu.parameters(), "status/eventCount", Long.toString(eventCountTotal));
                            Configurer.setValue(emu.parameters(), "status/wordCount", Long.toString(wordCountTotal));
//                            Configurer.newValue(emu.parameters(), "status/wordCount",
//                                                "CarlsModule", Long.toString(wordCountTotal));
                        }
                    }

                } catch (InterruptedException e) {
                    logger.info("EventBuilding2 thread " + name() + " interrupted");
                } catch (DataNotFoundException e) {
                    e.printStackTrace();
                }
            }
//System.out.println("EventBuilding2 module: quitting watcher thread");
        }
    }


    /**
     * This class defines a thread that makes instantaneous rate calculations
     * once every few seconds. Rates are sent to runcontrol.
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
                    }

                } catch (InterruptedException e) {
                    logger.info("EventBuilding2 thread " + name() + " interrupted");
                }
            }
//System.out.println("EventBuilding module: quitting watcher thread");
        }
    }



    /**
     * This class takes Data Transport Records from a queue (an input channel, eg. ROC),
     * extracts payload banks from those DTRs, and places the resulting banks in a payload
     * bank queue associated with that channel. All other types of events are ignored.
     * Nothing in this class depends on single event mode status.<p>
     */
    private class Qfiller extends Thread {

        BlockingQueue<EvioBank> channelQ;
        PayloadBankQueue<PayloadBank> payloadBankQ;

        Qfiller(PayloadBankQueue<PayloadBank> payloadBankQ, BlockingQueue<EvioBank> channelQ) {
            this.channelQ = channelQ;
            this.payloadBankQ = payloadBankQ;
        }

        public void run() {
            EvioBank channelBank;

            while (state == CODAState.ACTIVE || paused) {
                try {
                    while (state == CODAState.ACTIVE || paused) {
                        // block waiting for the next DTR from ROC.
//System.out.println("  QFiller thd: try taking bank from channel Q ... ");
                        channelBank = channelQ.take();  // blocks, throws InterruptedException
//System.out.println("  QFiller thd: got one");
if (printQSizes) {
    int size1 = channelQ.size();
    if (size1 > 400 && size1 % 100 == 0) System.out.println("in chan: " + size1);
}

                        // Is bank is in Data Transport Record format? If not, ignore it.
                        if ( Evio.isDataTransportRecord(channelBank) ) {
                            // Extract payload banks from DTR & place onto Q.
                            // May be blocked here waiting on a Q.
                            Evio.extractPayloadBanks(channelBank, payloadBankQ);
if (printQSizes) {
    int size2 = payloadBankQ.size();
    if (size2 > 400 && size2 % 100 == 0) System.out.println("payload Q: " + size2);
}
                        }
                        else {
System.out.println("Qfiller: got non-DTR bank, discard");
                        }
                    }

                } catch (EmuException e) {
// TODO: do something
                } catch (InterruptedException e) {
 // TODO: THIS IS NOT RIGHT!!!
                    if (state == CODAState.DOWNLOADED) return;
                }
            }
        }
    }


    private void bankToOutputChannel(PayloadBank bankOut, EventBuilder builder,
                                     BuildingThread thread) {

        // have output channels?
        if (outputChannelCount < 1) {
            return;
        }

        try {
            EvioBank bank;
            EventOrder evOrder;
            EventOrder eo = (EventOrder)bankOut.getAttachment();

            int recordId = eo.inputOrder;

            // wrap event-to-be-sent in Data Transport Record for next EB or ER
            EventType type = bankOut.getType();
            if (type == EventType.END) {
                System.out.println(" @@@@@@@ bankToOutputChannel: got END event");
            }
            else if (type != EventType.PHYSICS) {
                System.out.println(" @@@@@@@ bankToOutputChannel: got non-PHYSICS event");
            }

            int dtrTag = Evio.createCodaTag(type.getValue(), ebId);
            EvioEvent dtrEvent = new PayloadBank(dtrTag, DataType.BANK, recordId);
            builder.setEvent(dtrEvent);

            try {
                // add bank with full recordId
                bank = new EvioBank(Evio.RECORD_ID_BANK, DataType.INT32, 1);
                bank.appendIntData(new int[] {recordId});
                builder.addChild(dtrEvent, bank);
                // add event
                builder.addChild(dtrEvent, bankOut);

            } catch (EvioException e) {/* never happen */}

            synchronized (eo.lock) {
                // Is the bank we grabbed next to be output?
                // If not, put in waiting list and return.
                if (eo.inputOrder != outputOrders[eo.index]) {
                    dtrEvent.setAttachment(eo);
                    waitingLists[eo.index].add(dtrEvent);

                    if (waitingLists[eo.index].size() > 9) {
                        eo.lock.wait();
                    }
//System.out.println("out of order = " + eo.inputOrder);
//System.out.println("waiting list = ");
//                    for (EvioBank bk : waitingLists[eo.index]) {
//                        System.out.println("" + ((EventOrder)bk.getAttachment()).inputOrder);
//
//                    }
                    return;
                }

                // Place Data Transport Record on output channel
                eo.outputChannel.getQueue().put(dtrEvent);
                outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
//System.out.println("placing = " + eo.inputOrder);

                // take a look on the waiting list without removing ...
                bank = waitingLists[eo.index].peek();
                while (bank != null) {
                    evOrder = (EventOrder) bank.getAttachment();
                    if (evOrder.inputOrder == outputOrders[eo.index]) {
                        // remove from waiting list permanently
                        bank = waitingLists[eo.index].take();
                        eo.outputChannel.getQueue().put(bank);
                        outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
//System.out.println("placing = " + evOrder.inputOrder);
                    }
                    else {
                        break;
                    }
                    bank = waitingLists[eo.index].peek();
                }
                eo.lock.notifyAll();
            }

if (printQSizes) {
    int size = eo.outputChannel.getQueue().size();
    if (size > 400 && size % 100 == 0) System.out.println("output chan: " + size);
}

        } catch (InterruptedException e) {
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

        //LinkedBlockingQueue<EvioBank> waitingList = new LinkedBlockingQueue<EvioBank>();

        BuildingThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        BuildingThread() {
            super();
        }


        public void run() {

            boolean runChecks = true;

            // initialize
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
            EventOrder[] controlEventOrders = new EventOrder[outputChannelCount];
            EventBuilder builder = new EventBuilder(0, DataType.BANK, 0); // this event not used, just need a builder
            LinkedList<PayloadBank> userEventList = new LinkedList<PayloadBank>();

            int myInputOrder = -1;
            int myOutputChannelIndex = 0;
            Object myOutputLock = null;
            DataChannel myOutputChannel = null;

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
//System.out.println("    BuildingThread("+ name +"): try taking bank from payload bank Q #" +i);
                                    buildingBanks[i] = payloadBankQueues.get(i).take();
//System.out.println("    BuildingThread("+ name +"): GOT bank from Q #" + i);

                                    eventType = buildingBanks[i].getType();

                                    // If event needs to be built ...
                                    if (eventType.isROCRaw() || eventType.isPhysics()) {
                                        // One-time init stuff for a group of
                                        // records that will be built together.
                                        if (!gotBuildEvent) {

                                            // set flag
                                            gotBuildEvent = true;

                                            // Find the total # of events
                                            totalNumberEvents = buildingBanks[i].getHeader().getNumber();

                                            // Store first event number
                                            firstEventNumber = eventNumber;

                                            //calculate event number for next time through
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

                                        // go to next input channel
                                        break;
                                    }

                                    // Check if this is a user event.
                                    // If so, store it in a list and get another.
                                    if (eventType.isUser()) {
                                        if (outputChannelCount > 0) {
                                            // User or control events go into first channel
                                            myOutputChannel = outputChannels.get(0);
                                            myOutputChannelIndex = 0;

                                            // Order in which this will be placed into its output channel.
                                            myInputOrder = inputOrders[myOutputChannelIndex];
                                            myOutputLock = locks[myOutputChannelIndex];

                                            // Keep track of the next slot in this output channel.
                                            inputOrders[myOutputChannelIndex] =
                                                    ++inputOrders[myOutputChannelIndex] % Integer.MAX_VALUE;
                                        }

System.out.println("BuildingThread: Got user event, order = " + myInputOrder);
                                        // TODO: put this in the if-statement above
                                        EventOrder eo = new EventOrder();
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

                                    // If we're here, we've got control events.
                                    // We want one EventOrder object for each output channel
                                    // since we want one control event placed on each.
                                    if (!gotBuildEvent) {
                                        // set flag
                                        gotBuildEvent = true;

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

                                    // go to next input channel
                                    break;
                                }
                            }
                        }
                        finally {
                            getLock.unlock();
                        }

//System.out.println("BuildingThread: input order = " + myInputOrder);
//System.out.println("BuildingThread: got something from each input");

                        // store all channel & order info here
                        EventOrder evOrder = new EventOrder();
                        evOrder.index = myOutputChannelIndex;
                        evOrder.outputChannel = myOutputChannel;
                        evOrder.lock = myOutputLock;
                        evOrder.inputOrder = myInputOrder;

                        // If we have any user events, stick those on the Q first.
                        // Source may be any of the inputs.
                        if (userEventList.size() > 0) {
                            for (PayloadBank pbank : userEventList) {
                                bankToOutputChannel(pbank, builder, this);
                            }
                            userEventList.clear();
                        }

                        // Check endianness & source IDs
                        if (runChecks) {
                            for (int i=0; i < payloadBankQueues.size(); i++) {
                                // Check endianness
                                if (buildingBanks[i].getByteOrder() != ByteOrder.BIG_ENDIAN) {
System.out.println("All events sent to EMU must be BIG endian");
                                    throw new EmuException("events must be BIG endian");
                                }

                                // Check the source ID of this bank to see if it matches
                                // what should be coming over this channel.
                                if (buildingBanks[i].getSourceId() != payloadBankQueues.get(i).getSourceId()) {
                                    if (nonFatalError) System.out.println("bank tag = " + buildingBanks[i].getSourceId());
                                    if (nonFatalError) System.out.println("queue source id = " + payloadBankQueues.get(i).getSourceId());
                                    nonFatalError = true;
                                }
                            }
                        }

                        if (nonFatalError) System.out.println("\nERROR 1\n");

                        // All or none must be control events, else throw exception.
                        haveControlEvents = gotValidControlEvents(buildingBanks);
//System.out.println("BuildingThread: haveControlEvents = " + haveControlEvents);

                        // If they are all control events, just store their
                        // input order object and put 1 event on each output Q.
                        if (haveControlEvents) {
System.out.println("Have CONTROL event");
                            // Deal with the possibility that there are more output channels
                            // than input channels. In that case we must copy the control
                            // event and make sure one is placed on each output channel.
                            if (outputChannelCount <= buildingBanks.length) {
                                for (int j=0; j < outputChannelCount; j++) {
                                    // Store control event output order info in array
                                    buildingBanks[j].setAttachment(controlEventOrders[j]);
                                    bankToOutputChannel(buildingBanks[j], builder, this);
                                }
                            }
                            else {
                                buildingBanks[0].setAttachment(controlEventOrders[0]);
                                bankToOutputChannel(buildingBanks[0], builder, this);
                                for (int j=1; j < outputChannelCount; j++) {
                                    // copy first control event
                                    PayloadBank bb = new PayloadBank(buildingBanks[0]);
                                    bb.setAttachment(controlEventOrders[j]);
                                    // write to other output Q's
                                    bankToOutputChannel(bb, builder, this);
                                }
                            }

                            // If it is an END event, interrupt other build threads
                            // then quit this one.
                            if (buildingBanks[0].getType().isEnd()) {
System.out.println("Found END event in build thread");
                                haveEndEvent = true;
                                endBuildThreads(this, true);
                                return;
                            }

                            continue;
                        }

                        // At this point there are only physics or ROC raw events, which do we have?
                        havePhysicsEvents = buildingBanks[0].getType().isPhysics();

                        // Check for identical syncs, uniqueness of ROC ids,
                        // single-event-mode, and identical (physics or ROC raw) event types
                        nonFatalError |= checkConsistency(buildingBanks);

if (nonFatalError) System.out.println("\nERROR 2\n");

                        //--------------------------------------------------------------------
                        // Build trigger bank, number of ROCs given by number of buildingBanks
                        //--------------------------------------------------------------------
                        combinedTrigger = new EvioEvent(Evio.BUILT_TRIGGER_BANK,
                                                        DataType.SEGMENT,
                                                        buildingBanks.length + 2);
                        builder.setEvent(combinedTrigger);

                        // if building with Physics events ...
                        if (havePhysicsEvents) {
                            //-----------------------------------------------------------------------------------
                            // The actual number of rocs + 2 will replace num in combinedTrigger definition above
                            //-----------------------------------------------------------------------------------
                            // combine the trigger banks of input events into one (same if single event mode)
//System.out.println("BuildingThread: create trigger bank from built banks");
                            nonFatalError |= Evio.makeTriggerBankFromPhysics(buildingBanks, builder, ebId);
                        }
                        // else if building with ROC raw records ...
                        else {
                            // if in single event mode, build trigger bank differently
                            if (buildingBanks[0].isSingleEventMode()) {
                                // create a trigger bank from data in Data Block banks
//System.out.println("BuildingThread: create trigger bank in SEM");
                                nonFatalError |= Evio.makeTriggerBankFromSemRocRaw(buildingBanks, builder,
                                                                                   ebId, firstEventNumber, runNumber);
                            }
                            else {
                                // combine the trigger banks of input events into one
//System.out.println("BuildingThread: create trigger bank");
                                nonFatalError |= Evio.makeTriggerBankFromRocRaw(buildingBanks, builder,
                                                                                ebId, firstEventNumber, runNumber);
                            }
                        }

if (nonFatalError) System.out.println("\nERROR 3\n");
                        // print out trigger bank
//                        try {
//                            StringWriter sw2 = new StringWriter(1000);
//                            XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
//                            combinedTrigger.toXML(xmlWriter);
//                            System.out.println("\ncombined trigger bank:\n" + sw2.toString());
//
//                        }
//                        catch (XMLStreamException e) {
//                            e.printStackTrace();
//                        }

                        // check timestamps if requested
                        boolean requested = false;
                        if (requested) {
                            if (!Evio.timeStampsOk(combinedTrigger)) {
                                // Timestamps show problems with data
                                nonFatalError = true;
                                //throw new EmuException("Timestamps show problems with data");
                            }
                        }

                        // check payload banks for non-fatal errors when extracting them onto the payload queues
                        for (PayloadBank pBank : buildingBanks)  {
                            nonFatalError |= pBank.hasNonFatalBuildingError();
                        }

if (nonFatalError) System.out.println("\nERROR 4\n");

                        // create a physics event from payload banks and combined trigger bank
                        int tag = Evio.createCodaTag(buildingBanks[0].isSync(),
                                                     buildingBanks[0].hasError() || nonFatalError,
                                                     buildingBanks[0].isReserved(),
                                                     buildingBanks[0].isSingleEventMode(),
                                                     ebId);
//System.out.println("tag = " + tag + ", is sync = " + buildingBanks[0].isSync() +
//                   ", has error = " + (buildingBanks[0].hasError() || nonFatalError) +
//                   ", is reserved = " + buildingBanks[0].isReserved() +
//                   ", is single mode = " + buildingBanks[0].isSingleEventMode());

                        physicsEvent = new PayloadBank(tag, DataType.BANK, totalNumberEvents);
                        builder.setEvent(physicsEvent);
                        if (havePhysicsEvents) {
//System.out.println("BuildingThread: build physics event with physics banks");
                            Evio.buildPhysicsEventWithPhysics(combinedTrigger, buildingBanks, builder);
                        }
                        else {
//System.out.println("BuildingThread: build physics event with ROC raw banks");
                            Evio.buildPhysicsEventWithRocRaw(combinedTrigger, buildingBanks, builder);
                        }
                        physicsEvent.setAllHeaderLengths();
                        physicsEvent.setAttachment(evOrder); // store its input order info
                        physicsEvent.setType(EventType.PHYSICS);
                        physicsEvent.setEventCount(totalNumberEvents);
                        physicsEvent.setFirstEventNumber(firstEventNumber);

//                        try {
//                            StringWriter sw2 = new StringWriter(1000);
//                            XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
//                            physicsEvent.toXML(xmlWriter);
//                            System.out.println("\nphysics event:\n" + sw2.toString());
//
//                        }
//                        catch (XMLStreamException e) {
//                            e.printStackTrace();
//                        }
//                    ByteBuffer bbuf = ByteBuffer.allocate(2048);
//                    physicsEvent.write(bbuf);
//                    bbuf.flip();
//                    for (int j=0; j<bbuf.asIntBuffer().limit(); j++) {
//                        System.out.println(bbuf.asIntBuffer().get(j));
//                    }
//                    System.out.println("\n\n\n");

                        // Stick it on the local output Q (for this building thread).
                        // That way we don't waste time trying to coordinate between
                        // building threads right here - leave that to the QCollector thread.
                        bankToOutputChannel(physicsEvent, builder, this);

                        // stats  // TODO: protect since in multithreaded environs
                        eventCountTotal += totalNumberEvents;
                        wordCountTotal  += physicsEvent.getHeader().getLength() + 1;
                        lastEventNumberBuilt = firstEventNumber + eventCountTotal - 1;
                    }
                    catch (EmuException e) {
System.out.println("MAJOR ERROR building events");
                        state = CODAState.ERROR;
                        e.printStackTrace();
                        return; // ???
                    }

                    catch (InterruptedException e) {
                        //e.printStackTrace();
System.out.println("INTERRUPTED thread " + Thread.currentThread().getName());
                        //if (state == CODAState.DOWNLOADED) return;
                        return;
                    }
            }
            System.out.println("Building thread is ending !!!");

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

            if (buildingBanks[i].getType().isPhysics()) {
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
     * control events. If all are control events, eventually pass one copy to the output queue.
     * If only some are control events, throw exception as it must be all or none.
     * If none are control events, do nothing as the banks will be built into a single
     * event momentarily.
     *
     * @param buildingBanks array containing events that will be built together
     * @return <code>true</code> if a proper control event was built and output, else <code>false</code>
     * @throws org.jlab.coda.emu.EmuException if events contain mixture of control/data or control types
     * @throws InterruptedException if interrupted while putting control event on output queue
     */
    private boolean gotValidControlEvents(PayloadBank[] buildingBanks)
            throws EmuException, InterruptedException {

        int counter = 0;
        int controlEventCount = 0;
        int numberOfBanks = buildingBanks.length;
        EventType eventType;
        EventType[] types = new EventType[numberOfBanks];

        // count control events
        for (PayloadBank bank : buildingBanks) {
            // Might be a ROC Raw, Physics, or Control Event
            eventType = bank.getType();
            if (eventType.isControl()) {
                controlEventCount++;
            }
            types[counter++] = eventType;
        }

        // If one is a control event, all must be identical control events,
        // and only one gets passed to output.
        if (controlEventCount > 0) {
            // all events must be control events
            if (controlEventCount != numberOfBanks) {
System.out.println("     &&&&&&&&&&&&   Got " + controlEventCount +
" control events, but have " + numberOfBanks + " banks !!!");
                throw new EmuException("not all channels have control events");
            }

            // make sure all are the same type of control event
            eventType = types[0];
            for (int i=1; i < types.length; i++) {
                if (eventType != types[i]) {
                    throw new EmuException("different type control events on each channel");
                }
            }
System.out.println("Found control events of type " + eventType.name());

            return true;
        }

        return false;
    }


    /**
     * End all build threads because an END event came through one of them.
     * The build thread calling this method is not interrupted.
     *
     * @param thisThread the build thread calling this method; if null,
     *                   all build threads are interrupted
     * @param wait if <code>true</code> check if END event has arrived and
     *             if the Qs are empty, wait up to 1/2 second if not.
     */
    private void endBuildThreads(BuildingThread thisThread, boolean wait) {

        for (Thread thd : buildingThreadList) {
            if (thisThread != null && thisThread == thd) continue;

            if (wait) {
                // Look to see if anything still on the Qs
                int iter = 5, unprocessedEvents = 0;

                for (int i=0; i < payloadBankQueues.size(); i++) {
                    unprocessedEvents += payloadBankQueues.get(i).size();
                }

                // Wait up to 1/2 sec for events to be processed & END to arrive, then proceed
                while ((unprocessedEvents > 0 || !haveEndEvent) && (iter-- > 0)) {
                    try {Thread.sleep(100);}
                    catch (InterruptedException e) {}

                    unprocessedEvents = 0;
                    for (int i=0; i < payloadBankQueues.size(); i++) {
                        unprocessedEvents += payloadBankQueues.get(i).size();
                    }
                }

                if (unprocessedEvents > 0 || !haveEndEvent) {
System.out.println("Ending build thread but have no END event or Q not empty !!!");
                    state = CODAState.ERROR;
                }
            }

            thd.interrupt();
        }
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
     * Method getError returns the error of this EventBuilding2 object.
     *
     * @return the error (type Throwable) of this EventBuilding2 object.
     */
    public Throwable getError() {
        return lastError;
    }

    /** {@inheritDoc} */
    public void execute(Command cmd) {
        Date theDate = new Date();

        CODACommand emuCmd = cmd.getCodaCommand();

        if (emuCmd == END) {
            state = CODAState.DOWNLOADED;

            // The order in which these thread are shutdown does(should) not matter.
            // Rocs should already have been shutdown, followed by the input transports,
            // followed by this module (followed by the output transports).
            if (watcher  != null) watcher.interrupt();
            if (qFillers != null) {
                for (Thread qf : qFillers) {
                    qf.interrupt();
                }
            }

            // Build threads should be ended by END event
            endBuildThreads(null, true);

            watcher    = null;
            qFillers   = null;
            buildingThreadList.clear();

           if (inputOrders  != null) Arrays.fill(inputOrders, 0);
            if (outputOrders != null) Arrays.fill(outputOrders, 0);

            paused = false;

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            } catch (DataNotFoundException e) {
                e.printStackTrace();
            }
        }

        else if (emuCmd == RESET) {
            State previousState = state;
            state = CODAState.CONFIGURED;

            eventRate = wordRate = 0F;
            eventCountTotal = wordCountTotal = 0L;

            if (watcher  != null) watcher.interrupt();
            if (qFillers != null) {
                for (Thread qf : qFillers) {
                    qf.interrupt();
                }
            }

            endBuildThreads(null, false);

            watcher    = null;
            qFillers   = null;
            buildingThreadList.clear();

            if (inputOrders  != null) Arrays.fill(inputOrders, 0);
            if (outputOrders != null) Arrays.fill(outputOrders, 0);
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

        else if (emuCmd == PRESTART) {
            // make sure each input channel is associated with a unique rocId
            for (int i=0; i < inputChannels.size(); i++) {
                for (int j=i+1; j < inputChannels.size(); j++) {
                    if (inputChannels.get(i).getID() == inputChannels.get(j).getID()) {
                        // TODO: forget this exception ??
                        emu.getCauses().add(new EmuException("input channels duplicate rocIDs"));
                        state = CODAState.ERROR;
                        return;
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
                // add more queues
                if (add) {
                    // allow only 1000 items on the q at once
                    payloadBankQueues.add(new PayloadBankQueue<PayloadBank>(1000));
                }
                // remove excess queues
                else {
                    payloadBankQueues.remove();
                }
            }

            int qCount = payloadBankQueues.size();

            // clear all payload bank queues, associate each one with source ID, reset record ID
            for (int i=0; i < qCount; i++) {
                payloadBankQueues.get(i).clear();
                payloadBankQueues.get(i).setSourceId(inputChannels.get(i).getID());
                payloadBankQueues.get(i).setRecordId(0);
            }

            // how many output channels do we have?
            outputChannelCount = outputChannels.size();

            // allocate some arrays based on # of output channels
            waitingLists = null;
            if (outputChannelCount > 0) {
                locks        = new Object[outputChannelCount];
                for (int i=0; i < outputChannelCount; i++) {
                    locks[i] = new Object();
                }
                inputOrders  = new int[outputChannelCount];
                outputOrders = new int[outputChannelCount];
                outputOrders2 = new AtomicIntegerArray(outputChannelCount);

                waitingLists = new PriorityBlockingQueue[outputChannelCount];
                for (int i=0; i < outputChannelCount; i++) {
                    waitingLists[i] = new PriorityBlockingQueue<EvioBank>(100, comparator);
                }
            }

            // Reset some variables
            eventRate = wordRate = 0F;
            eventCountTotal = wordCountTotal = 0L;
            runNumber = emu.getRunNumber();
            ebRecordId = 0;
            eventNumber = 1L;
            lastEventNumberBuilt = 0L;

            // create threads objects (but don't start them yet)
            watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");
            for (int i=0; i < buildingThreadCount; i++) {
                BuildingThread thd1 = new BuildingThread(emu.getThreadGroup(), new BuildingThread(), name+":builder"+i);
                buildingThreadList.add(thd1);
            }
            qFillers = new Thread[qCount];
            for (int i=0; i < qCount; i++) {
                qFillers[i] = new Thread(emu.getThreadGroup(),
                                         new Qfiller(payloadBankQueues.get(i),
                                                        inputChannels.get(i).getQueue()),
                                         name+":qfiller"+i);
            }

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
            } catch (DataNotFoundException e) {
                emu.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        // currently NOT used
        else if (emuCmd == PAUSE) {
            System.out.println("EB: GOT PAUSE, DO NOTHING");
            paused = true;
        }

        else if (emuCmd == GO) {
            if (state == CODAState.ACTIVE) {
                System.out.println("WE musta hit go after PAUSE");
            }

            state = CODAState.ACTIVE;

            // start up all threads
            if (watcher == null) {
                watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");
            }
            if (watcher.getState() == Thread.State.NEW) {
                System.out.println("starting watcher thread");
                watcher.start();
            }

            if (buildingThreadList.size() < 1) {
                for (int i=0; i < buildingThreadCount; i++) {
                    BuildingThread thd1 = new BuildingThread(emu.getThreadGroup(), new BuildingThread(), name+":builder"+i);
                    buildingThreadList.add(thd1);
                }
            }
            else {
                System.out.println("EB: building thread Q is not empty, size = " + buildingThreadList.size());
            }
            int j=0;
            for (BuildingThread thd : buildingThreadList) {
                System.out.println("EB: building thread " + thd.getName() + " isAlive = " + thd.isAlive());
                if (thd.getState() == Thread.State.NEW) {
                    System.out.println("Start building thread " + (++j));
                    thd.start();
                }
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
                    System.out.println("Start qfiller thread " + i);
                    qFillers[i].start();
                }
            }

            paused = false;

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_start_time", theDate.toString());
            } catch (DataNotFoundException e) {
                emu.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        state = cmd.success();
    }

    protected void finalize() throws Throwable {
        logger.info("Finalize " + name);
        super.finalize();
    }

    /** {@inheritDoc} */
    public void setInputChannels(ArrayList<DataChannel> input_channels) {
        this.inputChannels = input_channels;
    }

    /** {@inheritDoc} */
    public void setOutputChannels(ArrayList<DataChannel> output_channels) {
        this.outputChannels = output_channels;
    }

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getInputChannels() {
        return inputChannels;
    }

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getOutputChannels() {
        return outputChannels;
    }
}