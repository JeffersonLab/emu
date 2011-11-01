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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
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
 *   in module's           \       |       /
 *  output channels         \      |      /
 *                           \     |     /
 *                            \    |    /
 *                             \   |   /
 *                              \  |  /
 *                               \ | /
 *   1 Payload Collector          PCT
 *   Thread which packages       / | \
 *   payload banks for          /  |  \
 *   Build Threads and         /   |   \
 *   stores on payload        /    |    \
 *   storage Q               /     |     \
 *                          /      |      \
 *                         /       |       \
 *                        V        V        V
 *  BuildingThreads:     BT1      BT2      BTM
 *  Grab 1 payload        |        |        |
 *  package from          |        |        |
 *  payload storage Q,    |        |        |
 *  build event,          |        |        |
 *  wrap in DTRs, &       |        |        |
 *  place (IN ORDER)      |        |        |
 *  in module's            \       |       /
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
 * PayloadCollector thread grabs one bank from each payload bank queue (and therefore
 * input channel), places them and other info into a payload storage object and places
 * that object onto the payload storage Q. (The idea here is that having one thread doing
 * that eliminates the needed for using a lock when accessing the payload bank Qs).
 * The BuildingThreads - of which there
 * may be any number - then each take one payload storage object off the payload storage
 * Q at a time, build it into a single event, wrap in a Data Transport Record,
 * and place it (in order) into one of the output channels (by round robin if
 * more than one or on all output channels if wrapping a control event).<p>
 *
 * NOTE: QFiller threads ignore any banks that are not DTRs. BuildingThread objects
 * immediately pass along any User events to their output queues. Any Control events
 * they find must appear on each payload queue in the same position. If not, an exception
 * is thrown. If so, the Control event is passed along to all output queues.
 * Finally, the Building threads place any User events in the first output
 * channel. Control & User events are not part of the round-robin output to each channel in turn.
 * If no output channels are defined in the config file, this module discards all events.
 */
public class EventBuilding2 implements EmuModule {


    /** Name of this event builder. */
    private final String name;

    /** ID number of this event builder obtained from config file. */
    private int ebId;

    /** Keep track of the number of records built in this event builder. Reset at prestart. */
    private volatile int ebRecordId;

    /** State of this module. */
    private volatile State state = CODAState.UNCONFIGURED;

    /** ArrayList of DataChannel objects that are inputs. */
    private ArrayList<DataChannel> inputChannels = new ArrayList<DataChannel>();

    /** ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    /**
     * There is one waiting list per output channel -
     * each of which stores built events until their turn to go over the
     * output channel has arrived.
     */
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

    /** Last error thrown by this module. */
    private final Throwable lastError = null;

    /**
     * Array of threads used to take Data Transport Records from
     * input channels, dissect them, and place resulting payload
     * banks onto payload queues.
     */
    private Thread qFillers[];

    private Thread payloadCollector;

    /** User hit PAUSE button if <code>true</code>. */
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

    // --------------------------------------------------------------

    /** Thread to update statistics. */
    private Thread watcher;

    /** Logger used to log messages to debug console. */
    private Logger logger;

    /** Emu this module belongs to. */
    private Emu emu;

    /** If <code>true</code>, then print sizes of various queues for debugging. */
    private boolean printQSizes;

    /**
     * If <code>true</code>, then each event building thread can put its built event
     * onto a waiting list if it is not next in line for the Q. That allows it
     * to continue building events instead of waiting for another thread to
     * build the event that is next in line.
     */
    private boolean useOutputWaitingList = true;

    /** If <code>true</code>, get debug print out. */
    private boolean debug = true;

    /** If <code>true</code>, check timestamps for consistency. */
    private boolean checkTimestamps;

    /**
     * The maximum difference in ticks for timestamps for a single event before
     * an error condition is flagged. Only used if {@link #checkTimestamps} is
     * <code>true</code>.
     */
    private int timestampSlop;

    /** Comparator which tells priority queue how to sort elements. */
    private BankComparator<EvioBank> comparator = new BankComparator<EvioBank>();


    /** Keep some data together and store as an event attachment. */
    private class EventOrder {
        /** Output channel to use. */
        DataChannel outputChannel;
        /** Index into arrays for this output channel. */
        int index;
        /** Place of event in output order of this output channel. */
        int inputOrder;
        /** Lock to use for output to this output channel. */
        Object lock;
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

    /**
     * Array of input orders - one for each output channel.
     * Keeps track of a built event's output order for a
     * particular output channel.
     * */
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
        }
        catch (NumberFormatException e) {}

        // default is to check timestamp consistency
        checkTimestamps = true;
        String str = attributeMap.get("tsCheck");
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
        @Override
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
                    logger.info("EventBuilding thread " + name() + " interrupted");
                }
            }
        }
    }


    /**
     * This class takes Data Transport Records from a queue (an input channel, eg. ROC),
     * extracts payload banks from those DTRs, and places the resulting banks in a payload
     * bank queue associated with that channel. All other types of events are ignored.
     * Nothing in this class depends on single event mode status.
     */
    private class Qfiller extends Thread {

        BlockingQueue<EvioBank> channelQ;
        PayloadBankQueue<PayloadBank> payloadBankQ;

        Qfiller(PayloadBankQueue<PayloadBank> payloadBankQ, BlockingQueue<EvioBank> channelQ) {
            this.channelQ = channelQ;
            this.payloadBankQ = payloadBankQ;
        }

        @Override
        public void run() {
            EvioBank channelBank;

            while (state == CODAState.ACTIVE || paused) {
                try {
                    while (state == CODAState.ACTIVE || paused) {
                        // block waiting for the next DTR from ROC.
                        channelBank = channelQ.take();  // blocks, throws InterruptedException
if (debug && printQSizes) {
    int size1 = channelQ.size();
    if (size1 > 400 && size1 % 100 == 0) System.out.println("in chan: " + size1);
}

                        // Is bank is in Data Transport Record format? If not, ignore it.
                        if ( Evio.isDataTransportRecord(channelBank) ) {
                            // Extract payload banks from DTR & place onto Q.
                            // May be blocked here waiting on a Q.
                            Evio.extractPayloadBanks(channelBank, payloadBankQ);
if (debug && printQSizes) {
    int size2 = payloadBankQ.size();
    if (size2 > 400 && size2 % 100 == 0) System.out.println("payload Q: " + size2);
}
                        }
                        else {
if (debug) System.out.println("Qfiller: got non-DTR bank, discard");
                        }
                    }
                } catch (EmuException e) {
                    // EmuException from Evio.extractPayloadBanks if dtrBank
                    // contains no data banks or record ID is out-of-sequence
if (debug) System.out.println("Qfiller: got empty Data Transport Record or record ID is out-of-sequence");
                    state = CODAState.ERROR;
                    return;
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }



    private class PayloadStorage {
        int totalNumberEvents = 1;
        long firstEventNumber = 1;
        EventType eventType;
        EventOrder eventOrder;
        PayloadBank[] buildingBanks = new PayloadBank[inputChannels.size()];
        // create these only if necessary
        EventOrder[] controlEventOrders;
        LinkedList<PayloadBank> userEventList;
    }


    LinkedBlockingQueue<PayloadStorage> payloadStorage = new LinkedBlockingQueue<PayloadStorage>(1000);


    /**
     * This class takes in payload banks associated with each input channel
     * (the output of the QFiller threads),
     * stores related banks together, and places them in a FIFO for the
     * BuildThreads to grab. This function was previously done by the BuildThread
     * objects themselves, but having a single dedicated thread do this eliminates
     * lock contention for the output of the QFiller threads.
     */
    private class PayloadCollector extends Thread {

        @Override
        public void run() {

            // initialize
            boolean gotBuildEvent;
            EventType eventType;

            int myInputOrder = -1;
            int myOutputChannelIndex = 0;
            Object myOutputLock = null;
            DataChannel myOutputChannel = null;

            while (state == CODAState.ACTIVE || paused) {

                try {

                    PayloadStorage storage = new PayloadStorage();

                    // The payload bank queues are filled by the QFiller thread.

                    // Here we have what we need to build:
                    //   ROC raw events from all ROCs, each with sequential record IDs.
                    //   However, there are also control events on queues.

                    // Put null into buildingBanks array elements
//                    Arrays.fill(storage.buildingBanks, null);

                    // reset flag
                    gotBuildEvent = false;

                    // Fill array with actual banks

                    // Grab one non-user bank from each channel.
                    // This algorithm retains the proper order of any user events.
                    for (int i=0; i < payloadBankQueues.size(); i++) {

                        // Loop until we get event which is NOT a user event
                        while (true) {

                            // will BLOCK here waiting for payload bank if none available
                            storage.buildingBanks[i] = payloadBankQueues.get(i).take();

                            eventType = storage.buildingBanks[i].getType();

                            // If event needs to be built ...
                            if (eventType.isROCRaw() || eventType.isPhysics()) {
                                // One-time init stuff for a group of
                                // records that will be built together.
                                if (!gotBuildEvent) {
                                    // Set flag
                                    gotBuildEvent = true;

                                    // Find the total # of events
                                    storage.totalNumberEvents = storage.buildingBanks[i].getHeader().getNumber();

                                    // Store first event number
                                    storage.firstEventNumber = eventNumber;

                                    // Calculate event number for next time through
                                    eventNumber += storage.totalNumberEvents;

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
                                EventOrder eo = null;

                                if (outputChannelCount > 0) {
                                    // User events go into first channel
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
                                }

if (debug) System.out.println("BuildingThread: Got user event");
                                // Store its output order info
                                storage.buildingBanks[i].setAttachment(eo);
                                // Stick it in a list
                                if (storage.userEventList == null) {
                                    storage.userEventList = new LinkedList<PayloadBank>();
                                }
                                storage.userEventList.add(storage.buildingBanks[i]);
                                // Since we got a user event, try again from the
                                // same input channel until we get one that isn't.
                                continue;
                            }

                            // If we're here, we've got control events.
                            // We want one EventOrder object for each output channel
                            // since we want one control event placed on each.
                            if (!gotBuildEvent) {
                                // Set flag
                                gotBuildEvent = true;

                                // Create control event output order array
                                if (storage.controlEventOrders == null) {
                                    storage.controlEventOrders = new EventOrder[outputChannelCount];
                                }

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
                                    storage.controlEventOrders[j] = eo;
                                }
                            }

                            // Go to next input channel
                            break;
                        }
                    }

                    // store all channel & order info here
                    EventOrder evOrder = new EventOrder();
                    evOrder.index = myOutputChannelIndex;
                    evOrder.outputChannel = myOutputChannel;
                    evOrder.lock = myOutputLock;
                    evOrder.inputOrder = myInputOrder;

                    storage.eventOrder = evOrder;

                    // put in Q
                    payloadStorage.put(storage);
                }
                catch (InterruptedException ex ) {
                    return;
                }
            }
        }
    }


    /**
     * This method is called by a build thread and is used to wrap a built
     * event in a Data Transport Record and place that onto the queue of an
     * output channel. If the event is not in next in line for the Q, it will
     * be put in a waiting list.
     *
     * @param bankOut the built event to wrap in a DTR and place on output channel queue
     * @param builder object used to build evio event
     *
     * @throws InterruptedException if wait, put, or take interrupted
     */
    private void bankToOutputChannel(PayloadBank bankOut, EventBuilder builder)
                    throws InterruptedException {

        // Have output channels?
        if (outputChannelCount < 1) {
            return;
        }

        EvioBank bank;
        EventOrder evOrder;
        EventOrder eo = (EventOrder)bankOut.getAttachment();

        int recordId = eo.inputOrder;

        // Wrap event-to-be-sent in Data Transport Record for next EB or ER
        EventType type = bankOut.getType();

        int dtrTag = Evio.createCodaTag(type.getValue(), ebId);
        EvioEvent dtrEvent = new PayloadBank(dtrTag, DataType.BANK, recordId);
        builder.setEvent(dtrEvent);

        try {
            // Add bank with full recordId
            bank = new EvioBank(Evio.RECORD_ID_BANK, DataType.INT32, 1);
            bank.appendIntData(new int[] {recordId});
            builder.addChild(dtrEvent, bank);
            // Add event
            builder.addChild(dtrEvent, bankOut);

        } catch (EvioException e) {/* never happen */}

        synchronized (eo.lock) {
            if (!useOutputWaitingList) {
                // Is the bank we grabbed next to be output? If not, wait.
                while (eo.inputOrder != outputOrders[eo.index]) {
                    eo.lock.wait();
                }
                // Place Data Transport Record on output channel
                eo.outputChannel.getQueue().put(dtrEvent);
                outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
                eo.lock.notifyAll();
            }
            // else if we're using waiting lists
            else {
                // Is the bank we grabbed next to be output?
                // If not, put in waiting list and return.
                if (eo.inputOrder != outputOrders[eo.index]) {
                    dtrEvent.setAttachment(eo);
                    waitingLists[eo.index].add(dtrEvent);

                    // If the waiting list gets too big, just wait here
                    if (waitingLists[eo.index].size() > 9) {
                        eo.lock.wait();
                    }
//if (debug) System.out.println("out of order = " + eo.inputOrder);
//if (debug) System.out.println("waiting list = ");
//                    for (EvioBank bk : waitingLists[eo.index]) {
//                        if (debug) System.out.println("" + ((EventOrder)bk.getAttachment()).inputOrder);
//                    }
                    return;
                }

                // Place Data Transport Record on output channel
                eo.outputChannel.getQueue().put(dtrEvent);
                outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
//if (debug) System.out.println("placing = " + eo.inputOrder);

                // Take a look on the waiting list without removing ...
                bank = waitingLists[eo.index].peek();
                while (bank != null) {
                    evOrder = (EventOrder) bank.getAttachment();
                    if (evOrder.inputOrder != outputOrders[eo.index]) {
                        break;
                    }
                    // Remove from waiting list permanently
                    bank = waitingLists[eo.index].take();
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

        BuildingThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        BuildingThread() {
            super();
        }

        @Override
        public void run() {

            // initialize
            boolean runChecks = true;
            PayloadStorage storage;
            boolean nonFatalError;
            boolean haveControlEvents;
            boolean havePhysicsEvents;
            EvioEvent combinedTrigger;
            PayloadBank physicsEvent;
            // this event not used, just need a builder
            EventBuilder builder = new EventBuilder(0, DataType.BANK, 0);

            while (state == CODAState.ACTIVE || paused) {

                try {
                    nonFatalError = false;

                    // Wait here to grab a package of payload inputs and other info
                    storage = payloadStorage.take();

                    // If we have any user events, stick those on the Q first.
                    // Source may be any of the inputs.
                    if (storage.userEventList != null && storage.userEventList.size() > 0) {
                        for (PayloadBank pbank : storage.userEventList) {
                            bankToOutputChannel(pbank, builder);
                        }
//                        storage.userEventList.clear();
                    }

                    // Check endianness & source IDs
                    if (runChecks) {
                        for (int i=0; i < payloadBankQueues.size(); i++) {
                            // Check endianness
                            if (storage.buildingBanks[i].getByteOrder() != ByteOrder.BIG_ENDIAN) {
if (debug) System.out.println("All events sent to EMU must be BIG endian");
                                throw new EmuException("events must be BIG endian");
                            }

                            // Check the source ID of this bank to see if it matches
                            // what should be coming over this channel.
                            if (storage.buildingBanks[i].getSourceId() != payloadBankQueues.get(i).getSourceId()) {
if (debug) System.out.println("bank tag = " + storage.buildingBanks[i].getSourceId());
if (debug) System.out.println("queue source id = " + payloadBankQueues.get(i).getSourceId());
                                nonFatalError = true;
                            }
                        }
                    }

if (debug && nonFatalError) System.out.println("\nERROR 1\n");

                    // All or none must be control events, else throw exception.
                    haveControlEvents = gotValidControlEvents(storage.buildingBanks);

                    // If they are all control events, just store their
                    // input order object and put 1 event on each output Q.
                    if (haveControlEvents) {
if (debug) System.out.println("Have CONTROL event");
                        // Deal with the possibility that there are more output channels
                        // than input channels. In that case we must copy the control
                        // event and make sure one is placed on each output channel.
                        if (outputChannelCount <= storage.buildingBanks.length) {
                            for (int j=0; j < outputChannelCount; j++) {
                                // Store control event output order info in array
                                storage.buildingBanks[j].setAttachment(storage.controlEventOrders[j]);
                                bankToOutputChannel(storage.buildingBanks[j], builder);
                            }
                        }
                        else {
                            storage.buildingBanks[0].setAttachment(storage.controlEventOrders[0]);
                            bankToOutputChannel(storage.buildingBanks[0], builder);
                            for (int j=1; j < outputChannelCount; j++) {
                                // Copy first control event
                                PayloadBank bb = new PayloadBank(storage.buildingBanks[0]);
                                bb.setAttachment(storage.controlEventOrders[j]);
                                // Write to other output Q's
                                bankToOutputChannel(bb, builder);
                            }
                        }

                        // If it is an END event, interrupt other build threads
                        // then quit this one.
                        if (storage.buildingBanks[0].getType().isEnd()) {
if (debug) System.out.println("Found END event in build thread");
                            haveEndEvent = true;
                            endBuildAndQFillerThreads(this, true);
                            return;
                        }

                        continue;
                    }

                    // At this point there are only physics or ROC raw events, which do we have?
                    havePhysicsEvents = storage.buildingBanks[0].getType().isPhysics();

                    // Check for identical syncs, uniqueness of ROC ids,
                    // single-event-mode, and identical (physics or ROC raw) event types
                    nonFatalError |= checkConsistency(storage.buildingBanks);

if (debug && nonFatalError) System.out.println("\nERROR 2\n");

                    //--------------------------------------------------------------------
                    // Build trigger bank, number of ROCs given by number of buildingBanks
                    //--------------------------------------------------------------------
                    combinedTrigger = new EvioEvent(Evio.BUILT_TRIGGER_BANK,
                                                    DataType.SEGMENT,
                                                    storage.buildingBanks.length + 2);
                    builder.setEvent(combinedTrigger);

                    // If building with Physics events ...
                    if (havePhysicsEvents) {
                        //-----------------------------------------------------------------------------------
                        // The actual number of rocs + 2 will replace num in combinedTrigger definition above
                        //-----------------------------------------------------------------------------------
                        // Combine the trigger banks of input events into one (same if single event mode)
//if (debug) System.out.println("BuildingThread: create trigger bank from built banks");
                        nonFatalError |= Evio.makeTriggerBankFromPhysics(storage.buildingBanks, builder, ebId,
                                                                         checkTimestamps, timestampSlop);
                    }
                    // else if building with ROC raw records ...
                    else {
                        // If in single event mode, build trigger bank differently
                        if (storage.buildingBanks[0].isSingleEventMode()) {
                            // Create a trigger bank from data in Data Block banks
//if (debug) System.out.println("BuildingThread: create trigger bank in SEM");
                            nonFatalError |= Evio.makeTriggerBankFromSemRocRaw(storage.buildingBanks, builder,
                                                                               ebId, storage.firstEventNumber,
                                                                               runNumber, checkTimestamps,
                                                                               timestampSlop);
                        }
                        else {
                            // Combine the trigger banks of input events into one
//if (debug) System.out.println("BuildingThread: create trigger bank");
                            nonFatalError |= Evio.makeTriggerBankFromRocRaw(storage.buildingBanks, builder,
                                                                            ebId, storage.firstEventNumber,
                                                                            runNumber, checkTimestamps,
                                                                            timestampSlop);
                        }
                    }

if (debug && nonFatalError) System.out.println("\nERROR 3\n");
                    // Print out trigger bank
//                    printEvent(combinedTrigger, "combined trigger");

                    // Check payload banks for non-fatal errors when
                    // extracting them onto the payload queues.
                    for (PayloadBank pBank : storage.buildingBanks)  {
                        nonFatalError |= pBank.hasNonFatalBuildingError();
                    }

if (debug && nonFatalError) System.out.println("\nERROR 4\n");

                    // Create a physics event from payload banks and combined trigger bank
                    int tag = Evio.createCodaTag(storage.buildingBanks[0].isSync(),
                                                 storage.buildingBanks[0].hasError() || nonFatalError,
                                                 storage.buildingBanks[0].isReserved(),
                                                 storage.buildingBanks[0].isSingleEventMode(),
                                                 ebId);
//if (debug) System.out.println("tag = " + tag + ", is sync = " + storage.buildingBanks[0].isSync() +
//                   ", has error = " + (storage.buildingBanks[0].hasError() || nonFatalError) +
//                   ", is reserved = " + storage.buildingBanks[0].isReserved() +
//                   ", is single mode = " + storage.buildingBanks[0].isSingleEventMode());

                    physicsEvent = new PayloadBank(tag, DataType.BANK, storage.totalNumberEvents);
                    builder.setEvent(physicsEvent);
                    if (havePhysicsEvents) {
//if (debug) System.out.println("BuildingThread: build physics event with physics banks");
                        Evio.buildPhysicsEventWithPhysics(combinedTrigger, storage.buildingBanks, builder);
                    }
                    else {
//if (debug) System.out.println("BuildingThread: build physics event with ROC raw banks");
                        Evio.buildPhysicsEventWithRocRaw(combinedTrigger, storage.buildingBanks, builder);
                    }

                    // setting header lengths done in Evio.buildPhysicsEventWith* methods
                    physicsEvent.setAllHeaderLengths();

                    physicsEvent.setAttachment(storage.eventOrder); // store its input order info
                    physicsEvent.setType(EventType.PHYSICS);
                    physicsEvent.setEventCount(storage.totalNumberEvents);
                    physicsEvent.setFirstEventNumber(storage.firstEventNumber);

//                    printEvent(physicsEvent, "physics event");

                    // Stick it on the local output Q (for this building thread).
                    // That way we don't waste time trying to coordinate between
                    // building threads right here - leave that to the QCollector thread.
                    bankToOutputChannel(physicsEvent, builder);

                    // stats  // TODO: protect since in multithreaded environs
                    eventCountTotal += storage.totalNumberEvents;
                    wordCountTotal  += physicsEvent.getHeader().getLength() + 1;
                    lastEventNumberBuilt = storage.firstEventNumber + eventCountTotal - 1;
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
//        ByteBuffer bbuf = ByteBuffer.allocate(2048);
//        bank.write(bbuf);
//        bbuf.flip();
//        for (int j = 0; j < bbuf.asIntBuffer().limit(); j++) {
//            System.out.println(bbuf.asIntBuffer().get(j));
//        }
//        System.out.println("\n\n\n");
//    }


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
     * @return <code>true</code> if non-fatal error occurred, else <code>false</code>
     * @throws org.jlab.coda.emu.EmuException if some events are in single event mode and others are not, or
     *                      if some physics and others ROC raw event types
     */
    private boolean checkConsistency(PayloadBank[] buildingBanks) throws EmuException {
        boolean nonFatalError = false;

        // For each ROC raw data record check the sync bit
        int syncBankCount = 0;

        // For each ROC raw data record check the single-event-mode bit
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

        // If one is a sync, all must be syncs
        if (syncBankCount > 0 && syncBankCount != buildingBanks.length) {
            // Some banks are sync banks and some are not
            nonFatalError = true;
        }

        // If one is a single-event-mode, all must be
        if (singleEventModeBankCount > 0 && singleEventModeBankCount != buildingBanks.length) {
            // Some banks are single-event-mode and some are not, so we cannot build at this point
            throw new EmuException("not all events are in single event mode");
        }

        // All must be physics or all must be ROC raw
        if (physicsEventCount > 0 && physicsEventCount != buildingBanks.length) {
            // Some banks are physics and some ROC raw
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
     * @throws org.jlab.coda.emu.EmuException if events contain mixture of control/data or control types
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
            eventType = bank.getType();
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
                throw new EmuException("not all channels have control events");
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
     * End all build and QFiller threads because an END event came through
     * one of them. The build thread calling this method is not interrupted.
     *
     * @param thisThread the build thread calling this method; if null,
     *                   all build & QFiller threads are interrupted
     * @param wait if <code>true</code> check if END event has arrived and
     *             if all the Qs are empty, if not, wait up to 1/2 second.
     */
    private void endBuildAndQFillerThreads(BuildingThread thisThread, boolean wait) {

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
            }

            if (haveUnprocessedEvents || !haveEndEvent) {
                if (debug) System.out.println("endBuildThreads: will end building/filling threads but no END event or Qs not empty !!!");
                state = CODAState.ERROR;
            }
        }

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


    /** {@inheritDoc} */
    public State state() {
        return state;
    }


    /**
     * Set the state of this object.
     * @param s the state of this object
     */
    public void setState(State s) {
        state = s;
    }


    /**
     * This method returns the error of this EventBuilding object.
     * @return error (type Throwable) of this EventBuilding object.
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
            if (watcher != null) watcher.interrupt();

            // Build & QFiller threads should already be ended by END event
            endBuildAndQFillerThreads(null, true);

            if (payloadCollector != null) payloadCollector.interrupt();

            watcher    = null;
            qFillers   = null;
            payloadCollector = null;
            buildingThreadList.clear();

            if (inputOrders  != null) Arrays.fill(inputOrders, 0);
            if (outputOrders != null) Arrays.fill(outputOrders, 0);

            payloadStorage.clear();

            paused = false;

            try {
                // Set end-of-run time in local XML config / debug GUI
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

            // Build & QFiller threads must be immediately ended
            endBuildAndQFillerThreads(null, false);

            if (payloadCollector != null) payloadCollector.interrupt();

            watcher    = null;
            qFillers   = null;
            payloadCollector = null;
            buildingThreadList.clear();

            if (inputOrders  != null) Arrays.fill(inputOrders, 0);
            if (outputOrders != null) Arrays.fill(outputOrders, 0);

            payloadStorage.clear();

            paused = false;

            if (previousState.equals(CODAState.ACTIVE)) {
                try {
                    // Set end-of-run time in local XML config / debug GUI
                    Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
                } catch (DataNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        else if (emuCmd == PRESTART) {
            // Make sure each input channel is associated with a unique rocId
            for (int i=0; i < inputChannels.size(); i++) {
                for (int j=i+1; j < inputChannels.size(); j++) {
                    if (inputChannels.get(i).getID() == inputChannels.get(j).getID()) {
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

            // Clear all payload bank queues, associate each one with source ID, reset record ID
            for (int i=0; i < qCount; i++) {
                payloadBankQueues.get(i).clear();
                payloadBankQueues.get(i).setSourceId(inputChannels.get(i).getID());
                payloadBankQueues.get(i).setRecordId(0);
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

            // Create threads objects (but don't start them yet)
            watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");
            payloadCollector = new Thread(emu.getThreadGroup(), new PayloadCollector(), name+":payloadCollector");
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
                // Set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
            } catch (DataNotFoundException e) {
                emu.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        // Currently NOT used
        else if (emuCmd == PAUSE) {
            paused = true;
        }

        else if (emuCmd == GO) {
            state = CODAState.ACTIVE;

            // Start up all threads
            if (watcher == null) {
                watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");
            }

            if (watcher.getState() == Thread.State.NEW) {
                watcher.start();
            }

            if (payloadCollector == null) {
                payloadCollector = new Thread(emu.getThreadGroup(), new PayloadCollector(), name+":payloadCollector");
            }

            if (payloadCollector.getState() == Thread.State.NEW) {
                payloadCollector.start();
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