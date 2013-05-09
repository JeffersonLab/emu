/*
 * Copyright (c) 2012, Jefferson Science Associates
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
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODAStateMachineAdapter;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <pre><code>
 * Input Channel
 * (evio bank Q):         IC_1
 *                        | \ \
 *                        |  \ \
 *                        |   \ \
 *                        |    \  \
 *                        |     \   \
 *                        |      \    \
 *                        |       \     \
 *                        |        \      \
 *                        V        V       V
 *  RecordingThreads:    RT_1      RT_2      RT_M
 *  Grab 1 bank &         |        |        |
 *  place (IN ORDER)      |        |        |
 *    in module's         |        |        |
 *  output channels       |        |        |
 *                        |        |        |
 *                         \       |       /
 *                          \      |      /
 *                           V     V     V
 * Output Channel(s):         OC_1 - OC_Z
 *
 *
 *  M = 1 by default
 * </code></pre><p>
 *
 * This class is the event recording module. It is a multithreaded module which can have
 * several recording threads. Each of these threads exists for the purpose of taking
 * Evio banks off of the 1 input channel and placing a copy of each bank into all of
 * the output channels. If no output channels are defined in the config file,
 * this module discards all events.
 */
public class EventRecording extends CODAStateMachineAdapter implements EmuModule {


    /** Name of this event recorder. */
    private final String name;

    /** ID number of this event recorder obtained from config file. */
    private int erId;

    /** State of this module. */
    private volatile State state = CODAState.BOOTED;

    /** Error message. */
    private String errorMsg;

    /** ArrayList of DataChannel objects that are inputs. */
    private ArrayList<DataChannel> inputChannels = new ArrayList<DataChannel>();

    /** Should only be one input DataChannel. */
    private DataChannel inputChannel;

    /** Input channel's queue. */
    private BlockingQueue<QueueItem> channelQ;

    /** ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    private QueueItemType   inType = QueueItemType.PayloadBank;
    private QueueItemType  outType = QueueItemType.PayloadBank;

    /**
     * There is one waiting list per output channel -
     * each of which stores built events until their turn to go over the
     * output channel has arrived.
     */
    private PriorityBlockingQueue<PayloadBank> waitingLists[];

    /** The number of RecordThread objects. */
    private int recordingThreadCount;

    /** Container for threads used to record events. */
    private LinkedList<RecordingThread> recordingThreadList = new LinkedList<RecordingThread>();

    /** Map containing attributes of this module given in config file. */
    private Map<String,String> attributeMap;

    /** Last error thrown by this module. */
    private final Throwable lastError = null;

    /** Lock to ensure that a RecordingThread grabs the same positioned event from each Q.  */
    private ReentrantLock getLock = new ReentrantLock();

    /** User hit PAUSE button if <code>true</code>. */
    private boolean paused;

    /** END event detected by one of the recording threads. */
    private volatile boolean haveEndEvent;

    /** Maximum time to wait when commanded to END but no END event received. */
    private long endingTimeLimit = 60000;

    /** Object used by Emu to be notified of END event arrival. */
    private EmuEventNotify endCallback;

    // The following members are for keeping statistics

    // TODO: make stats volatile??
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

    // ---------------------------------------------------

    /** Thread to update statistics. */
    private Thread watcher;

    /** Logger used to log messages to debug console. */
    private Logger logger;

    /** Emu this module belongs to. */
    private Emu emu;

    /**
     * If <code>true</code>, then each event recording thread can put its event
     * onto a waiting list if it is not next in line for the Q. That allows it
     * to continue recording events instead of waiting for another thread to
     * record the event that is next in line.
     */
    private boolean useOutputWaitingList = false;

    /** If <code>true</code>, get debug print out. */
    private boolean debug = false;

    /** If <code>true</code>, this module's statistics
     * accurately represent the statistics of the EMU. */
    private boolean representStatistics;

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
     * so recording threads can synchronize their output.
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
     * Constructor creates a new EventRecording instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public EventRecording(String name, Map<String, String> attributeMap, Emu emu) {
        this.emu = emu;
        this.name = name;
        this.attributeMap = attributeMap;

        logger = emu.getLogger();

        try {
            erId = Integer.parseInt(attributeMap.get("id"));
            if (erId < 0)  erId = 0;
        }
        catch (NumberFormatException e) { /* default to 0 */ }

        // default to 1 event recording thread
        recordingThreadCount = 1;
        try {
            recordingThreadCount = Integer.parseInt(attributeMap.get("threads"));
            if (recordingThreadCount < 1)  recordingThreadCount = 1;
            if (recordingThreadCount > 10) recordingThreadCount = 10;
        }
        catch (NumberFormatException e) {}
System.out.println("EventRecording constr: " + recordingThreadCount +
                           " number of event recording threads");

        // Does this module accurately represent the whole EMU's stats?
        String str = attributeMap.get("statistics");
        if (str != null) {
            if (str.equalsIgnoreCase("true") ||
                str.equalsIgnoreCase("on")   ||
                str.equalsIgnoreCase("yes"))   {
                representStatistics = true;
            }
        }

        // Do we want ByteBuffer or EvioEvent input (EvioEvent is default)?
        str = attributeMap.get("inType");
        if (str != null) {
            if (str.equalsIgnoreCase("ByteBuffer"))   {
                inType = QueueItemType.ByteBuffer;
            }
        }

        // Do we want ByteBuffer or EvioEvent output (EvioEvent is default)?
        str = attributeMap.get("outType");
        if (str != null) {
            if (str.equalsIgnoreCase("ByteBuffer"))   {
                outType = QueueItemType.ByteBuffer;
            }
        }

    }


    /** {@inheritDoc} */
    public String name() {
        return name;
    }

    public void registerEndCallback(EmuEventNotify callback) {
        endCallback = callback;
    };

    public EmuEventNotify getEndCallback() {return endCallback;};


    /** {@inheritDoc} */
    synchronized public Object[] getStatistics() {
        Object[] stats = new Object[4];

        // If we're not active, keep the accumulated
        // totals, but the rates are zero.
        if (state != CODAState.ACTIVE) {
            stats[0] = eventCountTotal;
            stats[1] = wordCountTotal;
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
        return representStatistics;
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

                        eventRate = (eventCountTotal - prevEventCount)*1000F/deltaT;
                        wordRate  = (wordCountTotal  - prevWordCount)*1000F/deltaT;

                        prevEventCount = eventCountTotal;
                        prevWordCount  = wordCountTotal;

                        t1 = t2;
                    }

                } catch (InterruptedException e) {
                    logger.info("EventRecording thread " + name() + " interrupted");
                }
            }
        }
    }


    /**
     * This method is called by a recording thread and is used to place
     * a bank onto the queue of an output channel. If the event is
     * not in next in line for the Q, it can be put in a waiting list.
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
                eo.outputChannel.getQueue().put(new QueueItem(bankOut));
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
//if (debug) System.out.println("out of order = " + eo.inputOrder);
//if (debug) System.out.println("waiting list = ");
//                    for (EvioBank bk : waitingLists[eo.index]) {
//                        if (debug) System.out.println("" + ((EventOrder)bk.getAttachment()).inputOrder);
//                    }
                    return;
                }

                // Place bank on output channel
                eo.outputChannel.getQueue().put(new QueueItem(bankOut));
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
                    eo.outputChannel.getQueue().put(new QueueItem(bank));
                    outputOrders[eo.index] = ++outputOrders[eo.index] % Integer.MAX_VALUE;
                    bank = waitingLists[eo.index].peek();
//if (debug) System.out.println("placing = " + evOrder.inputOrder);
                }
                eo.lock.notifyAll();
            }
        }

    }



    /**
     * This thread is started by the GO transition and runs while the state of the module is ACTIVE.
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * pulls one bank off the input DataChannel. The bank is copied and placed in each output
     * channel. The count of outgoing banks and the count of data words are incremented.
     */
    class RecordingThread extends Thread {

        RecordingThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        RecordingThread() {
            super();
        }

        @Override
        public void run() {
            if (recordingThreadCount == 1) {
                runOneThread();
            }
            else {
                runMultipleThreads();
            }
        }


        /**
         * When running more than 1 recording thread, things become
         *  more complex since they must play together nicely.
         */
        public void runMultipleThreads() {

System.out.println("Running runMultipleThreads()");
            // initialize
            int totalNumberEvents=1;
            QueueItem qItem;
            PayloadBank recordingBank;
            EventOrder[] eventOrders = new EventOrder[outputChannelCount];

            int myInputOrder = -1;
            int myOutputChannelIndex = 0;
            Object myOutputLock = null;

            DataChannel myOutputChannel = null;

            while (state == CODAState.ACTIVE || paused) {

                try {

                    try {
                        // Grab lock so we can get the next bank & fix its output order
                        getLock.lock();

                        // Will BLOCK here waiting for payload bank if none available
                        qItem = channelQ.take();  // blocks, throws InterruptedException
                        recordingBank = qItem.getPayloadBank();

                        // If we're here, we've got an event.
                        // We want one EventOrder object for each output channel
                        // since we want one identical event placed on each.

                        // Loop through the output channels and get
                        // them ready to accept an event.
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
                            eventOrders[j] = eo;
                        }
                    }
                    finally {
                        getLock.unlock();
                    }

                    if (outputChannelCount > 0) {
                        // We must copy the event and place one on each output channel.
                        recordingBank.setAttachment(eventOrders[0]);
                        bankToOutputChannel(recordingBank);
                        for (int j=1; j < outputChannelCount; j++) {
                            // Copy bank
                            PayloadBank bb = new PayloadBank(recordingBank);
                            bb.setAttachment(eventOrders[j]);
                            // Write to other output Q's
                            bankToOutputChannel(bb);
                        }
                    }

                    // If END event, interrupt other record threads then quit this one.
                    if (recordingBank.getControlType() == ControlType.END) {
if (true) System.out.println("Found END event in record thread");
                        haveEndEvent = true;
                        endRecordThreads(this, false);
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }

//                    synchronized (EventRecording.this) {
                    // stats  // TODO: protect since in multithreaded environs
                    eventCountTotal += totalNumberEvents;
                    wordCountTotal  += recordingBank.getHeader().getLength() + 1;
//                    }
                }
                catch (InterruptedException e) {
                    if (debug) System.out.println("INTERRUPTED thread " + Thread.currentThread().getName());
                    return;
                }
            }
            if (debug) System.out.println("recording thread is ending !!!");
        }


        /** When running only 1 recording thread, things can be greatly simplified. */
        public void runOneThread() {
System.out.println("Running runOneThread()");

            // initialize
            int totalNumberEvents=1;
            QueueItem qItem;
            PayloadBank recordingBank;

            while (state == CODAState.ACTIVE || paused) {

                try {
                    // Will BLOCK here waiting for payload bank if none available
                    qItem = channelQ.take();  // blocks, throws InterruptedException
                    recordingBank = qItem.getPayloadBank();

                    if (outputChannelCount > 0) {
                        // Place bank on first output channel queue
                        outputChannels.get(0).getQueue().put(qItem);

                        // Copy bank & write to other output channels' Q's
                        for (int j=1; j < outputChannelCount; j++) {
                            outputChannels.get(j).getQueue().put(new QueueItem(new PayloadBank(recordingBank)));
                        }
                    }

                    // If END event, quit this one & only recording thread
                    if (recordingBank.getControlType() == ControlType.END) {
if (true) System.out.println("Found END event in record thread");
                        haveEndEvent = true;
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }

                    eventCountTotal += totalNumberEvents;
                    wordCountTotal  += recordingBank.getHeader().getLength() + 1;
                }
                catch (InterruptedException e) {
                    if (debug) System.out.println("INTERRUPTED thread " + Thread.currentThread().getName());
                    return;
                }
            }
            if (debug) System.out.println("recording thread is ending !!!");
        }

    }


    /**
     * End all record threads because an END cmd or event came through.
     * The record thread calling this method is not interrupted.
     *
     * @param thisThread the record thread calling this method; if null,
     *                   all record threads are interrupted
     * @param wait if <code>true</code> check if END event has arrived and
     *             if all the Qs are empty, if not, wait up to 1/2 second.
     */
    private void endRecordThreads(RecordingThread thisThread, boolean wait) {

        if (wait) {
            // Look to see if anything still on the input channel Q
            long startTime = System.currentTimeMillis();

            boolean haveUnprocessedEvents = channelQ.size() > 0;

            // Wait up to endingTimeLimit millisec for events to
            // be processed & END event to arrive, then proceed
            while ((haveUnprocessedEvents || !haveEndEvent) &&
                   (System.currentTimeMillis() - startTime < endingTimeLimit)) {
                try {Thread.sleep(200);}
                catch (InterruptedException e) {}

                haveUnprocessedEvents = channelQ.size() > 0;
            }

            if (haveUnprocessedEvents || !haveEndEvent) {
                if (debug) System.out.println("endRecordThreads: will end recording threads but no END event or Q not empty !!!");
                state = CODAState.ERROR;
            }
        }

        // NOTE: EMU has a command executing thread which calls this ER module's execute
        // method which, in turn, calls this method when an END cmd is sent. In this case
        // all recording threads will be interrupted in the following code.

        // Interrupt all recording threads except the one calling this method
        for (Thread thd : recordingThreadList) {
            if (thd == thisThread) continue;
            thd.interrupt();
        }
    }



    /** {@inheritDoc} */
    public State state() {return state;}


    /** {@inheritDoc} */
    public String getError() {return errorMsg;}



    /** {@inheritDoc} */
    public void reset() {
        Date theDate = new Date();
        State previousState = state;
        state = CODAState.CONFIGURED;

        if (watcher != null) watcher.interrupt();

        // Recording threads must be immediately ended
        endRecordThreads(null, false);

        watcher = null;
        recordingThreadList.clear();

        if (inputOrders  != null) Arrays.fill(inputOrders, 0);
        if (outputOrders != null) Arrays.fill(outputOrders, 0);

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



    public void end() throws CmdExecException {
        state = CODAState.DOWNLOADED;

        // The order in which these thread are shutdown does(should) not matter.
        // Rocs should already have been shutdown, followed by the input transports,
        // followed by this module (followed by the output transports).
        if (watcher  != null) watcher.interrupt();

        // Recording threads should already be ended by END event
        endRecordThreads(null, true);

        watcher = null;
        recordingThreadList.clear();

        if (inputOrders  != null) Arrays.fill(inputOrders, 0);
        if (outputOrders != null) Arrays.fill(outputOrders, 0);

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
                if (inputChannels.get(i).getID() == inputChannels.get(j).getID()) {;
                    state = CODAState.ERROR;
                    throw new CmdExecException("input channels duplicate rocIDs");
                }
            }
        }

        state = CODAState.PAUSED;
        paused = true;

        // Make sure we have only one input channel
        if (inputChannels.size() != 1) {
            state = CODAState.ERROR;
            return;
        }

        // Clear input channel queue
        channelQ.clear();

        // How many output channels do we have?
//            outputChannelCount = outputChannels.size();

        // Allocate some arrays based on # of output channels
        waitingLists = null;
        if (outputChannelCount > 0 && recordingThreadCount > 1) {
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

        // Create & start threads
        createThreads();
        startThreads();

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
        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        } catch (DataNotFoundException e) {
            state = CODAState.ERROR;
            throw new CmdExecException("status/run_start_time entry not found in local config file");
        }
    }


    /**
     * Method to create thread objects for stats, filling Qs and recording events.
     */
    private void createThreads() {
        watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");

        for (int i=0; i < recordingThreadCount; i++) {
            RecordingThread thd1 = new RecordingThread(emu.getThreadGroup(), new RecordingThread(), name+":recorder"+i);
            recordingThreadList.add(thd1);
        }

        // Sanity check
        if (recordingThreadList.size() != recordingThreadCount) {
            System.out.println("Have " + recordingThreadList.size() + " recording threads, but want " +
                                       recordingThreadCount);
        }
    }

    /**
     * Method to start threads for stats, filling Qs, and recording events.
     * It creates these threads if they don't exist yet.
     */
    private void startThreads() {
        if (watcher == null) {
System.out.println("startThreads(): recreating watcher thread");
            watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");
        }

        if (watcher.getState() == Thread.State.NEW) {
            watcher.start();
        }

        if (recordingThreadList.size() < 1) {
            for (int i=0; i < recordingThreadCount; i++) {
                RecordingThread thd1 = new RecordingThread(emu.getThreadGroup(), new RecordingThread(), name+":recorder"+i);
                recordingThreadList.add(thd1);
            }
System.out.println("startThreads(): recreated recording threads, # = " +
                               recordingThreadList.size());
        }

        for (RecordingThread thd : recordingThreadList) {
            if (thd.getState() == Thread.State.NEW) {
                thd.start();
            }
        }
    }

    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {
        if (input_channels == null) return;
        this.inputChannels.addAll(input_channels);
        if (inputChannels.size() > 0) {
            inputChannel = inputChannels.get(0);
            channelQ = inputChannel.getQueue();
        }
    }

    /** {@inheritDoc} */
    public void addOutputChannels(ArrayList<DataChannel> output_channels) {
        if (output_channels == null) return;
        this.outputChannels.addAll(output_channels);
        outputChannelCount = outputChannels.size();
    }

    /**
     * Get the one input channel in use.
     * @return  the one input channel in use.
     */
    public DataChannel getInputChannel() {
        return inputChannel;
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
        inputChannel = null;
        channelQ = null;
    }
}