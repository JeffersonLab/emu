/*
 * Copyright (c) 2011, Jefferson Science Associates
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class simulates a Roc. It is a module which uses a single thread
 * to create events and send them to a single output channel.<p>
 * TODO: ET buffers have the number of events in them which varies from ROC to ROC.
 * @author timmer
 * (2011)
 */
public class RocSimulation extends CODAStateMachineAdapter implements EmuModule {


    /** Name of this ROC. */
    private final String name;

    /** ID number of this ROC obtained from config file. */
    private int rocId;

    /** Keep track of the number of records built in this ROC. Reset at prestart. */
    private volatile int rocRecordId;

    /** State of the module. */
    private volatile State state = CODAState.BOOTED;

    /**
     * Possible error message. reset() sets it back to null.
     * Making this an atomically settable String ensures that only 1 thread
     * at a time can change its value. That way it's only set once per error.
     */
    private AtomicReference<String> errorMsg = new AtomicReference<String>();

    /** OutputChannels is an ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    /** Thread used for generating events. */
    private EventGeneratingThread eventGeneratingThread;

    /** Map containing attributes of this module given in config file. */
    private Map<String,String> attributeMap;

    /** User hit pause button if <code>true</code>. */
    private boolean paused;

    /** Type of trigger sent from trigger supervisor. */
    private int triggerType;

    /** Is this ROC in single event mode? */
    private boolean isSingleEventMode;

    /** Number of events in each ROC raw record. */
    private int eventBlockSize;

    /** The id of the detector which produced the data in block banks of the ROC raw records. */
    private int detectorId;

    /**
     * Number of Evio events generated & sent before an END event is sent.
     * Value of 0 means don't end any END events automatically.
     */
    private int endLimit;

    /** Object used by Emu to be notified of END event arrival. */
    private EmuEventNotify endCallback;

    /** Number of writing threads to ask for in generating data for ROC Raw banks. */
    private int writeThreads;

    /** Limit the number of write jobs submitted to thread pool at any one time. */
    private Semaphore semaphore;

    /** Object used as lock to ensure ordered placement of events on output channel. */
    private Object lock = new Object();

    /** Number of Roc raw events generated by one call to data generating method. */
    private int numEvents;

    /** Size of a single generated Roc raw event in 32-bit words (including header). */
    private int eventWordSize;

    /** Keeps track of a generated event's output order for the output channel. */
    private int inputOrder;

    /** Keeps track of which generated event is next to be output on the output channel. */
    private int outputOrder;


    // The following members are for keeping statistics


    /** The number of the event to be assigned to that which is built next. */
    private long eventNumber;

    /** The number of the last event that this ROC created. */
    private long lastEventNumberCreated;

    /** Total number of EvioBank objects written to the outputs. */
    private long eventCountTotal;

    /** Sum of the sizes, in 32-bit words, of all EvioBank objects written to the outputs. */
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

    /** Used in debugging output to track each time an array of evio events is generated. */
    static int jobNumber;


    /**
     * Constructor RocSimulation creates a simulated ROC instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public RocSimulation(String name, Map<String, String> attributeMap, Emu emu) {
        String s;
        this.emu = emu;
        this.name = name;
        this.attributeMap = attributeMap;
        if (attributeMap == null) return;
        logger = emu.getLogger();

        // CODA id of this ROC
        try { rocId = Integer.parseInt(attributeMap.get("id")); }
        catch (NumberFormatException e) { /* defaults to 0 */ }
System.out.println("                                      SET ROCID TO " + rocId);
        emu.setCodaid(rocId);

        // Value for trigger type from trigger supervisor
        triggerType = 15;
        try { triggerType = Integer.parseInt(attributeMap.get("triggerType")); }
        catch (NumberFormatException e) { /* defaults to 15 */ }
        if (triggerType <  0) triggerType = 0;
        else if (triggerType > 15) triggerType = 15;

        // Id of detector producing data
        detectorId = 111;
        try { detectorId = Integer.parseInt(attributeMap.get("detectorId")); }
        catch (NumberFormatException e) { /* defaults to 111 */ }
        if (detectorId < 0) detectorId = 0;

        // How many entangled events in one data block?
        eventBlockSize = 1;
        try { eventBlockSize = Integer.parseInt(attributeMap.get("blockSize")); }
        catch (NumberFormatException e) { /* defaults to 1 */ }
        if (eventBlockSize <   1) eventBlockSize = 1;
        else if (eventBlockSize > 255) eventBlockSize = 255;

        // Number of data generating threads at a time
        writeThreads = 5;
        try { writeThreads = Integer.parseInt(attributeMap.get("threads")); }
        catch (NumberFormatException e) { /* defaults to 5 */ }
        if (writeThreads < 1) writeThreads = 1;
        else if (writeThreads > 20) writeThreads = 20;

        // Is this ROC in single-event-mode?
        s = attributeMap.get("SEMode");
        if (s != null) {
            if (s.equalsIgnoreCase("true") ||
                s.equalsIgnoreCase("on")   ||
                s.equalsIgnoreCase("yes"))   {
                isSingleEventMode = true;
            }
        }

        if (isSingleEventMode) {
            eventBlockSize = 1;
        }

        // How many events to generate before sending END event, defaults to 0 (no limit)
        endLimit = 0;
        try { endLimit = Integer.parseInt(attributeMap.get("end")); }
        catch (NumberFormatException e) { /* defaults to 0 */ }
        if (endLimit < 0) endLimit = 0;

        // the module sets the type of CODA class it is.
        emu.setCodaClass(CODAClass.ROC);
    }


    /** {@inheritDoc} */
    public String name() {return name;}

    /** {@inheritDoc} */
    public State state() {return state;}

    /** {@inheritDoc} */
    public void registerEndCallback(EmuEventNotify callback) {endCallback = callback;}

    /** {@inheritDoc} */
    public EmuEventNotify getEndCallback() {return endCallback;}

    /** {@inheritDoc} */
    public String getError() {return errorMsg.get();}

    /** {@inheritDoc} */
    protected void finalize() throws Throwable {super.finalize();}

    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {}

    /** {@inheritDoc} */
    public void addOutputChannels(ArrayList<DataChannel> output_channels) {
        this.outputChannels.addAll(output_channels);
    }

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getInputChannels() {return null;}

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getOutputChannels() {return outputChannels;}

    /** {@inheritDoc} */
    public void clearChannels() {outputChannels.clear();}

    /** {@inheritDoc} */
    public boolean representsEmuStatistics() {
        String stats = attributeMap.get("statistics");
        return (stats != null && stats.equalsIgnoreCase("on"));
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


    //---------------------------------------
    // Threads
    //---------------------------------------


    /** End all threads because a RESET/END cmd or END event came through.  */
    private void endThreads() {
        // The order in which these threads are shutdown does(should) not matter.
        // Transport objects should already have been shutdown followed by this module.
        if (watcher != null) watcher.interrupt();
        watcher = null;

        if (eventGeneratingThread != null) {
            try {
                // Kill this thread before thread pool threads to avoid exception.
//System.out.println("          RocSim endThreads: try joining ev-gen thread ...");
                eventGeneratingThread.join();
//System.out.println("          RocSim endThreads: done");
            }
            catch (InterruptedException e) {}

            try {
//System.out.println("          RocSim endThreads: try joining thread pool threads ...");
                eventGeneratingThread.getWriteThreadPool().shutdown();
                eventGeneratingThread.getWriteThreadPool().awaitTermination(100L, TimeUnit.MILLISECONDS);
//System.out.println("          RocSim endThreads: done");
            }
            catch (InterruptedException e) {}
        }
        eventGeneratingThread = null;
    }


    /**
     * This class defines a thread that makes instantaneous rate calculations
     * once every few seconds. Rates are sent to run control.
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
            long totalT=0L, offset=0L;
            float avgByteRate=0.F;
            boolean isFirstRound = true;

            while ((state == CODAState.ACTIVE) || paused) {
                try {
                    // In the paused state only wake every two seconds.
                    sleep(2000);

                    t1 = System.currentTimeMillis();

                    while (state == CODAState.ACTIVE) {
                        sleep(statGatheringPeriod);

                        t2 = System.currentTimeMillis();
                        deltaT = t2 - t1;
                        if (isFirstRound) {
                            offset = wordCountTotal;
                        }
                        else {
                            totalT += deltaT;
                        }

                        // calculate rates
                        eventRate   = (eventCountTotal - prevEventCount)*1000F/deltaT;
                        wordRate    = (wordCountTotal  - prevWordCount)*1000F/deltaT;
                        if (!isFirstRound) {
                            avgByteRate = (wordCountTotal-offset)*4000F/totalT;
                        }
                        isFirstRound = false;

                        t1 = t2;
                        prevEventCount = eventCountTotal;
                        prevWordCount  = wordCountTotal;
//System.out.println("evRate = " + eventRate + ", byteRate = " + 4*wordRate + ", avg = " + avgByteRate);
                    }

                } catch (InterruptedException e) {
                }
            }
System.out.println("RocSimulation module: quitting watcher thread");
        }
    }


    /**
     * This thread generates events with a bank of a single int array of junk - just to
     * take up space.
     * It is started by the GO transition and runs while the state of the module is ACTIVE.
     * <p/>
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * selects an output by taking the next one from a simple iterator. This thread then creates
     * data transport records with payload banks containing ROC raw records and places them on the
     * output DataChannel.
     * <p/>
     */
    class EventGeneratingThreadFake extends Thread {

        private volatile boolean quit;

        EventGeneratingThreadFake(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        EventGeneratingThreadFake() {
            super();
        }

        void killThread() {
            quit = true;
        }

        public void run() {

            int type = EventType.ROC_RAW.getValue();
            int counter = 0, totalCount = 0;
            long timestamp = 0L, start_time = System.currentTimeMillis();
            int tag = Evio.createCodaTag(type, rocId);

            int numEvents = 9200;
            //int numEvents = 920;

            // create event with jevio package
            EventBuilder eventBuilder = new EventBuilder(tag, DataType.BANK, rocRecordId);
            EvioBank event = eventBuilder.getEvent();

            // add a bank with no meaningful data in it - just to take up some size
            EvioBank intBank = new EvioBank(1, DataType.INT32, 0 /* updated later */);
            int[] fakeData = new int[499289-4];
            //int[] fakeData = new int[49928-4];
            try {
                eventBuilder.appendIntData(intBank, fakeData);
                eventBuilder.addChild(event, intBank);
            }
            catch (EvioException e) {/* never happen */}


            while (state == CODAState.ACTIVE || paused) {

                if (quit) return;

                try {
                    // Stick it on the output Q.
                    outputChannels.get(0).getQueue().put(new QueueItem(event));

                    // stats
                    rocRecordId++;
                    timestamp       += 4*numEvents;
                    eventNumber     += numEvents;
                    eventCountTotal += numEvents;
                    //wordCountTotal  += 49928;
                    wordCountTotal  += 499289;
                    lastEventNumberCreated = eventNumber - 1;
                    counter++;
                    totalCount++;

                    //dtr byte size = 1997156, numEv/dtr = 9200

                    long now = System.currentTimeMillis();
                    long deltaT = now - start_time;
                    if (deltaT > 2000) {
// TODO: DTR ? rate
                        System.out.println("DTR rate = " + String.format("%.3g", (counter*1000./deltaT) ) + " Hz");
                        start_time = now;
                        counter = 0;
                    }
                }
// TODO: problems here !!!!
                catch (InterruptedException e) {
System.out.println("INTERRUPTED thread " + Thread.currentThread().getName());
                    if (state == CODAState.DOWNLOADED) return;
                }
            }

        }

    }



    /**
     * This method is called by a DataGenerateJob running in a thread from a pool.
     * It generates many ROC Raw events in it with simulated
     * FADC250 data, and places them onto the queue of an output channel.
     *
     * @param bank the event to place on output channel queue
     * @throws InterruptedException if put or wait interrupted
     */
    private void eventToOutputQueue(PayloadBank bank) throws InterruptedException {

        int inputOrder = (Integer) bank.getAttachment();

        synchronized (lock) {
            // Is the bank we grabbed next to be output? If not, wait.
            while (inputOrder != outputOrder) {
                lock.wait();
            }

            // Place Data Transport Record on output channel
            outputChannels.get(0).getQueue().put(new QueueItem(bank));

            // next one to be put on output channel
            outputOrder = ++outputOrder % Integer.MAX_VALUE;
            lock.notifyAll();

            // stats
            eventCountTotal += numEvents;
            wordCountTotal  += eventWordSize;
        }
    }


    /**
     * This method is called by a DataGenerateJob running in a thread from a pool.
     * It generates many ROC Raw events in it with simulated
     * FADC250 data, and places them onto the queue of an output channel.
     *
     * @param banks the events to place on output channel queue
     * @throws InterruptedException if put or wait interrupted
     */
    private void eventToOutputQueue(PayloadBank[] banks) throws InterruptedException {

        int inputOrder = (Integer) banks[0].getAttachment();

        synchronized (lock) {
            // Is the bank we grabbed next to be output? If not, wait.
            while (inputOrder != outputOrder) {
                lock.wait();
            }

            // Place banks on output channel
            for (PayloadBank bank : banks) {
                outputChannels.get(0).getQueue().put(new QueueItem(bank));
            }

            // next group to be put on output channel
            outputOrder = ++outputOrder % Integer.MAX_VALUE;
            lock.notifyAll();

            // stats
            eventCountTotal += eventBlockSize*banks.length;
            wordCountTotal  += eventWordSize*banks.length;
        }
    }


    /**
     * This thread generates events with simulated FADC250 data in it.
     * It is started by the GO transition and runs while the state of the module is ACTIVE.
     * <p/>
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * selects an output by taking the next one from a simple iterator. This thread then creates
     * data transport records with payload banks containing ROC raw records and places them on the
     * output DataChannel.
     * <p/>
     */
    class EventGeneratingThread extends Thread {

        private ThreadPoolExecutor writeThreadPool;

        EventGeneratingThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
            // Run thread pool with "writeThreads" number of threads & fixed-sized queue.
            writeThreadPool = new ThreadPoolExecutor(writeThreads, writeThreads,
                                                     0L, TimeUnit.MILLISECONDS,
                                                     new LinkedBlockingQueue<Runnable>(2*writeThreads));

            writeThreadPool.prestartAllCoreThreads();

        }

        EventGeneratingThread() {
            super();
            writeThreadPool = new ThreadPoolExecutor(writeThreads, writeThreads,
                                                     0L, TimeUnit.MILLISECONDS,
                                                     new LinkedBlockingQueue<Runnable>(2*writeThreads));

            writeThreadPool.prestartAllCoreThreads();

        }

        ThreadPoolExecutor getWriteThreadPool() {
            return writeThreadPool;
        }

        public void run() {

           boolean sentOneAlready = false;
           int  status=0;
           long oldVal=0L, timestamp=0L, start_time = System.currentTimeMillis();

           DataGenerateJobNew job;
           semaphore = new Semaphore(2*writeThreads);

System.out.println("ROC SIM write thds = " + writeThreads);

           // Found out how many events are generated per method call, and the event size
           try {
               // don't use 1, cause you'll get single event mode
               numEvents = 2;
               PayloadBank[] evs = Evio.createRocDataEvents(rocId, triggerType,
                                                            detectorId, status,
                                                            0, eventBlockSize,
                                                            0L, 0,
                                                            numEvents,
                                                            isSingleEventMode);
               eventWordSize = evs[0].getHeader().getLength() + 1;
//System.out.println("ROCSim: each generated event data = " + (4*(eventWordSize - 2)) +
//                " bytes, words = " + (eventWordSize -2 ) + ", tag = " +
//                evs[0].getHeader().getTag() + ", num = " + evs[0].getHeader().getNumber());
//System.out.println("ROCSim: each generated event = " + evs[0].toXML());

               while (state == CODAState.ACTIVE || paused) {

                   numEvents = 2;

                   semaphore.acquire();

                   if (sentOneAlready && (endLimit > 0) && (eventNumber + numEvents > endLimit)) {
System.out.println("\nRocSim: hit event number limit of " + endLimit + ", quitting\n");

                       // Put in END event
                       try {
System.out.println("          RocSim: Putting in END control event");
                           EvioEvent controlEvent = Evio.createControlEvent(ControlType.END, 0, 0,
                                                                            (int)eventCountTotal, 0);
                           PayloadBank bank = new PayloadBank(controlEvent);
                           bank.setEventType(EventType.CONTROL);
                           bank.setControlType(ControlType.END);
                           outputChannels.get(0).getQueue().put(new QueueItem(bank));
                           if (endCallback != null) endCallback.endWait();
                       }
                       catch (InterruptedException e) {}
                       catch (EvioException e) {/* never happen */}

                       return;
                   }
                   sentOneAlready = true;

                   job = new DataGenerateJobNew(timestamp, status, rocRecordId, numEvents,
                                               (int) eventNumber, inputOrder);
                   writeThreadPool.execute(job);

                   inputOrder   = ++inputOrder % Integer.MAX_VALUE;
                   timestamp   += 4*eventBlockSize*numEvents;
                   eventNumber += eventBlockSize*numEvents;
                   rocRecordId++;

                   long now = System.currentTimeMillis();
                   long deltaT = now - start_time;
                   if (deltaT > 2000) {
System.out.println("event rate = " + String.format("%.3g", ((eventNumber-oldVal)*1000./deltaT) ) + " Hz");
                       start_time = now;
                       oldVal = eventNumber;
                   }
               }
           }
           catch (InterruptedException e) {}
           catch (Exception e) {
               // If we haven't yet set the cause of error, do so now & inform run control
               errorMsg.compareAndSet(null, e.getMessage());
               state = CODAState.ERROR;
               emu.sendStatusMessage();
               return;
           }
        }



        /**
         * This class is designed to create an evio bank's
         * contents by way of a thread pool.
         */
        private class DataGenerateJobNew implements Runnable {
            private int jobNum;

            private long timeStamp;
            private int status;
            private int recordId;
            private int numEvs;
            private int evNum;
            private int inputOrder;

            /** Constructor. */
            DataGenerateJobNew(long timeStamp, int status, int recordId, int numEvs, int evNum, int inputOrder) {
                this.evNum      = evNum;
                this.numEvs     = numEvs;
                this.status     = status;
                this.recordId   = recordId;
                this.timeStamp  = timeStamp;
                this.inputOrder = inputOrder;
                jobNum = jobNumber++;
            }

            // write bank into et event buffer
            public void run() {
                try {
                    // turn event into byte array
//System.out.println("RocSim("+jobNum+"): executing job");
                    PayloadBank[] evs = Evio.createRocDataEvents(rocId, triggerType,
                                                          detectorId, status,
                                                          evNum, eventBlockSize,
                                                          timeStamp, recordId,
                                                          numEvs,
                                                          isSingleEventMode);

                    // put generated events into output channel
//System.out.println("RocSim("+jobNum + "): generated " + evs.length + " Roc Raw events");
                    evs[0].setAttachment(inputOrder);
                    eventToOutputQueue(evs);
//System.out.println("RocSim("+jobNum + "): put evs on output Q");
                    semaphore.release();
                }
                catch (InterruptedException e) {}
            }
        }

   }


    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        Date theDate = new Date();
        State previousState = state;
        state = CODAState.CONFIGURED;

        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;

        endThreads();

        paused = false;

        if (previousState.equals(CODAState.ACTIVE)) {
            // set end-of-run time in local XML config / debug GUI
            try {
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            }
            catch (DataNotFoundException e) {}
        }
    }


    /** {@inheritDoc} */
    public void end() throws CmdExecException {
        state = CODAState.DOWNLOADED;

        endThreads();

        paused = false;

//            // Put in END event
//            try {
//System.out.println("          RocSim: Putting in END control event");
//                EvioEvent controlEvent = Evio.createControlEvent(ControlType.END, 0, 0,
//                                                                 (int)eventCountTotal, 0);
//                PayloadBank bank = new PayloadBank(controlEvent);
//                bank.setEventType(EventType.CONTROL);
//                bank.setControlType(ControlType.END);
//                outputChannels.get(0).getQueue().put(new QueueItem(bank));
//                if (endCallback != null) endCallback.endWait();
//            }
//            catch (InterruptedException e) {}
//            catch (EvioException e) {/* never happen */}

        // set end-of-run time in local XML config / debug GUI
        try {
            // Set end-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }


    /** {@inheritDoc} */
    public void prestart() {

        state = CODAState.PAUSED;

        // Reset some variables
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;
        rocRecordId = 0;
        eventNumber = 1L;
        lastEventNumberCreated = 0L;

        // create threads objects (but don't start them yet)
        watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");
        eventGeneratingThread = new EventGeneratingThread(emu.getThreadGroup(),
                                                          new EventGeneratingThread(),
                                                          name+":generator");

        // Put in PRESTART event
        try {
//System.out.println("          RocSim: Putting in PRESTART control event");
            EvioEvent controlEvent = Evio.createControlEvent(ControlType.PRESTART, emu.getRunNumber(),
                                                             emu.getRunType(), 0, 0);
            PayloadBank bank = new PayloadBank(controlEvent);
            bank.setEventType(EventType.CONTROL);
            bank.setControlType(ControlType.PRESTART);
            outputChannels.get(0).getQueue().put(new QueueItem(bank));
        }
        catch (InterruptedException e) {}
        catch (EvioException e) {/* never happen */}

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}
    }


    /** {@inheritDoc} */
    public void pause() {
//System.out.println("          RocSim: GOT PAUSE, DO NOTHING");
        paused = true;

        // Put in PAUSE event
        try {
//System.out.println("          RocSim: Putting in PAUSE control event");
            EvioEvent controlEvent = Evio.createControlEvent(ControlType.PAUSE, 0, 0,
                                                             (int)eventCountTotal, 0);
            PayloadBank bank = new PayloadBank(controlEvent);
            bank.setEventType(EventType.CONTROL);
            bank.setControlType(ControlType.PAUSE);
            outputChannels.get(0).getQueue().put(new QueueItem(bank));
        }
        catch (InterruptedException e) {}
        catch (EvioException e) {/* never happen */}
    }


    /** {@inheritDoc} */
    public void go() {
        if (state == CODAState.ACTIVE) {
//System.out.println("          RocSim: We musta hit go after PAUSE");
        }

        // Put in GO event
        try {
//System.out.println("          RocSim: Putting in GO control event");
            EvioEvent controlEvent = Evio.createControlEvent(ControlType.GO, 0, 0,
                                                             (int)eventCountTotal, 0);
            PayloadBank bank = new PayloadBank(controlEvent);
            bank.setEventType(EventType.CONTROL);
            bank.setControlType(ControlType.GO);
            outputChannels.get(0).getQueue().put(new QueueItem(bank));
        }
        catch (InterruptedException e) {}
        catch (EvioException e) {/* never happen */}

        state = CODAState.ACTIVE;

        // start up all threads
        if (watcher == null) {
            watcher = new Thread(emu.getThreadGroup(), new Watcher(), name+":watcher");
        }

        if (watcher.getState() == Thread.State.NEW) {
            watcher.start();
        }

        if (eventGeneratingThread == null) {
            eventGeneratingThread = new EventGeneratingThread(emu.getThreadGroup(),
                    new EventGeneratingThread(),
                    name+":generator");
        }

//System.out.println("ROC: event generating thread " + eventGeneratingThread.getName() + " isAlive = " +
//                    eventGeneratingThread.isAlive());
        if (eventGeneratingThread.getState() == Thread.State.NEW) {
//System.out.println("starting event generating thread");
            eventGeneratingThread.start();
        }

        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}

    }


}