package modules;

import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.emu.support.data.EventType;
import org.jlab.coda.emu.support.data.PayloadBank;
import org.jlab.coda.emu.support.data.PayloadBankQueue;
import org.jlab.coda.jevio.*;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.ByteOrder;

/**
 * The event building module. Has separate thread for ordering output of multiple building threads
 * (and wrapping in DTR events). Control and User events are taken care.
 * TODO: ET buffers have the number of events in them which varies from ROC to ROC.
 */
public class EventBuilding3 implements EmuModule, Runnable {


    /** Name of this event builder. */
    private final String name;

    /** ID number of this event builder obtained from config file. */
    private int ebId;

    /** Keep track of the number of events built in this event builder. */
    private int ebRecordId;

    /** Field state is the state of the module */
    private State state = CODAState.UNCONFIGURED;

    /** Field inputChannels is an ArrayList of DataChannel objects that are inputs. */
    private ArrayList<DataChannel> inputChannels = new ArrayList<DataChannel>();

    /** Field outputChannels is an ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    /** Container for queues used to hold payload banks taken from Data Transport Records. */
    private LinkedList<PayloadBankQueue<PayloadBank>> payloadBankQueues =
            new LinkedList<PayloadBankQueue<PayloadBank>>();

    private int buildingThreadCount;

    /** Container for queues used to hold payload banks taken from Data Transport Records. */
    private LinkedList<BuildingThread> buildingThreadQueue = new LinkedList<BuildingThread>();

    /** Map containing attributes of this module given in config file. */
    private Map<String,String> attributeMap;

    /** Thread object that is the main thread of this module. */
    private Thread actionThread;

    /** Field lastError is the last error thrown by the module */
    private final Throwable lastError = null;

    /** Threads used to take Data Transport Records from input channels, dissect them,
     *  and place resulting payload banks onto payload queues. */
    private Thread qFillers[];

    private Thread qCollector;

    /** Lock to ensure that a bulding thread grabs the same event from each Q.  */
    private ReentrantLock getLock = new ReentrantLock();


    private int inputOrder;
    private int outputOrder;


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

    /** Field watcher */
    private Thread watcher;

    private boolean paused;

    /**
     * This class defines a thread that copies the event number and data count into the EMU status
     * once every 1/2 second. This is much more efficient than updating the status
     * every time that the counters are incremented. Not currently used. Can't remember why
     * we needed to write values into xml doc.
     */
    private class WatcherOld extends Thread {
        /**
         * Method run is the action loop of the thread. It executes while the module is in the
         * state ACTIVE or PRESTARTED. It is exited on end of run or reset.
         * It is started by the GO transition.
         */
        public void run() {
            while ((state == CODAState.ACTIVE) || (state == CODAState.PRESTARTED)) {
                try {
                    // In the paused state only wake every two seconds.
                    sleep(2000);

                    // synchronized to prevent problems if multiple watchers are running
                    synchronized (this) {
                        while (state == CODAState.ACTIVE) {
                            sleep(500);
                            Configurer.setValue(Emu.INSTANCE.parameters(), "status/eventCount", Long.toString(eventCountTotal));
                            Configurer.setValue(Emu.INSTANCE.parameters(), "status/wordCount", Long.toString(wordCountTotal));
//                            Configurer.newValue(Emu.INSTANCE.parameters(), "status/wordCount",
//                                                "CarlsModule", Long.toString(wordCountTotal));
                        }
                    }

                } catch (InterruptedException e) {
                    Logger.info("ProcessTest thread " + name() + " interrupted");
                } catch (DataNotFoundException e) {
                    e.printStackTrace();
                }
            }
System.out.println("ProcessTest module: quitting watcher thread");
        }
    }


    /**
     * This class defines a thread that makes instantaneous rate calculations
     * once every few seconds. Rates are sent to runcontrol.
     */
    private class Watcher extends Thread {
        /**
         * Method run is the action loop of the thread. It executes while the module is in the
         * state ACTIVE or PRESTARTED. It is exited on end of run or reset.
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
                    Logger.info("ProcessTest thread " + name() + " interrupted");
                }
            }
System.out.println("ProcessTest module: quitting watcher thread");
        }
    }



    /**
     * This class places payload banks parsed from a Data Transport Record or DTR (taken from
     * a channel (ROC)) onto a payload bank queue associated with that channel. All other types
     * of events are ignored. Nothing in this class depends on single event mode status.<p>
     */
    private class Qfiller extends Thread {

        BlockingQueue<EvioBank> channelQ;
        PayloadBankQueue<PayloadBank> payloadBankQ;

        public Qfiller(PayloadBankQueue<PayloadBank> payloadBankQ, BlockingQueue<EvioBank> channelQ) {
            this.channelQ = channelQ;
            this.payloadBankQ = payloadBankQ;
        }

        public void run() {
            EvioBank channelBank;

            while (state == CODAState.ACTIVE || paused) {
                try {
                    while (state == CODAState.ACTIVE || paused) {
                        // block waiting for the next DTR from ROC.
                        channelBank = channelQ.take();  // blocks, throws InterruptedException

                        // Is bank is in Data Transport Record format? If not, ignore it.
                        if ( Evio.isDataTransportRecord(channelBank) ) {
                            // Extract payload banks from DTR & place onto Q.
                            // May be blocked here waiting on a Q.
                            Evio.extractPayloadBanks(channelBank, payloadBankQ);
//if (payloadBankQ.size() > 998) System.out.println("payQ " + Thread.currentThread().getName() + " = " + payloadBankQ.size());
                        }
                        else {
System.out.println("Qfiller: got non-DTR bank, discard");
                        }
                    }

                } catch (EmuException e) {
                    // TODO: do something
                } catch (InterruptedException e) {
                    if (state == CODAState.DOWNLOADED) return;
                }
            }
        }
    }


    /**
     * This class places payload banks parsed from a Data Transport Record or DTR (taken from
     * a channel (ROC)) onto a payload bank queue associated with that channel. All other types
     * of events are ignored. Nothing in this class depends on single event mode status.<p>
     */
    private class QCollector extends Thread {

        public void run() {
            int dtrTag;
            EvioEvent dtrEvent;
            EvioBank bank;
            EventBuilder builder = new EventBuilder(0, DataType.BANK, 0); // this event not used, just need a builder
            EventType eventType;
            int counter = 0;

            while (state == CODAState.ACTIVE) {

                // have output channels?
                boolean hasOutputs = !outputChannels.isEmpty();

                try {

                    while (state == CODAState.ACTIVE || paused) {

                        if (hasOutputs) {

                            int index=0;
                            int size = buildingThreadQueue.size();
                            PayloadBank[] banks = new PayloadBank[size];
                            BuildingThread[] threads = new BuildingThread[size];
                            for (BuildingThread thread : buildingThreadQueue) {
                                threads[index] = thread;
                                banks[index] = thread.outputQueue.take();
                                index++;
                            }
                            index = 0;

                            while (state == CODAState.ACTIVE || paused) {

                                if ((Integer)(banks[index].getAttachment()) == outputOrder) {
// System.out.print("\nGood, banks[" + index + "] = " + outputOrder + "\n");
                                    // wrap event-to-be-sent in Data Transport Record for next EB or ER
                                    dtrTag = Evio.createCodaTag(EventType.PHYSICS.getValue(), ebId);
                                    dtrEvent = new PayloadBank(dtrTag, DataType.BANK, ebRecordId);
                                    builder.setEvent(dtrEvent);

                                    try {
                                        // add bank with full recordId
                                        bank = new EvioBank(Evio.RECORD_ID_BANK, DataType.INT32, 1);
                                        bank.appendIntData(new int[] {ebRecordId});
                                        builder.addChild(dtrEvent, bank);
                                        // add event
                                        builder.addChild(dtrEvent, banks[index]);
                                        // TODO: how do we set this initially????
                                        ebRecordId++;

                                        dtrEvent.setAllHeaderLengths();  // TODO: necessary?

                                    } catch (EvioException e) {/* never happen */}


                                    eventType = banks[index].getType();
                                    if (eventType.isPhysics()) {
//System.out.println("out Chan " + Thread.currentThread().getName() + " = " +
//   outputChannels.get(outputOrder % outputChannels.size()).getQueue().size());
                                        outputChannels.get(outputOrder % outputChannels.size()).getQueue().put(banks[index]);
                                        // stats
                                        eventCountTotal += banks[index].getEventCount();              // event count
                                        wordCountTotal  += banks[index].getHeader().getLength() + 1;  //  word count
                                    }
                                    else {
                                        // usr or control events are not part of the round-robin output
                                        outputChannels.get(0).getQueue().put(banks[index]);
                                    }
                                    banks[index] = threads[index].outputQueue.take();
                                    outputOrder = (outputOrder + 1) % Integer.MAX_VALUE;
                                }
//                                else {
//                                    for (int i=0; i < banks.length; i++) {
//                                        System.out.print(" banks[" + i + "] = " + (Integer)(banks[i].getAttachment()));
//                                    }
//                                    System.out.println("");
//                                    if (counter++ > 300) System.exit(-1);

                                    index = (index + 1) % size;
//                                }
                            }
                        }
                    }

                } catch (InterruptedException e) {
                    if (state == CODAState.DOWNLOADED) return;
                }
            }
        }
    }


    /**
     * Constructor ProcessTest creates a new EventBuilding instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public EventBuilding3(String name, Map<String,String> attributeMap) {
        this.name = name;
        this.attributeMap = attributeMap;
        try {
            ebId = Integer.parseInt(attributeMap.get("id"));
        }
        catch (NumberFormatException e) { /* default to 0 */ }

        // default to 3 event building threads
        buildingThreadCount = 3;
        try {
            buildingThreadCount = Integer.parseInt(attributeMap.get("threads"));
System.out.println("\nSetting #### of threads to " + buildingThreadCount + "\n");
        }
        catch (NumberFormatException e) { /* default to 0 */ }

// System.out.println("**** HEY, HEY someone created one of ME (modules.ProcessTest object) ****");
// System.out.println("**** LOADED NEW CLASS, DUDE!!! (modules.ProcessTest object) ****");
    }

    public String name() {
        return name;
    }

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
//        buildingThreadQueue.add(builder1);
//        buildingThreadQueue.add(builder2);
//        buildingThreadQueue.add(builder3);
//        builder1.start();
//        builder2.start();
//        builder3.start();
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

        BlockingQueue<PayloadBank> outputQueue = new ArrayBlockingQueue<PayloadBank>(1000);

        public BuildingThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        public BuildingThread() {
            super();
        }

        public void run() {

            // initialize
            int myInputOrder=-1;
            int totalNumberEvents;
            boolean gotUserEvent;
            boolean nonFatalError;
            boolean gotControlEvents;
            EvioEvent combinedTrigger;
            PayloadBank physicsEvent;
            PayloadBank[] buildingBanks = new PayloadBank[inputChannels.size()];
            EventBuilder builder = new EventBuilder(0, DataType.BANK, 0); // this event not used, just need a builder
            LinkedList<PayloadBank> userEventList = new LinkedList<PayloadBank>();


            while (state == CODAState.ACTIVE || paused) {

                try {

                    try {
                        nonFatalError = false;

                        // The payload bank queues are filled by the QFiller thread.

                        // Here we have what we need to build:
                        // ROC raw events from all ROCs, each with sequential record IDs.
                        // However, there are also control events on queues.

                        Arrays.fill(buildingBanks, null);

                        try {
                            getLock.lock();

                            // Grab one non-user bank from each channel.
                            // This algorithm retains the proper order of
                            // any user events.
                            do {
                                gotUserEvent = false;

                                for (int i=0; i < payloadBankQueues.size(); i++) {
                                    // will block waiting for payload bank
                                    if (buildingBanks[i] == null) {
                                        buildingBanks[i] = payloadBankQueues.get(i).take();
                                    }

                                    // Check immediately if it is a user event.
                                    // If it is, stick it in a list and get another.
                                    if (buildingBanks[i].getType() == EventType.USER) {
                                        myInputOrder = inputOrder++ % Integer.MAX_VALUE;
//System.out.println("BuldingThread: Got user event, order = " + myInputOrder);
                                         // store its output order
                                        buildingBanks[i].setAttachment(myInputOrder);
                                        // stick it in a list
                                        userEventList.add(buildingBanks[i]);
                                        // try for another
                                        buildingBanks[i] = null;
                                        gotUserEvent = true;
                                    }
                                }

                            // if we got a user event go around again until we get a non-user event in that slot
                            } while (gotUserEvent);

                            myInputOrder = inputOrder++ % Integer.MAX_VALUE;
                        }
                        finally {
                            getLock.unlock();
                        }
//System.out.println("BuldingThread: out");

                        // If we got any user events, stick those on the Q first.
                        // Source may be any of the inputs.
                        if (userEventList.size() > 0) {
                            for (PayloadBank pbank : userEventList) {
                                outputQueue.put(pbank);
                            }
                            userEventList.clear();
                        }

                        for (int i=0; i < payloadBankQueues.size(); i++) {
                            // Check endianness
                            if (buildingBanks[i].getByteOrder() != ByteOrder.BIG_ENDIAN) {
                                // TODO: major error, do something
                                System.out.println("All events sent to EMU must be BIG endian");
                            }

                            // Check the source ID of this bank to see if it matches
                            // what should be coming over this channel.
                            if (!Evio.idsMatch(buildingBanks[i], payloadBankQueues.get(i).getSourceId())) {
                                if (nonFatalError) System.out.println("bank tag = " + buildingBanks[i].getHeader().getTag());
                                if (nonFatalError) System.out.println("queue source id = " + payloadBankQueues.get(i).getSourceId());
                                nonFatalError = true;
                            }
                        }
                        if (nonFatalError) System.out.println("\nERROR 1\n");

                        // all or none must be control events, else throw exception
                        gotControlEvents = gotValidControlEvents(buildingBanks);

                        // if its a control event, just store its input order
                        if (gotControlEvents) {
                            buildingBanks[0].setAttachment(myInputOrder); // store its output order
                            outputQueue.put(buildingBanks[0]);
                        }
                        else {
                            // Check for identical syncs, uniqueness of ROC ids,
                            // single-event-mode, and identical (physics or ROC raw) event types
                            nonFatalError |= checkConsistency(buildingBanks);

                            if (nonFatalError) System.out.println("\nERROR 2\n");

                            // are we building with physics events or not (ROC raw records)?
                            boolean havePhysicsEvents = buildingBanks[0].getType().isPhysics();

                            // Build trigger bank, number of ROCs given by number of buildingBanks
                            combinedTrigger = new EvioEvent(Evio.BUILT_TRIGGER_BANK,
                                                            DataType.SEGMENT,
                                                            buildingBanks.length + 1);
                            builder.setEvent(combinedTrigger);

                            // if building with Physics events ...
                            if (havePhysicsEvents) {
                                //-----------------------------------------------------------------------------------
                                // The actual number of rocs + 1 will replace num in combinedTrigger definition above
                                //-----------------------------------------------------------------------------------
                                // combine the trigger banks of input events into one (same if single event mode)
                                nonFatalError |= Evio.makeTriggerBankFromPhysics(buildingBanks, builder, ebId);
                            }
                            // else if building with ROC raw records ...
                            else {
                                // if in single event mode, build trigger bank differently
                                if (buildingBanks[0].isSingleEventMode()) {
                                    // create a trigger bank from data in Data Block banks
                                    nonFatalError |= Evio.makeTriggerBankFromSemRocRaw(buildingBanks, builder, ebId);
                                }
                                else {
                                    // combine the trigger banks of input events into one
                                    nonFatalError |= Evio.makeTriggerBankFromRocRaw(buildingBanks, builder, ebId);
                                }
                            }

                            // at this point we found (and cleverly stored)
                            // the first event number & total # of events
                            totalNumberEvents = buildingBanks[0].getEventCount();
                            // lowest 16 bits only if from physics event
//                            firstEventNumber  = buildingBanks[0].getFirstEventNumber();

                            if (nonFatalError) System.out.println("\nERROR 3\n");

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
//                   ", is single mode = " + buildingBanks[0].isSingleEventMode());/

                            physicsEvent = new PayloadBank(tag, DataType.BANK, 0xCC);
                            builder.setEvent(physicsEvent);
                            if (havePhysicsEvents) {
                                Evio.buildPhysicsEventWithPhysics(combinedTrigger, buildingBanks, builder);
                            }
                            else {
                                Evio.buildPhysicsEventWithRocRaw(combinedTrigger, buildingBanks, builder);
                            }
                            physicsEvent.setAllHeaderLengths();
                            physicsEvent.setAttachment(myInputOrder); // store its output order
                            physicsEvent.setType(EventType.PHYSICS);
                            physicsEvent.setEventCount(totalNumberEvents);

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
if (outputQueue.size() > 998) System.out.println("outQ " + Thread.currentThread().getName() + " = " + outputQueue.size());
                            outputQueue.put(physicsEvent);
                        } // if we had no control events

                    }
                    catch (EmuException e) {
                        // TODO: major error getting data events to build, do something ...
                        System.out.println("MAJOR ERROR building events");
                        e.printStackTrace();
                    }


                } catch (InterruptedException e) {
                    //e.printStackTrace();
System.out.println("INTERRUPTED thread " + Thread.currentThread().getName());
                    if (state == CODAState.DOWNLOADED) return;
                }
            }

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
                throw new EmuException("some channels have control events and some do not");
            }

            // make sure all are the same type of control event
            eventType = types[0];
            for (int i=1; i < types.length; i++) {
                if (eventType != types[i]) {
                    throw new EmuException("different type control events on each channel");
                }
            }

            return true;
        }

        return false;
    }



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
     * Method getError returns the error of this ProcessTest object.
     *
     * @return the error (type Throwable) of this ProcessTest object.
     */
    public Throwable getError() {
        return lastError;
    }

    public void execute(Command cmd) {
        Date theDate = new Date();

        if (cmd.equals(CODATransition.END)) {
            state = CODAState.DOWNLOADED;

            // The order in which these thread are shutdown does(should) not matter.
            // Rocs should already have been shutdown, followed by the ET transport objects,
            // followed by this module.
            if (watcher  != null) watcher.interrupt();
            if (qFillers != null) {
                for (Thread qf : qFillers) {
                    qf.interrupt();
                }
            }
            for (Thread thd : buildingThreadQueue) {
                thd.interrupt();
            }
            if (qCollector != null) qCollector.interrupt();

            watcher    = null;
            qFillers   = null;
            qCollector = null;
            buildingThreadQueue.clear();

            inputOrder = 0;
            outputOrder = 0;
            paused = false;

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_end_time", theDate.toString());
            } catch (DataNotFoundException e) {
                e.printStackTrace();
            }
        }

        else if (cmd.equals(CODATransition.RESET)) {
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
            for (Thread thd : buildingThreadQueue) {
                thd.interrupt();
            }
            if (qCollector != null) qCollector.interrupt();

            watcher    = null;
            qFillers   = null;
            qCollector = null;
            buildingThreadQueue.clear();

            inputOrder = 0;
            outputOrder = 0;
            paused = false;

            if (previousState.equals(CODAState.ACTIVE)) {
                try {
                    // set end-of-run time in local XML config / debug GUI
                    Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_end_time", theDate.toString());
                } catch (DataNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        else if (cmd.equals(CODATransition.PRESTART)) {
            // make sure each input channel is associated with a unique rocId
            for (int i=0; i < inputChannels.size(); i++) {
                for (int j=i+1; j < inputChannels.size(); j++) {
                    if (inputChannels.get(i).getID() == inputChannels.get(j).getID()) {
                        // TODO: forget this exception ??
                        CODAState.ERROR.getCauses().add(new EmuException("input channels duplicate rocIDs"));
                        state = CODAState.ERROR;
                        return;
                    }
                }
            }

            state = CODAState.PRESTARTED;

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

            // clear all payload bank queues & associate each one with source ID
            for (int i=0; i < qCount; i++) {
                payloadBankQueues.get(i).clear();
                payloadBankQueues.get(i).setSourceId(inputChannels.get(i).getID());
            }

            eventRate = wordRate = 0F;
            eventCountTotal = wordCountTotal = 0L;

            // create threads objects (but don't start them yet)
            watcher = new Thread(Emu.THREAD_GROUP, new Watcher(), name+":watcher");
            for (int i=0; i < buildingThreadCount; i++) {
                BuildingThread thd1 = new BuildingThread(Emu.THREAD_GROUP, new BuildingThread(), name+":builder"+i);
                buildingThreadQueue.add(thd1);
            }
            qCollector = new Thread(Emu.THREAD_GROUP, new QCollector(), name+":qcollector");
            qFillers = new Thread[qCount];
            for (int i=0; i < qCount; i++) {
                qFillers[i] = new Thread(Emu.THREAD_GROUP,
                                         new Qfiller(payloadBankQueues.get(i),
                                                        inputChannels.get(i).getQueue()),
                                         name+":qfiller"+i);
            }

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_start_time", "--prestart--");
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        // currently NOT used
        else if (cmd.equals(CODATransition.PAUSE)) {
            System.out.println("EB: GOT PAUSE, DO NOTHING");
            paused = true;
        }

        else if (cmd.equals(CODATransition.GO)) {
            if (state == CODAState.ACTIVE) {
                System.out.println("WE musta hit go after PAUSE");
            }

            state = CODAState.ACTIVE;

            // start up all threads
            if (watcher == null) {
                watcher = new Thread(Emu.THREAD_GROUP, new Watcher(), name+":watcher");
            }
            if (watcher.getState() == Thread.State.NEW) {
                System.out.println("starting watcher thread");
                watcher.start();
            }

            if (buildingThreadQueue.size() < 1) {
                for (int i=0; i < buildingThreadCount; i++) {
                    BuildingThread thd1 = new BuildingThread(Emu.THREAD_GROUP, new BuildingThread(), name+":builder"+i);
                    buildingThreadQueue.add(thd1);
                }
            }
            else {
                System.out.println("EB: building thread Q is not empty, size = " + buildingThreadQueue.size());
            }
            int j=0;
            for (BuildingThread thd : buildingThreadQueue) {
                System.out.println("EB: building thread " + thd.getName() + " isAlive = " + thd.isAlive());
                if (thd.getState() == Thread.State.NEW) {
                    System.out.println("Start building thread " + (++j));
                    thd.start();
                }
            }

            if (qCollector == null) {
                qCollector = new Thread(Emu.THREAD_GROUP, new QCollector(), name+":qcollector");
            }
            else {
                System.out.println("EB: qCollector is not null");
            }
            if (qCollector.getState() == Thread.State.NEW) {
                System.out.println("EB: qCollector is not alive, so start qCollector thread");
                qCollector.start();
            }

            if (qFillers == null) {
                qFillers = new Thread[payloadBankQueues.size()];
                for (int i=0; i < payloadBankQueues.size(); i++) {
                    qFillers[i] = new Thread(Emu.THREAD_GROUP,
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
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_start_time", theDate.toString());
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        state = cmd.success();
    }

    protected void finalize() throws Throwable {
        Logger.info("Finalize " + name);
        super.finalize();
    }

    public void setInputChannels(ArrayList<DataChannel> input_channels) {
        this.inputChannels = input_channels;
    }

    public void setOutputChannels(ArrayList<DataChannel> output_channels) {
        this.outputChannels = output_channels;
    }

}