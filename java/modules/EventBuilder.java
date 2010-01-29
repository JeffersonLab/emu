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
import org.jlab.coda.jevio.EvioBank;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The event building module.
 */
public class EventBuilder implements EmuModule, Runnable {


    /** Field name is the name of this module */
    private final String name;

    /** Field state is the state of the module */
    private State state = CODAState.UNCONFIGURED;

    /** Field inputChannels is an ArrayList of DataChannel objects that are inputs. */
    private ArrayList<DataChannel> inputChannels = new ArrayList<DataChannel>();

    /** Field outputChannels is an ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    /** Container for queues used to hold payload banks taken from Data Transport Records. */
    private LinkedList<LinkedBlockingQueue<EvioBank>> payloadBankQueues =
            new LinkedList<LinkedBlockingQueue<EvioBank>>();

    /** Map containing attributes of this module given in config file. */
    private Map<String,String> attributeMap;

    /** Field actionThread is a Thread object that is the main thread of this module. */
    private Thread actionThread;

    /** Field lastError is the last error thrown by the module */
    private final Throwable lastError = null;

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
    private Watcher watcher;


    /**
     * This class codes a thread that copies the event number and data count into the EMU status
     * once every two hundred milliseconds this is much more efficient than updating the status
     * every time that the counters are incremented.
     */
    private class Watcher extends Thread {
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
     * Constructor ProcessTest creates a new ProcessTest instance.
     * This does nothing except set the name.
     *
     * @param name         name of module
     * @param attributeMap map containing attributes of module
     */
    public EventBuilder(String name, Map<String,String> attributeMap) {
        this.name = name;
        this.attributeMap = attributeMap;
//System.out.println("**** HEY, HEY someone created one of ME (modules.ProcessTest object) ****");
        System.out.println("**** LOADED NEW CLASS, DUDE!!! (modules.ProcessTest object) ****");
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

    /**
     * Method run is the action loop of the main thread of the module.
     * <pre>
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
     * </pre>
     */
    public void run() {


System.out.println("Action Thread state " + state);

        // have output channels?
        boolean hasOutputs = !outputChannels.isEmpty();
        // iterator through output channels
        Iterator<DataChannel> outputIter = null;
        if (hasOutputs) outputIter = outputChannels.iterator();

        //
        EvioBank[] inputBanks = new EvioBank[inputChannels.size()];

        // initialize
        DataChannel outC;
        BlockingQueue<EvioBank> outputQueue = null;
        // array in which to store Data Transport Records' record Ids so can ensure they're sequential
        int[] recordIds = new int[inputChannels.size()];
        EvioBank[] buildingBanks = new EvioBank[inputChannels.size()];
        EvioBank finalEvent  = null;
        EvioBank triggerBank = null;

        // variables for instantaneous stats
        long deltaT, t1, t2, prevEventCount=0L, prevWordCount=0L;
        t1 = System.currentTimeMillis();

        while (state == CODAState.ACTIVE) {

            try {

                // round-robin through all output channels
                if (hasOutputs) {
                    // if we reached the end of the iterator, start again at the beginning
                    if (!outputIter.hasNext()) {
                        outputIter = outputChannels.iterator();
                    }
                    outC = outputIter.next();
                    outputQueue = outC.getQueue();
                }

                try {

                    do {
                        // may get stuck here waiting on a Q
                        fillPayloadBankQueues(outputQueue, recordIds);

                        // Here we have everything we need to build:
                        // ROC raw or physics events from all ROCs
                        // each with proper source ID and sequential record IDs.
                        // However, there are also control events on queues.

                        // Grab one bank from each channel
                        for (int i=0; i < payloadBankQueues.size(); i++) {
                            buildingBanks[i] = payloadBankQueues.get(i).take();
                        }

                    } while (buildControlEvent(buildingBanks, outputQueue));

                    // check for sync here?
    //                checkSync(buildingBanks);

                    // Start by combining the trigger banks of input event into one
                    triggerBank = combineTriggerBanks(buildingBanks);

                    finalEvent = buildFinalEvent(triggerBank, buildingBanks);
                }
                catch (EmuException e) {
                    // TODO: major error getting data events to build, do something ...
                    System.out.println("MAJOR ERROR building events");
                    e.printStackTrace();
                }


                if (hasOutputs) {
//System.out.println("ProcessTest: put bank on output Q");
                    outputQueue.put(finalEvent);

                    eventCountTotal++;                                         // event count
                    wordCountTotal += finalEvent.getHeader().getLength() + 1;  //  word count

                    t2 = System.currentTimeMillis();
                    deltaT = t2 - t1;

                    // calculate rates again if time period exceeded
                    if (deltaT >= statGatheringPeriod) {
                        synchronized (this) {
                            eventRate = (eventCountTotal - prevEventCount)*1000F/deltaT;
                            wordRate  = (wordCountTotal  - prevWordCount)*1000F/deltaT;
                        }
                        t1 = t2;
                        prevEventCount = eventCountTotal;
                        prevWordCount  = wordCountTotal;
                    }

                }

                //Thread.sleep(2);

            } catch (InterruptedException e) {
                if (state == CODAState.DOWNLOADED) return;
            }
        }

    }


    /**
     * Check each payload bank - one from each input channel - to see if there are any
     * control events. If all are control events, pass one copy to the output queue.
     * If only some are control events, throw exception as it must be all or none.
     * If none are control events, do nothing as the banks will be built into a single
     * event momentarily.
     *
     * @param buildingBanks array containing events that will be built together
     * @param outputQueue queue on which to place any control events read
     * @return <code>true</code> if a proper control event was built and output, else <code>false</code>
     * @throws EmuException if events contain mixture of control/data or control types
     * @throws InterruptedException while putting control event on output queue
     */
    private boolean buildControlEvent(EvioBank[] buildingBanks, BlockingQueue<EvioBank> outputQueue)
            throws EmuException, InterruptedException {

        int counter = 0;
        int controlEventCount = 0;
        int numberOfBanks = buildingBanks.length;
        EventType[] types = new EventType[numberOfBanks];

        // count control events
        for (EvioBank bank : buildingBanks) {
            // Might be a DTR or Control Event
            if (Evio.isControlEvent(bank)) {
                controlEventCount++;
            }
            types[counter++] = Evio.getEventType(bank);
        }

        // If one is a control event, all must be identical control events,
        // and only one gets passed to output.
        if (controlEventCount > 0) {
            // all event must be control events
            if (controlEventCount != numberOfBanks) {
                throw new EmuException("some channels have control events and some do not");
            }

            // make sure all are the same type of control event
            EventType eventType = types[0];
            for (int i=1; i < types.length; i++) {
                if (eventType != types[i]) {
                    throw new EmuException("different type control events on each channel");
                }
            }

            outputQueue.put(buildingBanks[0]);
            return true;
        }

        return false;
    }

    
    /**
     * Place payload banks parsed from a Data Transport Record or DTR (taken from a channel (ROC))
     * onto a payload bank queue associated with that channel. Control events are also placed
     * in these queues. However, all non-data, non-control types of events are passed directly
     * to one of the output queues.<p>
     * The idea is to place not much more than 20 payload banks on each queue. This translates
     * to roughly no more than 30MB of data across all ROCs stored in these queues (15kB/ev *
     * 20 pbanks * 100 ev/pbank). We do NOT want to block while filling them unless there are
     * no banks in a queue and also no DTRs in its corresponding channel.
     *
     * @param outputQueue queue on which to place any non-data/control events read
     * @param recordIds array containing the record IDs of the last DTR banks taken off the channel Qs
     *                  and whose payload banks were put onto the payload bank Qs.
     * @return list of all Data Transport Records
     * @throws EmuException for major error in event building
     * @throws InterruptedException when interrupted while trying to get a bank from a queue
     */
    private void fillPayloadBankQueues(BlockingQueue<EvioBank> outputQueue, int[] recordIds)
            throws EmuException, InterruptedException {

        if (inputChannels.size() != payloadBankQueues.size()) {
            throw new EmuException("number of channels does not match number of queues");
        }

        int counter = -1;
        DataChannel channel;
        EvioBank channelBank;
        BlockingQueue<EvioBank> channelQ;

        // Loop thru all queues from which we grab banks to be built
        for (LinkedBlockingQueue<EvioBank> payloadBankQ : payloadBankQueues) {

            // To fill this payload bank Q, grabbing a DTR from the channel Q associated
            // with it and move its payload banks to payload bank Q.
            channel     = inputChannels.get(++counter);
            channelQ    = channel.getQueue();
            channelBank = null;

            // Think about getting more payload banks cause we're running low ...
            while (payloadBankQ.size() < 20) {

                // If channel Q is empty ...
                if (channelQ.size() < 1) {
                    // But payload bank Q has banks, forget about it & move on to next Q
                    if (payloadBankQ.size() > 0) {
                        break;
                    }

                    // And payload bank is also empty, block waiting for the next DTR from ROC.
                    channelBank = channelQ.take();  // blocks, throws InterruptedException
                }
                // Otherwise, just grab next DTR from channel Q.
                else {
                    channelBank = channelQ.poll();  // return null if empty, should never happen
                }

                //----------------------------------------------------------------------
                // channelBank might be a DTR, Control Event, Physics Event, or garbage.
                //----------------------------------------------------------------------

                // Is bank is a control or DTR format event?
                boolean isDTR     = Evio.isDtrEvent(channelBank);
                boolean isControl = Evio.isControlEvent(channelBank);

                if (isDTR) {
                    // Check it's source ID to see if it matches what should be coming over this channel.
                    if (!Evio.idsMatch(channelBank, channel.getID())) {
                        throw new EmuException("data transport record has wrong source ID");
                    }
                    
                    // Extract payload banks from DTR
                    EvioBank[] payloadBanks = Evio.extractPayloadBanks(channelBank, recordIds, counter);
                    
                    // Put ROC raw or physics events on queue.
                    payloadBankQ.addAll(Arrays.asList(payloadBanks));
                }
                else if (isControl) {
                    // Put control event on queue without any analysis.
                    payloadBankQ.add(channelBank);
                }
                else {
                    // If neither control nor DTR format event, put it on any output queue.
                    outputQueue.put(channelBank);
                }
            }
        }
    }



    /**
     * Combine the trigger banks of all input Data Transport Record events into a single
     * trigger bank which will be used in the built event.
     *
     * @param inputBanks array in which to place events that will be built together
     * @return bank combining all input trigger banks into one
     * @throws EmuException for major error in event building
     */
    private EvioBank combineTriggerBanks(EvioBank[] inputBanks) throws EmuException {

        // pull out all trigger banks
        EvioBank[] triggerBanks = new EvioBank[inputBanks.length];
        for (EvioBank dtrBank : inputBanks) {
            //dtrBank.
        }

        return null;
    }

    /**
     * Combine the trigger banks of all input Data Transport Record events into a single
     * trigger bank which will be used in the built event.
     *
     * @param triggerBank bank containing merged trigger info
     * @param buildingBanks array containing events that will be built together
     * @return bank final built event
     * @throws EmuException for major error in event building
     */
    private EvioBank buildFinalEvent(EvioBank triggerBank, EvioBank[] buildingBanks) throws EmuException {
        return null;
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

            if (actionThread != null) actionThread.interrupt();
            actionThread = null;
            if (watcher != null) watcher.interrupt();
            watcher = null;

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_end_time", theDate.toString());
            } catch (DataNotFoundException e) {
                e.printStackTrace();
            }
        }

        else if (cmd.equals(CODATransition.RESET)) {
            State previousState = state;
            state = CODAState.UNCONFIGURED;

            eventRate = wordRate = 0F;
            eventCountTotal = wordCountTotal = 0L;

            if (actionThread != null) actionThread.interrupt();
            actionThread = null;
            if (watcher != null) watcher.interrupt();
            watcher = null;

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
                    payloadBankQueues.add(new LinkedBlockingQueue<EvioBank>());
                }
                // remove excess queues
                else {
                    payloadBankQueues.remove();
                }
            }

            // clear all payload bank queues
            for (BlockingQueue q : payloadBankQueues) {
                q.clear();
            }

            eventRate = wordRate = 0F;
            eventCountTotal = wordCountTotal = 0L;

            watcher = new Watcher();
            actionThread = new Thread(Emu.THREAD_GROUP, this, name);

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
//        else if (cmd.equals(CODATransition.PAUSE)) {
//            state = CODAState.PRESTARTED;
//            actionThread.interrupt();
//            watcher.interrupt();
//            watcher = new Watcher();
//            actionThread = new Thread(Emu.THREAD_GROUP, this, name);
//        }

        else if (cmd.equals(CODATransition.GO)) {
            state = CODAState.ACTIVE;
            if (watcher == null) {
                watcher = new Watcher();
            }
            watcher.start();

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_start_time", theDate.toString());
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            if (actionThread == null) {
                actionThread = new Thread(Emu.THREAD_GROUP, this, name);
            }
            actionThread.start();
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
