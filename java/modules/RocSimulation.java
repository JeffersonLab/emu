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

package modules;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.data.Evio;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * This class simulates a Roc. It is a module which uses a single thread
 * to create events and send them to a single output channel.<p>
 * TODO: ET buffers have the number of events in them which varies from ROC to ROC.
 */
public class RocSimulation implements EmuModule, Runnable {


    /** Name of this event builder. */
    private final String name;

    /** ID number of this Roc obtained from config file. */
    private int rocId;

    /** Keep track of the number of records built in this Roc. Reset at prestart. */
    private volatile int rocRecordId;

    /** State of the module. */
    private State state = CODAState.UNCONFIGURED;

    /** OutputChannels is an ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    /** Thread used for generating events. */
    private EventGeneratingThread eventGeneratingThread;

    /** Map containing attributes of this module given in config file. */
    private Map<String,String> attributeMap;

    /** Field lastError is the last error thrown by the module */
    private final Throwable lastError = null;


    private boolean paused;
    private String subject;
    private String type;

    private int delay = 2000; // 2 second default timeout
    private int triggerType = 15;
    private boolean isSingleEventMode = false;

    private int numEventsInPayloadBank = 1; // number of events in first payload bank (incremented for each additional bank)

    private int numPayloadBanks = 2;

    // TODO: should construct this from detector ID & 4 status bits
    private int dataBankTag = 111; // starting data bank tag


    // The following members are for keeping statistics


    /** The number of the event to be assigned to that which is built next. */
    private long eventNumber;

    /** The number of the last event that this Event Builder completely built. */
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

    /** Field watcher */
    private Thread watcher;


    /**
     * Constructor ProcessTest creates a new EventBuilding instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public RocSimulation(String name, Map<String, String> attributeMap) {
        this.name = name;
        this.attributeMap = attributeMap;
        try {
            rocId = Integer.parseInt(attributeMap.get("id"));
        }
        catch (NumberFormatException e) { /* default to 0 */ }
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
//        EventGeneratingThread builder1 = new EventGeneratingThread();
//        EventGeneratingThread builder2 = new EventGeneratingThread();
//        EventGeneratingThread builder3 = new EventGeneratingThread();
//        buildingThreadQueue.add(builder1);
//        buildingThreadQueue.add(builder2);
//        buildingThreadQueue.add(builder3);
//        builder1.start();
//        builder2.start();
//        builder3.start();
    }


    /**
     * This class defines a thread that makes instantaneous rate calculations
     * once every few seconds. Rates are sent to runcontrol.
     */
    private class Watcher extends Thread {
        /**
         * Method run is the action loop of the thread. It's created while the module is in the
         * ACTIVE or PRESTARTED state. It is exited on end of run or reset.
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
     * This thread is started by the GO transition and runs while the state of the module is ACTIVE.
     * <p/>
     * When the state is ACTIVE and the list of output DataChannels is not empty, this thread
     * selects an output by taking the next one from a simple iterator. This thread then creates
     * data transport records with payload banks containing ROC raw records and places them on the
     * output DataChannel.
     * <p/>
     */
    class EventGeneratingThread extends Thread {

        EventGeneratingThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }

        EventGeneratingThread() {
            super();
        }

        public void run() {

            if (isSingleEventMode) {
                numEventsInPayloadBank = 1;
            }

            EvioEvent ev;
            int timestamp=0, numEvents;
            ByteBuffer bbuf = ByteBuffer.allocate(2048);

            StringWriter sw = new StringWriter(2048);
            PrintWriter wr = new PrintWriter(sw, true);
            long start_time = System.currentTimeMillis();
            EventWriter evWriter;


            while (state == CODAState.ACTIVE || paused) {

                try {
                    // turn event into byte array
                    ev = Evio.createDataTransportRecord(rocId, triggerType,
                                                        dataBankTag, (int)eventNumber,
                                                        numEventsInPayloadBank,
                                                        timestamp,   rocRecordId,
                                                        numPayloadBanks, isSingleEventMode);

                    bbuf.clear();
                    try {
                        evWriter = new EventWriter(bbuf, 128000, 10, null, null);
                        evWriter.writeEvent(ev);
                        evWriter.close();
                    }
                    catch (EvioException e) {
                        /* never happen */
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                    bbuf.flip();

//                    try {
//                        StringWriter sw2 = new StringWriter(1000);
//                        XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw2);
//                        ev.toXML(xmlWriter);
//                        System.out.println("\nSending msg:\n" + sw2.toString());
//
//                        System.out.println("Sending msg (bin):");
//                        while (bbuf.hasRemaining()) {
//                            wr.printf("%#010x\n", bbuf.getInt());
//                        }
//                        System.out.println(sw.toString() + "\n\n");
//                    }
//                    catch (XMLStreamException e) {
//                        e.printStackTrace();
//                    }

                    Thread.sleep(delay);

                    long now = System.currentTimeMillis();
                    long deltaT = now - start_time;
                    if (deltaT > 2000) {
                        wr.printf("%d  Hz\n", 3L/deltaT);
                        System.out.println(sw.toString());
                        start_time = now;
                    }

                    // Stick it on the output Q.
                    outputChannels.get(0).getQueue().put(ev);

                    // stats
                    numEvents = numEventsInPayloadBank*numPayloadBanks;
                    rocRecordId++;
                    timestamp       += numEvents;
                    eventNumber     += numEvents;
                    eventCountTotal += numEvents;
                    wordCountTotal  += ev.getHeader().getLength() + 1;
                    lastEventNumberBuilt = eventNumber - 1;
                }
                catch (EvioException e) {
System.out.println("MAJOR ERROR generating events");
                    e.printStackTrace();
                }
                catch (InterruptedException e) {
                    //e.printStackTrace();
System.out.println("INTERRUPTED thread " + Thread.currentThread().getName());
                    if (state == CODAState.DOWNLOADED) return;
                }
            }
System.out.println("Roc data creation thread is ending !!!");

        }


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
            eventGeneratingThread.interrupt();
            eventGeneratingThread = null;
            watcher = null;
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
            eventGeneratingThread.interrupt();
            eventGeneratingThread = null;
            watcher = null;
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

            state = CODAState.PRESTARTED;

            // Reset some variables
            eventRate = wordRate = 0F;
            eventCountTotal = wordCountTotal = 0L;
            rocRecordId = 0;
            eventNumber = 1L;
            lastEventNumberBuilt = 0L;

            // create threads objects (but don't start them yet)
            watcher = new Thread(Emu.THREAD_GROUP, new Watcher(), name+":watcher");
            eventGeneratingThread = new EventGeneratingThread(Emu.THREAD_GROUP,
                                                                   new EventGeneratingThread(),
                                                                   name+":generator");

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
            System.out.println("ROC: GOT PAUSE, DO NOTHING");
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

            if (eventGeneratingThread == null) {
                eventGeneratingThread = new EventGeneratingThread(Emu.THREAD_GROUP,
                                                                  new EventGeneratingThread(),
                                                                  name+":generator");
            }
            int j=0;
System.out.println("ROC: event generating thread " + eventGeneratingThread.getName() + " isAlive = " +
                           eventGeneratingThread.isAlive());
            if (eventGeneratingThread.getState() == Thread.State.NEW) {
                System.out.println("starting event generating thread");
                eventGeneratingThread.start();
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
    }

    public void setOutputChannels(ArrayList<DataChannel> output_channels) {
        this.outputChannels = output_channels;
    }

}