package modules;

import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.codaComponent.CODAState;

import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.jevio.EventBuilder;
import org.jlab.coda.jevio.*;

import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * Test version of Process module.
 * @author timmer
 * @date Dec 9, 2009
 */
public class ProcessTest implements EmuModule, Runnable {


    /** Field name is the name of this module */
    private final String name;

    /** Field state is the state of the module */
    private State state = CODAState.UNCONFIGURED;

    /** Field input_channels is an ArrayList of DataChannel objects that are inputs. */
    private ArrayList<DataChannel> input_channels = new ArrayList<DataChannel>();

    /** Field output_channels is an ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> output_channels = new ArrayList<DataChannel>();

    /** Map containing attributes of this module given in config file. */
    private Map<String,String> attributeMap;

    /** Field actionThread is a Thread object that is the main thread of this module. */
    private Thread actionThread;

    /** Field last_error is the last error thrown by the module */
    private final Throwable last_error = null;

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

    private Logger logger;

    private Emu emu;

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
                            Configurer.setValue(emu.parameters(), "status/eventCount", Long.toString(eventCountTotal));
                            Configurer.setValue(emu.parameters(), "status/wordCount", Long.toString(wordCountTotal));
//                            Configurer.newValue(Emu.INSTANCE.parameters(), "status/wordCount",
//                                                "CarlsModule", Long.toString(wordCountTotal));
                        }
                    }

                } catch (InterruptedException e) {
                    logger.info("ProcessTest thread " + name() + " interrupted");
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
     * @param emu EMU object
     */
    public ProcessTest(String name, Map<String,String> attributeMap, Emu emu) {
        this.emu = emu;
        this.name = name;
        this.attributeMap = attributeMap;
        logger = emu.getLogger();

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
        boolean hasOutputs = !output_channels.isEmpty();
        // iterator through output channels
        Iterator<DataChannel> outputIter = null;
        if (hasOutputs) outputIter = output_channels.iterator();

        // initialize
        DataChannel outC;
        BlockingQueue<EvioBank> out_queue = null;
        // variables for instantaneous stats
        long deltaT, t1, t2, prevEventCount=0L, prevWordCount=0L;
        t1 = System.currentTimeMillis();

        while (state == CODAState.ACTIVE) {

            try {

                // round-robin through all output channels
                if (hasOutputs) {
                    // if we reached the end of the iterator, start again at the beginning
                    if (!outputIter.hasNext()) {
                        outputIter = output_channels.iterator();
                    }
                    outC = outputIter.next();
                    out_queue = outC.getQueue();
                }

                // Grab one data bank from each input, waiting if necessary
                ArrayList<EvioBank> inList = new ArrayList<EvioBank>();
                for (DataChannel c : input_channels) {
                    // blocking operation to grab a Bank
                    EvioBank bank = c.getQueue().take();
//System.out.println("ProcessTest: Grabbed bank off " + c.getName() + "  Q");
                    inList.add(bank);
                }


                // make one event with banks from both input events concatenated
                int tag = 5, num = 2;
                EventBuilder eventBuilder = new EventBuilder(tag, DataType.BANK, num); // args -> tag, type, num
                EvioEvent event = eventBuilder.getEvent();

                // take all input banks and put them into one output bank (event)
                int cc=0;
                for (EvioBank bank : inList) {
                    try {
System.out.println("Process: Added bank's children to built event, event = " + cc++);
                        for (BaseStructure b : bank.getChildren()) {
                            eventBuilder.addChild(event, b);
                        }
                    }
                    catch (EvioException e) { /* problems only if not adding banks */ }

                    eventCountTotal++;                        // event count
                    wordCountTotal += bank.getTotalBytes()/4; // word count
                }
                event.setAllHeaderLengths();


                if (hasOutputs) {
//                    for (EvioBank bank : inList) {
////System.out.println("ProcessTest: put bank on output Q");
//                        out_queue.put(bank);
//
//                        eventCountTotal++;                        // event count
//                        wordCountTotal += bank.getTotalBytes()/4; // word count
//                    }
                    out_queue.put(event);

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
        return last_error;
    }

    public void execute(Command cmd) {
        Date theDate = new Date();

        CODACommand emuCmd = cmd.getCodaCommand();

        if (emuCmd == END) {
            state = CODAState.DOWNLOADED;

            if (actionThread != null) actionThread.interrupt();
            actionThread = null;
            if (watcher != null) watcher.interrupt();
            watcher = null;

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            } catch (DataNotFoundException e) {
                e.printStackTrace();
            }
        }

        else if (emuCmd == RESET) {
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
                    Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
                } catch (DataNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        else if (emuCmd == PRESTART) {
            state = CODAState.PRESTARTED;

            eventRate = wordRate = 0F;
            eventCountTotal = wordCountTotal = 0L;

            watcher = new Watcher();
            actionThread = new Thread(emu.getThreadGroup(), this, name);

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        else if (emuCmd == PAUSE) {
            state = CODAState.PRESTARTED;
            actionThread.interrupt();
            watcher.interrupt();
            watcher = new Watcher();
            actionThread = new Thread(emu.getThreadGroup(), this, name);
        }

        else if (emuCmd == GO) {
            state = CODAState.ACTIVE;
            if (watcher == null) {
                watcher = new Watcher();
            }
            watcher.start();

            try {
                // set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_start_time", theDate.toString());
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            if (actionThread == null) {
                actionThread = new Thread(emu.getThreadGroup(), this, name);
            }
            actionThread.start();
        }

        state = cmd.success();
    }

    protected void finalize() throws Throwable {
        logger.info("Finalize " + name);
        super.finalize();
    }

    public void setInputChannels(ArrayList<DataChannel> input_channels) {
        this.input_channels = input_channels;
    }

    public void setOutputChannels(ArrayList<DataChannel> output_channels) {
        this.output_channels = output_channels;
    }

}
