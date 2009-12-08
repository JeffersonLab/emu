/*
 * Copyright (c) 2008, Jefferson Science Associates
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
import org.jlab.coda.support.codaComponent.CODAState;
import org.jlab.coda.support.codaComponent.CODATransition;
import org.jlab.coda.support.configurer.Configurer;
import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.nio.IntBuffer;

/**
 * This class codes an object that implements the EmuModule interface and can be loaded
 * into an EMU to process data. It reads EvioBanks from  one or more DataChannel objects
 * and writes them to one or more output DataChannel objects.
 * <p/>
 * This simple version of process pulls one bank of each input, concatenates the banks
 * as the payload of a new bank that is written on an output DataChannel. If there are
 * more than one output DataChannels a simple round robbin algorithm is used to select
 * the DataChannel to write to.
 */
public class Process implements EmuModule, Runnable {

    /** Field name is the name of this module */
    private final String name;

    /** Field state is the state of the module */
    private State state = CODAState.UNCONFIGURED;

    /** Field input_channels is an ArrayList of DataCannel objects that are inputs */
    private ArrayList<DataChannel> input_channels = new ArrayList<DataChannel>();

    /** Field output_channels is an ArrayList of DataCannel objects that are outputs */
    private ArrayList<DataChannel> output_channels = new ArrayList<DataChannel>();

    /** Field actionThread is a Thread object that is the main thread of this module. */
    private Thread actionThread;

    /** Field last_error is the last error thrown by the module */
    private final Throwable last_error = null;

    /** Field count is a count of the number of DataBank objects written to the outputs. */
    private int count;

    /** Field data_count is the sum of the sizes in 32-bit words of DataBank objects written to the outputs. */
    private long data_count;

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
                            sleep(200);
                            Configurer.setValue(Emu.INSTANCE.parameters(), "status/events", Long.toString(count));
                            Configurer.setValue(Emu.INSTANCE.parameters(), "status/data_count", Long.toString(data_count));
                        }
                    }

                } catch (InterruptedException e) {
                    Logger.info("Process thread " + name() + " interrupted");
                } catch (DataNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Constructor Process creates a new Process instance.
     * This does nothing except set the name.
     *
     * @param name of type String
     */
    public Process(String name) {
        this.name = name;
//System.out.println("**** HEY, HEY someone created one of ME (modules.Process object) ****");
        System.out.println("**** LOADED NEW CLASS, DUDE!!! (modules.Process object) ****");
    }

    /** @see org.jlab.coda.emu.EmuModule#name() */
    public String name() {
        return name;
    }

    /**
     * Method run is the action loop of the main thread of the module.
     * <pre>
     * The thread is started by the GO transition and runs while the state of the module is
     * ACTIVE or PAUSED.
     * <p/>
     * When the state is ACTIVE and the list of output DataChannels is not empty the thread
     * selects an output by taking the next one from a simple iterator. The thread then pulls
     * one DataBank off each input DataChannel and stores them in an ArrayList.
     * <p/>
     * An empty DataBank long enough to store all of the banks pulled off the inputs is created.
     * Each incoming bank from the ArrayList is copied into the new bank.
     * The count of outgoing banks and the count of data words are incremented.
     * If the Module has an output the bank of banks is put on the output DataChannel.
     * </pre>
     */
    public void run() {

        boolean hasOutputs = !output_channels.isEmpty();

System.out.println("Action Thread state " + state);

        while ((state == CODAState.ACTIVE) || (state == CODAState.PAUSED)) {
            try {
                Iterator<DataChannel> outputIter = null;

                if (hasOutputs) outputIter = output_channels.iterator();

                while (state == CODAState.ACTIVE) {

                    if (hasOutputs && !outputIter.hasNext()) {
                        outputIter = output_channels.iterator();
                    }
                    DataChannel outC;
                    BlockingQueue<EvioBank> out_queue = null;
                    if (hasOutputs) {
                        outC = outputIter.next();
                        out_queue = outC.getQueue();
                    }

                    // Grab one data bank from each input, waiting if necessary
                    ArrayList<EvioBank> inList = new ArrayList<EvioBank>();
                    for (DataChannel c : input_channels) {
                        // blocking operation to grab a Bank
                        EvioBank bank = c.getQueue().take();
                        inList.add(bank);
                    }

                    // make one bank the size of all inputs together
                    // second event, more traditional bank of banks
                    int tag = 5; // sourceID
                    //TODO set tag/sourceID to something that makes sense
                    EventBuilder eventBuilder = new EventBuilder(tag, DataType.BANK, count); // args -> tag, type, num
                    EvioEvent event = eventBuilder.getEvent();

                    // take all input banks and put them into one output bank (event)
                    for (EvioBank bank : inList) {
                        try {
                            eventBuilder.addChild(event, bank);
                        }
                        catch (EvioException e) { /* problems only if not adding banks */ }
                    }
                    event.setAllHeaderLengths();

                    data_count += event.getHeader().getLength();
                    count++;

                    if (hasOutputs) {
                        out_queue.put(event);
                    }
                }

                Thread.sleep(2);

            } catch (InterruptedException e) {
                if (state == CODAState.ENDED) return;
            }
        }

    }

    /** @return the state */
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
     * Method getError returns the error of this Process object.
     *
     * @return the error (type Throwable) of this Process object.
     */
    public Throwable getError() {
        return last_error;
    }

    /** {@inheritDoc} */
    public void execute(Command cmd) {
        Date theDate = new Date();
        
        if (cmd.equals(CODATransition.END)) {
            state = CODAState.ENDED;
            actionThread.interrupt();

            if (watcher != null) watcher.interrupt();
            watcher = null;

            try {
                // set end-of-run time in local XML config
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_end_time", theDate.toString());
            } catch (DataNotFoundException e) {
                e.printStackTrace();
            }
        }

        else if (cmd.equals(CODATransition.PRESTART)) {
            count = 0;
            data_count = 0;

            actionThread = new Thread(Emu.THREAD_GROUP, this, name);

            try {
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_start_time", "--prestart--");
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        else if (cmd.equals(CODATransition.PAUSE)) {
            state = CODAState.PAUSED;
            actionThread.interrupt();
            watcher.interrupt();
        }

        else if (cmd.equals(CODATransition.GO)) {
System.out.println("GO in Process module");
            State old_state = state;
            state = CODAState.ACTIVE;
            if (old_state != CODAState.PAUSED) {
                watcher = new Watcher();
                watcher.start();
            }

            try {
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_start_time", theDate.toString());
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
            // TODO: cannot restart an interrupted thread !!! bug bug
            actionThread.start();
            if (old_state != CODAState.PAUSED) {
                System.out.println("Start actionThread in Process module");
                actionThread.start();
            }
        }

        state = cmd.success();
    }

    /**
     * finalize is called when the module is unloaded and cleans up allocated resources.
     *
     * @throws Throwable
     */
    protected void finalize() throws Throwable {
        Logger.info("Finalize " + name);
        super.finalize();
    }

    /**
     * This method allows the input_channels list to be set.
     *
     * @param input_channels the input_channels of this EmuModule object.
     */
    public void setInputChannels(ArrayList<DataChannel> input_channels) {
        this.input_channels = input_channels;
    }

    /**
     * This method allows the output_channels list to be set
     *
     * @param output_channels the output_channels of this EmuModule object.
     */
    public void setOutputChannels(ArrayList<DataChannel> output_channels) {
        this.output_channels = output_channels;
    }
}
