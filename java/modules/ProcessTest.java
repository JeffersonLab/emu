package modules;

import org.jlab.coda.support.control.State;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.codaComponent.CODAState;
import org.jlab.coda.support.codaComponent.CODATransition;
import org.jlab.coda.support.transport.DataChannel;
import org.jlab.coda.support.configurer.Configurer;
import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.jevio.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;
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
            while ((state == CODAState.ACTIVE) || (state == CODAState.PRESTARTED)) {
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
     * @param name of type String
     */
    public ProcessTest(String name) {
        this.name = name;
//System.out.println("**** HEY, HEY someone created one of ME (modules.ProcessTest object) ****");
        System.out.println("**** LOADED NEW CLASS, DUDE!!! (modules.ProcessTest object) ****");
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

        while ((state == CODAState.ACTIVE) || (state == CODAState.PRESTARTED)) {
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
//System.out.println("ProcessTest: Grabbed bank off " + c  Q");
                        inList.add(bank);
                    }

                    count++;

                    if (hasOutputs) {
                        for (EvioBank bank : inList) {
System.out.println("ProcessTest: put bank on output Q");
                            out_queue.put(bank);
                        }
                    }
                }

                Thread.sleep(2);

            } catch (InterruptedException e) {
                if (state == CODAState.DOWNLOADED) return;
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
     * Method getError returns the error of this ProcessTest object.
     *
     * @return the error (type Throwable) of this ProcessTest object.
     */
    public Throwable getError() {
        return last_error;
    }

    /** {@inheritDoc} */
    public void execute(Command cmd) {
        Date theDate = new Date();

        if (cmd.equals(CODATransition.END)) {
            state = CODAState.DOWNLOADED;

            if (actionThread != null) actionThread.interrupt();
            actionThread = null;
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
            state = CODAState.PRESTARTED;
            count = 0;
            data_count = 0;

            watcher = new Watcher();
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
            state = CODAState.PRESTARTED;
            actionThread.interrupt();
            watcher.interrupt();
            watcher = new Watcher();
            actionThread = new Thread(Emu.THREAD_GROUP, this, name);
        }

        else if (cmd.equals(CODATransition.GO)) {
System.out.println("GO in ProcessTest module");
            state = CODAState.ACTIVE;
            if (watcher == null) {
                watcher = new Watcher();
            }
            watcher.start();

            try {
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_start_time", theDate.toString());
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            // TODO: cannot restart an interrupted thread !!! bug bug
            if (actionThread == null) {
                actionThread = new Thread(Emu.THREAD_GROUP, this, name);
            }
            actionThread.start();
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
