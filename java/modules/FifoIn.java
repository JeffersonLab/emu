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
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.codaComponent.CODAState;

import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import static org.jlab.coda.emu.support.codaComponent.CODACommand.END;

/**
 * This class codes an object that implements the EmuModule interface and can be loaded
 * into an EMU to process data. It writes EvioBanks, containing random stuff, to one fifo.
 * <p/>
 */
public class FifoIn implements EmuModule, Runnable {

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

    /** Map containing attributes of this module given in config file. */
    private Map<String,String> attributeMap;

    /** Field last_error is the last error thrown by the module */
    private final Throwable last_error = null;

    /** Field count is a count of the number of DataBank objects written to the outputs. */
    private int eventCount;

    /** Field wordCount is the sum of the sizes in 32-bit words of DataBank objects written to the outputs. */
    private long wordCount;

    /** Field watcher */
    private Watcher watcher;

    private Logger logger;

    private Emu emu;


    /**
     * This class codes a thread that copies the event number and data count into the EMU status
     * once every two hundred milliseclogger.onds this is much more efficient than updating the status
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
                            Configurer.setValue(emu.parameters(), "status/eventCount", Long.toString(eventCount));
                            Configurer.setValue(emu.parameters(), "status/wordCount", Long.toString(wordCount));
                        }
                    }

                } catch (InterruptedException e) {
                    logger.info("Process thread " + name() + " interrupted");
                } catch (DataNotFoundException e) {
                    e.printStackTrace();
                }
            }
System.out.println("Process module: quitting watcher thread");
        }
    }

    /**
     * Constructor Process creates a new Process instance.
     * This does nothing except set the name.
     *
     * @param name of type String
     */
    public FifoIn(String name, Map<String, String> attributeMap, Emu emu) {
        this.emu = emu;
        this.name = name;
        this.attributeMap = attributeMap;
        logger = emu.getLogger();
    }


    public Object[] getStatistics() {
        return null;
    }

    public boolean representsEmuStatistics() {
        return false;
    }

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

System.out.println("Action Thread state " + state);

        while ((state == CODAState.ACTIVE) || (state == CODAState.PAUSED)) {
System.out.println("FifoIn: is active or paused");
            try {

                while (state == CODAState.ACTIVE) {
System.out.println("FifoIn: is while loop");

                    // Grab one data bank from each input, waiting if necessary
                    int i;
                    for (DataChannel c : input_channels) {
                        // blocking operation to grab a Bank
System.out.println("FifoIn: Try grabbing bank off Q");
                        EvioBank bank = c.getQueue().take();
System.out.println("FifoIn: Grabbed bank off Q");
                        i = ((EvioBank)bank.getChildAt(0)).getIntData()[0];
System.out.println("Got int = " + i);
                    }

                    eventCount++;
                }

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
     * Method getError returns the error of this Process object.
     *
     * @return the error (type Throwable) of this Process object.
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
                // set end-of-run time in local XML config
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            } catch (DataNotFoundException e) {
                e.printStackTrace();
            }
        }

        else if (emuCmd == PRESTART) {
            eventCount = 0;
            wordCount = 0;

            watcher = new Watcher();
            actionThread = new Thread(emu.getThreadGroup(), this, name);

            try {
                Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        else if (emuCmd == PAUSE) {
            state = CODAState.PAUSED;
            actionThread.interrupt();
            watcher.interrupt();
            actionThread = null;
            watcher = null;
        }

        else if (emuCmd == GO) {
System.out.println("GO in FifoIn module");
            State old_state = state;
            state = CODAState.ACTIVE;
            if (watcher == null) {
                watcher = new Watcher();
            }
            watcher.start();

            try {
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
        super.finalize();
    }

    public void setInputChannels(ArrayList<DataChannel> input_channels) {
        this.input_channels = input_channels;
    }

    public void setOutputChannels(ArrayList<DataChannel> output_channels) {
        this.output_channels = output_channels;
    }
}
