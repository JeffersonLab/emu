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
import org.jlab.coda.support.data.DataBank;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.support.transport.DataChannel;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

/**
 * <pre>
 * Class <b>Process </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class Process implements EmuModule, Runnable {

    /** Field name */
    private final String name;

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /** Field input_channels */
    private ArrayList<DataChannel> input_channels = new ArrayList<DataChannel>();

    /** Field output_channels */
    private ArrayList<DataChannel> output_channels = new ArrayList<DataChannel>();

    /** Field actionThread */
    private Thread actionThread;

    /** Field last_error */
    private final Throwable last_error = null;

    /** Field count */
    private int count = 0;

    /** Field data_count */
    private long data_count = 0;

    /**
     * <pre>
     * Class <b>Watcher </b>
     * </pre>
     *
     * @author heyes
     *         Created on Sep 17, 2008
     */
    private class Watcher extends Thread {
        /** Method run ... */
        public void run() {
            while ((state == CODAState.ACTIVE) || (state == CODAState.PAUSED)) {
                try {
                    sleep(2000);

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

    /** Field watcher */
    private Watcher watcher = null;

    /**
     * Constructor Process creates a new Process instance.
     *
     * @param name of type String
     */
    public Process(String name) {
        this.name = name;
    }

    /** @see org.jlab.coda.emu.EmuModule#name() */
    public String name() {
        return name;
    }

    /** Method run ... */
    public void run() {

        boolean hasOutputs = !output_channels.isEmpty();

        System.out.println("Action Thread state " + state);
        while ((state == CODAState.ACTIVE) || (state == CODAState.PAUSED)) {
            try {
                Iterator<DataChannel> outputIter = null;

                if (hasOutputs) outputIter = output_channels.iterator();

                while (state == CODAState.ACTIVE) {

                    if (hasOutputs && !outputIter.hasNext()) outputIter = output_channels.iterator();
                    DataChannel outC = null;
                    BlockingQueue<DataBank> out_queue = null;
                    if (hasOutputs) {
                        outC = outputIter.next();
                        out_queue = outC.getQueue();
                    }
                    ArrayList<DataBank> inList = new ArrayList<DataBank>();
                    int outLen = 0;
                    for (DataChannel c : input_channels) {
                        DataBank dr = c.getQueue().take();
                        outLen += (dr.getLength() + 1);
                        inList.add(dr);
                    }

                    //TODO setSourceID to something that makes sense
                    DataBank outDr = new DataBank(outLen);
                    outDr.getPayload();

                    for (DataBank dr : inList) {
                        dr.getBuffer().rewind();
                        outDr.getBuffer().put(dr.getBuffer());
                    }

                    outDr.setNumber(count);
                    outDr.setDataType(0x20);
                    data_count += outDr.getLength();
                    count++;

                    if (hasOutputs) {
                        out_queue.put(outDr);
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
     * Method getError returns the error of this Process object.
     *
     * @return the error (type Throwable) of this Process object.
     */
    public Throwable getError() {
        return last_error;
    }

    /** @see org.jlab.coda.emu.EmuModule#execute(Command) */
    public void execute(Command cmd) {
        Date theDate = new Date();
        if (cmd.equals(CODATransition.end)) {
            state = CODAState.ENDED;
            actionThread.interrupt();

            if (watcher != null) watcher.interrupt();
            watcher = null;

            try {
                Configurer.setValue(Emu.INSTANCE.parameters(), "status/run_end_time", theDate.toString());
            } catch (DataNotFoundException e) {
                e.printStackTrace();
            }
        }

        if (cmd.equals(CODATransition.prestart)) {
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

        if (cmd.equals(CODATransition.pause)) {
            state = CODAState.PAUSED;
            actionThread.interrupt();
            watcher.interrupt();
        }

        if (cmd.equals(CODATransition.go)) {
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
            actionThread.start();
            if (old_state != CODAState.PAUSED) {
                System.out.println("Start actionThread in Process module");
                actionThread.start();
            }
        }

        state = cmd.success();
    }

    protected void finalize() throws Throwable {
        Logger.info("Finalize " + name);
        super.finalize();
    }

    public void setInput_channels(ArrayList<DataChannel> input_channels) {
        this.input_channels = input_channels;
    }

    public void setOutput_channels(ArrayList<DataChannel> output_channels) {
        this.output_channels = output_channels;
    }
}
