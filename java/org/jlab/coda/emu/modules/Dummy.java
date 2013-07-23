/*
 * Copyright (c) 2013, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.modules;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuEventNotify;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAStateMachineAdapter;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.codaComponent.State;
import org.jlab.coda.emu.support.data.ControlType;
import org.jlab.coda.emu.support.data.PayloadBank;
import org.jlab.coda.emu.support.data.QueueItem;
import org.jlab.coda.emu.support.data.QueueItemType;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is a bare bones module used for testing and as a template.
 * It's not designed to be a data producer, but will consume events through
 * input channels and pass each input item to each of the output channels.
 *
 * @author timmer
 * @date 4/26/2013
 */
public class Dummy extends CODAStateMachineAdapter implements EmuModule {


    /** ID number of this event recorder obtained from config file. */
    private int id;

    /** Name of this event recorder. */
    private final String name;

    /**
     * Possible error message. reset() sets it back to null.
     * Making this an atomically settable String ensures that only 1 thread
     * at a time can change its value. That way it's only set once per error.
     */
    private AtomicReference<String> errorMsg = new AtomicReference<String>();

    /** Emu this module belongs to. */
    private Emu emu;

    /** Logger used to log messages to debug console. */
    private Logger logger;

    /** State of this module. */
    private volatile State state = CODAState.BOOTED;

    /** ArrayList of DataChannel objects that are inputs. */
    private ArrayList<DataChannel> inputChannels = new ArrayList<DataChannel>();

    /** ArrayList of DataChannel objects that are outputs. */
    private ArrayList<DataChannel> outputChannels = new ArrayList<DataChannel>();

    /** User hit PAUSE button if {@code true}. */
    private boolean paused;

    /** Object used by Emu to be notified of END event arrival. */
    private EmuEventNotify endCallback;

    /** Thread which moves events from inputs to outputs. */
    private EventMovingThread eventMovingThread;

    /** Flag used to kill eventMovingThread. */
    private volatile boolean killThread;

    // ---------------------------------------------------




    /**
     * Constructor creates a new EventRecording instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public Dummy(String name, Map<String, String> attributeMap, Emu emu) {
        this.emu = emu;
        this.name = name;
        logger = emu.getLogger();

        try {
            id = Integer.parseInt(attributeMap.get("id"));
            if (id < 0)  id = 0;
        }
        catch (NumberFormatException e) { /* default to 0 */ }
System.out.println("Dummy: created object");
    }


    /** {@inheritDoc} */
    public String name() {return name;}

    /** {@inheritDoc} */
    public State state() {return state;}

    /** {@inheritDoc} */
    public String getError() {return errorMsg.get();}

    /** {@inheritDoc} */
    public void registerEndCallback(EmuEventNotify callback) {endCallback = callback; }

    /** {@inheritDoc} */
    public EmuEventNotify getEndCallback() {return endCallback;}

    /** {@inheritDoc} */
    public QueueItemType getInputQueueItemType() {return QueueItemType.PayloadBank;}

    /** {@inheritDoc} */
    public QueueItemType getOutputQueueItemType() {return QueueItemType.PayloadBank;}

    /** {@inheritDoc} */
    public boolean representsEmuStatistics() {return false;}

    /** {@inheritDoc} */
    synchronized public Object[] getStatistics() {
        return new Object[] {0L, 0L, 0F, 0F};
    }

    /** {@inheritDoc} */
    public void reset() {
System.out.println("Dummy: reset");
        state = CODAState.CONFIGURED;
        paused = false;
    }

    /** {@inheritDoc} */
    public void end() throws CmdExecException {
System.out.println("Dummy: end");
        state = CODAState.DOWNLOADED;
        paused = false;
        endThread();
    }

    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
System.out.println("Dummy: prestart");
        state = CODAState.PAUSED;
        killThread = false;
System.out.println("Dummy: create & start event moving thread");
        eventMovingThread = new EventMovingThread();
        eventMovingThread.start();
        paused = true;
    }

    /** {@inheritDoc} */
    public void pause() {
System.out.println("Dummy: pause");
        paused = true;
    }

    /** {@inheritDoc} */
    public void go() throws CmdExecException {
System.out.println("Dummy: go");
        state = CODAState.ACTIVE;
        paused = false;
    }

    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {
        this.inputChannels.addAll(input_channels);
    }

    /** {@inheritDoc} */
    public void addOutputChannels(ArrayList<DataChannel> output_channels) {
        this.outputChannels.addAll(output_channels);
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
    }


    /**
      * End all record threads because an END cmd or event came through.
      * The record thread calling this method is not interrupted.
      */
     private void endThread() {
         // Interrupt the event moving thread
         killThread = true;
         eventMovingThread.interrupt();
     }

     /**
     * This thread is started by the GO transition and runs while the state
     * of the module is ACTIVE. When the state is ACTIVE, this thread pulls
     * one bank off an input DataChannel. That bank is copied and placed in
     * each output channel.
     */
    private class EventMovingThread extends Thread {

        EventMovingThread() {
            super();
        }

        @Override
        public void run() {
System.out.println("Dummy: running event moving thread");

            // initialize variables
            int currentInputChannel = -1;
            int inputChannelCount   = inputChannels.size();
            int outputChannelCount  = outputChannels.size();

            QueueItem qItem;
            BlockingQueue<QueueItem> queue;
            PayloadBank payloadBank;

            while (state == CODAState.ACTIVE || paused) {
                if (killThread) return;

                try {
                    // Grab input event ...
                    while (true) {
                        // Take turns reading from different input channels
                        currentInputChannel = (currentInputChannel+1) % inputChannelCount;

                        // Will BLOCK here waiting for payload bank if none available
                        queue = inputChannels.get(currentInputChannel).getQueue();
                        qItem = queue.poll(1L, TimeUnit.MILLISECONDS);

                        // If nothing on this channel go to the next
                        if (qItem == null) {
                            if (killThread) return;
                            continue;
                        }

                        payloadBank = qItem.getPayloadBank();
                        break;
                    }

                    // Place input event on all output channels ...
                    if (outputChannelCount > 0) {
                        // Place bank on first output channel queue
                        outputChannels.get(0).getQueue().put(qItem);

                        // Copy bank & write to other output channels' Q's
                        for (int j=1; j < outputChannelCount; j++) {
                            outputChannels.get(j).getQueue().put((QueueItem)qItem.clone());
                        }
                    }

                    // If END event, quit this thread
                    if (payloadBank != null && payloadBank.getControlType() == ControlType.END) {
System.out.println("Dummy: found END event");
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }
                }
                catch (InterruptedException e) {
                    System.out.println("Dummy: interrupted thread " + Thread.currentThread().getName());
                    return;
                }
            }
            System.out.println("Dummy: event moving thread ending");
        }

    }


}