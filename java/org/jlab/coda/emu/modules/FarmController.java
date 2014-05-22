/*
 * Copyright (c) 2014, Jefferson Science Associates
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
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class is a Farm Controlling module.
 * It's not designed to be a data producer, but will consume events through
 * input channels and pass each input item to each of the output channels.
 *
 * @author timmer
 * Feb 13, 2014
 */
public class FarmController extends ModuleAdapter {

    /** Thread which moves events from inputs to outputs. */
    private EventMovingThread eventMovingThread;

    // ---------------------------------------------------


    /**
     * Constructor creates a new EventRecording instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public FarmController(String name, Map<String, String> attributeMap, Emu emu) {
        super(name, attributeMap, emu);
    }



    /** {@inheritDoc} */
    public void reset() {
System.out.println("FarmController: reset");
        state = CODAState.CONFIGURED;
        paused = false;
    }

    /** {@inheritDoc} */
    public void end() throws CmdExecException {
System.out.println("FarmController: end");
        state = CODAState.DOWNLOADED;
        paused = false;
        endThread();
    }

    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
System.out.println("FarmController: prestart");
        state = CODAState.PAUSED;
        killThread = false;
System.out.println("FarmController: create & start event moving thread");
        eventMovingThread = new EventMovingThread();
        eventMovingThread.start();
        paused = true;
    }

    /** {@inheritDoc} */
    public void pause() {
        super.pause();
System.out.println("FarmController: pause");
    }

    /** {@inheritDoc} */
    public void go() throws CmdExecException {

        // Run #, type, id, session
        int runNumber  = emu.getRunNumber();
        int runTypeId  = emu.getRunTypeId();
        String runType = emu.getRunType();
        String session = emu.getSession();

System.out.println("FarmController: go");
        state = CODAState.ACTIVE;
        paused = false;
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
System.out.println("FarmController: running event moving thread");

            // initialize variables
            int currentInputChannel = -1;
            int inputChannelCount   = inputChannels.size();
            int outputChannelCount  = outputChannels.size();

            BlockingQueue<RingItem> queue;
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
// TODO:Before we cast, we need to find out what it is!!!
                        payloadBank = (PayloadBank) queue.poll(1L, TimeUnit.MILLISECONDS);

                        // If nothing on this channel go to the next
                        if (payloadBank == null) {
                            if (killThread) return;
                            continue;
                        }

System.out.println("FarmController: got event of type " + payloadBank.getControlType());
                        break;
                    }

                    // Place input event on all output channels ...
                    if (outputChannelCount > 0) {
                        // Place bank on first output channel queue
                        outputChannels.get(0).getQueue().put(payloadBank);

                        // Copy bank & write to other output channels' Q's
                        for (int j=1; j < outputChannelCount; j++) {
                            outputChannels.get(j).getQueue().put((RingItem)payloadBank.clone());
                        }
                    }

                    // If END event, quit this thread
                    if (payloadBank != null && payloadBank.getControlType() == ControlType.END) {
System.out.println("FarmController: found END event");
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }
                }
                catch (InterruptedException e) {
                    System.out.println("FarmController: interrupted thread " + Thread.currentThread().getName());
                    return;
                }
            }
            System.out.println("FarmController: event moving thread ending");
        }

    }


}