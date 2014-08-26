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

import com.lmax.disruptor.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;

import java.util.Map;

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

    private boolean debug = false;

    //-------------------------------------------
    // Disruptor (RingBuffer)  stuff
    //-------------------------------------------

    /** One RingBuffer. */
    private RingBuffer<RingItem> ringBufferIn;

    /** Sequence of ring items. */
    private Sequence sequenceIn;

    /** Thread has one barrier. */
    private SequenceBarrier barrierIn;

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
        if (debug) System.out.println("FarmController: reset");
        state = CODAState.CONFIGURED;
        paused = false;
    }

    /** {@inheritDoc} */
    public void end() throws CmdExecException {
        if (debug) System.out.println("FarmController: end");
        state = CODAState.DOWNLOADED;
        paused = false;
        endThread();
    }

    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        if (debug) System.out.println("FarmController: prestart");

        //--------------------------------------------------
        // Disruptor (RingBuffer) stuff for 1 input channel
        //--------------------------------------------------
        // Get FIRST input channel's ring buffer
        ringBufferIn = inputChannels.get(0).getRingBufferIn();

        // We have 1 sequence of control events
        sequenceIn = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

        // This sequence is the last consumer before producer comes along
        ringBufferIn.addGatingSequences(sequenceIn);

        // We have 1 barrier
        barrierIn = ringBufferIn.newBarrier();
        //--------------------------------------------------

        state = CODAState.PAUSED;
        eventMovingThread = new EventMovingThread();
        eventMovingThread.start();
        paused = true;
    }

    /** {@inheritDoc} */
    public void pause() {
        super.pause();
        if (debug) System.out.println("FarmController: pause");
    }

    /** {@inheritDoc} */
    public void go() throws CmdExecException {

        // Run #, type, id, session
        int runNumber  = emu.getRunNumber();
        int runTypeId  = emu.getRunTypeId();
        String runType = emu.getRunType();
        String session = emu.getSession();

        if (debug) System.out.println("FarmController: go");
        state = CODAState.ACTIVE;
        paused = false;
    }


    /**
      * End all record threads because an END cmd or event came through.
      * The record thread calling this method is not interrupted.
      */
     private void endThread() {
         // Interrupt the event moving thread
         eventMovingThread.interrupt();
     }


    /**
     * This thread is started by the GO transition and runs while the state
     * of the module is ACTIVE. When the state is ACTIVE, this thread pulls
     * one bank off the first input DataChannel. That bank is copied and placed
     * in the first output channel.
     *
     * Each Farm Controller only needs one set of control events, so getting
     * events from one input channel is enough. There should only be one (1)
     * output channel, a single ER.
     */
    private class EventMovingThread extends Thread {

        // RingBuffer Stuff
        /** Available sequence (largest index of items desired). */
        private long availableSequence;

        /** Next sequence (index of next item desired). */
        private long nextSequence;


        EventMovingThread() {
            super(emu.getThreadGroup(), name+":main");
        }


        @Override
        public void run() {

            boolean debug = false;
            int totalNumberEvents, wordCount;
            RingItem ringItem;
            ControlType controlType;
            PayloadBuffer recordingBuf;
            outputChannelCount = outputChannels.size();

            // Ring Buffer stuff
            availableSequence = -2L;
            nextSequence = sequenceIn.get() + 1L;


            while (state == CODAState.ACTIVE || paused) {

                try {
                    // Only wait or read-volatile-memory if necessary ...
                    if (availableSequence < nextSequence) {
                        // Will BLOCK here waiting for item if none available.
                        // Available sequence may be larger than what we desired
                        availableSequence = barrierIn.waitFor(nextSequence);
                    }

                    ringItem = ringBufferIn.get(nextSequence);
                    controlType = ringItem.getControlType();
                    totalNumberEvents = ringItem.getEventCount();

                    recordingBuf = (PayloadBuffer)ringItem;
                    wordCount = recordingBuf.getNode().getLength() + 1;

                    if (outputChannelCount > 0) {
                        // Place event on only the first output channel
                        eventToOutputChannel(ringItem, 0, 0);
                    }

                    // If END event, end this thread
                    if (controlType == ControlType.END) {
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }

                    eventCountTotal += totalNumberEvents;
                    wordCountTotal  += wordCount;

                    // Release the reusable ByteBuffers back to their supply
                    ringItem.releaseByteBuffer();

                    // Release the events back to the ring buffer for re-use
                    sequenceIn.set(nextSequence++);

                }
                catch (InterruptedException e) {
                    if (debug) System.out.println("INTERRUPTED thread " + Thread.currentThread().getName());
                    return;
                }
                catch (AlertException e) {
                    if (debug) System.out.println("Ring buf alert, " + Thread.currentThread().getName());
                    return;
                }
                catch (TimeoutException e) {
                    if (debug) System.out.println("Ring buf timeout, " + Thread.currentThread().getName());
                    return;
                }
            }

        }

    }

}