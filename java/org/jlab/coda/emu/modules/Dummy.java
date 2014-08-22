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

import com.lmax.disruptor.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.*;

import java.util.*;

/**
 * This class is a (not so) bare bones module used for testing and as a template.
 * It handles multiple input and output channels.
 * It will consume events through input channels and pass each input item to each
 * of the output channels.
 *
 * @author timmer
 * 4/26/2013
 */
public class Dummy extends ModuleAdapter {

    private boolean debug;

    /** Number of output channels. */
    private int inputChannelCount;

    /** Number of output channels. */
    private int outputChannelCount;

    /** Container for threads used to move events. */
    private ArrayList<EventMovingThread> threadList = new ArrayList<EventMovingThread>();

    private boolean isBufferIoType;

    //-------------------------------------------
    // Disruptor (RingBuffer)  stuff
    //-------------------------------------------

    /** One RingBuffer per input channel (references to channels' rings). */
    private RingBuffer<RingItem>[] ringBuffersIn;

    /** For each input channel, 1 sequence per event-moving thread. */
    public Sequence[][] buildSequenceIn;

    /** For each input channel, all event-moving threads share one barrier. */
    public SequenceBarrier[] buildBarrierIn;

    // ---------------------------------------------------


    /**
     * Constructor.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     * @param emu emu that contains this module
     */
    public Dummy(String name, Map<String, String> attributeMap, Emu emu) {
        super(name, attributeMap, emu);

        // This module may be configured to input & output either
        // EvioEvent objects or ByteBuffer objects.
        isBufferIoType = true;
        String attribString = attributeMap.get("buffer");
        if (attribString != null) {
            if (attribString.equalsIgnoreCase("false") ||
                    attribString.equalsIgnoreCase("off")   ||
                    attribString.equalsIgnoreCase("no"))   {
                isBufferIoType = false;
            }
        }
    }


    /** {@inheritDoc} */
    public ModuleIoType getInputRingItemType() {
        if (isBufferIoType) {
            return ModuleIoType.PayloadBuffer;
        }
        return ModuleIoType.PayloadBank;
    }


    /** {@inheritDoc} */
    public ModuleIoType getOutputRingItemType() {
        if (isBufferIoType) {
            return ModuleIoType.PayloadBuffer;
        }
        return ModuleIoType.PayloadBank;
    }


    /** {@inheritDoc} */
    public void reset() {
        if (debug) System.out.println("  Dummy: reset");
        state = CODAState.CONFIGURED;
        paused = false;
        endThreads();
    }

    /** {@inheritDoc} */
    public void end() throws CmdExecException {
        if (debug) System.out.println("  Dummy: end");
        state = CODAState.DOWNLOADED;
        paused = false;
        endThreads();
    }

    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {
        if (debug) System.out.println("  Dummy: prestart");

        state = CODAState.PAUSED;

        inputChannelCount  = inputChannels.size();
        outputChannelCount = outputChannels.size();

        //------------------------------------------------
        // Disruptor (RingBuffer) stuff for input channels
        //------------------------------------------------

        // 1 ring buffer for each input channel.
        ringBuffersIn = new RingBuffer[inputChannelCount];

        // For each input channel, 1 sequence per event-moving thread
        buildSequenceIn = new Sequence[eventProducingThreads][inputChannelCount];

        // For each input channel, all event-moving threads share one barrier
        buildBarrierIn = new SequenceBarrier[inputChannelCount];

        // For each input channel ...
        for (int i=0; i < inputChannelCount; i++) {
            // Get channel's ring buffer
            RingBuffer<RingItem> rb = inputChannels.get(i).getRingBufferIn();
            ringBuffersIn[i] = rb;

            // We have 1 barrier for each channel (shared by event-moving threads)
            buildBarrierIn[i] = rb.newBarrier();

            // For each event-moving thread ...
            for (int j=0; j < eventProducingThreads; j++) {
                // We have 1 sequence for each event-moving thread & input channel combination
                buildSequenceIn[j][i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

                // This sequence may be the last consumer before producer comes along
                rb.addGatingSequences(buildSequenceIn[j][i]);
            }
        }

        //------------------------------------------------

        if (debug) System.out.println("  Dummy: create & start event moving threads");

        // Create & start event-moving threads
        threadList.clear();
        for (int i=0; i < eventProducingThreads; i++) {
            EventMovingThread thd1 = new EventMovingThread(i, emu.getThreadGroup(), name+":moving"+i);
            threadList.add(thd1);
            thd1.start();
        }

        paused = true;
    }


    /** {@inheritDoc} */
    public void pause() {
        if (debug) System.out.println("Dummy: pause");
        paused = true;
    }


    /** {@inheritDoc} */
    public void go() throws CmdExecException {
        if (debug) System.out.println("Dummy: go");
        state = CODAState.ACTIVE;
        paused = false;
    }


    /**
     * End all event processing threads because an END cmd or event came through.
     */
    private void endThreads() {
        for (Thread thd : threadList) {
            thd.interrupt();
            try {
                thd.join(250);
                if (thd.isAlive()) {
                    thd.stop();
                }
            }
            catch (InterruptedException e) {}
        }
    }


    /**
     * This method is used to place an item onto a specified ring buffer of a
     * single, specified output channel.
     *
     * @param eventOut   the event to place on output channel
     * @param ringNum    which output channel ring buffer to place item on
     * @param channelNum which output channel to place item on
     */
    private void eventToOutputChannel(RingItem eventOut, int channelNum, int ringNum) {

        // Have output channels?
        if (outputChannelCount < 1) {
            return;
        }

        RingBuffer rb = outputChannels.get(channelNum).getRingBuffersOut()[ringNum];
        long nextRingItem = rb.next();

        RingItem ri = (RingItem) rb.get(nextRingItem);
        ri.copy(eventOut);
        rb.publish(nextRingItem);
    }



    /**
     * These threads (may be more than one) are started by the GO transition
     * and run while the state of the module is ACTIVE. When the state is ACTIVE,
     * these threads pull one bank off an input DataChannel. That bank is placed
     * in a single output channel.
     */
    private class EventMovingThread extends Thread {

        /** The order of this thread, relative to other event moving threads, starting at 0. */
        private final int order;

        // RingBuffer Stuff
        /** 1 sequence for each input channel in this particular moving thread. */
        private Sequence[] buildSequences;

        /** Array of available sequences (largest index of items desired), one per input channel. */
        private long availableSequences[];

        /** Array of next sequences (index of next item desired), one per input channel. */
        private long nextSequences[];



        EventMovingThread(int order, ThreadGroup group, String name) {
              super(group, name);
              this.order = order;
         }


        @Override
        public void run() {

            int inputChan = -1, outputChan = -1;
            EventType eventType;
            RingItem ringItem;

            // Ring Buffer stuff - define arrays for convenience
            nextSequences = new long[inputChannelCount];
            availableSequences = new long[inputChannelCount];
            Arrays.fill(availableSequences, -2L);
            buildSequences = new Sequence[inputChannelCount];

            for (int i=0; i < inputChannelCount; i++) {
                buildSequences[i] = buildSequenceIn[order][i];
                nextSequences[i]  = buildSequences[i].get() + 1L;
            }

            while (state == CODAState.ACTIVE || paused) {

                try {
                    // Take turns reading from different input channels
                    inputChan = (inputChan+1) % inputChannelCount;

                    // Only wait if necessary ...
                    if (availableSequences[inputChan] < nextSequences[inputChan]) {
                        // Will BLOCK here waiting for item if none available.
                        // Available sequence may be larger than what we desired.
                        availableSequences[inputChan] = buildBarrierIn[inputChan].waitFor(nextSequences[inputChan]);
                    }

                    if (debug) System.out.println("  Dummy: get input item, chan# " + inputChan);

                    ringItem  = ringBuffersIn[inputChan].get(nextSequences[inputChan]);
                    eventType = ringItem.getEventType();

                    // If END event, clean up and quit
                    if (eventType.isControl() && ringItem.getControlType().isEnd()) {
                        if (endCallback != null) endCallback.endWait();
                        return;
                    }
                    else {
                        // PROCESS EVENT HERE
                    }

                    // If we have any output channels ...
                    if  (outputChannelCount > 0) {
                        // Take turns writing to different output channels
                        outputChan = (outputChan+1) % outputChannelCount;
                        if (outputChannelCount > 0) {
                            if (debug) System.out.println("  Dummy: output item");
                            eventToOutputChannel(ringItem, outputChan, order);
                        }
                    }
                    // If NO output channels, we're done with this data so release buffer
                    else {
                        ringItem.releaseByteBuffer();
                    }

                    // Tell input ring buffer we're done with this event
                    buildSequences[inputChan].set(nextSequences[inputChan]++);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }
        }

    }


}
