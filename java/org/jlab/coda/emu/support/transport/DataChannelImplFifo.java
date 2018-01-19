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

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.data.*;
import org.jlab.coda.jevio.EvioException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * This class implements a DataChannel to act as a fifo.
 * The FIFO channel is unlike other channels. Other channels are either
 * input or output meaning they either supply data to a module or
 * allow a module to write data out. The FIFO channel acts as both.
 * On one side of a FIFO is a module which produces data and expects
 * the FIFO to accept it into ring buffers (like a standard output channel),
 * and on the other is a module which reads data from the FIFO (like a
 * standard input channel). Thus the FIFO is both an input and output channel.
 * The thread which this class contains moves the data from the output
 * side to the input side - seems backwards doesn't it?.
 *
 * @author heyes
 * @author timmer
 * (Nov 10, 2008)
 */
public class DataChannelImplFifo extends DataChannelAdapter {

    private Thread movingThread;

    private int rbIndex;


    /**
     * Constructor DataChannelImplFifo creates a new DataChannelImplFifo instance.
     *
     * @param name          of type String
     * @param transport     of type DataTransport
     * @param input         true if this is an input
     * @param module        module this channel belongs to
     *
     * @throws DataTransportException - unable to create fifo buffer.
     */
    DataChannelImplFifo(String name, DataTransportImplFifo transport,
                        Map<String, String> attributeMap, boolean input, Emu emu,
                        EmuModule module) {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module, 0);

        channelState = CODAState.PAUSED;

        DataMover mover = new DataMover();
        movingThread = new Thread(emu.getThreadGroup(), mover, name());
        movingThread.start();
        mover.waitUntilStarted();
    }


    /** {@inheritDoc} */
    public TransportType getTransportType() {
        return TransportType.FIFO;
    }


    /** {@inheritDoc} */
    public void go() {
        pause = false;
        channelState = CODAState.ACTIVE;
    }

    /** {@inheritDoc} */
    public void pause() {
        pause = true;
        channelState = CODAState.PAUSED;
    }

    /** {@inheritDoc} */
    public void end() {
        gotEndCmd = true;
        gotResetCmd = false;
        if (movingThread != null) {
            movingThread.interrupt();
        }
        channelState = CODAState.DOWNLOADED;
    }

    /** {@inheritDoc} */
    public void reset() {
        gotEndCmd   = false;
        gotResetCmd = true;
        if (movingThread != null) {
            movingThread.interrupt();
        }
        channelState = CODAState.CONFIGURED;
    }


    /**
     * Class <b>DataMover</b>
     * Moves data from input ring buffers to output ring buffer.
     */
    private class DataMover implements Runnable {

        /** Let a single waiter know that the main thread has been started. */
        private CountDownLatch latch = new CountDownLatch(1);

        /** Help in pausing DAQ. */
        private int pauseCounter;


        /** A single waiter can call this method which returns when thread was started. */
        private void waitUntilStarted() {
            try {
                latch.await();
            }
            catch (InterruptedException e) {}
        }


        private final void writeEvioData(RingItem ri) throws IOException, EvioException {

            long nextRingItem = ringBufferIn.next();
            RingItem riOutput = ringBufferIn.get(nextRingItem);
            riOutput.copy(ri);
            ringBufferIn.publish(nextRingItem);
        }



        /** {@inheritDoc} */
        public void run() {

            // Tell the world I've started
            latch.countDown();

            try {
                RingItem ringItem;
                EventType pBankType;
                ControlType pBankControlType;
                boolean gotPrestart = false;

                // The 1st event may be a user event or a prestart.
                // After the prestart, the next event may be "go", "end", or a user event.
                // The non-END control events are placed on ring 0 of all output channels.
                // The END event is placed in the ring in which the next data event would
                // have gone. The user events are placed on ring 0 of only the first output
                // channel.

                // Keep reading user & control events (all of which will appear in ring 0)
                // until the 2nd control event (go or end) is read.
                while (true) {
                    // Read next event
                    ringItem = getNextOutputRingItem(0);
                    pBankType = ringItem.getEventType();
                    pBankControlType = ringItem.getControlType();

                    // If control event ...
                    if (pBankType == EventType.CONTROL) {
                        // if prestart ..
                        if (pBankControlType == ControlType.PRESTART) {
                            if (gotPrestart) {
                                throw new EmuException("got 2 prestart events");
                            }
                            logger.debug("      DataChannel Fifo " + outputIndex + ": send prestart event");
                            gotPrestart = true;
                            writeEvioData(ringItem);
                            releaseCurrentAndGoToNextOutputRingItem(0);
                        }
                        else {
                            if (!gotPrestart) {
                                throw new EmuException("prestart, not " + pBankControlType +
                                                               ", must be first control event");
                            }

                            if (pBankControlType != ControlType.GO &&
                                pBankControlType != ControlType.END) {
                                throw new EmuException("second control event must be go or end");
                            }

                            logger.debug("      DataChannel Fifo " + outputIndex + ": send " + pBankControlType + " event");
                            writeEvioData(ringItem);

                            // Go to the next event
                            releaseCurrentAndGoToNextOutputRingItem(0);

                            // Done looking for the 2 control events
                            break;
                        }
                    }
                    // If user event ...
                    else if (pBankType == EventType.USER) {
                        // Write user event
                        writeEvioData(ringItem);
                        releaseCurrentAndGoToNextOutputRingItem(0);
                    }
                    // Only user and control events should come first, so error
                    else {
                        throw new EmuException(pBankType + " type of events must come after go event");
                    }

                    // Keep reading events till we hit go/end
                    gotoNextRingItem(0);
                    releaseCurrentAndGoToNextOutputRingItem(0);
                }


                if (pBankControlType == ControlType.END) {
System.out.println("      DataChannel Fifo: " + name + " I got END event, quitting");
                    return;
                }


                while ( channelState == CODAState.PAUSED || channelState == CODAState.ACTIVE ) {

                    if (pause) {
                        if (pauseCounter++ % 400 == 0) {
                            try {Thread.sleep(5);}
                            catch (InterruptedException e1) {}
                        }
                        continue;
                    }

//logger.debug("      DataChannel Fifo helper: get next buffer from ring " + rbIndex);
                    ringItem = getNextOutputRingItem(rbIndex);
                    pBankControlType = ringItem.getControlType();
                    writeEvioData(ringItem);

//logger.debug("      DataChannel Fifo helper: sent event");

//logger.debug("      DataChannel Fifo helper: release ring item");
                    releaseCurrentAndGoToNextOutputRingItem(rbIndex);

                    rbIndex = ++rbIndex % outputRingCount;
//System.out.println("      DataChannel Fifo helper: switch ring to "+ rbIndex);

                    if (pBankControlType == ControlType.END) {
System.out.println("      DataChannel Fifo helper: " + name + " I got END event, quitting");
                        return;
                    }

                    // If I've been told to RESET ...
                    if (gotResetCmd) {
                        return;
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("      DataChannel Fifo helper: " + name + "  interrupted thd, exiting");
            } catch (Exception e) {
                channelState = CODAState.ERROR;
                emu.setErrorState("DataChannel fifo in: " + e.getMessage());
logger.warn("      DataChannel Fifo helper : exit thd: " + e.getMessage());
            }
        }

    }


}
