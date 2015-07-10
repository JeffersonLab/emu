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
        channelState = CODAState.DOWNLOADED;
    }

    /** {@inheritDoc} */
    public void reset() {
        gotEndCmd   = false;
        gotResetCmd = true;
        channelState = CODAState.CONFIGURED;
        if (movingThread != null) {
            movingThread.stop();
        }
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

                 // First event will be "prestart", by convention in ring 0
                 ringItem = getNextOutputRingItem(0);
                 writeEvioData(ringItem);
                 releaseCurrentAndGoToNextOutputRingItem(0);
 logger.debug("      DataChannel Fifo helper: sent prestart");

                 // First event will be "go" or "end", by convention in ring 0
                 ringItem = getNextOutputRingItem(0);
                 ControlType pBankControlType = ringItem.getControlType();
                 writeEvioData(ringItem);
                 releaseCurrentAndGoToNextOutputRingItem(0);

                if (pBankControlType == ControlType.END) {
System.out.println("      DataChannel Fifo helper: " + name + " I sent end, quitting");
                    return;
                }

 logger.debug("      DataChannel Fifo out helper: sent go");

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
