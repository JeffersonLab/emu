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
 * Implementation of a DataChannel reading/writing from/to a fifo.
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
                        Map<String, String> attributeMap, boolean input, Emu emu ,
                        EmuModule module) {

        // constructor of super class
        super(name, transport, attributeMap, input, emu, module);

        DataMover mover = new DataMover();
        movingThread = new Thread(emu.getThreadGroup(), mover, name());
        movingThread.start();
        mover.waitUntilStarted();
    }


    /** {@inheritDoc} */
    public void go() {
        pause = false;
        state = CODAState.ACTIVE;
    }

    /** {@inheritDoc} */
    public void pause() {
        pause = true;
        state = CODAState.PAUSED;
    }

    /** {@inheritDoc} */
    public void end() {
        gotEndCmd = true;
        gotResetCmd = false;
        state = CODAState.DOWNLOADED;
    }

    /** {@inheritDoc} */
    public void reset() {
        gotEndCmd   = false;
        gotResetCmd = true;
        state = CODAState.CONFIGURED;
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
         @Override
         public void run() {
 logger.debug("      DataChannel Fifo out helper: started");

             // Tell the world I've started
             latch.countDown();

             try {
                 RingItem ringItem;
                 int ringChunkCounter = ringChunk;

                 // First event will be "prestart", by convention in ring 0
                 ringItem = getNextOutputRingItem(0);
                 writeEvioData(ringItem);
                 releaseOutputRingItem(0);
 logger.debug("      DataChannel Fifo out helper: sent prestart");

                 // First event will be "go", by convention in ring 0
                 ringItem = getNextOutputRingItem(0);
                 writeEvioData(ringItem);
                 releaseOutputRingItem(0);
 logger.debug("      DataChannel Fifo out helper: sent go");
          // TODO: change the condition ???
                 while ( true ) {

                     if (pause) {
                         if (pauseCounter++ % 400 == 0) {
                             try {Thread.sleep(5);}
                             catch (InterruptedException e1) {}
                         }
                         continue;
                     }

 //logger.debug("      DataChannel Fifo out helper: get next buffer from ring");
                     ringItem = getNextOutputRingItem(rbIndex);
                     ControlType pBankControlType = ringItem.getControlType();
                     writeEvioData(ringItem);

 //logger.debug("      DataChannel Fifo out helper: sent event");

 //logger.debug("      DataChannel Fifo out helper: release ring item");
                     releaseOutputRingItem(rbIndex);
                     if (--ringChunkCounter < 1) {
                         rbIndex = ++rbIndex % ringCount;
                         ringChunkCounter = ringChunk;
 //                        System.out.println("switch ring to "+ rbIndex);
                     }
                     else {
 //                        System.out.println(""+ ringChunkCounter);
                     }

                     if (pBankControlType == ControlType.END) {
 System.out.println("      DataChannel Fifo out helper: " + name + " I got END event, quitting");
                         return;
                     }

                     // If I've been told to RESET ...
                     if (gotResetCmd) {
                         return;
                     }
                 }

             } catch (InterruptedException e) {
                 logger.warn("      DataChannel Fifo out helper: " + name + "  interrupted thd, exiting");
             } catch (Exception e) {
                 logger.warn("      DataChannel Fifo out helper : exit thd: " + e.getMessage());
                 // If we haven't yet set the cause of error, do so now & inform run control
                 errorMsg.compareAndSet(null, e.getMessage());

                 // set state
                 state = CODAState.ERROR;
                 emu.sendStatusMessage();

                 e.printStackTrace();
             }

         }

     }



}
