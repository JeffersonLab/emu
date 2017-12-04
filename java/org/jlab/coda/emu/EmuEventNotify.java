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

package org.jlab.coda.emu;

import java.util.concurrent.*;

/**
 * This class is designed for allowing the Emu to wait
 * until a DataChannel or Transport or Module has received
 * a desired event. An object of this class can only be
 * used once (created and then endWait() called),
 * before the reset() method needs to be called in order
 * to be able to call endWait() again.
 *
 * @author timmer
 * Date: 4/22/13
 */
public class EmuEventNotify {

    /** Object to sync with. */
    private CountDownLatch latch = new CountDownLatch(1);
    // This does NOT work for some reason
    //private CyclicBarrier barrier = new CyclicBarrier(2);


    /** Constructor with default wait time of 1 seconds. */
    public EmuEventNotify() {}

    /** This method enables reuse of this object. Call this before endWait and waitForEvent. */
    public void reset() {
        latch = new CountDownLatch(1);
    }

    /** This method allows the waitForEvent() method to return immediately. */
    public void endWait() {
        latch.countDown();
    }

    /**
     * This method waits indefinitely for the endWait() method to be called before it returns.
     * @throws InterruptedException if interrupted during wait
     */
    public void waitForEvent() throws InterruptedException {
        latch.await();
    }

    /**
     * This method waits for the endWait() method to be called
     * or for the timeout to expire before it returns.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the {@code timeout} argument
     * @throws InterruptedException if interrupted during wait
     * @return {@code true} if endWait() called and {@code false}
     *         if the waiting time elapsed before endWait() called
     */
    public boolean waitForEvent(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

}
