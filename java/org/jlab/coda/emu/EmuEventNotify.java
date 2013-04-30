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

import java.util.concurrent.CountDownLatch;

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

    /** This method enables reuse of this object. Call this before endWait and waitForEvent. */
    public void reset() { latch = new CountDownLatch(1);  }

    /** Calling this method allows the waitForEvent() method to return immediately. */
    public void endWait() { latch.countDown(); }

    /**
     * This method waits for the endWait() method to be called before it returns.
     * @throws InterruptedException
     */
    public void waitForEvent() throws InterruptedException { latch.await(); }
}
