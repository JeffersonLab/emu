package org.jlab.coda.emu;

import java.util.concurrent.CountDownLatch;

/**
 * This class is designed for allowing the Emu to wait
 * until a DataChannel or Transport or Module has received
 * a desired event.
 *
 * @author timmer
 * Date: 4/22/13
 */
public class EmuEventNotify {

    CountDownLatch latch = new CountDownLatch(1);

    public void reset() {
        latch = new CountDownLatch(1);
    }

    public void endWait() {
        latch.countDown();
    }

    public void callback(Object object) {
        latch.countDown();
    }

    public void waitForEvent() throws InterruptedException {
        latch.await();
    }
}
