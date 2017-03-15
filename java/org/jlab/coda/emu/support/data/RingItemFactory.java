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

package org.jlab.coda.emu.support.data;

import com.lmax.disruptor.EventFactory;


/**
 * Class used by the Disruptor's RingBuffer to populate itself
 * with PayloadBuffers or PayloadBanks.
 * @author timmer (4/7/14)
 */
public class RingItemFactory implements EventFactory<RingItem> {

    public RingItemFactory() {}

    public RingItem newInstance() {
        return new PayloadBuffer();
    }
}
