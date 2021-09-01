//
// Copyright 2021, Jefferson Science Associates, LLC.
// Subject to the terms in the LICENSE file found in the top-level directory.
//
// EPSCI Group
// Thomas Jefferson National Accelerator Facility
// 12000, Jefferson Ave, Newport News, VA 23606
// (757)-269-7100

package org.jlab.coda.emu.support.data;

import com.lmax.disruptor.EventFactory;


/**
 * Class used by the Disruptor's RingBuffer to populate itself
 * with TimeSliceBankItems (only contains a reference to a PayloadBank).
 * @author timmer (8/25/21)
 */
public class TimeSliceBankItemFactory implements EventFactory<TimeSliceBankItem> {

    public TimeSliceBankItemFactory() {}

    public TimeSliceBankItem newInstance() {
        return new TimeSliceBankItem();
    }
}
