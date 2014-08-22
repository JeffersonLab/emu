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

package org.jlab.coda.emu;

import org.jlab.coda.emu.support.data.EventType;

import java.util.BitSet;

/**
 * Collection of useful methods.
 * @author timmer (8/6/14)
 */
public class EmuUtilities {

    /**
     * Encode the event type into the bit info word
     * which will be in each evio block header.
     *
     * @param bSet bit set which will become part of the bit info word
     * @param eType event type to be encoded
     */
    static public void setEventType(BitSet bSet, EventType eType) {
        int type = eType.getValue();

        // check args
        if (type < 0) type = 0;
        else if (type > 15) type = 15;

        if (bSet == null || bSet.size() < 6) {
            return;
        }
        // do the encoding
        for (int i=2; i < 6; i++) {
            bSet.set(i, ((type >>> i - 2) & 0x1) > 0);
        }
    }


}