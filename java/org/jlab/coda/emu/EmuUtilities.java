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


    /**
     * Encode the "is first event" into the bit info word
     * which will be in evio block header.
     *
     * @param bSet bit set which will become part of the bit info word
     */
    static public void setFirstEvent(BitSet bSet) {
        // check arg
        if (bSet == null || bSet.size() < 7) {
            return;
        }

        // Encoding bit #15 (#6 since first is bit #9)
        bSet.set(6, true);
    }


    /**
     * Encode the "is NOT first event" into the bit info word
     * which will be in evio block header.
     *
     * @param bSet bit set which will become part of the bit info word
     */
    static public void unsetFirstEvent(BitSet bSet) {
        // check arg
        if (bSet == null || bSet.size() < 7) {
            return;
        }

        // Encoding bit #15 (#6 since first is bit #9)
        bSet.set(6, false);
    }


    /**
     * Return the power of 2 closest to the given argument.
     *
     * @param x value to get the power of 2 closest to.
     * @param roundUp if true, round up, else down
     * @return -1 if value is negative or the closest power of 2 to value
     */
    static public int powerOfTwo(int x, boolean roundUp) {
        if (x < 0) return -1;

        // The following algorithms are found in
        // "Hacker's Delight" by Henry Warren Jr.

        if (roundUp) {
            x = x - 1;
            x |= (x>>1);
            x |= (x>>2);
            x |= (x>>4);
            x |= (x>>8);
            x |= (x>>16);
            return x + 1;
        }

        int y;
        do {
            y = x;
            x &= (x - 1);
        } while (x != 0);
        return y;
    }


}
