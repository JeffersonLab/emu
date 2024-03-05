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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;

/**
 * Collection of useful methods.
 * @author timmer (8/6/14)
 */
public class EmuUtilities {

    private static final long SLEEP_PRECISION = TimeUnit.MILLISECONDS.toNanos(2);
    private static final long SPIN_YIELD_PRECISION = TimeUnit.MICROSECONDS.toNanos(2);

    /**
     * Spin-yield loop based alternative to Thread.sleep
     * Based on the code of Andy Malakov
     * http://andy-malakov.blogspot.fr/2010/06/alternative-to-threadsleep.html .
     * @param nanoDuration nonoseconds to sleep.
     * @throws InterruptedException if thread interrupted.
     */
    public static void sleepNanos(long nanoDuration) throws InterruptedException {
        final long end = System.nanoTime() + nanoDuration;
        long timeLeft = nanoDuration;
        do {
            if (timeLeft > SLEEP_PRECISION) {
                Thread.sleep(1);
            } else {
                if (timeLeft > SPIN_YIELD_PRECISION) {
                    Thread.yield();
                }
            }
            timeLeft = end - System.nanoTime();

            if (Thread.interrupted())
                throw new InterruptedException();
        } while (timeLeft > 0);
    }

    
    /** This method prints out the current stack trace. */
    static public void printStackTrace() {
        for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
            System.out.println(ste);
        }
    }


    /**
     * Method to deep copy a ByteBuffer object.
     *
     * @param source  source ByteBuffer.
     * @return a copy of the source ByteBuffer.
     */
    static public ByteBuffer deepCopy(ByteBuffer source) {

        if (source == null) return null;
        
        int sourceP = source.position();
        int sourceL = source.limit();
        source.clear();

        ByteBuffer target = ByteBuffer.allocate(source.capacity());
        target.put(source);
        target.flip();

        source.position(sourceP);
        source.limit(sourceL);
        return target;
    }


    /**
     * Method to convert a double to a string with a specified number of decimal places.
     *
     * @param d double to convert to a string
     * @param places number of decimal places
     * @return string representation of the double
     */
    static public String doubleToString(double d, int places) {
        if (places < 0) places = 0;

        double factor = Math.pow(10,places);
        String s = "" + (double) (Math.round(d * factor)) / factor;

        if (places == 0) {
            return s.substring(0, s.length()-2);
        }

        while (s.length() - s.indexOf('.') < places+1) {
            s += "0";
        }

        return s;
    }


    /**
     * Method to wait on string from keyboard.
     * @param s prompt string to print
     * @return string typed in keyboard
     */
    static public String inputStr(String s) {
        String aLine = "";
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        System.out.print(s);
        try {
            aLine = input.readLine();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return aLine;
    }
    

    /**
     * Return the power of 2 closest to the given argument.
     *
     * @param x value to get the power of 2 closest to.
     * @param roundUp if true, round up, else down
     * @return -1 if x is negative or the closest power of 2 to value
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


    //    /**
    //     * For the given number, return the closest power of two.
    //     * @param value    number to find the closest power of two to.
    //     * @param roundUp  if value argument is not a power of 2 already,
    //     *                 {@code true} if caller wants to round up to
    //     *                 number higher than value arg, or {@code false}
    //     *                 if rounding to number lower than value arg.
    //     * @return closest power of two
    //     */
    //    static final public int powerOfTwoL(int value, boolean roundUp) {
    //        if (value < 2) return 1;
    //
    //        // If "value" is not a power of 2 ...
    //        if (Integer.bitCount(value) != 1) {
    //            int origValue = value;
    //            int newVal = value / 2;
    //            value = 1;
    //            while (newVal > 0) {
    //                value *= 2;
    //                newVal /= 2;
    //            }
    //
    //            if (roundUp && (value < origValue)) {
    //                value *= 2;
    //            }
    //        }
    //
    //        return value;
    //    }


    /**
     * Combine 2 ints into 1 long.
     * @param low32bits   low 32 bits of resulting long.
     * @param high32bits high 32 bits of resulting long.
     */
    static public long intsToLong(int low32bits, int high32bits) {
        return ( (((long)low32bits) & 0xffffffffL)  |  (((long)high32bits) << 32) );
    }


}
