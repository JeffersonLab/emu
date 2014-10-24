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


/**
 * This class keeps some statistics on time to build an event.
 * @author timmer (10/24/14)
 */
public final class Statistics {

    /** Array to hold data. */
    private final int[] data;

    /** Number of data points stored. */
    private final int window;

    /** Storage location of next data value in array. */
    private int dataIndex;

    /** Did we enter "window" number of data points yet? */
    private boolean once;

    private int mean;
    private int max=Integer.MIN_VALUE;
    private int min=Integer.MAX_VALUE;



    public Statistics(int window) {
        this.window = window;
        data = new int[window];
    }


    /**
     * Method to add data point.
     * @param val data to add
     */
    public void addValue(int val) {
        data[dataIndex] = val;

        if (!once && dataIndex == window - 1) {
            once = true;
        }

        dataIndex = ++dataIndex % window;
    }


    /**
     * Calculate the statistics.
     * @return the number of valid data points stored.
     */
    private int calculateStats() {
        long total = 0L;

        int size = window;
        if (!once) size = dataIndex;

        for (int i=0; i < size; i++) {
            if (data[i] < min) min = data[i];
            if (data[i] > max) max = data[i];
            total += data[i];
        }

        mean = (int) (total/size);
        return size;
    }


    /**
     * Print out a histogram of data points. Only data points
     * of maximum 5*mean are printed out.
     *
     * @param label   header string to print
     * @param units   unit string to print
     * @param numBins number of histogram bins to use
     */
    public void printBuildTimeHistogram(String label, String units, int numBins) {

        int validDataPts = calculateStats();

        final int[] result = new int[numBins];

        int cutOffMax = 5*mean;
        int binSize = (cutOffMax - min)/numBins;
        if (binSize < 1) binSize = 1;

        for (int i=0; i < validDataPts; i++) {
            int bin = (data[i] - min) / binSize;

            if (bin < 0) { /* this data is smaller than min */ }
            else if (bin >= numBins) { /* this data point is bigger than max */ }
            else {
                result[bin] += 1;
            }
        }

        System.out.println("\n" + label);
        System.out.println("    Mean = " + mean + " " + units + ", min = " + min + ", max = " + max);
        for (int i=0; i < numBins; i++) {
            System.out.println( ((int)(min + i*binSize)) + " - " +
                                        ((int)(min + (i+1)*binSize)) +
                                        " " + units + " = " + result[i]);
        }
        System.out.println();
    }

}
