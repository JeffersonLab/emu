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
 *
 * @author timmer (10/24/14)
 */
public final class Statistics {

    /** Array to hold resulting histogram. First element holds bin size,
     *  2nd holds the starting value of the first bin, 3rd
     *  holds the mean, 4th the min, 5th the max, and the
     *  rest of the array holds the histogram. */
    private int[] histogram;

    /** Array to hold data. */
    private final int[] data;

    /** Number of data points stored. */
    private final int window;

    /** Storage location of next data value in array. */
    private int dataIndex;

    /** Did we enter "window" number of data points yet? */
    private boolean once;

    /** Mean value of data. */
    private int mean;

    /** Minimum value of data. */
    private int max=Integer.MIN_VALUE;

    /** Maximum value of data. */
    private int min=Integer.MAX_VALUE;

    /** Size of a single histogram bin. */
    private int binSize;

    /** Number of histogram bins. */
    private int binCount;

    /** The starting value of the histogram's first bin. */
    private int startingBinValue;


    /**
     * Constructor.
     * As new values are added to data and their total number exceeds
     * "window", old points are deleted. Use 40 histogram bins.
     * Size of each bin is determined automatically by dividing the
     * range ( (5*mean) minus the number rounded to nearest 100
     * which is less than the min data point) by the number of bins.
     *
     * @param window max number of data points to hold at once.
     */
    public Statistics(int window) {
        this(window, 40, 0);
    }


    /**
     * Constructor.
     * As new values are added to data and their total number exceeds
     * "window", old points are deleted.
     * Size of each bin is determined automatically by dividing the
     * range ( (5*mean) minus the number rounded to nearest 100
     * which is less than the min data point) by the number of bins.
     *
     * @param window max number of data points to hold at once.
     * @param binCount number of histogram bins.
     */
    public Statistics(int window, int binCount) {
        this(window, binCount, 0);
    }


    /**
     * Constructor.
     * As new values are added to data and their total number exceeds
     * "window", old points are deleted.
     *
     * @param window   max number of data points to hold at once.
     * @param binCount number of histogram bins.
     * @param binSize  size of each histogram bin.
     */
    public Statistics(int window, int binCount, int binSize) {
        if (window   < 100) window   = 100;
        if (binCount < 10)  binCount = 10;

        this.window = window;
        this.binSize = binSize;
        this.binCount = binCount;

        data = new int[window];
        histogram = new int[binCount + 5];
    }


    /**
     * Method to get the histogram array of ints. It contains
     * metadata before the actual data:
     * <ol>
     * <li>The 1st element contains the bin size.<p>
     * <li>The 2nd element contains the first bin's starting value<p>
     * <li>The 3rd element contains the data's mean value<p>
     * <li>The 4rd element contains the data's minimum value<p>
     * <li>The 5rd element contains the data's maximum value<p>
     * </ol>
     * The rest of the array contains the histogram itself.
     * @return histogram
     */
    public int[] getHistogram() {
        return histogram;
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

        if (size != 0) mean = (int) (total/size);
        return size;
    }


    /**
     * Fill the histogram integer array with data points.
     * Only data points with a value < 5*mean are used.
     *
     * @return histogram just filled
     */
    public int[] fillHistogram() {

        int numValidDataPts = calculateStats();
        int cutOffMax = 5*mean;

        // If binSize has not been given, find a reasonable value
        if (binSize < 1) {
            binSize = (cutOffMax - min)/binCount;
            // Round it up
            if (binSize < 100) binSize = 100;
            // Round it off
            if (binSize > 100) binSize = (binSize/100) * 100;
        }

        // Find a nice, round starting value for the first bin
        startingBinValue = (min/100) * 100;

        // Store histogram metadata in first 5 elements
        histogram[0] = binSize;
        histogram[1] = startingBinValue;
        histogram[2] = mean;
        histogram[3] = min;
        histogram[4] = max;

        for (int i=0; i < numValidDataPts; i++) {
            int bin = (data[i] - startingBinValue) / binSize;

            if (bin < 0) { /* this data is smaller than min */ }
            else if (bin >= binCount) { /* this data point is bigger than max */ }
            else {
                histogram[bin+5] += 1;
            }
        }

        return histogram;
    }


    /**
     * Print out a histogram of data points. Only data points
     * of value < 5*mean are used.
     *
     * @param label  header string to print.
     * @param units  unit string to print.
     */
    public void printBuildTimeHistogram(String label, String units) {

        fillHistogram();

        System.out.println("\n" + label);
        System.out.println("    Mean = " + mean + " " + units + ", min = " + min + ", max = " + max);
        for (int i=0; i < binCount; i++) {
            System.out.println( (startingBinValue + i*binSize) + " - " +
                                (startingBinValue + ((i+1)*binSize - 1)) +
                                 " " + units + " = " + histogram[i+5]);
        }
        System.out.println();
    }


}
