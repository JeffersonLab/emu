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

package org.jlab.coda.emu.test;


import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is designed to test the proper way to release
 * input channel ring items in the Event Builder's build threads.
 * @author timmer
 * (Oct 17, 2014)
 */
public class BuildThreadTest {

    private boolean debug, global, volLong;
    private int buildThreadCount = 2;
    private AtomicLong releaseIndex = new AtomicLong(0L);
    private  long releaseIndexLong = 0L;

    private long[] waitHistogram;
    private long[] nextReleaseIndexes;

    private Phaser phaser;


    /** Constructor. */
    BuildThreadTest(String[] args) {
        decodeCommandLine(args);
    }


    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private void decodeCommandLine(String[] args) {

        // loop over all args
        for (int i = 0; i < args.length; i++) {

            if (args[i].equalsIgnoreCase("-h")) {
                usage();
                System.exit(-1);
            }
            else if (args[i].equalsIgnoreCase("-t")) {
                String threads = args[i + 1];
                i++;
                try {
                    buildThreadCount = Integer.parseInt(threads);
                    if (buildThreadCount < 2) {
                        buildThreadCount = 2;
                    }
                }
                catch (NumberFormatException e) {
                }
            }
            else if (args[i].equalsIgnoreCase("-g")) {
                global = true;
            }
            else if (args[i].equalsIgnoreCase("-v")) {
                volLong = true;
            }
            else if (args[i].equalsIgnoreCase("-debug")) {
                debug = true;
            }
            else {
                usage();
                System.exit(-1);
            }
        }

        return;
    }


    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage:\n\n" +
            "   java BuildThreadTest\n" +
            "        [-t <threads>]   number of simulated build threads to run\n" +
            "        [-debug]         turn on printout\n" +
            "        [-h]             print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
        try {
            BuildThreadTest receiver = new BuildThreadTest(args);
            receiver.run();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    /** This method is executed as a thread. */
    public void run() {

        waitHistogram      = new long[buildThreadCount];
        nextReleaseIndexes = new long[buildThreadCount];
        BuildThread[]    bt    = new BuildThread[buildThreadCount];
        BuildThreadVol[] btVol = new BuildThreadVol[buildThreadCount];

        phaser = new Phaser(buildThreadCount);

        System.out.println("Use " + buildThreadCount + " build threads");

        for (int i=0; i < buildThreadCount; i++) {
            if (volLong) {
                btVol[i] = new BuildThreadVol(i);
                btVol[i].start();
            }
            else {
                bt[i] = new BuildThread(i);
                bt[i].start();
            }
        }

        while (true) {
            try {
                Thread.sleep(3000);
            }
            catch (InterruptedException e) {}

            if (debug) {
                for (int i=0; i < buildThreadCount; i++) {
                    System.out.println((i+1) + " - " + waitHistogram[i]);
                }
                System.out.println();
            }
        }

    }


    class BuildThreadVol extends Thread {
        int btIndex;
        long nextReleaseIndex;

        public BuildThreadVol(int btIndex) {
            this.btIndex = btIndex;
            nextReleaseIndexes[btIndex] = nextReleaseIndex = btIndex;
        }

        public void run() {

            phaser.arriveAndAwaitAdvance();

            while (true) {
                long counter = 0L;

                if (global) {
                    while (nextReleaseIndexes[btIndex] > releaseIndexLong) {
                        Thread.yield();

                        if (debug && ++counter > 2000000) {
                            counter = 0L;
                            System.out.print(btIndex + ", " + releaseIndex.get() + ": ");
                            for (int j=0; j < buildThreadCount; j++) {
                                System.out.print(nextReleaseIndexes[j] + ", ");
                            }
                            System.out.println();
                        }
                    }
                }
                else {
                    while (nextReleaseIndex > releaseIndexLong) {
                        Thread.yield();

                        if (debug && ++counter > 2000000) {
                            counter = 0L;
                            waitHistogram[btIndex]++;
                            System.out.println(btIndex + ", " + releaseIndex.get() + ": " + nextReleaseIndex);
                        }
                    }
                }

                System.out.print(btIndex + " ");

                 // Wait until it's my turn again
                if (global) {
                    nextReleaseIndexes[btIndex] += buildThreadCount;
                }
                else {
                    nextReleaseIndex += buildThreadCount;
                }

                // Tell next build thread it's his turn to go
                releaseIndexLong++;

            }

        }
    }




    class BuildThread extends Thread {
        int btIndex;
        long nextReleaseIndex;

        public BuildThread(int btIndex) {
            this.btIndex = btIndex;
            nextReleaseIndexes[btIndex] = nextReleaseIndex = btIndex;
        }

        public void run() {

            phaser.arriveAndAwaitAdvance();

            while (true) {
                long counter = 0L;

                if (global) {
                    while (nextReleaseIndexes[btIndex] > releaseIndex.get()) {
                        Thread.yield();

                        if (debug && ++counter > 2000000) {
                            counter = 0L;
                            waitHistogram[btIndex]++;
                            System.out.print(btIndex + ", " + releaseIndex.get() + ": ");
                            for (int j=0; j < buildThreadCount; j++) {
                                System.out.print(nextReleaseIndexes[j] + ", ");
                            }
                            System.out.println();
                        }
                    }
                }
                else {
                    while (nextReleaseIndex > releaseIndex.get()) {
                        Thread.yield();

                        if (debug && ++counter > 2000000) {
                            counter = 0L;
                            waitHistogram[btIndex]++;
                            System.out.println(btIndex + ", " + releaseIndex.get() + ": " + nextReleaseIndex);
                        }
                    }
                }

                System.out.print(btIndex + " ");

                 // Wait until it's my turn again
                if (global) {
                    nextReleaseIndexes[btIndex] += buildThreadCount;
                }
                else {
                    nextReleaseIndex += buildThreadCount;
                }

                // Tell next build thread it's his turn to go
                releaseIndex.getAndIncrement();
            }

        }
    }


}
