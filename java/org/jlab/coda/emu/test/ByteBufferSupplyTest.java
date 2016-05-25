/*
 * Copyright (c) 2009, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.test;


import org.jlab.coda.emu.support.data.ByteBufferItem;
import org.jlab.coda.emu.support.data.ByteBufferSupply;

import java.util.concurrent.CountDownLatch;

/**
 * This class is designed to receive output from an EMU.
 * User: timmer
 * Date: Dec 4, 2009
 */
public class ByteBufferSupplyTest {

    private int size = 32, count = 4096, getChunk=1;

    // Let us know that the consumer thread has been started
    CountDownLatch startLatch;
    ByteBufferSupply bbSupply;
    boolean simpleTest = true;

    /** Constructor. */
    ByteBufferSupplyTest(String[] args) {
        decodeCommandLine(args);
    }


    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private void decodeCommandLine(String[] args) {

        // loop over all args
        for (int i=0; i < args.length; i++) {

            if (args[i].equalsIgnoreCase("-h")) {
                usage();
                System.exit(-1);
            }
            else if (args[i].equalsIgnoreCase("-s")) {
                size = Integer.parseInt(args[i + 1]);
                if (size < 1) {
                    System.out.println("Buffer size must be > 0");
                    System.exit(-1);
                }
                i++;
            }
            else if (args[i].equalsIgnoreCase("-c")) {
                count = Integer.parseInt(args[i + 1]);
                if (count < 1) {
                    System.out.println("Buffer count must be > 0");
                    System.exit(-1);
                }
                i++;
            }
            else if (args[i].equalsIgnoreCase("-g")) {
                getChunk = Integer.parseInt(args[i + 1]);
                if (getChunk < 1) {
                    System.out.println("Get chunk must be > 0");
                    System.exit(-1);
                }
                System.out.println("Get chunk = " + getChunk);
                i++;
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
            "   java ByteBufferSupplyTest\n" +
            "        [-s <size>]    size in bytes of buffers\n" +
            "        [-c <count>]   number of buffer in ring (must be power of 2)\n" +
            "        [-g <chunk>]   number of buffers to get from ring at once (> 0)\n" +
            "        [-h]           print this help\n");
    }


    /**
     * Run as a stand-alone application.
     */
    public static void main(String[] args) {
            ByteBufferSupplyTest receiver = new ByteBufferSupplyTest(args);
            receiver.run();
    }


    /**
     * This method is executed as a thread.
     */
    public void run() {

        // Create a reusable supply of ByteBuffer objects
        bbSupply = new ByteBufferSupply(count, size);
        int index = 0;
        boolean releaseBuf = true;
        boolean publishBuf = false;

        if (!simpleTest) {
            // Wait until consumer thread starts
            try {
                startLatch = new CountDownLatch(1);
                RingConsumerThread rcThread = new RingConsumerThread();
                rcThread.start();
                startLatch.await();
            }
            catch (InterruptedException e) {}
        }
        else {
            publishBuf = false;
        }

        while (true) {
            // Grab a stored ByteBuffer
            //System.out.println("Try getting producer buf " + index++ + " ...");
            ByteBufferItem bufItem = bbSupply.get();
            System.out.println(", got producer buf with id = " + bufItem.getMyId());

            if (!releaseBuf) {
                System.out.println("do NOT release producer buf");
            }
            else {
                //System.out.println("release producer buf");
                bbSupply.release(bufItem);
            }

            if (!publishBuf) {
               // System.out.println("do NOT publish producer buf");
            }
            else {
                System.out.println("publish producer buf");
                bbSupply.publish(bufItem);
            }


//            System.out.println("wait 2 sec\n");
//            try {
//                Thread.sleep(1000);
//            }
//            catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }


    /**
     * Thread to do consumer get & release on ring buffer items.
     */
    class RingConsumerThread extends Thread {

        @Override
        public void run() {
            boolean releaseBuf = true;

            // Tell the world I've started
            startLatch.countDown();

            try {
                ByteBufferItem[] bufItems = new ByteBufferItem[getChunk];

                while (true) {
                    for (int i=0; i < getChunk; i++) {
                        System.out.println("     Try getting consumer buf ... ");
                        bufItems[i] = bbSupply.consumerGet();
                        System.out.println("     Got consumer buf with id = " + bufItems[i].getMyId());
                    }

                    if (!releaseBuf) {
                        System.out.println("     do NOT release consumer bufs");
                    }
                    else {
                        for (int i=0; i < getChunk; i++) {
                            System.out.println("     Release consumer buf with id = " + bufItems[i].getMyId());
                            bbSupply.consumerRelease(bufItems[i]);
                        }
                    }

//                    System.out.println("     wait 4 sec\n");
//                    try {
//                        Thread.sleep(4000);
//                    }
//                    catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }




}
