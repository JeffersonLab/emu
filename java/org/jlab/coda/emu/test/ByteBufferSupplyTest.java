package org.jlab.coda.emu.test;


import org.jlab.coda.emu.support.data.ByteBufferItem;
import org.jlab.coda.emu.support.data.ByteBufferSupply;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is designed to receive output from an EMU.
 * User: timmer
 * Date: Dec 4, 2009
 */
public class ByteBufferSupplyTest {

    private int size = 32, count = 4;


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
        for (int i = 0; i < args.length; i++) {

            if (args[i].equalsIgnoreCase("-h")) {
                usage();
                System.exit(-1);
            }
            else if (args[i].equalsIgnoreCase("-s")) {
                size = Integer.parseInt(args[i + 1]);
                if (size < 1)
                    System.exit(-1);
                i++;
            }
            else if (args[i].equalsIgnoreCase("-c")) {
                count = Integer.parseInt(args[i + 1]);
                if (count < 1)
                    System.exit(-1);
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
        ByteBufferSupply bbSupply = new ByteBufferSupply(count, size);
        int index = 0;
        boolean releaseBuf = true;

        while (true) {
            // Grab a stored ByteBuffer
            System.out.print("Get buf " + index++);
            ByteBufferItem bufItem = bbSupply.get();
            System.out.print(", id = " + bufItem.getMyId());
            if (!releaseBuf) {
                System.out.print(", do NOT release buf");
            }
            else {
                System.out.print(", release buf");
                bbSupply.release(bufItem);
            }
            System.out.println(", wait 1 sec");
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



    }


}
