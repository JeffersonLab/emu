package org.jlab.coda.emu.test;


import com.lmax.disruptor.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * This class is an example of how one might take 2 producers (one for each ring)
 * and have a consumer that reads one item from each ring and puts them both into
 * a third, output ring. That output ring has a consumer that looks at each item
 * in the output ring.
 *
 * Note: there are more efficient ways of programming this.
 * This is the simplest and most straight forward.
 * If it works, we can think about making it faster.
 *
 * @author Carl Timmer
 */
public class VardanERSAP extends Thread {

    // FOR ONE CRATE:
    // Note: You may substitute your own Class for ByteBuffer so the ring can contain
    // whatever you want.

    /** Number of streams in 1 crate. */
    int streamCount = 2;

    /** Size of ByteBuffers in ring. */
    int byteBufferSize = 2048;

    /** Number of items in each ring buffer. Must be power of 2. */
    int crateRingItemCount = 256;

    /** 1 RingBuffer per stream. */
    RingBuffer<ByteBuffer>[] crateRingBuffers= new RingBuffer[streamCount];

    /** 1 sequence per stream */
    Sequence[] crateSequences = new Sequence[streamCount];

    /** 1 barrier per stream */
    SequenceBarrier[] crateBarriers = new SequenceBarrier[streamCount];

    /** Track which sequence the aggregating consumer wants next from each of the crate rings. */
    long[] crateNextSequences = new long[streamCount];

    /** Track which sequence is currently available from each of the crate rings. */
    long[] crateAvailableSequences = new long[streamCount];



    // OUTPUT RING FOR AGGREGATING CONSUMER

    /** 1 output RingBuffer. */
    RingBuffer<ByteBuffer> outputRingBuffer;

    /** 1 sequence for the output ring's consumer */
    Sequence outputSequence;

    /** 1 barrier for output ring's consumer */
    SequenceBarrier outputBarrier;

    /** Track which sequence the output consumer wants next from output ring. */
    long outputNextSequence;

    /** Track which sequence is currently available from the output ring. */
    long outputAvailableSequence = -1;



    /** Class used by the Disruptor's RingBuffer to populate itself with ByteBuffers. */
    public class ByteBufferFactory implements EventFactory<ByteBuffer> {
        public ByteBufferFactory() {}
        public ByteBuffer newInstance() {
            return ByteBuffer.allocate(byteBufferSize);
        }
    }


    public VardanERSAP() {

        // You may substitute a different wait strategy. This is one I created and is not
        // part of the original Disruptor distribution.

        //-----------------------
        // INPUT
        //-----------------------

        // For each stream ...
        for (int i=0; i < streamCount; i++) {
            // Create a ring
            crateRingBuffers[i] = createSingleProducer(new ByteBufferFactory(), crateRingItemCount,
                    new SpinCountBackoffWaitStrategy(30000, new LiteBlockingWaitStrategy()));

            // Create a sequence
            crateSequences[i] = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

            // Create a barrier in the ring
            crateBarriers[i] = crateRingBuffers[i].newBarrier();

            // Tell ring that after this sequence is "put back" by the consumer,
            // its associated ring item (ByteBuffer) will be
            // available for the producer to reuse (i.e. it's the last or gating consumer).
            crateRingBuffers[i].addGatingSequences(crateSequences[i]);

            // What sequence ring item do we want to get next?
            crateNextSequences[i] = crateSequences[i].get() + 1L;
        }

        // Initialize these values to indicate nothing is currently available from the ring
        Arrays.fill(crateAvailableSequences, -1L);

        //-----------------------
        // OUTPUT
        //-----------------------

        // Now create output ring
        outputRingBuffer = createSingleProducer(new ByteBufferFactory(), crateRingItemCount,
                new SpinCountBackoffWaitStrategy(30000, new LiteBlockingWaitStrategy()));

        outputSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        outputBarrier = outputRingBuffer.newBarrier();
        outputRingBuffer.addGatingSequences(outputSequence);
        outputNextSequence = outputSequence.get() + 1L;
    }


    /**
     * Run a setup with 2 crate producer threads, one crate consumer thread and one output ring
     * consumer thread.
     */
    public void run() {

        try {
            // Create 2 producers
            CrateProducer producer1 = new CrateProducer(0);
            CrateProducer producer2 = new CrateProducer(1);

            // Create one crate consumer
            CrateAggregatingConsumer crateConsumer = new CrateAggregatingConsumer();

            // Create one output ring consumer
            OutputRingConsumer outputConsumer = new OutputRingConsumer();

            // Now get all these threads running
            outputConsumer.start();
            crateConsumer.start();
            producer1.start();
            producer2.start();

            Thread.sleep(100000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /** Thread to produce one stream in one crate. */
    class CrateProducer extends Thread {

        /** Is this stream 0 or stream 1? */
        int streamNum;
        /** Current spot in ring from which an item was claimed. */
        long getSequence;


        CrateProducer(int streamNumber) {
            streamNum = streamNumber;
        }


        /**
         * Get the next available item in ring buffer for writing/reading data.
         * @return next available item in ring buffer.
         * @throws InterruptedException if thread interrupted.
         */
        public ByteBuffer get() throws InterruptedException {

            // Next available item for producer.
            // Note: this is a method I added to Disruptor which will throw an InterruptedException
            // if thread is interrupted. (Normally you would call next()).
            getSequence = crateRingBuffers[streamNum].nextIntr(1);

            // Get object in that position (sequence) of ring
            ByteBuffer buf = crateRingBuffers[streamNum].get(getSequence);
            return buf;
        }


        /**
         * Used to tell the consumer that the ring buffer item gotten with this producer's
         * last call to {@link #get()} (and all previously gotten items) is ready for consumption.
         * To be used in after {@link #get()}.
         */
        public void publish() {
//System.out.println("Producer" + (streamNum + 1) + ": publish " + getSequence);
            crateRingBuffers[streamNum].publish(getSequence);
        }

        public void run() {
            try {
                while (true) {
                    // Get an empty item from ring
                    ByteBuffer buf = get();

                    // Do something with buffer here, like write data into it ...
                    // For now, just clear it.
                    buf.clear();

                    // Make the buffer available for consumers
                    publish();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /** Thread to consume from two streams in one crate and send (be a producer for) an output ring. */
    class CrateAggregatingConsumer extends Thread {

        /** Array to store items obtained from both the crate (input) rings. */
        ByteBuffer[] inputItems = new ByteBuffer[streamCount];

        /** Array to store items obtained from both the output ring. */
        ByteBuffer[] outputItems = new ByteBuffer[streamCount];

        /** Current spot in output ring from which an item was claimed. */
        long getOutSequence;


        CrateAggregatingConsumer() {}


        /**
         * Get the next available item from each crate ring buffer.
         * Do NOT call this multiple times in a row!
         * Be sure to call "put" before calling this again.
         * @throws InterruptedException if thread interrupted.
         */
        public void get() throws InterruptedException {

            try  {
                // Grab one ring item from each ring ...

                for (int i=0; i < streamCount; i++) {
                    // Only wait for read-volatile-memory if necessary ...
                    if (crateAvailableSequences[i] < crateNextSequences[i]) {
                        // Note: the returned (available) sequence may be much larger than crateNextSequence[i]
                        // which means in the next iteration, we do NOT have to wait here.
                        crateAvailableSequences[i] = crateBarriers[i].waitFor(crateNextSequences[i]);
                    }

                    inputItems[i] = crateRingBuffers[i].get(crateNextSequences[i]);

                    // Get next available slot in output ring (as producer)
                    getOutSequence = outputRingBuffer.nextIntr(1);

                    // Get object in that position (sequence or slot) of output ring
                    outputItems[i] = outputRingBuffer.get(getOutSequence);
                }
            }
            catch (final com.lmax.disruptor.TimeoutException ex) {
                // never happen since we don't use timeout wait strategy
                ex.printStackTrace();
            }
            catch (final AlertException ex) {
                ex.printStackTrace();
            }
        }


        /**
         * This "consumer" is also a producer for the output ring.
         * So get items from the output ring and fill them with items claimed from the input rings.
         */
        public void put() {

            // Tell output ring, we're done with all items we took from it.
            // Make them available to output ring's consumer.
            //
            // By releasing getOutputSequence, we release that item and all
            // previously obtained items, so we only have to call this once
            // with the last sequence.
            outputRingBuffer.publish(getOutSequence);

            for (int i=0; i < streamCount; i++) {
                // Tell input (crate) ring that we're done with the item we're consuming
//System.out.println("    CrateConsumer: set seq = " + crateNextSequences[i]);
                crateSequences[i].set(crateNextSequences[i]);

                // Go to next item to consume from input ring
                crateNextSequences[i]++;
            }
        }


        public void run() {
            try {
                while (true) {
                    // Get one item from each of a single crate's rings
                    get();

                    // Do something with buffers here, like write data into them.
                    // Buffers are in "inputItems" and "outputItems" arrays.
                    // Copy data from crate ring item to output ring item, or do something else .....
                    for (int i=0; i < streamCount; i++) {
                        outputItems[i].put(inputItems[i]);
                    }

                    // Done with buffers so make them available for all rings again for reuse
                    put();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }



    /** Thread to consume from output ring. */
    class OutputRingConsumer extends Thread {

        /** Current spot in output ring from which an item was claimed. */
        long getOutSequence;

        OutputRingConsumer() {}


        /**
         * Get the next available item from outupt ring buffer.
         * Do NOT call this multiple times in a row!
         * Be sure to call "put" before calling this again.
         * @return next available item in ring buffer.
         * @throws InterruptedException if thread interrupted.
         */
        public ByteBuffer get() throws InterruptedException {

            ByteBuffer item = null;

            try  {
                    if (outputAvailableSequence < outputNextSequence) {
                        outputAvailableSequence = outputBarrier.waitFor(outputNextSequence);
                    }

                    item = outputRingBuffer.get(outputNextSequence);
            }
            catch (final com.lmax.disruptor.TimeoutException ex) {
                // never happen since we don't use timeout wait strategy
                ex.printStackTrace();
            }
            catch (final AlertException ex) {
                ex.printStackTrace();
            }

            return item;
        }


        /**
         * This "consumer" is also a producer for the output ring.
         * So get items from the output ring and fill them with items claimed from the input rings.
         */
        public void put() {

            // Tell input (crate) ring that we're done with the item we're consuming
//System.out.println("        OutputRingConsumer: put seq = " + outputNextSequence);
            outputSequence.set(outputNextSequence);

            // Go to next item to consume on input ring
            outputNextSequence++;
        }


        public void run() {
            try {
                while (true) {
                    // Get an empty item from ring
                    ByteBuffer buf = get();

                    // Do something with buffer here, like write data into it ...
                    buf.clear();

                    // Make the buffer available for consumers
                    put();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }



    public static void main(String[] args) {

        VardanERSAP test = new VardanERSAP();

        System.out.println("IN main, start all threads");

        test.start();
    }

}
