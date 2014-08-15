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


import com.lmax.disruptor.*;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * This class is used to provide a very fast supply of EtEvent
 * arrays for reuse in 2 different modes (uses Disruptor software package).<p>
 *
 * First, it can be used as a simple supply of arrays.
 * In this mode, only get() and release() are called. A user does a get(),
 * uses that array, then calls release() when done with it. If there are
 * multiple users of a single array (say 5), then call item.setUsers(5)
 * before it is used and the array is only released when all 5 users have
 * called release().<p>
 *
 * Alternatively, it can be used as a supply of arrays in which a single
 * producer provides data for a single consumer which is waiting for that data.
 * The producer does a get(), fills the array with data, and finally does a publish()
 * to let the consumer know the data is ready. Simultaneously, a consumer does a
 * consumerGet() to access the data once it is ready. The consumer then calls
 * consumerRelease() when finished which allows the producer to reuse the
 * now unused array.
 *
 * @author timmer (4/7/14)
 */
public class EtEventsSupply {

    /** Ring buffer. */
    private final RingBuffer<EtEventsItem> ringBuffer;

    /** Barrier to prevent arrays from being used again, before being released. */
    private final SequenceBarrier barrier;

    /** Which array is this one? */
    private final Sequence sequence;

    /** Which array is next for the consumer? */
    private long nextConsumerSequence;

    /** Up to which array is available for the consumer? */
    private long availableConsumerSequence = -1L;



    /** Class used to initially create all items in ring buffer. */
    private final class EtEventsFactory implements EventFactory<EtEventsItem> {
        public EtEventsItem newInstance() {
            return new EtEventsItem();
        }
    }


    /**
     * Constructor.
     *
     * @param ringSize  number of EtEventsItem objects (arrays of ET events) in ring buffer.
     * @throws IllegalArgumentException if args < 1 or ringSize not power of 2.
     */
    public EtEventsSupply(int ringSize)
            throws IllegalArgumentException {

        if (ringSize < 1) {
            throw new IllegalArgumentException("positive arg only");
        }

        if (Integer.bitCount(ringSize) != 1) {
            throw new IllegalArgumentException("ringSize must be a power of 2");
        }

        // Create ring buffer with "ringSize" # of elements,
        // each with EtEvent array of size "arraySize" elements.
        // The array can be changed by the user by using
        // the setArray() method of the EtEventsItem object.
        ringBuffer = createSingleProducer(new EtEventsFactory(), ringSize,
                                          new YieldingWaitStrategy());

        // Barrier to keep unreleased buffers from being reused
        barrier  = ringBuffer.newBarrier();
        sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        ringBuffer.addGatingSequences(sequence);
        nextConsumerSequence = sequence.get() + 1;
    }


    /**
     * Get the next available item in ring buffer for writing data into.
     * @return next available item in ring buffer for writing data into.
     */
    public EtEventsItem get() {
        // Next available item claimed by data producer
        long getSequence = ringBuffer.next();

        // Get object in that position (sequence) of ring buffer
        EtEventsItem item = ringBuffer.get(getSequence);

        // Store sequence for later releasing of the buffer
        item.setProducerSequence(getSequence);

        return item;
    }


    /**
     * Get the next available item in ring buffer for getting data already written into.
     * @return next available item in ring buffer for getting data already written into.
     */
    public EtEventsItem consumerGet() throws InterruptedException {

        EtEventsItem item = null;

        try  {
            // Only wait for read-volatile-memory if necessary ...
            if (availableConsumerSequence < nextConsumerSequence) {
                availableConsumerSequence = barrier.waitFor(nextConsumerSequence);
            }

            item = ringBuffer.get(nextConsumerSequence);
            item.setConsumerSequence(nextConsumerSequence);
        }
        catch (final TimeoutException ex) {
            // never happen since we don't use timeout wait strategy
            ex.printStackTrace();
        }
        catch (final AlertException ex) {
            ex.printStackTrace();
        }

        return item;
    }


    /**
     * Release claim on the given ring buffer item so it becomes available for reuse.
     * To be used only in conjunction with {@link #get()}.
     * @param item item in ring buffer to release for reuse.
     */
    public void release(EtEventsItem item) {
        if (item == null) return;

        // Each item may be used by several objects/threads. It will
        // only be released for reuse if everyone releases their claim.
        if (item.decrementCounter()) {
            sequence.set(item.getProducerSequence());
        }
    }


    /**
     * Consumer releases claim on the given ring buffer item so it becomes available for reuse.
     * To be used in conjunction with {@link #get()} and {@link #consumerGet()}.
     * @param item item in ring buffer to release for reuse.
     */
    public void consumerRelease(EtEventsItem item) {
        if (item == null) return;

        sequence.set(item.getConsumerSequence());
        nextConsumerSequence++;
    }


    /**
     * Used to tell that the consumer that the ring buffer item is ready for consumption.
     * To be used in conjunction with {@link #get()} and {@link #consumerGet()}.
     * @param item item available for consumer's use.
     */
    public void publish(EtEventsItem item) {
        if (item == null) return;
        ringBuffer.publish(item.getProducerSequence());
    }

}
