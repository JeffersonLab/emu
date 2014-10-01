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

import java.nio.ByteOrder;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * This class is used to provide a very fast supply of ByteBuffer
 * objects for reuse in 2 different modes (uses Disruptor software package).<p>
 *
 * First, it can be used as a simple supply of ByteBuffers.
 * In this mode, only get() and release() are called. A user does a get(),
 * uses that buffer, then calls release() when done with it. If there are
 * multiple users of a single buffer (say 5), then call bufferItem.setUsers(5)
 * before it is used and the buffer is only released when all 5 users have
 * called release().<p>
 *
 * Alternatively, it can be used as a supply of ByteBuffers in which a single
 * producer provides data for a single consumer which is waiting for that data.
 * The producer does a get(), fills the buffer with data, and finally does a publish()
 * to let the consumer know the data is ready. Simultaneously, a consumer does a
 * consumerGet() to access the data once it is ready. The consumer then calls
 * consumerRelease() when finished which allows the producer to reuse the
 * now unused buffer.
 *
 * @author timmer (4/7/14)
 */
public class ByteBufferSupply {

    /** Initial size, in bytes, of ByteBuffers contained in each ByteBufferItem in ring. */
    private final int bufferSize;

    /** Byte order of ByteBuffer in each ByteBufferItem. */
    private ByteOrder order;

    /** Ring buffer. */
    private final RingBuffer<ByteBufferItem> ringBuffer;

    /** Barrier to prevent buffers from being used again, before being released. */
    private final SequenceBarrier barrier;

    /** Which buffer is this one? */
    private final Sequence sequence;

    /** Which buffer is next for the consumer? */
    private long nextConsumerSequence;

    /** Up to which buffer is available for the consumer? */
    private long availableConsumerSequence = -1L;



    /** Class used to initially create all items in ring buffer. */
    private final class ByteBufferFactory implements EventFactory<ByteBufferItem> {
        public ByteBufferItem newInstance() {
            return new ByteBufferItem(bufferSize, order);
        }
    }


    /**
     * Constructor.
     *
     * @param ringSize    number of ByteBufferItem objects in ring buffer.
     * @param bufferSize  initial size (bytes) of ByteBuffer in each ByteBufferItem object.
     * @throws IllegalArgumentException if args < 1 or ringSize not power of 2.
     */
    public ByteBufferSupply(int ringSize, int bufferSize)
            throws IllegalArgumentException {

        this(ringSize, bufferSize, ByteOrder.BIG_ENDIAN);
    }


    /**
     * Constructor.
     *
     * @param ringSize    number of ByteBufferItem objects in ring buffer.
     * @param bufferSize  initial size (bytes) of ByteBuffer in each ByteBufferItem object.
     * @param order       byte order of ByteBuffer in each ByteBufferItem object.
     * @throws IllegalArgumentException if args < 1 or ringSize not power of 2.
     */
    public ByteBufferSupply(int ringSize, int bufferSize, ByteOrder order)
            throws IllegalArgumentException {

        if (ringSize < 1 || bufferSize < 1) {
            throw new IllegalArgumentException("positive args only");
        }

        if (Integer.bitCount(ringSize) != 1) {
            throw new IllegalArgumentException("ringSize must be a power of 2");
        }

        this.order = order;
        this.bufferSize = bufferSize;

        // Create ring buffer with "ringSize" # of elements,
        // each with ByteBuffers of size "bufferSize" bytes.
        // The ByteBuffer can be changed by the user by using
        // the setBuffer() method of the ByteBufferItem object.
        ringBuffer = createSingleProducer(new ByteBufferFactory(), ringSize,
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
    public ByteBufferItem get() {
        // Next available item claimed by data producer
        long getSequence = ringBuffer.next();

        // Get object in that position (sequence) of ring buffer
        ByteBufferItem bufItem = ringBuffer.get(getSequence);

        // Store sequence for later releasing of the buffer
        bufItem.setProducerSequence(getSequence);

        // Get ByteBuffer ready for being written into
        bufItem.getBuffer().clear();

        return bufItem;
    }


    /**
     * Get the next available item in ring buffer for getting data already written into.
     * @return next available item in ring buffer for getting data already written into.
     */
    public ByteBufferItem consumerGet() throws InterruptedException {

        ByteBufferItem item = null;

        try  {
            // Only wait for read-volatile-memory if necessary ...
            if (availableConsumerSequence < nextConsumerSequence) {
                availableConsumerSequence = barrier.waitFor(nextConsumerSequence);
            }

            item = ringBuffer.get(nextConsumerSequence);
            item.setConsumerSequence(nextConsumerSequence++);
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
     * Release claim on the given ring buffer item so it becomes available for reuse.
     * To be used only in conjunction with {@link #get()}.
     * @param byteBufferItem item in ring buffer to release for reuse.
     */
    public void release(ByteBufferItem byteBufferItem) {
        if (byteBufferItem == null) return;

        // Each item may be used by several objects/threads. It will
        // only be released for reuse if everyone releases their claim.
        if (byteBufferItem.decrementCounter()) {
            sequence.set(byteBufferItem.getProducerSequence());
        }
    }


    /**
     * Consumer releases claim on the given ring buffer item so it becomes available for reuse.
     * To be used in conjunction with {@link #get()} and {@link #consumerGet()}.
     * @param byteBufferItem item in ring buffer to release for reuse.
     */
    public void consumerRelease(ByteBufferItem byteBufferItem) {
        if (byteBufferItem == null) return;

        sequence.set(byteBufferItem.getConsumerSequence());
    }


    /**
     * Used to tell that the consumer that the ring buffer item is ready for consumption.
     * To be used in conjunction with {@link #get()} and {@link #consumerGet()}.
     * @param byteBufferItem item available for consumer's use.
     */
    public void publish(ByteBufferItem byteBufferItem) {
        if (byteBufferItem == null) return;
        ringBuffer.publish(byteBufferItem.getProducerSequence());
    }

}
