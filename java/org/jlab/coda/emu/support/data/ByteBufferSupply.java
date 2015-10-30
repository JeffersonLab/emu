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

// For debugging:
//    private static int id;
//    private int myId;

    /** Initial size, in bytes, of ByteBuffers contained in each ByteBufferItem in ring. */
    private final int bufferSize;

    /** Byte order of ByteBuffer in each ByteBufferItem. */
    private final ByteOrder order;

    /** Are the buffers created, direct? */
    private final boolean direct;

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

    //------------------------------------------
    // For thread safety
    //------------------------------------------

    /** True if user releases ByteBufferItems in same order as acquired. */
    private final boolean orderedRelease;

    /** When releasing in sequence, the last sequence to have been released. */
    private long lastSequenceReleased = -1L;

    /** When releasing in sequence, the highest sequence to have asked for release. */
    private long maxSequence = -1L;

    /** When releasing in sequence, the number of sequences between maxSequence &
     * lastSequenceReleased which have called release(), but not been released yet. */
    private int between;



    /** Class used to initially create all items in ring buffer. */
    private final class ByteBufferFactory implements EventFactory<ByteBufferItem> {
        public ByteBufferItem newInstance() {
            return new ByteBufferItem(bufferSize, order, direct, orderedRelease);
        }
    }


    /**
     * Constructor.
     * Buffers are big endian and not direct.
     * @param ringSize    number of ByteBufferItem objects in ring buffer.
     * @param bufferSize  initial size (bytes) of ByteBuffer in each ByteBufferItem object.
     * @throws IllegalArgumentException if args < 1 or ringSize not power of 2.
     */
    public ByteBufferSupply(int ringSize, int bufferSize)
            throws IllegalArgumentException {

        this(ringSize, bufferSize, ByteOrder.BIG_ENDIAN, false);
    }

    /**
     * Constructor.
     *
     * @param ringSize    number of ByteBufferItem objects in ring buffer.
     * @param bufferSize  initial size (bytes) of ByteBuffer in each ByteBufferItem object.
     * @param order       byte order of ByteBuffer in each ByteBufferItem object.
     * @throws IllegalArgumentException if args < 1 or ringSize not power of 2.
     */
    public ByteBufferSupply(int ringSize, int bufferSize, ByteOrder order, boolean direct)
            throws IllegalArgumentException {
        this(ringSize, bufferSize, order, direct, false);
    }


    /**
     * Constructor. Used when wanting to avoid locks for speed purposes. Say a ByteBufferItem
     * is used by several users. This is true in ET or emu input channels in which many evio
     * events all contain a reference to the same buffer. If the user can guarantee that all
     * the users of one buffer release it before any of the users of the next, then synchronization
     * is not necessary. If that isn't the case, then locks take care of preventing a later
     * acquired buffer from being released first and consequently everything that came before
     * it in the ring.
     *
     * @param ringSize        number of ByteBufferItem objects in ring buffer.
     * @param bufferSize      initial size (bytes) of ByteBuffer in each ByteBufferItem object.
     * @param order           byte order of ByteBuffer in each ByteBufferItem object.
     * @param direct          if true, make ByteBuffers direct.
     * @param orderedRelease  if true, the user promises to release the ByteBufferItems
     *                        in the same order as acquired. This avoids using
     *                        synchronized code (no locks).
     * @throws IllegalArgumentException if args < 1 or ringSize not power of 2.
     */
    public ByteBufferSupply(int ringSize, int bufferSize, ByteOrder order,
                            boolean direct, boolean orderedRelease)
            throws IllegalArgumentException {

        if (ringSize < 1 || bufferSize < 1) {
            throw new IllegalArgumentException("positive args only");
        }

        if (Integer.bitCount(ringSize) != 1) {
            throw new IllegalArgumentException("ringSize must be a power of 2");
        }

        this.order = order;
        this.direct = direct;
        this.bufferSize = bufferSize;
        this.orderedRelease = orderedRelease;
//        myId = id++;

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
     * Not sure if this method is thread-safe.
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
     * Not sure if this method is thread-safe.
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
     * This method <b>ensures</b> that sequences are released in order and is
     * thread-safe. To be used only in conjunction with {@link #get()}.
     * @param byteBufferItem item in ring buffer to release for reuse.
     */
    public void release(ByteBufferItem byteBufferItem) {
        if (byteBufferItem == null) return;

        // Each item may be used by several objects/threads. It will
        // only be released for reuse if everyone releases their claim.
        if (byteBufferItem.decrementCounter()) {
            if (orderedRelease) {
                sequence.set(byteBufferItem.getProducerSequence());
                return;
            }

            synchronized (this) {
                // Sequence we want to release
                long seq = byteBufferItem.getProducerSequence();

                // If we got a new max ...
                if (seq > maxSequence) {
                    // If the old max was > the last released ...
                    if (maxSequence > lastSequenceReleased) {
                        // we now have a sequence between last released & new max
                        between++;
                    }

                    // Set the new max
                    maxSequence = seq;
//System.out.println(myId + " bbSupply: release, new max = " + maxSequence);
                }
                // If we're < max and > last, then we're in between
                else if (seq > lastSequenceReleased) {
                    between++;
//System.out.println(myId + " bbSupply: release, between = " + between);
                }

                // If we now have everything between last & max, release it all.
                // This way higher sequences are never released before lower.
                if ((maxSequence - lastSequenceReleased - 1L) == between) {
//System.out.println(myId + " bbSupply: release, free up to seq " + maxSequence);
                    sequence.set(maxSequence);
                    lastSequenceReleased = maxSequence;
                    between = 0;
                }
            }
        }
    }


    /**
     * Consumer releases claim on the given ring buffer item so it becomes available for reuse.
     * This method <b>ensures</b> that sequences are released in order and is thread-safe.
     * To be used in conjunction with {@link #get()} and {@link #consumerGet()}.
     * @param byteBufferItem item in ring buffer to release for reuse.
     */
    synchronized public void consumerRelease(ByteBufferItem byteBufferItem) {
        if (byteBufferItem == null) return;

        // Sequence we want to release
        long seq = byteBufferItem.getConsumerSequence();

        // If we got a new max ...
        if (seq > maxSequence) {
            // If the old max was > the last released ...
            if (maxSequence > lastSequenceReleased) {
                // we now have a sequence between last released & new max
                between++;
            }

            // Set the new max
            maxSequence = seq;
        }
        // If we're < max and > last, then we're in between
        else if (seq > lastSequenceReleased) {
            between++;
        }

        // If we now have everything between last & max, release it all.
        // This way higher sequences are never released before lower.
        if ( (maxSequence - lastSequenceReleased - 1L) == between) {
            sequence.set(maxSequence);
            lastSequenceReleased = maxSequence;
            between = 0;
        }
    }


    /**
     * Used to tell that the consumer that the ring buffer item is ready for consumption.
     * Not sure if this method is thread-safe.
     * To be used in conjunction with {@link #get()} and {@link #consumerGet()}.
     * @param byteBufferItem item available for consumer's use.
     */
    public void publish(ByteBufferItem byteBufferItem) {
        if (byteBufferItem == null) return;
        ringBuffer.publish(byteBufferItem.getProducerSequence());
    }

}
