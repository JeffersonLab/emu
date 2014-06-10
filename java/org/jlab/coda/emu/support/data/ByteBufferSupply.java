package org.jlab.coda.emu.support.data;


import com.lmax.disruptor.*;

import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * This class is used to provide a very fast supply of ByteBuffer
 * objects for reuse. Uses the Disruptor software package.
 * @author timmer (4/7/14)
 */
public class ByteBufferSupply {

    /** Initial size, in bytes, of ByteBuffers contained in each ByteBufferItem in ring. */
    private final int bufferSize;

    /** Ring buffer. */
    private final RingBuffer<ByteBufferItem> ringBuffer;

    /** Barrier to prevent buffers from being used again, before being released. */
    private final SequenceBarrier barrier;

    /** Which buffer is this one? */
    private final Sequence sequence;


    /** Class used to initially create all items in ring buffer. */
    private final class ByteBufferFactory implements EventFactory<ByteBufferItem> {
        public ByteBufferItem newInstance() {
            return new ByteBufferItem(bufferSize);
        }
    }


    /**
     * Constructor.
     *
     * @param ringSize   number of ByteBufferItem objects in ring buffer.
     * @param bufferSize initial size (bytes) of ByteBuffer in each ByteBufferItem object.
     * @throws IllegalArgumentException if args < 1 or ringSize not power of 2.
     */
    public ByteBufferSupply(int ringSize, int bufferSize) throws IllegalArgumentException {

        if (ringSize < 1 || bufferSize < 1) {
            throw new IllegalArgumentException("positive args only");
        }

        if (Integer.bitCount(ringSize) != 1) {
            throw new IllegalArgumentException("ringSize must be a power of 2");
        }

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
        bufItem.setSequence(getSequence);

        // Get ByteBuffer ready for being written into
        bufItem.getBuffer().clear();

        return bufItem;
    }


    /**
     * Release claim on the given ring buffer item so it becomes available for reuse.
     * @param byteBufferItem item in ring buffer to release for reuse.
     */
    public void release(ByteBufferItem byteBufferItem) {
        if (byteBufferItem == null) return;

        // Each item may be used by several objects/threads. It will
        // only be released for reuse if everyone releases their claim.
        if (byteBufferItem.decrementCounter()) {
            sequence.set(byteBufferItem.getSequence());
        }
    }

}
