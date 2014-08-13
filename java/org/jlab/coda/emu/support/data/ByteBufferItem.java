package org.jlab.coda.emu.support.data;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used in conjunction with the {@link ByteBufferSupply} class
 * to provide a very fast supply of ByteBuffer objects for reuse. Objects of
 * this class are used to populate the ring buffer in ByteBufferSupply.
 * Uses the Disruptor software package.
 * @author timmer (4/14/14)
 */
public class ByteBufferItem {

    /** Size of ByteBuffer in bytes. */
    private int bufferSize;

    /** ByteBuffer object. */
    private ByteBuffer buffer;

    /** Sequence in which this object was taken from ring for use by a producer with get(). */
    private long producerSequence;

    /** Sequence in which this object was taken from ring for use by a consumer with consumerGet(). */
    private long ConsumerSequence;

    /** How many users does this object have? */
    private volatile int users;

    /** Track more than one user so this object can be released for reuse. */
    private AtomicInteger atomicCounter;

    // For testing purposes

    /** Counter for assigning unique id to each buffer item. */
    static int idCounter=0;

    /** Unique id for each object of this class. */
    private int myId;

    /**
     * Get the unique id of this object.
     * @return unique id of this object.
     */
    public int getMyId() {return myId;}

    //--------------------------------

    /**
     * Constructor.
     * @param bufferSize size in bytes of ByteBuffer to construct.
     */
    public ByteBufferItem(int bufferSize) {
        this.bufferSize = bufferSize;
        buffer = ByteBuffer.allocate(bufferSize);
        myId = idCounter++;
    }


    /**
     * Get the sequence of this item for producer.
     * @return sequence of this item for producer.
     */
    public long getProducerSequence() {return producerSequence;}


    /**
     * Set the sequence of this item for producer.
     * @param sequence sequence of this item for producer.
     */
    public void setProducerSequence(long sequence) {this.producerSequence = sequence;}


    /**
     * Get the sequence of this item for consumer.
     * @return sequence of this item for consumer.
     */
    public long getConsumerSequence() {return ConsumerSequence;}


    /**
     * Set the sequence of this item for consumer.
     * @param sequence sequence of this item for consumer.
     */
    public void setConsumerSequence(long sequence) {this.ConsumerSequence = sequence;}


    /**
     * Get the size in bytes of the contained ByteBuffer.
     * @return size in bytes of the contained ByteBuffer.
     */
    public int getBufferSize() {return bufferSize;}


    /**
     * Get the contained ByteBuffer.
     * @return contained ByteBuffer.
     */
    public ByteBuffer getBuffer() {return buffer;}


    /**
     * Make sure the buffer is the size needed.
     * @param capacity minimum necessary size of buffer in bytes.
     */
    public void ensureCapacity(int capacity) {
        if (bufferSize < capacity) {
            buffer = ByteBuffer.allocate(capacity);
            bufferSize = capacity;
        }
    }


    /**
     * Set the contained ByteBuffer.
     * Generally called when existing buffer is too small
     * and size must be increased. Allows for dynamic adjustment
     * to need for bigger buffers.
     *
     * @param buffer the ByteBuffer to be contained.
     */
    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        bufferSize = buffer.capacity();
    }


    /**
     * Set the number of users of this buffer.
     * If multiple users of the buffer exist,
     * keep track of all until last one is finished.
     *
     * @param users number of buffer users
     */
    public void setUsers(int users) {
        this.users = users;
        // Only need to use atomic counter if more than 1 user
        if (users > 1) {
            atomicCounter = new AtomicInteger(users);
        }
    }


    /**
     * Called by buffer user if longer using it so it may be reused later.
     * @return {@code true} if no one using buffer now, else {@code false}.
     */
    public boolean decrementCounter() {
        // Only use atomic object if "users" initially > 1
        if (users > 1) {
            return atomicCounter.decrementAndGet() < 1;
        }
        return true;
    }

}
