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

import org.jlab.coda.et.EtEvent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used in conjunction with the
 * {@link org.jlab.coda.emu.support.data.EtEventsSupply} class
 * to provide a very fast supply of EtEvent arrays for reuse. Objects of
 * this class are used to populate the ring buffer in EtEventsSupply.
 * Uses the Disruptor software package.
 * @author timmer (7/28/2014)
 */
public class EtEventsItem {

    /** EtEvent array. */
    private EtEvent[] events;

    /** Sequence in which this object was taken from ring for use by a producer with get(). */
    private long producerSequence;

    /** Sequence in which this object was taken from ring for use by a consumer with consumerGet(). */
    private long ConsumerSequence;

    /** How many users does this object have? */
    private volatile int users;

    /** Track more than one user so this object can be released for reuse. */
    private AtomicInteger atomicCounter;

    /** How many of the array elements do we want to put back into ET system? */
    private int eventsToWrite;

    /** How many of the array elements contain meaningful objects? */
    private int totalEvents;

    // For testing purposes

    /** Counter for assigning unique id to each item. */
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
     */
    public EtEventsItem() {
        myId = idCounter++;
    }


    /**
     * Get the number of events to write from this array starting at item 0.
     * @return number of events to write from this array starting at item 0.
     */
    public int getEventsToWrite() {
        return eventsToWrite;
    }


    /**
     * Setthe number of events to write from this array starting at item 0.
     * @param eventsToWrite the number of events to write from this array starting at item 0.
     */
    public void setEventsToWrite(int eventsToWrite) {
        this.eventsToWrite = eventsToWrite;
    }


    /**
     * Get the number of array elements that contain meaningful objects.
     * @return number of array elements that contain meaningful objects.
     */
    public int getTotalEvents() {
        return totalEvents;
    }


    /**
     * Set the number of array elements that contain meaningful objects.
     * @param totalEvents the number of array elements that contain meaningful objects.
     */
    public void setTotalEvents(int totalEvents) {
        this.totalEvents = totalEvents;
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
     * Get the number of events in the array.
     * @return number of events in the array.
     */
    public int getArraySize() {
        if (events == null) return 0;
        return events.length;
    }


    /**
     * Get the contained EtEvent array.
     * @return contained EtEvent array.
     */
    public EtEvent[] getArray() {return events;}


    /**
     * Set the contained EtEvents array.
     * Generally called when array is too small
     * and size must be increased. Allows for dynamic adjustment.
     *
     * @param events the EtEvents array to be contained.
     */
    public void setArray(EtEvent[] events) {
        this.events = events;
    }


    /**
     * Set the contained EtEvents array, the number of events to write,
     * and the total number of significant array elements.
     *
     * @param events        the EtEvents array to be contained.
     * @param eventsToWrite the number of elements in the events array to be written.
     * @param totalEvents   the total number of meaningful elements in the events array.
     */
    public void setParameters(EtEvent[] events, int eventsToWrite, int totalEvents) {
        this.events = events;
        this.totalEvents = totalEvents;
        this.eventsToWrite = eventsToWrite;
    }


    /**
     * Set the number of users of this array.
     * If multiple users exist, keep track of all until last one is finished.
     *
     * @param users number of array users
     */
    public void setUsers(int users) {
        this.users = users;
        // Only need to use atomic counter if more than 1 user
        if (users > 1) {
            atomicCounter = new AtomicInteger(users);
        }
    }


    /**
     * Called by user if longer using array so it may be reused later.
     * @return {@code true} if no one using array now, else {@code false}.
     */
    public boolean decrementCounter() {
        // Only use atomic object if "users" initially > 1
        if (users > 1) {
            return atomicCounter.decrementAndGet() < 1;
        }
        return true;
    }

}
