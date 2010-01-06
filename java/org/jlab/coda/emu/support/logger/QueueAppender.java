/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */
package org.jlab.coda.emu.support.logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 * Class <b>QueueAppender </b> which keeps a queue of logging events appended.
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class QueueAppender implements LoggerAppender {

    /** Field queue */
    private final ArrayBlockingQueue<LoggingEvent> queue;

    /**
     * Constructor QueueAppender creates a new QueueAppender instance.
     *
     * @param bufferSize size of queue to create
     */
    public QueueAppender(int bufferSize) {
        queue = new ArrayBlockingQueue<LoggingEvent>(bufferSize);
    }

    /**
     * Method append ...
     *
     * @param event of type LoggingEvent
     */
    public void append(LoggingEvent event) {
        // Returns true if able to add event, else it throws
        // an IllegalStateException if the queue is full.
        queue.add(event);
        
        // if Q size over 1000, get rid of the first item put on the queue
        if (queue.size() > 1000) {
            try {
                // will block if Q is empty
                queue.take();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * Return a LoggingEvent object from the internal storage queue.
     * If the queue is empty, return null.
     *
     * @return LoggingEvent object from the internal queue or null if queue is empty
     */
    public LoggingEvent poll() {
        // returns null if Q is empty
        return queue.poll();
    }

    /**
     * Return a LoggingEvent object from the internal storage queue.
     * If the queue is empty, return null.
     *
     * @param wait time in seconds to wait if queue is empty
     * @return LoggingEvent object from the internal queue or null if queue is empty
     *         and timeout has expired
     */
    public LoggingEvent poll(int wait) {

        try {
            // returns null if Q is empty & time expired
            return queue.poll(wait, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

}
