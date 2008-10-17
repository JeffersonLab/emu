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
package org.jlab.coda.support.log;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 * Class <b>QueueAppender </b>
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
     * @param buferSize of type int
     */
    public QueueAppender(int buferSize) {
        queue = new ArrayBlockingQueue<LoggingEvent>(buferSize);

    }

    /**
     * Method append ...
     *
     * @param event of type LoggingEvent
     */
    public void append(LoggingEvent event) {
        queue.add(event);
        if (queue.size() > 1000) {
            try {
                queue.take();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * Method poll ...
     *
     * @return LoggingEvent
     */
    public LoggingEvent poll() {
        return queue.poll();
    }

    /**
     * Method poll ...
     *
     * @return LoggingEvent
     */
    public LoggingEvent poll(int wait) {

        try {
            return queue.poll(wait, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

}
