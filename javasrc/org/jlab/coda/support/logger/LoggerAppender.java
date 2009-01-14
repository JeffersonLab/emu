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
package org.jlab.coda.support.logger;

/**
 * Interface LoggerAppender ...
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public interface LoggerAppender {

    /**
     * Method append ...
     *
     * @param event of type LoggingEvent
     */
    public void append(LoggingEvent event);
}
