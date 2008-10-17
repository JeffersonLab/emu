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

import java.io.PrintStream;

/** @author vlads */
public class StdOutAppender implements LoggerAppender {

    /** Field enabled */
    public static boolean enabled = true;

    /**
     * Method formatLocation ...
     *
     * @param ste of type StackTraceElement
     * @return String
     */
    private static String formatLocation(StackTraceElement ste) {
        if (ste == null) {
            return "";
        }
        // Make Line# clickable in eclipse
        return ste.getClassName() + "." + ste.getMethodName() + "(" + ste.getFileName() + ":" + ste.getLineNumber() + ")";
    }

    /**
     * Method append ...
     *
     * @param event of type LoggingEvent
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.cmsglogger.log.log.LoggerAppender#append(org.jlab.coda.cmsglogger.log.log.LoggingEvent)
    */
    public void append(LoggingEvent event) {
        if (!enabled) {
            return;
        }
        PrintStream out = System.out;
        if (event.getLevel() == LoggingEvent.ERROR) {
            out = System.err;
        }
        String data = "";
        if (event.hasData()) {
            data = " [" + event.getFormatedData() + "]";
        }
        out.println(event.getMessage() + data + "\t  " + formatLocation(event.getLocation()));
        if (event.getThrowable() != null) {
            event.getThrowable().printStackTrace(out);
        }

    }

}
