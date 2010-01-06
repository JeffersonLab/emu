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

import java.io.PrintWriter;

/** @author vlads */
public class PrintAppender implements LoggerAppender {

    /** Field out */
    private final PrintWriter out;

    /**
     * Constructor PrintAppender creates a new PrintAppender instance.
     *
     * @param outp of type PrintWriter
     */
    public PrintAppender(PrintWriter outp) {
        out = outp;
    }

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
