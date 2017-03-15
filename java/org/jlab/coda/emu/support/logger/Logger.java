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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

/**
 * This class is used as abstraction layer for log messages
 * Minimum Log4j implementation with multiple overloaded functions
 *
 * @author vlads
 */
public class Logger {

    /** Field FQCN */
    private final String FQCN = Logger.class.getName();

    /** Field fqcnSet */
    private final Set<String> fqcnSet = new HashSet<>();

    /** Field logFunctionsSet */
    private final Set<String> logFunctionsSet = new HashSet<>();

    /** Field java13 */
    private boolean java13 = false;

    /** Field loggerAppenders */
    private final List<LoggerAppender> loggerAppenders = new Vector<>();

    /** Field enable_debug */
    private boolean enable_debug = true;

    /** Field enable_error */
    private boolean enable_error = true;


    public Logger () {
        fqcnSet.add(FQCN);
        // Message class can be moved to different sub project, See call to addLogOrigin
        // Also Message class can be refactored by ProGuard

        addAppender(new StdOutAppender());

        logFunctionsSet.add("debug");
        logFunctionsSet.add("log");
        logFunctionsSet.add("error");
        logFunctionsSet.add("fatal");
        logFunctionsSet.add("info");
        logFunctionsSet.add("warn");

    }

    /** Method toggleDebug ... */
    public void toggleDebug() {
        enable_debug = !enable_debug;
    }

    /** Method toggleError ... */
    public void toggleError() {
        enable_error = !enable_error;
    }

    /**
     * Method isDebugEnabled returns the debugEnabled of this Logger object.
     *
     * @return the debugEnabled (type boolean) of this Logger object.
     */
    public boolean isDebugEnabled() {
        return enable_debug;
    }

    /**
     * Method isErrorEnabled returns the errorEnabled of this Logger object.
     *
     * @return the errorEnabled (type boolean) of this Logger object.
     */
    public boolean isErrorEnabled() {
        return enable_error;
    }

    /**
     * Method getLocation ...
     *
     * @param level of type int
     * @return StackTraceElement
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    private StackTraceElement getLocation(int level) {

        if (java13 || (level < LoggingEvent.BUG)) {
            return null;
        }
        try {
            StackTraceElement[] ste = new Throwable().getStackTrace();
            boolean wrapperFound = false;
            for (int i = 0; i < ste.length - 1; i++) {
                if (fqcnSet.contains(ste[i].getClassName())) {
                    wrapperFound = false;
                    String nextClassName = ste[i + 1].getClassName();
                    if (nextClassName.startsWith("java.") || nextClassName.startsWith("sun.")) {
                        continue;
                    }
                    if (!fqcnSet.contains(nextClassName)) {
                        if (logFunctionsSet.contains(ste[i + 1].getMethodName())) {
                            wrapperFound = true;
                        } else {
                            // if dynamic proxy classes
                            if (nextClassName.startsWith("$Proxy")) {
                                return ste[i + 2];
                            } else {
                                return ste[i + 1];
                            }
                        }
                    }
                } else if (wrapperFound) {
                    if (!logFunctionsSet.contains(ste[i].getMethodName())) {
                        return ste[i];
                    }
                }
            }
        } catch (Throwable e) {
            java13 = true;
        }
        return null;
    }

    /**
     * Method write ...
     *
     * @param level     of type int
     * @param message   of type String
     * @param throwable of type Throwable
     */
    private void write(int level, String message, Throwable throwable) {
        callAppenders(new LoggingEvent(level, message, getLocation(level), throwable));
    }

    /**
     * Method write ...
     *
     * @param level     of type int
     * @param message   of type String
     * @param throwable of type Throwable
     * @param data      of type Object
     */
    private void write(int level, String message, Throwable throwable, Object data) {
        callAppenders(new LoggingEvent(level, message, getLocation(level), throwable, data));
    }

    /**
     * Method write ...
     *
     * @param level     of type int
     * @param message   of type String
     * @param data      of type Object
     */
    private void write(int level, String message, Object data) {
        callAppenders(new LoggingEvent(level, message, null, null, data));
    }

    /**
     * Method debug ...
     *
     * @param message of type String
     */
    public void debug(String message) {
        if (isDebugEnabled()) {
            write(LoggingEvent.DEBUG, message, null);
        }
    }

    /**
     * Method debug ...
     *
     * @param message of type String
     * @param t       of type Throwable
     */
    public void debug(String message, Throwable t) {
        if (isDebugEnabled()) {
            write(LoggingEvent.DEBUG, message, t);
        }
    }

    /**
     * Method debug ...
     *
     * @param t of type Throwable
     */
    public void debug(Throwable t) {
        if (isDebugEnabled()) {
            write(LoggingEvent.DEBUG, "error", t);
        }
    }

    /**
     * Method debug ...
     *
     * @param message of type String
     * @param v       of type String
     */
    public void debug(String message, String v) {
        if (isDebugEnabled()) {
            write(LoggingEvent.DEBUG, message, null, v);
        }
    }

    /**
     * Method debug ...
     *
     * @param message of type String
     * @param o       of type Object
     */
    public void debug(String message, Object o) {
        if (isDebugEnabled()) {
            write(LoggingEvent.DEBUG, message, null, new LoggerDataWrapper(o));
        }
    }

    /**
     * Method debug ...
     *
     * @param message of type String
     * @param v1      of type String
     * @param v2      of type String
     */
    public void debug(String message, String v1, String v2) {
        if (isDebugEnabled()) {
            write(LoggingEvent.DEBUG, message, null, new LoggerDataWrapper(v1, v2));
        }
    }

    /**
     * Method debug ...
     *
     * @param message of type String
     * @param v       of type long
     */
    public void debug(String message, long v) {
        if (isDebugEnabled()) {
            write(LoggingEvent.DEBUG, message, null, new LoggerDataWrapper(v));
        }
    }

    /**
     * Method debug ...
     *
     * @param message of type String
     * @param v1      of type long
     * @param v2      of type long
     */
    public void debug(String message, long v1, long v2) {
        if (isDebugEnabled()) {
            write(LoggingEvent.DEBUG, message, null, new LoggerDataWrapper(v1, v2));
        }
    }

    /**
     * Method debug ...
     *
     * @param message of type String
     * @param v       of type boolean
     */
    public void debug(String message, boolean v) {
        if (isDebugEnabled()) {
            write(LoggingEvent.DEBUG, message, null, new LoggerDataWrapper(v));
        }
    }

    /**
     * Method debugClassLoader ...
     *
     * @param message of type String
     * @param obj     of type Object
     */
    public void debugClassLoader(String message, Object obj) {
        if (obj == null) {
            write(LoggingEvent.DEBUG, message + " no class, no object", null, null);
            return;
        }
        Class klass;
        StringBuilder buf = new StringBuilder();
        buf.append(message).append(" ");
        if (obj instanceof Class) {
            klass = (Class) obj;
            buf.append("class ");
        } else {
            klass = obj.getClass();
            buf.append("instance ");
        }
        buf.append(klass.getName()).append(" loaded by ");
        if (klass.getClassLoader() != null) {
            buf.append(klass.getClassLoader().hashCode());
            buf.append(" ");
            buf.append(klass.getClassLoader().getClass().getName());
        } else {
            buf.append("system");
        }
        write(LoggingEvent.DEBUG, buf.toString(), null, null);
    }

    /**
     * Method info ...
     *
     * @param message of type String
     */
    public void info(String message) {
        if (isErrorEnabled()) {
            write(LoggingEvent.INFO, message, null);
        }
    }

    /**
     * Method info ...
     *
     * @param message of type String
     * @param data    of type String
     */
    public void info(String message, String data) {
        if (isErrorEnabled()) {
            write(LoggingEvent.INFO, message, null, data);
        }
    }

    /**
     * Method warn ...
     *
     * @param message of type String
     */
    public void warn(String message) {
        if (isErrorEnabled()) {
            write(LoggingEvent.WARN, message, null);
        }
    }

    /**
     * Method error ...
     *
     * @param message of type String
     */
    public void error(String message) {
        if (isErrorEnabled()) {
            write(LoggingEvent.ERROR, message, null);
        }
    }

    /**
     * Method error ...
     *
     * @param message of type String
     * @param v       of type long
     */
    public void error(String message, long v) {
        if (isErrorEnabled()) {
            write(LoggingEvent.ERROR, message, null, new LoggerDataWrapper(v));
        }
    }

    /**
     * Method error ...
     *
     * @param message of type String
     * @param v       of type String
     */
    public void error(String message, String v) {
        if (isErrorEnabled()) {
            write(LoggingEvent.ERROR, message, null, v);
        }
    }

    /**
     * Method error ...
     *
     * @param message of type String
     * @param v       of type String
     * @param t       of type Throwable
     */
    public void error(String message, String v, Throwable t) {
        if (isErrorEnabled()) {
            write(LoggingEvent.ERROR, message, t, v);
        }
    }

    /**
     * Method error ...
     *
     * @param t of type Throwable
     */
    public void error(Throwable t) {
        if (isErrorEnabled()) {
            write(LoggingEvent.ERROR, t.toString(), t);
        }
    }

    /**
     * Method error ...
     *
     * @param message of type String
     * @param t       of type Throwable
     */
    public void error(String message, Throwable t) {
        if (isErrorEnabled()) {
            write(LoggingEvent.ERROR, message + " " + t.toString(), t);
        }
    }

    /**
     * Method rc gui console ...
     *
     * @param message of type String
     * @param severityText text describing severity of message,
     *                     in this case it's user settable
     */
    public void rcConsole(String message, String severityText) {
        if (isErrorEnabled()) {
            write(LoggingEvent.RC_GUI_CONSOLE, message, severityText);
        }
    }

    /**
     * Method callAppenders ...
     *
     * @param event of type LoggingEvent
     */
    private void callAppenders(LoggingEvent event) {
        for (LoggerAppender a : loggerAppenders) {
            a.append(event);
        }
    }

    /**
     * Add the Class which serves as entry point for log message location.
     *
     * @param origin Class
     */
    public void addLogOrigin(Class origin) {
        fqcnSet.add(origin.getName());
    }

    /**
     * Method addAppender ...
     *
     * @param newAppender of type LoggerAppender
     */
    public void addAppender(LoggerAppender newAppender) {
        loggerAppenders.add(newAppender);
    }

    /**
     * Method removeAppender ...
     *
     * @param appender of type LoggerAppender
     */
    public void removeAppender(LoggerAppender appender) {
        loggerAppenders.remove(appender);
    }
}
