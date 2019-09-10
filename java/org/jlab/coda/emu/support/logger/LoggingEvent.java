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

/** @author vlads */
public class LoggingEvent {

    // Severity ID #:
    // 0     = reserved
    // 1-4   = info
    // 5-8   = warning
    // 9-12  = error
    // 13-14 = severe
    // 15    = rc gui console info msg, severity text is settable (green)
    // 16    = special warning known only to CODA (yellow)
    //
    // < 9 is ignored by rc gui

    /** DEBUG msg. Does not show up on RC gui. */
    public final static int DEBUG = 1;

    /** INFO msg. Does not show up on RC gui. */
    public final static int INFO = 2;

    /** WARNING msg, 5 - 8. Does not show up on RC gui. */
    public final static int WARN = 8;

    /** ERROR msg, 9 - 12. Will show up on RC gui as red.*/
    public final static int ERROR = 9;

    /** BUG msg, 13. Will show up as severe error on RC gui. */
    public final static int BUG = 13;

    /** RC GUI CONSOLE msg, 15. Will show up on RC gui as green. */
    public final static int RC_GUI_CONSOLE = 15;

    /** CODA_WARNING msg, 16. Will show up on RC gui as yellow. */
    public final static int CODA_WARN = 16;

    /** Field level */
    private int level;

    /** Field message */
    private String message;

    /** Field location */
    private StackTraceElement location;

    /** Field hasData */
    private boolean hasData = false;

    /** Field data */
    private Object data;

    /** Field throwable */
    private Throwable throwable;

    /** Field eventTime */
    private final long eventTime;

    /** Constructor LoggingEvent creates a new LoggingEvent instance. */
    private LoggingEvent() {
        this.eventTime = System.currentTimeMillis();
    }

    /**
     * Constructor LoggingEvent creates a new LoggingEvent instance.
     *
     * @param level     of type int
     * @param message   of type String
     * @param location  of type StackTraceElement
     * @param throwable of type Throwable
     */
    public LoggingEvent(int level, String message, StackTraceElement location, Throwable throwable) {
        this();
        this.level = level;
        this.message = message;
        this.location = location;
        this.throwable = throwable;
    }

    /**
     * Constructor LoggingEvent creates a new LoggingEvent instance.
     *
     * @param level     of type int
     * @param message   of type String
     * @param location  of type StackTraceElement
     * @param throwable of type Throwable
     * @param data      of type Object
     */
    public LoggingEvent(int level, String message, StackTraceElement location, Throwable throwable, Object data) {
        this(level, message, location, throwable);
        setData(data);
    }

    /**
     * Method getData returns the data of this LoggingEvent object.
     *
     * @return the data (type Object) of this LoggingEvent object.
     */
    public Object getData() {
        return data;
    }

    /**
     * Method setData sets the data of this LoggingEvent object.
     *
     * @param data the data of this LoggingEvent object.
     */
    public void setData(Object data) {
        this.data = data;
        hasData = true;
    }

    /**
     * Method hasData ...
     *
     * @return boolean
     */
    public boolean hasData() {
        return this.hasData;
    }

    /**
     * Method getFormatedData returns the formatedData of this LoggingEvent object.
     *
     * @return the formatedData (type String) of this LoggingEvent object.
     */
    public String getFormatedData() {
        if (hasData()) {
            if (data == null) {
                return "{null}";
            } else {
                return data.toString();
            }
        } else {
            return "";
        }
    }

    /**
     * Method getEventTime returns the eventTime of this LoggingEvent object.
     *
     * @return the eventTime (type long) of this LoggingEvent object.
     */
    public long getEventTime() {
        return this.eventTime;
    }

    /**
     * Method getLevel returns the level of this LoggingEvent object.
     *
     * @return the level (type int) of this LoggingEvent object.
     */
    public int getLevel() {
        return this.level;
    }

    /**
     * Method getLocation returns the location of this LoggingEvent object.
     *
     * @return the location (type StackTraceElement) of this LoggingEvent object.
     */
    public StackTraceElement getLocation() {
        return this.location;
    }

    /**
     * Method getMessage returns the message of this LoggingEvent object.
     *
     * @return the message (type String) of this LoggingEvent object.
     */
    public String getMessage() {
        return this.message;
    }

    /**
     * Method getThrowable returns the throwable of this LoggingEvent object.
     *
     * @return the throwable (type Throwable) of this LoggingEvent object.
     */
    public Throwable getThrowable() {
        return this.throwable;
    }


    /**
     * Get the string associated with the format level of this event.
     * @return string associated with the format level of this event.
     */
    public String getFormatedLevel() {
        switch (level) {
            case BUG:
                return "BUG";
            case ERROR:
                return "ERROR";
            case WARN:
                return "WARN";
            case INFO:
                return "INFO";
            case DEBUG:
                return "DEBUG";
            case RC_GUI_CONSOLE:
                if (hasData) {
                    return (String)data;
                }
                return "NO_LEVEL";
            case CODA_WARN:
                if (hasData) {
                    return (String)data;
                }
                return "WARN";
            default:
                return "UNKNOWN";
        }
    }

}
