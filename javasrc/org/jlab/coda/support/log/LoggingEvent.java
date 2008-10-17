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

/** @author vlads */
public class LoggingEvent {

    /** Field DEBUG */
    public final static int DEBUG = 1;

    /** Field INFO */
    public final static int INFO = 2;

    /** Field WARN */
    public final static int WARN = 3;

    /** Field ERROR */
    public final static int ERROR = 4;

    /** Field BUG */
    public final static int BUG = 5;

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
    Object getData() {
        return this.data;
    }

    /**
     * Method setData sets the data of this LoggingEvent object.
     *
     * @param data the data of this LoggingEvent object.
     */
    void setData(Object data) {
        this.data = data;
        this.hasData = true;
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
            if (getData() == null) {
                return "{null}";
            } else {
                return getData().toString();
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
            default:
                return "UNKNOWN";
        }
    }

}
