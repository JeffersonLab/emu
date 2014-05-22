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


import java.nio.ByteOrder;

/**
 * This interface defines the type of item which may be passed to the EMU
 * through its transport channels.
 * This interface may be implemented to give EMU modules some flexibility
 * in the type of data they can handle.
 *
 * @author timmer
 * (Feb 27 2014)
 */
public interface RingItem extends Cloneable, Attached {

    /**
     * Get the type of object implementing this interface -
     * either a PayloadBank, or PayloadBuffer.
     * @return type of object implementing this interface.
     */
    public QueueItemType getQueueItemType();
    /**
     * Get the byte order of the data contained in this object.
     * @return byte order of the data contained in this object.
     */
    public ByteOrder getByteOrder();


    /**
     * Is this object a control event?
     * @return {@code true} if control event, else {@code false}.
     */
    public boolean isControlEvent();
    /**
     * If this is control event, this method returns the type of
     * control event, otherwise it returns null.
     * @return type of control event, else null.
     */
    public ControlType getControlType();
    /**
     * Set the type of control event this is.
     * @param type type of control event this is.
     */
    public void setControlType(ControlType type);


    /**
     * Get the type of event (ROC raw, physics, user, control , etc) this is.
     * @return type of event this is.
     */
    public EventType getEventType();
    /**
     * Set the type of event (ROC raw, physics, user, control , etc) this is.
     * @param type type of event this is.
     */
    public void setEventType(EventType type);


    /**
     * If emu input channel is reading ROC raw data, then this method
     * gets the CODA id number of the source.
     * @return  CODA id number of input ROC raw data source.
     */
    public int getSourceId();
    /**
     * If emu input channel is reading Roc Raw data, then this method
     * sets the CODA id number of the source.
     * @param sourceId CODA id number of input Roc Raw data source.
     */
    public void setSourceId(int sourceId);


    /**
     * Does the source id match that of the input channel?
     * @return {@code true} if source id matches that of the input channel,
     *         else {@code false}.
     */
    public boolean matchesId();

    /**
     * Set whether the source id matches that of the input channel.
     * @param matchId {@code true} if source id matches that of the input channel,
     *                else {@code false}.
     */
    public void matchesId(boolean matchId);


    /**
     * The recordId, for a physics or ROC Raw type, starts at zero and
     * increases by one in each successive "evio-file-format" data buffer.
     * It is set to -1 for other data types.
     * This id is copied into each QueueItem and many items can have the
     * same id. This method gets the record id.
     *
     * @return record id
     */
    public int getRecordId();
    /**
     * This method sets the record id.
     * @param recordId  the record id
     */
    public void setRecordId(int recordId);


    /**
     * Get the name of the data input channel (from xml config file)
     * which read the data into this object.
     * @return name of the data input channel from xml config file.
     */
    public String getSourceName();

    /**
     * Set the name of the data input channel (from xml config file)
     * which read the data into this object.
     * @param sourceName name of the data input channel from xml config file.
     */
    public void setSourceName(String sourceName);

    /**
     * Get the number of ROC events in this object's data.
     * @return number of ROC events in this object's data.
     */
    public int getEventCount();
    /**
     * Set the number of ROC events in this object's data.
     * @param eventCount number of ROC events in this object's data.
     */
    public void setEventCount(int eventCount);


    /**
     * Get the first event number in this object's data.
     * @return first event number in this object's data.
     */
    public long getFirstEventNumber();
    /**
     * Set the first event number in this object's data.
     * @param firstEventNumber first event number in this object's data.
     */
    public void setFirstEventNumber(long firstEventNumber);


    /**
     * Is this object is a sync event?
     * This condition is set by the ROC and it is only read - never set.
     * @return {@code true} if this object is a sync event, else {@code false}.
     */
    public boolean isSync();
    /**
     * Sets whether or not this object is a sync event.
     * This condition is set by the ROC and so this method is only used to
     * pass on that information.
     * @param sync {@code true} if this object is a sync event, else {@code false}.
     */
    public void setSync(boolean sync);


    /**
     * Is this object in single event mode (only 1 physics event in a block)?
     * @return {@code true} if this object is in single event mode, else {@code false}.
     */
    public boolean isSingleEventMode();
    /**
     * Sets whether or not this object is in single event mode.
     * @param singleEventMode {@code true} if this object is in single event mode,
     *                        else {@code false}.
     */
    public void setSingleEventMode(boolean singleEventMode);


    /**
     * Did this object already have an error when first received from transport?
     * @return {@code true} if this object had an error when first received,
     *         else {@code false}.
     */
    public boolean hasError();
    /**
     * Sets whether or not this object has an error.
     * @param hasError {@code true} if this object has an error,  else {@code false}.
     */
    public void setError(boolean hasError);


    /**
     * Did this object generate a non-fatal error while being built
     * (still allows build to continue) ?
     * @return {@code true} if this object generated a non-fatal build error, else {@code false}.
     */
    public boolean hasNonFatalBuildingError();
    /**
     * Sets whether or not this object generated a non-fatal error while being built.
     * @param nonFatalBuildingError {@code true} if this object generate a non-fatal
     *                              error while being built, else {@code false}.
     */
    public void setNonFatalBuildingError(boolean nonFatalBuildingError);


//    public void setSequence(long sequence);
//    public long getSequence();


    /**
     * Releases a ByteBuffer (if any) contained by this object which was obtained
     * from a ByteBufferSupply object. This allows the buffer to be reused.
     */
    public void releaseByteBuffer();

    /**
     * Set the objects needed to release (by calling {@link #releaseByteBuffer()})
     * the contained ByteBuffer when no longer needed.
     * @param byteBufferSupply object which supplied the ByteBuffer
     * @param byteBufferItem   object wrapping ByteBuffer in the supply
     */
    public void setReusableByteBuffer(ByteBufferSupply byteBufferSupply,
                                      ByteBufferItem byteBufferItem);

    /**
     * Get the ByteBufferSupply object used to create the contained ByteBuffer (if any).
     * @return ByteBufferSupply object used to create the contained ByteBuffer (if any, else null).
     */
    public ByteBufferSupply getByteBufferSupply();

    /**
     * Get the ByteBufferItem object used to wrap the contained ByteBuffer (if any).
     * @return ByteBufferItem object used to wrap the contained ByteBuffer (if any, else null).
     */
    public ByteBufferItem getByteBufferItem();


}
