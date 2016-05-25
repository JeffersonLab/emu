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


import org.jlab.coda.jevio.EvioEvent;
import org.jlab.coda.jevio.EvioNode;

import java.nio.ByteBuffer;
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
     * Get the byte order of the data contained in this object.
     * @return byte order of the data contained in this object.
     */
    public ByteOrder getByteOrder();

    /**
     * Copy the argument's data into this RingItem object.
     * @param ringItem RingItem to copy from
     */
    public void copy(RingItem ringItem);

    /**
     * Get the evio event object contained in this ring item if any.
     * @return evio event object contained in this ring item if any (null if none).
     */
    public EvioEvent getEvent();

    /**
     * Set the evio event object contained in this ring item.
     * @param event evio event object to be contained in this ring item.
     */
    public void setEvent(EvioEvent event);

    /**
     * Get the ByteBuffer object (containing evio data) referenced by this ring item if any.
     * Used in conjunction with {{@link #getNode()}}.
     * @return ByteBuffer object referenced by this ring item if any (null if none).
     */
    public ByteBuffer getBuffer();

    /**
     * Set the ByteBuffer object (containing evio data) referenced by this ring item if any.
     * Used in conjunction with {{@link #setNode(org.jlab.coda.jevio.EvioNode)}}.
     * @param buffer ByteBuffer object to be referenced by this ring item.
     */
    public void setBuffer(ByteBuffer buffer);

    /**
     * Get the EvioNode object which selects a particular event in the ByteBuffer
     * referenced by this ring item, if any.
     * Used in conjunction with {{@link #getBuffer()}}.
     * @return EvioNode object which selects a particular event in the ByteBuffer
     *         referenced by this ring item, if any (null if none).
     */
    public EvioNode getNode();

    /**
     * Set the EvioNode object which selects a particular event in the ByteBuffer
     * referenced by this ring item, if any.
     * Used in conjunction with {{@link #setBuffer(java.nio.ByteBuffer)}}.
     * @param node EvioNode object which selects a particular event in the ByteBuffer
     *             referenced by this ring item.
     */
    public void setNode(EvioNode node);

    /**
     * Get the length of this structure in bytes, including the header.
     * @return the length of this structure in bytes, including the header.
     */
    public int getTotalBytes();

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
     * Is this object a USER event type?
     * @return {@code true} if USEr event type, else {@code false}.
     */
    public boolean isUser();

    /**
     * Is this object a first event?
     * @return {@code true} if first event, else {@code false}.
     */
    public boolean isFirstEvent();

    /**
     * Set whether this object a first event.
     * @param isFirst {@code true} if first event, else {@code false}.
     */
    public void isFirstEvent(boolean isFirst);

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
     * @return CODA id number of input ROC raw data source.
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
     * This id is copied into each RingItem and many items can have the
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

    /**
     * Releases a ByteBuffer (if any) referenced by this object which was obtained
     * from a ByteBufferSupply object. This allows the buffer to be reused.
     */
    public void releaseByteBuffer();

    /**
     * Set the objects needed to release (by calling {@link #releaseByteBuffer()})
     * the referenced ByteBuffer when no longer needed.
     * @param byteBufferSupply object which supplied the ByteBuffer
     * @param byteBufferItem   object wrapping ByteBuffer in the supply
     */
    public void setReusableByteBuffer(ByteBufferSupply byteBufferSupply,
                                      ByteBufferItem byteBufferItem);

    /**
     * Get the ByteBufferSupply object used to create the referenced ByteBuffer (if any).
     * @return ByteBufferSupply object used to create the referenced ByteBuffer (if any, else null).
     */
    public ByteBufferSupply getByteBufferSupply();

    /**
     * Get the ByteBufferItem object used to wrap the referenced ByteBuffer (if any).
     * @return ByteBufferItem object used to wrap the referenced ByteBuffer (if any, else null).
     */
    public ByteBufferItem getByteBufferItem();


    /**
     * For a ringItem producer, when a new item is obtained, it must have all
     * of its values set or reset. This does so in one convenient method.
     * Note that in general, only 1 of ev, buf, or node is specified.
     *
     * @param ev          EvioEvent object   (1st way of specifying evio data)
     * @param buf         ByteBuffer object  (2nd way of specifying evio data)
     * @param nd          EvioNode object    (3rd way of specifying evio data)
     * @param eType       type of event: roc raw, physics, user, control, etc.
     * @param cType       type of control event: prestart, go, end, etc.
     * @param user        is this a USER event type
     * @param first       is this a "first" or "beginning-of-run" event
     * @param chanId      ID of data channel this method is called for
     * @param rId         record ID
     * @param sId         CODA ID of data source
     * @param evCount     number of events in data
     * @param sName       name of data source
     * @param bbItem      ByteBufferItem object holding the ByteBuffer containing
     *                    the data
     * @param bbSupply    Supply object containing the ByteBufferItem
     */
    public void setAll(EvioEvent ev, ByteBuffer buf, EvioNode nd, EventType eType,
                       ControlType cType, boolean user, boolean first, int chanId,
                       int rId, int sId, int evCount, String sName,
                       ByteBufferItem bbItem, ByteBufferSupply bbSupply);

}
