/*
 * Copyright (c) 2013, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Wrapper for a ByteBuffer object that has an attachment.
 * This class plays the same role that PayloadBank plays
 * except for ByteBuffer instead of EvioBank objects.
 *
 * @author timmer
 * (5/16/13)
 */
public class PayloadBuffer implements Cloneable, Attached {

    /** ByteBuffer to wrap. */
    private ByteBuffer buffer;

    /** Associated object. */
    private Object attachment;

    /** What type of CODA events are contained in this bank (RocRaw, Physics, Control, ...)?
     *  Only one type is stored in one PayloadBank object.
     *  Only one control event is stored in one PayloadBank object. */
    private EventType eventType;

    /** If this is a control event, what type of control is it (SYNC, GO, END, ...)? */
    private ControlType controlType;

    /** If the event type is RocRaw, this is the CODA id of the source. */
    private int sourceId;

    /** The name of the source of these CODA events. */
    private String sourceName;

    /** If the event type is RocRaw or Physics, this is the record id of this CODA events.
     *  The record id is incremented by one for each ET event. Many CODA events (triggers)
     *  may have the same record id. */
    private int recordId;


    /**
     * Constructor that sets attachment to null.
     * @param buffer ByteBuffer to wrap.
     */
    public PayloadBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        this.attachment = null;
    }

    /**
     * Constructor that sets attachment to null.
     * @param buffer      ByteBuffer to wrap.
     * @param eventType   type of CODA events contained.
     * @param controlType if Control eventType, the type of control.
     * @param recordId    if Physics or RocRaw, the record id of CODA events.
     * @param sourceId    If RocRaw, the CODA id of the source.
     * @param sourceName  The name of the source of these CODA events.
     */
    public PayloadBuffer(ByteBuffer buffer, EventType eventType, ControlType controlType,
                         int recordId, int sourceId, String sourceName) {
        this.buffer      = buffer;
        this.eventType   = eventType;
        this.controlType = controlType;
        this.recordId    = recordId;
        this.sourceId    = sourceId;
        this.sourceName  = sourceName;
        this.attachment  = null;
    }

    /**
     * Constructor.
     * @param buffer ByteBuffer to wrap.
     * @param attachment object to associate with the ByteBuffer.
     */
    public PayloadBuffer(ByteBuffer buffer, Object attachment) {
        this.buffer = buffer;
        this.attachment = attachment;
    }

    /**
     * Copy constructor.
     * @param buffer ByteBuffer to wrap.
     */
    public PayloadBuffer(PayloadBuffer buffer) {
        // Share content but keep different limit, position, mark.
        // This will work if and only if the buffer is written to.
        ByteBuffer buf   = buffer.getBuffer();
        this.buffer      = buf.duplicate();

        this.eventType   = buffer.eventType;
        this.controlType = buffer.controlType;
        this.recordId    = buffer.recordId;
        this.sourceId    = buffer.sourceId;
        this.sourceName  = buffer.sourceName;
    }

    /**
     * Get the ByteBuffer.
     * @return the ByteBuffer object.
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }

    /**
     * Get the length of this structure in bytes, including the header.
     * @return the length of this structure in bytes, including the header.
     */
    public int getTotalBytes() {
        return buffer.remaining();
    }

    /**
     * What is the byte order of this data?
     * @return {@link java.nio.ByteOrder#BIG_ENDIAN} or {@link java.nio.ByteOrder#LITTLE_ENDIAN}
     */
    public ByteOrder getByteOrder() {
        return buffer.order();
    }

    /**
     * Get the attachment object.
     * @return the attachment object.
     */
    public Object getAttachment() {
        return attachment;
    }

 // TODO: Deal with this!
    /**
     * Get the length of the evio bank's header in words.
     * @return length of the evio bank's header in words.
     */
    public int getHeaderLength() {
        return 123;
    }

    /**
     * Set the attachment object.
     * @param attachment the attachment object.
     */
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    public ControlType getControlType() {
        return controlType;
    }

    public void setControlType(ControlType type) {
        this.controlType = type;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType type) {
        this.eventType = type;
    }

    public int getSourceId() {
        return sourceId;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public int getRecordId() {
        return recordId;
    }

    public void setRecordId(int recordId) {
        this.recordId = recordId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }


    /** Clones this object setting the attachment to null. */
    public Object clone() {
        try {
            // Creates a bit wise copy (including only
            // references for payloadBuffer & payloadBank).
            PayloadBuffer item = (PayloadBuffer) super.clone();
            item.attachment = null;

            // Allocate memory
            item.buffer = ByteBuffer.allocate(buffer.capacity());

            // Store our current position and limit
            int pos = buffer.position();
            int lim = buffer.limit();

            // Copy all data
            buffer.limit(buffer.capacity()).position(0);
            item.buffer.put(buffer);

            // restore original values
            buffer.limit(lim).position(pos);

            return item;
        }
        catch (CloneNotSupportedException e) {
            return null;
        }
    }

}
