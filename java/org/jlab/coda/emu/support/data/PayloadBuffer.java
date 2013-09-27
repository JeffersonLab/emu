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

    private ControlType controlType;

    private EventType eventType;

    private int sourceId;

    private int recordId;

    private int headerLength;

    /**
     * Constructor that sets attachment to null.
     * @param buffer ByteBuffer to wrap.
     */
    public PayloadBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        this.attachment = null;
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
        this.buffer = buffer.getBuffer().duplicate();
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

    public int getHeaderLength() {
        return headerLength;
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
