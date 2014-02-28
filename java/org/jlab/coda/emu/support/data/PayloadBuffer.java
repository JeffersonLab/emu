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

import org.jlab.coda.jevio.EvioNode;

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
public class PayloadBuffer extends QueueItemAdapter {

    /** ByteBuffer to wrap. */
    private ByteBuffer buffer;

    /** Object containing info about the buffer. */
    private EvioNode node;



    /**
     * Constructor that sets attachment to null.
     * @param buffer ByteBuffer to wrap.
     */
    public PayloadBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Constructor.
     * @param buffer     ByteBuffer to wrap.
     * @param attachment object to associate with the ByteBuffer.
     * @param node       object holding details about the buffer.
     */
    public PayloadBuffer(ByteBuffer buffer, Object attachment, EvioNode node) {
        this.node = node;
        this.buffer = buffer;
        this.attachment = attachment;
    }

    /**
     * Constructor that sets attachment to null.
     * @param buffer      ByteBuffer to wrap.
     * @param eventType   type of CODA events contained.
     * @param controlType if Control eventType, the type of control.
     * @param recordId    if Physics or RocRaw, the record id of CODA events.
     * @param sourceId    If RocRaw, the CODA id of the source.
     * @param sourceName  The name of the source of these CODA events.
     * @param node        object holding details about the buffer.
     */
    public PayloadBuffer(ByteBuffer buffer, EventType eventType, ControlType controlType,
                         int recordId, int sourceId, String sourceName, EvioNode node) {
        this.buffer      = buffer;
        this.eventType   = eventType;
        this.controlType = controlType;
        this.recordId    = recordId;
        this.sourceId    = sourceId;
        this.sourceName  = sourceName;
        this.attachment  = null;
        this.node        = node;
    }

    /**
     * Copy constructor.
     * @param buf ByteBuffer to wrap.
     */
    public PayloadBuffer(PayloadBuffer buf) {
        super(buf);
        // Share content but keep different limit, position, mark.
        // This will work if and only if the buffer is written to.
        buffer = buf.buffer.duplicate();
        node   = buf.node;
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


 // TODO: Deal with this!
    /**
     * Get the length of the evio bank's header in words.
     * @return length of the evio bank's header in words.
     */
    public int getHeaderLength() {
        return 123;
    }

    public EvioNode getNode() {
        return node;
    }

    public QueueItemType getQueueItemType() {return QueueItemType.PayloadBuffer;}
//    public void setQueueItemType(QueueItemType queueItemType) {;}

    /** Clones this object setting the attachment to null. */
    public Object clone() {
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

}
