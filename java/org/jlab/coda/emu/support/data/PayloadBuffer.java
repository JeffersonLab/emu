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
 * Wrapper for a ByteBuffer object that has external, associated data.
 *
 * @author timmer
 * (5/16/13)
 */
public class PayloadBuffer extends RingItemAdapter {


    /**
     * Zero-arg constructor.
     */
    public PayloadBuffer() {}


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

        // If data is in the buffer ...
        if (buf.buffer != null) {
            // Share content but keep different limit, position, mark.
            buffer = buf.buffer.duplicate();
            buffer.order(buf.buffer.order());
        }
        // If data is in the node ...
        else if (buf.node != null) {
            // Copying a buffer is easy, just duplicate it. However, copying an
            // EvioNode object is not so simple since it contains lots of references
            // to objects. The best solution (since this is only used in the ER to
            // copy user events, control events, and events going to ET output channel)
            // is to copy the data directly into a buffer and ditch the node object.

            // Get a copy of the node data into a new buffer
            buffer = buf.node.getStructureBuffer(true);
            node = null;
            // No longer tied to buffer supply
            byteBufferItem = null;
            byteBufferSupply = null;
        }
    }

    /**
     * Get the length of this structure in bytes, including the header.
     * @return the length of this structure in bytes, including the header.
     */
    public int getTotalBytes() {
        if (buffer != null) {
            return buffer.limit();
        }
        else if (node != null) {
            return node.getTotalBytes();
        }
        return 0;
    }

    /**
     * What is the byte order of this data?
     * @return {@link java.nio.ByteOrder#BIG_ENDIAN}, {@link java.nio.ByteOrder#LITTLE_ENDIAN}, or null
     */
    public ByteOrder getByteOrder() {
        if (buffer != null) {
            return buffer.order();
        }
        else if (node != null) {
            return node.getBuffer().order();
        }
        return null;
    }


    /** Clones this object setting the attachment to null. */
    public Object clone() {
        // Creates a bit wise copy (including only
        // references for payloadBuffer).
        PayloadBuffer item = (PayloadBuffer) super.clone();
        item.attachment = null;

        if (buffer != null) {
            item.event = null;
            // If we're cloning this, the buffer does not belong to a supply anymore
            item.byteBufferItem   = null;
            item.byteBufferSupply = null;

            // To make this thread safe, we must duplicate the existing buffer
            // so it's position and limit are never changed.
            // Also, be sure to take care of byte order.
            ByteOrder order = buffer.order();
            ByteBuffer buf  = buffer.duplicate().order(order);
            int pos = buf.position();
            int lim = buf.limit();

            // Allocate memory
            item.buffer = ByteBuffer.allocate(buf.capacity()).order(order);

            // Copy all data
            buf.limit(buf.capacity()).position(0);
            // Setting position to zero keeps temporarily
            // setting the limit to below pos from happening
            item.buffer.put(buf).position(0);

            // Restore original values
            item.buffer.limit(lim).position(pos);
        }
        else {
            item.buffer = null;
        }

        return item;
    }

}
