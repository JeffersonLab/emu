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
import org.jlab.coda.jevio.EvioNodePool;
import org.jlab.coda.jevio.EvioNodeSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class provides the boilerplate methods of the RingItem interface.
 * This class is meant to be extended to handle a specific type of data
 * object to be placed in ring buffers for consumption by emu
 * modules and transport channels.
 * {@see PayloadBuffer}.
 *
 * @author: timmer
 * Date: Feb 28, 2014
 */
abstract class RingItemAdapter implements RingItem {

    // If contains event

    /** Event to wrap. If defined, buffer & node are null. */
    protected EvioEvent event;

    // If contains buffer

    /** ByteBuffer to wrap. If defined, event & node are null. */
    protected ByteBuffer buffer;

    /** Object containing info about specific evio event in a buffer. */
    protected EvioNode node;

   //---------------------

    /** What type of CODA events are contained in this bank (RocRaw, Physics, Control, User, ...)?
     *  Only one type is stored in one RingItem object.
     *  Only one control event is stored in one RingItem object. */
    protected EventType eventType;

    /** If this is a control event, what type of control is it (SYNC, GO, END, ...)? */
    protected ControlType controlType;

    /** Is this a USER event type? */
    protected boolean isUser;

    /** Is this USER event also a "first event" to be written in each file split? */
    protected boolean isFirst;

    /** If the event type is RocRaw, this is the CODA id of the source. */
    protected int sourceId;

    /** Does sourceId match id of input channel? */
    protected boolean matchesId = true;

    /** The name of the source of these CODA events. */
    protected String sourceName;

    /** If the event type is RocRaw or Physics, this is the record id of this CODA events.
     *  The record id is incremented by one for each ET event. Many CODA events (triggers)
     *  may have the same record id. */
    protected int recordId;

    /** The number of CODA events (triggers) contained in this evio bank. */
    protected int eventCount;

    /** The event number of the first CODA event in this evio bank. */
    protected long firstEventNumber;

    /** When reading from multiple rings, is it time to change to next ring? */
    protected boolean switchRing;

    /** Is sync bank? from ROC raw data record 4-bit status. */
    protected boolean isSync;

    /** Has error? from ROC raw data record 4-bit status. */
    protected boolean hasError;

    /** Reserved. from ROC raw data record 4-bit status. */
    protected boolean reserved;

    /** Was there an non-fatal error generated while trying to build an event? */
    protected boolean nonFatalBuildingError;

    /** Associated object used to store things. */
    protected Object attachment;

    // Deal with ring buffers

    /** This refers to a supply of reusable ByteBuffers from which buffer came (if any). */
    protected ByteBufferSupply byteBufferSupply;

    /** This refers to the wrapper object of buffer in reusable ByteBufferSupply (if any). */
    protected ByteBufferItem byteBufferItem;



    /** Constructor. */
    public RingItemAdapter() {}


    /**
     * Constructor.
     * @param nodeArraySize  number of EvioNode objects to create in pool.
     */
    public RingItemAdapter(int nodeArraySize) {}


    /**
     * Copy constructor which copies references and doesn't clone.
     * @param ringItem ring item to copy
     */
    public RingItemAdapter(RingItem ringItem) {
        copy(ringItem);
    }


    /**
     * Copy members of argument into this object (except for "reserved").
     * @param ringItem ring item to copy.
     */
    public void copy(RingItem ringItem) {
        event                  = ringItem.getEvent();
        buffer                 = ringItem.getBuffer();
        node                   = ringItem.getNode();
        eventType              = ringItem.getEventType();
        controlType            = ringItem.getControlType();
        isUser                 = ringItem.isUser();
        isFirst                = ringItem.isFirstEvent();
        sourceId               = ringItem.getSourceId();
        matchesId              = ringItem.matchesId();
        sourceName             = ringItem.getSourceName();
        recordId               = ringItem.getRecordId();
        eventCount             = ringItem.getEventCount();
        firstEventNumber       = ringItem.getFirstEventNumber();
        switchRing             = ringItem.getSwitchRing();
        isSync                 = ringItem.isSync();
        hasError               = ringItem.hasError();
        nonFatalBuildingError  = ringItem.hasNonFatalBuildingError();
        attachment             = ringItem.getAttachment();
        byteBufferItem         = ringItem.getByteBufferItem();
        byteBufferSupply       = ringItem.getByteBufferSupply();
    }


    /** {@inheritDoc} */
    abstract public ByteOrder getByteOrder();

    /** {@inheritDoc} */
    abstract public int getTotalBytes();


    /** {@inheritDoc} */
    public void setAll(EvioEvent ev, ByteBuffer buf, EvioNode nd, EventType eType,
                       ControlType cType, boolean user, boolean first, int chanId,
                       int rId, int sId, int evCount, String sName,
                       ByteBufferItem bbItem, ByteBufferSupply bbSupply) {
        event                  = ev;
        buffer                 = buf;
        node                   = nd;
        eventType              = eType;
        controlType            = cType;
        isUser                 = user;
        isFirst                = first;
        recordId               = rId;
        sourceId               = sId;
        matchesId              = sId == chanId;
        sourceName             = sName;
        firstEventNumber       = 1;
        switchRing             = false;
        isSync                 = false;
        hasError               = false;
        nonFatalBuildingError = false;
        attachment             = null;
        byteBufferItem         = bbItem;
        byteBufferSupply       = bbSupply;
        eventCount             = evCount;
    }



    /** {@inheritDoc} */
    public EvioEvent getEvent() {return event;}

    /** {@inheritDoc} */
    public void setEvent(EvioEvent event) {this.event = event;}

    /** {@inheritDoc} */
    public ByteBuffer getBuffer() {return buffer;}

    /** {@inheritDoc} */
    public void setBuffer(ByteBuffer buffer) { this.buffer = buffer; }

    /** {@inheritDoc} */
    public EvioNode getNode() { return node; }

    /** {@inheritDoc} */
    public void setNode(EvioNode node) { this.node = node; }

    /** {@inheritDoc} */
    public void releaseByteBuffer() {
        if (byteBufferSupply == null) return;
        byteBufferSupply.release(byteBufferItem);
        // Don't want to release it again
        byteBufferSupply = null;
        byteBufferItem   = null;
    }

    /** {@inheritDoc} */
    public void setReusableByteBuffer(ByteBufferSupply byteBufferSupply,
                                      ByteBufferItem byteBufferItem) {
        this.byteBufferItem   = byteBufferItem;
        this.byteBufferSupply = byteBufferSupply;
    }

    /** {@inheritDoc} */
    public ByteBufferSupply getByteBufferSupply() {
        return byteBufferSupply;
    }

    /** {@inheritDoc} */
    public ByteBufferItem getByteBufferItem() {
        return byteBufferItem;
    }


    /** {@inheritDoc} */
    public Object getAttachment() {return attachment;}
    /** {@inheritDoc} */
    public void setAttachment(Object attachment) {this.attachment = attachment;}

    /** {@inheritDoc} */
    public boolean isControlEvent() {return controlType != null;}
    /** {@inheritDoc} */
    public ControlType getControlType() {return controlType;}
    /** {@inheritDoc} */
    public void setControlType(ControlType type) {this.controlType = type;}

    /** {@inheritDoc} */
    public boolean isUser() {return isUser;}
    /** {@inheritDoc} */
    public void isUser(boolean isUser) {this.isUser = isUser;}

    /** {@inheritDoc} */
    public boolean isFirstEvent() {return isFirst;}
    /** {@inheritDoc} */
    public void isFirstEvent(boolean isFirst) {this.isFirst = isFirst;}

    /** {@inheritDoc} */
    public EventType getEventType() {return eventType;}
    /** {@inheritDoc} */
    public void setEventType(EventType type) {this.eventType = type;}

    /** {@inheritDoc} */
    public int getSourceId() {return sourceId;}
    /** {@inheritDoc} */
    public void setSourceId(int sourceId) {this.sourceId = sourceId;}

    /** {@inheritDoc} */
    public boolean matchesId() {return matchesId;}
    /** {@inheritDoc} */
    public void matchesId(boolean matchesId) {this.matchesId = matchesId;}

    /** {@inheritDoc} */
    public int getRecordId() {return recordId;}
    /** {@inheritDoc} */
    public void setRecordId(int recordId) {this.recordId = recordId;}

    /** {@inheritDoc} */
    public String getSourceName() {return sourceName;}
    /** {@inheritDoc} */
    public void setSourceName(String sourceName) {this.sourceName = sourceName;}

    /** {@inheritDoc} */
    public int getEventCount() {return eventCount;}
    /** {@inheritDoc} */
    public void setEventCount(int eventCount) {this.eventCount = eventCount;}

    /** {@inheritDoc} */
    public long getFirstEventNumber() {return firstEventNumber;}
    /** {@inheritDoc} */
    public void setFirstEventNumber(long firstEventNumber) {this.firstEventNumber = firstEventNumber;}

    /** {@inheritDoc} */
    public boolean getSwitchRing() {return switchRing;}
    /** {@inheritDoc} */
    public void setSwitchRing(boolean switchRing) {this.switchRing = switchRing;}

    /** {@inheritDoc} */
    public boolean isSync() {return isSync;}
    /** {@inheritDoc} */
    public void setSync(boolean sync) {isSync = sync;}

    /** {@inheritDoc} */
    public boolean hasError() {return hasError;}
    /** {@inheritDoc} */
    public void setError(boolean hasError) {this.hasError = hasError;}

    /** {@inheritDoc} */
    public boolean hasNonFatalBuildingError() {return nonFatalBuildingError;}
    /** {@inheritDoc} */
    public void setNonFatalBuildingError(boolean nonFatalBuildingError) {
        this.nonFatalBuildingError = nonFatalBuildingError;
    }


    public Object clone() {
        try {
            return super.clone();
        }
        catch (Exception e) {
            return null;
        }
    }

}
