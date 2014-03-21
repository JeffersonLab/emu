package org.jlab.coda.emu.support.data;


import java.nio.ByteOrder;

/**
 * This class provides the boilerplate methods of the QueueItem interface.
 * This class is meant to be extended to handle a specific type of data
 * object to be placed in queues for consumption by emu modules and
 * transport channels.
 *
 * @author: timmer
 * Date: Feb 28, 2014
 */
class QueueItemAdapter implements QueueItem {

    /** What type of CODA events are contained in this bank (RocRaw, Physics, Control, ...)?
     *  Only one type is stored in one PayloadBank object.
     *  Only one control event is stored in one PayloadBank object. */
    protected EventType eventType;

    /** If this is a control event, what type of control is it (SYNC, GO, END, ...)? */
    protected ControlType controlType;

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

    /** Is sync bank? from ROC raw data record 4-bit status. */
    protected boolean isSync;

    /** Is single event mode? from ROC raw data record 4-bit status. */
    protected boolean isSingleEventMode;

    /** Has error? from ROC raw data record 4-bit status. */
    protected boolean hasError;

    /** Reserved. from ROC raw data record 4-bit status. */
    protected boolean reserved;

    /** Was there an non-fatal error generated while trying to build an event? */
    protected boolean nonFatalBuildingError;

    /** Associated object used to store things. */
    protected Object attachment;



    /** Constructor. */
    public QueueItemAdapter() {}


    /**
     * Constructor that sets several parameters
     * and copies references from queue item (doesn't clone).
     *
     * @param qItem       queueItem to copy
     * @param eventType   type of CODA events contained.
     * @param controlType if Control eventType, the type of control.
     * @param recordId    if Physics or RocRaw, the record id of CODA events.
     * @param sourceId    If RocRaw, the CODA id of the source.
     * @param sourceName  The name of the source of these CODA events.
     */
    public QueueItemAdapter(QueueItem qItem, EventType eventType, ControlType controlType,
                            int recordId, int sourceId, String sourceName) {
        this(qItem);
        this.eventType   = eventType;
        this.controlType = controlType;
        this.recordId    = recordId;
        this.sourceId    = sourceId;
        this.sourceName  = sourceName;
    }


    /**
     * Copy constructor which copies references and doesn't clone.
     * @param qItem QueueItem to copy
     */
    public QueueItemAdapter(QueueItem qItem) {
        eventType             = qItem.getEventType();
        controlType           = qItem.getControlType();
        sourceId              = qItem.getSourceId();
        matchesId             = qItem.matchesId();
        sourceName            = qItem.getSourceName();
        recordId              = qItem.getRecordId();
        eventCount            = qItem.getEventCount();
        firstEventNumber      = qItem.getFirstEventNumber();
        isSync                = qItem.isSync();
        isSingleEventMode     = qItem.isSingleEventMode();
        hasError              = qItem.hasError();
        nonFatalBuildingError = qItem.hasNonFatalBuildingError();
        attachment            = qItem.getAttachment();
    }

    // Will need to be overwritten
    /** {@inheritDoc} */
    public QueueItemType getQueueItemType() {return null;}

    /** {@inheritDoc} */
    public ByteOrder getByteOrder() {return ByteOrder.BIG_ENDIAN;}
    //----------------------------

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
    public int getEventCount() {return eventCount;}
    /** {@inheritDoc} */
    public void setEventCount(int eventCount) {this.eventCount = eventCount;}

    /** {@inheritDoc} */
    public long getFirstEventNumber() {return firstEventNumber;}
    /** {@inheritDoc} */
    public void setFirstEventNumber(long firstEventNumber) {this.firstEventNumber = firstEventNumber;}

    /** {@inheritDoc} */
    public boolean isSync() {return isSync;}
    /** {@inheritDoc} */
    public void setSync(boolean sync) {isSync = sync;}

    /** {@inheritDoc} */
    public boolean isSingleEventMode() {return isSingleEventMode;}
    /** {@inheritDoc} */
    public void setSingleEventMode(boolean singleEventMode) {isSingleEventMode = singleEventMode;}

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

    /**
     * All members of this class (except attachment) are
     * primitives/enums so bitwise copies are fine.
     */
    public Object clone() {
        try {
            QueueItemAdapter result = (QueueItemAdapter) super.clone();
            return result;
        }
        catch (Exception e) {
            return null;
        }
    }

}
