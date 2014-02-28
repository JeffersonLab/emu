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
 * This interface defines type of item which may be passed to the EMU
 * through its transport channels.
 * This interface may be implemented to give EMU modules some flexibility
 * in the type of data they can handle.
 *
 * @author timmer
 * (Feb 27 2014)
 */
public interface QueueItem extends Cloneable, Attached {

    /**
     * Get the type of data item stored in this object -
     * either a PayloadBank, or PayloadBuffer.
     * @return type of data item stored in this object.
     */
    public QueueItemType getQueueItemType();


    public ByteOrder getByteOrder();

    /**
     * Is the stored data item a control event?
     * @return {@code true} if control event, else {@code false}.
     */
    public boolean isControlEvent();

    /**
     * If a control event is store, this method returns the type of
     * control event, otherwise it returns null.
     * @return type of control event stored, else null.
     */
    public ControlType getControlType();
    public void setControlType(ControlType type);

    public EventType getEventType();
    public void setEventType(EventType type);

    public int getSourceId();
    public void setSourceId(int sourceId);

    public int getRecordId();
    public void setRecordId(int recordId);

    public String getSourceName();
    public void setSourceName(String sourceName);

    public int getEventCount();
    public void setEventCount(int eventCount);

    public long getFirstEventNumber();
    public void setFirstEventNumber(long firstEventNumber);

    public boolean isSync();
    public void setSync(boolean sync);

    public boolean isSingleEventMode();
    public void setSingleEventMode(boolean singleEventMode);

    public boolean hasError();
    public void setError(boolean hasError);

    public boolean isReserved();
    public void setReserved(boolean reserved);

    public boolean hasNonFatalBuildingError();
    public void setNonFatalBuildingError(boolean nonFatalBuildingError);
}
