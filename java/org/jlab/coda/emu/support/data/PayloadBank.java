package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.*;

import java.nio.ByteOrder;


/**
 * Convenience class designed to keep extra data associated with an evio event.
 *
 * @author timmer
 * Date: Jan 29, 2010
 */
public class PayloadBank extends RingItemAdapter {

    /**
     * Copy constructor which stores reference to event and doesn't copy or clone.
     * @param event even to store
     */
    public PayloadBank(EvioEvent event) {
        this.event = event;
    }

    /**
     * Copy constructor which copies references and doesn't clone.
     * @param bank payload bank to copy
     */
    public PayloadBank(PayloadBank bank) {
        super(bank);
    }

    /**
     * This is a general constructor to use for an EvioEvent.
     *
     * @param tag the tag for the event header (which is just a bank header).
     * @param dataType the (enum) data type for the content of the bank.
     * @param num sometimes, but not necessarily, an ordinal enumeration.
     */
    public PayloadBank(int tag, DataType dataType, int num) {
        event = new EvioEvent(tag, dataType, num);
    }

    /**
     * Constructor that sets several parameters
     * and copies references from bank (doesn't clone).
     *
     * @param event       event to store
     * @param eventType   type of CODA events contained.
     * @param controlType if Control eventType, the type of control.
     * @param recordId    if Physics or RocRaw, the record id of CODA events.
     * @param sourceId    If RocRaw, the CODA id of the source.
     * @param sourceName  The name of the source of these CODA events.
     */
    public PayloadBank(EvioEvent event, EventType eventType, ControlType controlType,
                       int recordId, int sourceId, String sourceName) {

        this.event       = event;
        this.eventType   = eventType;
        this.controlType = controlType;
        this.recordId    = recordId;
        this.sourceId    = sourceId;
        this.sourceName  = sourceName;
    }

    public EvioEvent getEvent() {
        return event;
    }

    public void setEvent(EvioEvent event) {
        this.event = event;
    }

    public QueueItemType getQueueItemType() {return QueueItemType.PayloadBank;}

    /**
     * What is the byte order of this data?
     * @return {@link ByteOrder#BIG_ENDIAN} or {@link ByteOrder#LITTLE_ENDIAN}
     */
    public ByteOrder getByteOrder() {return event.getByteOrder();}

    /**
     * Get the length of this structure in bytes, including the header.
     * @return the length of this structure in bytes, including the header.
     */
    public int getTotalBytes() {
        return 4*(event.getHeader().getLength() + 1);
    }

    /**
   	 * This is a method from the IEvioStructure Interface. Return the header for this structure.
   	 *
   	 * @return the header for this structure.
   	 */
       public BaseStructureHeader getHeader() {
   		return event.getHeader();
   	}

    /**
     * Shallow clone is fine.
     */
    public Object clone() {
        try {
            PayloadBank result = (PayloadBank) super.clone();
            return result;
        }
        catch (Exception e) {
            return null;
        }
    }


}
