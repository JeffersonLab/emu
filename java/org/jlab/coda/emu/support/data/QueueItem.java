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

import org.jlab.coda.jevio.EvioBank;

import java.nio.ByteBuffer;

/**
 * The class contains one of the multiple possible types of items
 * which may be passed to the EMU through its transport channels.
 * A single item and its type are stored in an object of this class.
 * This gives EMU modules some flexibility in the type of data they
 * can handle.
 *
 * @author timmer
 * (Apr 3 2013)
 */
public class QueueItem implements Cloneable {

    /** If item is an EvioBank, store it here. */
    private EvioBank bank;

    /** If item is a ByteBuffer, store it here. */
    private ByteBuffer  buffer;

    /** If item is a PayloadBank, store it here. */
    private PayloadBank payloadBank;

    /** Type of item contained. */
    private QueueItemType qItemType;

    /** If control event contained, what type is it? */
    private ControlType controlType;

    /**
     * Constructor that stores an EvioBank object.
     * @param bank EvioBank data object.
     */
    public QueueItem(EvioBank bank) {
        this.bank = bank;
        qItemType = QueueItemType.EvioBank;
        if (Evio.isControlEvent(bank)) {
            controlType = Evio.getControlType(bank);
        }
    }

    /**
     * Constructor that stores a ByteBuffer with data in evio file format.
     * @param buffer ByteBuffer with data in evio file format.
     */
    public QueueItem(ByteBuffer buffer) {
        this.buffer = buffer;
        qItemType = QueueItemType.ByteBuffer;
        controlType = Evio.getControlType(buffer);
    }

    /**
     * Constructor that stores a PayloadBank object (extended EvioEvent object).
     * The payloadBank contains metadata along with data obtained from parsing
     * an EvioBank.
     * @param payloadBank PayloadBank data object (extended EvioEvent object).
     */
    public QueueItem(PayloadBank payloadBank) {
        this.payloadBank = payloadBank;
        qItemType = QueueItemType.PayloadBank;
        controlType = payloadBank.getControlType();
    }

    /**
     * Constructor that stores a PayloadBank object (extended EvioEvent object)
     * which represents a control event along with its control type .
     * The payloadBank contains metadata along with data obtained from parsing
     * an EvioBank.
     *
     * @param payloadBank PayloadBank object (extended EvioEvent object).
     * @param controlType type of control event represented by payloadBank arg.
     *                    Either sync, prestart, go, pause, or end.
     */
    public QueueItem(PayloadBank payloadBank, ControlType controlType) {
        this.payloadBank = payloadBank;
        qItemType = QueueItemType.PayloadBank;
        this.controlType = controlType;
    }


    /**
     * Get the type of data item stored in this object -
     * either an EvioBank, PayloadBank, or ByteBuffer.
     * @return type of data item stored in this object.
     */
    public QueueItemType getQueueItemType() {
        return qItemType;
    }

    /**
     * Is the stored data item a control event?
     * @return {@code true} if control event, else {@code false}.
     */
    public boolean isControlEvent() {
        return controlType != null;
    }

    /**
     * If a control event is store, this method returns the type of
     * control event, otherwise it returns null.
     * @return type of control event stored, else null.
     */
    public ControlType getControlType() {
        return controlType;
    }

    /**
     * Get the stored EvioBank object. If none, null is returned.
     * @return stored EvioBank object. If none, null is returned.
     */
    public EvioBank getBank() {
        return bank;
    }

    /**
     * Get the stored ByteBuffer object. If none, null is returned.
     * @return stored ByteBuffer object. If none, null is returned.
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }

    /**
     * Get the stored PayloadBank object. If none, null is returned.
     * @return stored PayloadBank object. If none, null is returned.
     */
    public PayloadBank getPayloadBank() {
        return payloadBank;
    }

    /** Clones this object including any PayloadBank, EvioBank, or ByteBuffer contained. */
    public Object clone() {
        try {
            // Creates a bit wise copy (including only
            // references for bank, buffer, & payloadBank).
            QueueItem item = (QueueItem) super.clone();

            if (payloadBank != null)  {
                item.payloadBank = (PayloadBank)payloadBank.clone();
            }

            if (bank != null)  {
                item.bank = (EvioBank)bank.clone();
            }

            if (buffer != null)  {
                // allocate memory
                item.buffer = ByteBuffer.allocate(buffer.capacity());
                // store current position and limit
                int pos = buffer.position();
                int lim = buffer.limit();
                // copy all data
                buffer.limit(buffer.capacity()).position(0);
                item.buffer.put(buffer);
                // restore original values
                buffer.limit(lim).position(pos);
            }

            return item;
        }
        catch (CloneNotSupportedException e) {
            return null;
        }
    }


}
