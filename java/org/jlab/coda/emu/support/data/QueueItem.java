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
public class QueueItem implements Cloneable, Attached {

//    /** If item is an EvioBank, store it here. */
//    private EvioBank bank;

    /** If item is a PayloadBuffer, store it here. */
    private PayloadBuffer payloadBuffer;

    /** If item is a PayloadBank, store it here. */
    private PayloadBank payloadBank;

    /** Type of item contained. */
    private QueueItemType qItemType;

    /** If control event contained, what type is it? */
    private ControlType controlType;


//    /**
//     * Constructor that stores an EvioBank object.
//     * @param bank EvioBank data object.
//     */
//    public QueueItem(EvioBank bank) {
//        this.bank = bank;
//        qItemType = QueueItemType.EvioBank;
//        if (Evio.isControlEvent(bank)) {
//            controlType = Evio.getControlType(bank);
//        }
//    }

    /**
     * Constructor that stores a PayloadBuffer with data in evio file format.
     * @param payloadBuffer PayloadBuffer with data in evio file format.
     */
    public QueueItem(PayloadBuffer payloadBuffer) {
        this.payloadBuffer = payloadBuffer;
        qItemType   = QueueItemType.PayloadBuffer;
        //controlType = Evio.getControlType(payloadBuffer.getBuffer());
        controlType = payloadBuffer.getControlType();
    }

    /**
     * Constructor that stores a PayloadBank object (extended EvioEvent object).
     * The payloadBank contains metadata along with data obtained from parsing
     * an EvioBank.
     * @param payloadBank PayloadBank data object (extended EvioEvent object).
     */
    public QueueItem(PayloadBank payloadBank) {
        this.payloadBank = payloadBank;
        qItemType   = QueueItemType.PayloadBank;
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
     * Return any attachment.
     * @return any attachment.
     */
    public Object getAttachment() {
        if (payloadBank != null)  {
            return payloadBank.getAttachment();
        }
        else if (payloadBuffer != null)  {
            return payloadBuffer.getAttachment();
        }

        return null;
    }

    /**
     * Get the type of data item stored in this object -
     * either a PayloadBank, or PayloadBuffer.
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

//    /**
//     * Get the stored EvioBank object. If none, null is returned.
//     * @return stored EvioBank object. If none, null is returned.
//     */
//    public EvioBank getBank() {
//        return bank;
//    }

    /**
     * Get the stored PayloadBuffer object. If none, null is returned.
     * @return stored PayloadBuffer object. If none, null is returned.
     */
    public PayloadBuffer getBuffer() {
        return payloadBuffer;
    }

    /**
     * Get the stored PayloadBank object. If none, null is returned.
     * @return stored PayloadBank object. If none, null is returned.
     */
    public PayloadBank getPayloadBank() {
        return payloadBank;
    }

    /** Clones this object including any PayloadBank or PayloadBuffer contained. */
    public Object clone() {
        try {
            // Creates a bit wise copy (including only
            // references for payloadBuffer & payloadBank).
            QueueItem item = (QueueItem) super.clone();

            if (payloadBank != null)  {
                item.payloadBank = (PayloadBank)payloadBank.clone();
            }

//            if (bank != null)  {
//                item.bank = (EvioBank)bank.clone();
//            }

            if (payloadBuffer != null)  {
                item.payloadBuffer = (PayloadBuffer)payloadBuffer.clone();
            }

            return item;
        }
        catch (CloneNotSupportedException e) {
            return null;
        }
    }


}
