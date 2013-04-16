package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.EvioBank;

import java.nio.ByteBuffer;

/**
 * The class contains one of the multiple possible types of items
 * which may be passed to the EMU through its transport channels.
 * A single item and its type are stored in an object of this class.
 * This gives EMU modules some flexibility in the type of data they
 * receive.
 *
 * @author timmer
 * @Date 4/3/13
 */
public class QueueItem {

    /** If item is EvioBank, store it here. */
    private EvioBank    bank;
    private ByteBuffer  buffer;
    private PayloadBank payloadBank;

    /** Type of item contained. */
    private QueueItemType qItemType;

    /** If control event contained, what type is it? */
    private ControlType controlType;


    public QueueItem(EvioBank bank) {
        this.bank = bank;
        qItemType = QueueItemType.EvioBank;
        if (Evio.isControlEvent(bank)) {
            controlType = Evio.getControlType(bank);
        }
    }

    public QueueItem(ByteBuffer buffer) {
        this.buffer = buffer;
        qItemType = QueueItemType.ByteBuffer;
        controlType = Evio.getControlType(buffer);
    }

    public QueueItem(PayloadBank payloadBank) {
        this.payloadBank = payloadBank;
        qItemType = QueueItemType.PayloadBank;
        controlType = payloadBank.getControlType();
    }

    public QueueItem(PayloadBank payloadBank, ControlType controlType) {
        this.payloadBank = payloadBank;
        qItemType = QueueItemType.PayloadBank;
        this.controlType = controlType;
    }


    public QueueItemType getQueueItemType() {
        return qItemType;
    }

    public boolean isControlEvent() {
        return controlType != null;
    }

    public ControlType getControlType() {
        return controlType;
    }


    public EvioBank getBank() {
        return bank;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public PayloadBank getPayloadBank() {
        return payloadBank;
    }

//    public void setPayloadBank(PayloadBank payloadBank) {
//        this.payloadBank = payloadBank;
//    }



}
