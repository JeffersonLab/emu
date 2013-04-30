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
