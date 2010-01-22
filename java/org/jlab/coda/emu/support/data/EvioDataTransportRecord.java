package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.EvioEvent;
import org.jlab.coda.jevio.BankHeader;
import org.jlab.coda.jevio.DataType;

/**
 * This class is a wrapper around EvioEvent for convenience.
 *
 * <code><pre>
 *
 * Data Transport Record:
 *
 * MSB(31)                          LSB(0)
 * <---  32 bits ------------------------>
 * _______________________________________
 * |           Record Length             |
 * |_____________________________________|
 * |    Source ID     |  0x10  |    N    |
 * |_____________________________________|
 * |                 2                   |
 * |_____________________________________|
 * |    0x0c01        |  0x01  |    M    |
 * |_____________________________________|
 * |        Record ID (counter)          |
 * |_____________________________________|
 * |                                     |
 * |           Payload Bank              |
 * |  (from 1 ROC, multiple events of    |
 * |      ROCRaw or Physics types,       |
 * |      but not both)                  |
 * |_____________________________________|
 * |                                     |
 * |           Payload Bank              |
 * |_____________________________________|
 * |                                     |
 * |           Payload Bank              |
 * |_____________________________________|
 * |                                     |
 * |           Payload Bank              |
 * |_____________________________________|
 *
 *
 *      M is the number of payload banks.
 *      N is lower 8 bits of Record ID
 * 0x0c01 is the Transport Record identifier.
 * </pre></code>
 *
 *  @author timmer
 */
public class EvioDataTransportRecord extends EvioEvent {

    private int recordId;
    private int sourceId;


    /**
     * Explicit null constructor for evio event.
     */
    public EvioDataTransportRecord() {
    }

    /**
     * Constructor using a provided BankHeader
     *
     * @param bankHeader the header to use.
     * @see org.jlab.coda.jevio.BankHeader
     */
    public EvioDataTransportRecord(BankHeader bankHeader) {
        super(bankHeader);
    }

    /**
     * This is a general constructor to use for an EvioEvent
     *
     * @param tag the tag for the event header (which is just a bank header).
     * @param dataType the (enum) data type for the content of the bank.
     * @param num sometimes, but not necessarily, an ordinal enumeration.
     */
    public EvioDataTransportRecord(int tag, DataType dataType, int num) {
        this(new BankHeader(tag, dataType, num));
    }

    /**
     * This is a general constructor to use for an EvioEvent.
     * 
     * @param tag the tag for the event header (which is just a bank header).
     * @param dataType the (int) data type for the content of the bank.
     * @param num sometimes, but not necessarily, an ordinal enumeration.
     */
    public EvioDataTransportRecord(int tag, int dataType, int num) {
        this(new BankHeader(tag, dataType, num));
    }

}
