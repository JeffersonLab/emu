package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.*;
import org.jlab.coda.jevio.DataType;

/**
 * This class is used as a layer on top of evio to handle CODA3 specific details.
 * The EMU will received evio data as Data Transport Records which contain
 * ROC Raw Records both of which are in formats given below.<p>
 *
 * <code><pre>
 *
 * ROC Raw Record:
 * 
 * MSB(31)                          LSB(0)
 * <---  32 bits ------------------------>
 * _______________________________________
 * |           Event Length              |
 * |_____________________________________|
 * | S |   ROC ID     |  0x10  |    M    |
 * |_____________________________________| ------
 * |        Trigger Bank Length          |      ^
 * |_____________________________________|      |
 * |    0x0c02        |  0x20  |    M    |      |
 * |_____________________________________|      |
 * | ID 1   |  0x01   |     ID len 1     |   Trigger Bank
 * |_____________________________________|      |
 * |           Event Number 1            |      |
 * |_____________________________________|      |
 * |         Timestamp 1 (48 bits)       |      |
 * |_____________________________________|      |
 * |                  ...                |      |
 * |_____________________________________|      |
 * |                                     |      |
 * |       (Repeat M-1 times)            |      |
 * |                                     |      |
 * |_____________________________________|      |
 * | ID M   |  0x01   |     ID len M     |      |
 * |_____________________________________|      |
 * |           Event Number M            |      |
 * |_____________________________________|      |
 * |         Timestamp M (48 bits)       |      |
 * |_____________________________________|      |
 * |                  ...                |      V
 * |_____________________________________| ------
 * |                                     |
 * |            Data Block               |
 * |     (opaque data of M events        |
 * |      from 1 to multiple modules)    |
 * |                                     |
 * |_____________________________________|
 * |                                     |
 * |            Data Block               |
 * |   (there will be only 1 block       |
 * |    unless user splits data into     |
 * |    pieces from multiple detectors)  |
 * |                                     |
 * |_____________________________________|
 *
 *
 *      M is the number of events.
 * 0x0c02 is the Trigger Bank identifier.
 *
 * S is the 4-bit status:
 * |_____________________________________|
 * | Single| Reserved |  Error |  Sync   |
 * | Event |          |        |  Event  |
 * |  Mode |          |        |         |
 * |_____________________________________|
 *
 *
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
 * |      ROCRaw or Physics types)       |
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
 * 0x0c01 is the Transport Record identifier.
 * </pre></code>
 *
 *
 * @author timmer
 * @date Oct 16, 2009
 */
public class Evio {

    /** In ROC Raw Record, mask to get rocID from tag. */
    private static final int ROCID_BIT_MASK  = 0x0fff;
    /** In ROC Raw Record, mask to get 4 status bits from tag. */
    private static final int STATUS_BIT_MASK = 0xf000;

    /** In ROC Raw Record, mask to get sync-event status bit from tag. */
    private static final int SYNC_BIT_MASK               = 0x1000;
    /** In ROC Raw Record, mask to get has-error status bit from tag. */
    private static final int ERROR_BIT_MASK              = 0x2000;
    /** In ROC Raw Record, mask to get reserved status bit from tag. */
    private static final int RESERVED_BIT_MASK           = 0x4000;
    /** In ROC Raw Record, mask to get single-event status bit from tag. */
    private static final int SINGLE_EVENT_MODE_BIT_MASK  = 0x8000;

    /** ID number designating a ROC Raw Record. */
    private static final int ROC_RAW_RECORD_ID         = 0x0C02;
    /** ID number designating a Data Transport Record. */
    private static final int DATA_TRANSPORT_RECORD_ID  = 0x0C01;


    /**
     * Private constructor since everything is static.
     */
    private Evio() { }


    /**
     * Create a 16-bit tag for a ROC Raw record out of a 4-bit status and 12-bit ROC id.
     *
     * @param status lowest 4 bits are status of ROC
     * @param rocId  lowest 12 bits are ROC id
     * @return a 16-bit tag for a ROC Raw record out of a 4-bit status and 12-bit ROC id
     */
    public static int createRocRawTag(int status, int rocId) {
        return ( ((status << 12) & STATUS_BIT_MASK) | (rocId & ROCID_BIT_MASK) );
    }

    /**
     * Get the ROC id which is the lower 12 bits of the ROC Raw record tag.
     *
     * @return the ROC id of the ROC Raw record tag.
     */
    public static int getRocRawId(int rocRawTag) {
        return ROCID_BIT_MASK & rocRawTag;
    }

    /**
     * See if the given tag from a ROC Raw record indicates the ROC is in single event mode.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @return <code>true</code> if the ROC is in single event mode.
     */
    public static boolean isRocRawSingleEventMode(int rocRawTag) {
        return (rocRawTag & SINGLE_EVENT_MODE_BIT_MASK) != 0;
    }

    /**
     * See if the given tag from a ROC Raw record indicates it is a sync event.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @return <code>true</code> if the ROC Raw record is a sync event.
     */
    public static boolean isRocRawSyncEvent(int rocRawTag) {
        return (rocRawTag & SYNC_BIT_MASK) != 0;
    }

    /**
     * See if the given tag from a ROC Raw record indicates the ROC has an error.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @return <code>true</code> if the ROC has an error.
     */
    public static boolean hasRocRawError(int rocRawTag) {
        return (rocRawTag & ERROR_BIT_MASK) != 0;
    }


    /**
     * Create an Evio ROC Raw record event/bank to be placed in a Data Transport record.
     *
     * @param rocID       ROC id number
     * @param eventID     starting event id number
     * @param dataBankTag starting data bank tag
     * @param dataBankNum starting data bank num
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param timestamp   starting event's timestamp
     * @param recordId    record count
     *
     * @return created ROC Raw Rrcord (EvioEvent object)
     * @throws EvioException
     */
    public static EvioEvent createRocRawRecord(int rocID,       int eventID,
                                               int dataBankTag, int dataBankNum,
                                               int eventNumber, int numEvents,
                                               int timestamp,   int recordId) throws EvioException {

        // create a ROC Raw Data Record event/bank with numEvents physics events in it
        int status = 0;  // TODO: may want to make status a method arg
        int rocTag = createRocRawTag(status, rocID);
        EventBuilder eventBuilder = new EventBuilder(rocTag, org.jlab.coda.jevio.DataType.BANK, recordId);
        EvioEvent rocRawEvent = eventBuilder.getEvent();

        // create the trigger bank (of segments)
        EvioBank triggerBank = new EvioBank(ROC_RAW_RECORD_ID, DataType.SEGMENT, numEvents);
        eventBuilder.addChild(rocRawEvent, triggerBank);

        // generate one segment per event
        int[] segmentData = new int[2];
        EvioSegment segment;
        for (int i = 0; i < numEvents; i++) {
            // each segment contains eventNumber & timestamp of corresponding event in data bank
            segment = new EvioSegment(eventID, DataType.UINT32);
            eventBuilder.addChild(triggerBank, segment);
            segmentData[0] = eventNumber++;
            segmentData[1] = timestamp++;
            eventBuilder.appendIntData(segment, segmentData);
        }

        // put some data into event -- one int per event
        int[] data = new int[numEvents];
        for (int i = 0; i < numEvents; i++) {
            data[i] = 1000 + i;
        }

        // create a single data bank (of ints) -- NOT SURE WHAT A DATA BANK LOOKS LIKE !!!
        EvioBank dataBank = new EvioBank(dataBankTag, DataType.INT32, dataBankNum);
        eventBuilder.addChild(rocRawEvent, dataBank);
        eventBuilder.appendIntData(dataBank, data);
        
        eventBuilder.setAllHeaderLengths();

        return rocRawEvent;
    }


    /**
     * Create an Evio Data Transport Record event to send to the event building EMU.
     *
     * @param rocID       ROC id number
     * @param eventID     starting event id number
     * @param dataBankTag starting data bank tag
     * @param dataBankNum starting data bank num
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param timestamp   starting event's timestamp
     * @param recordId    record count
     * @param numPayloadBanks number of payload banks in this record
     *
     * @return
     * @throws EvioException
     */
    public static EvioEvent createDataTransportRecord(int rocID,       int eventID,
                                                      int dataBankTag, int dataBankNum,
                                                      int eventNumber, int numEvents,
                                                      int timestamp,   int recordId,
                                                      int numPayloadBanks) throws EvioException {
        // create event with jevio package
        EventBuilder eventBuilder = new EventBuilder(rocID, DataType.BANK, recordId);
        EvioEvent ev = eventBuilder.getEvent();

        // add a bank with record ID in it
        EvioBank recordIdBank = new EvioBank(DATA_TRANSPORT_RECORD_ID, DataType.INT32, numPayloadBanks);
        eventBuilder.appendIntData(recordIdBank, new int[]{recordId++});
        eventBuilder.addChild(ev, recordIdBank);

        // add ROC Raw Records as payload banks
        EvioEvent rocRawRecord;
        for (int i=0; i < numPayloadBanks; i++) {
            rocRawRecord = createRocRawRecord(rocID, eventID, dataBankTag, dataBankNum,
                                              eventNumber, numEvents, timestamp, recordId);
            eventBuilder.addChild(ev, rocRawRecord);

            eventNumber += numEvents;
            timestamp   += numEvents;
            recordId++;
        }
        eventBuilder.setAllHeaderLengths();

        return ev;
    }



}
