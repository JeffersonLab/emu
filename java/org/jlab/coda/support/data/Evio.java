package org.jlab.coda.support.data;

import org.jlab.coda.jevio.*;
import org.jlab.coda.jevio.DataType;
import org.jlab.coda.jevio.records.Coda3Utilities;

/**
 * This class is used to represent a block of raw data produced by a ROC.
 * The EMU will received this wrapped inside of a Transport record (@link Transport}.<p>
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
 * @date Sep 18, 2009
 * @time 3:17:22 PM
 */
public class Evio {

    private static final int ROCID_BIT_MASK  = 0x0fff;
    private static final int STATUS_BIT_MASK = 0xf000;

    private static final int SYNC_BIT_MASK               = 0x1000;
    private static final int ERROR_BIT_MASK              = 0x2000;
    private static final int RESERVED_BIT_MASK           = 0x4000;
    private static final int SINGLE_EVENT_MODE_BIT_MASK  = 0x8000;


    /**
     * Private constructor since all methods are static.
     */
    private Evio() { }


    /**
     * Create a 16-bit tag out of a 4-bit status and 12-bit ROC id.
     *
     * @param status lowest 4 bits are status of ROC
     * @param rocId lowest 12 bits are ROC id
     * @return a 16-bit tag out of a 4-bit status and 12-bit ROC id
     */
    public static int createRocRawTag(int status, int rocId) {
        return ( ((status << 12) & STATUS_BIT_MASK) | (rocId & ROCID_BIT_MASK) );
    }

    /**
     * Get the ROC id which is the lower 12 bits of the tag.
     *
     * @return the ROC id.
     */
    public static int getRocRawId(int rocRawTag) {
        return ROCID_BIT_MASK & rocRawTag;
    }

    /**
     * Method to see if the given tag from a RocRaw data record indicates the Roc is in single event mode.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @return <code>true</code> if the Roc is in single event mode.
     */
    public static boolean isRocRawSingleEventMode(int rocRawTag) {
        return (rocRawTag & SINGLE_EVENT_MODE_BIT_MASK) != 0;
    }

    /**
     * Method to see if the given tag from a RocRaw data record indicates the Roc is a sync event.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @return <code>true</code> if the Roc is a sync event.
     */
    public static boolean isRocRawSyncEvent(int rocRawTag) {
        return (rocRawTag & SYNC_BIT_MASK) != 0;
    }

    /**
     * Method to see if the given tag from a RocRaw data record indicates the Roc has an error.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @return <code>true</code> if the Roc has an error.
     */
    public static boolean hasRocRawError(int rocRawTag) {
        return (rocRawTag & ERROR_BIT_MASK) != 0;
    }


    /**
     * Create an Evio ROC Raw Record bank to be placed in a Data Transport Record.
     *
     * @param rocID       ROC id number
     * @param eventID     starting event id number
     * @param dataBankTag starting data bank tag
     * @param dataBankNum starting data bank num
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param timestamp   starting event's timestamp
     * @param recordId    record count - sequential number
     *
     * @return created ROC Raw Record (evio event)
     * @throws org.jlab.coda.jevio.EvioException
     */
    public static EvioEvent createRocRawRecord(int rocID,       int eventID,
                                               int dataBankTag, int dataBankNum,
                                               int eventNumber, int numEvents,
                                               int timestamp,   int recordId) throws EvioException {

        int rocRawId = 0x0C02; // tag specifying ROC Raw Record

        // create a ROC Raw Data Record event/bank with numEvents physics events in it
        int rocTag = Coda3Utilities.createRocRawTag(0, rocID);
        EventBuilder eventBuilder = new EventBuilder(rocTag, org.jlab.coda.jevio.DataType.BANK, recordId);
        EvioEvent rocRawEvent = eventBuilder.getEvent();

        // create the trigger bank (of segments)
        EvioBank triggerBank = new EvioBank(rocRawId, DataType.SEGMENT, numEvents);
        eventBuilder.addChild(rocRawEvent, triggerBank);

        // generate one segment per event
        int[] segData = new int[2];
        EvioSegment ev;
        for (int i = 0; i < numEvents; i++) {
            // each segment contains eventNumber & timestamp of corresponding event in data bank
            ev = new EvioSegment(eventID, DataType.UINT32);
            eventBuilder.addChild(triggerBank, ev);
            segData[0] = eventNumber++;
            segData[1] = timestamp++;
            eventBuilder.appendIntData(ev, segData);
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

        return rocRawEvent;
    }


    /**
     * Create an Evio Data Transport Record event to send to the event builder.
     *
     * @param rocID       ROC id number
     * @param eventID     starting event id number
     * @param dataBankTag starting data bank tag
     * @param dataBankNum starting data bank num
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param timestamp   starting event's timestamp
     * @param recordId    record count - sequential number
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
        int dataTransportID = 0x0C01;

        // create event with jevio package
        EventBuilder eventBuilder = new EventBuilder(rocID, DataType.BANK, recordId);
        EvioEvent ev = eventBuilder.getEvent();

        // add a bank with record ID in it
        EvioBank recordIdBank = new EvioBank(dataTransportID, DataType.INT32, numPayloadBanks);
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

        return ev;
    }



}
