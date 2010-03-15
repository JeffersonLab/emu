package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.*;
import org.jlab.coda.emu.EmuException;

import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.nio.ByteBuffer;

/**
 * This class is used as a layer on top of evio to handle CODA3 specific details.
 * The EMU will received evio data as Data Transport Records which contain
 * ROC Raw Records and Physics Events all of which are in formats given below.<p>
 *
 * <code><pre>
 * ############################
 * Data Transport Record (DTR):
 * ############################
 *
 * MSB(31)                          LSB(0)
 * <---  32 bits ------------------------>
 * _______________________________________
 * |           Record Length             |
 * |_____________________________________|
 * | T |  Source ID   |  0x10  |   RID   |
 * |_____________________________________|
 * |                 2                   |
 * |_____________________________________|
 * |    0x0F00        |  0x01  |   PBs   |
 * |_____________________________________|
 * |        Record ID (counter)          |
 * |_____________________________________|
 * |                                     |
 * |           Payload Bank              |
 * |       (ROCRaw, Physics, or          |
 * |        other type of event)         |
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
 *      RID = the lowest 8 bits of the Record ID.
 *      PBs = number of payload banks
 *   0x0F00 = the Record ID bank identifier.
 *        T = type of event contained in payload bank:
 *              0 = ROC Raw
 *              1 = Physics
 *              2 = User
 *              3 = Sync
 *              4 = Prestart
 *              5 = Go
 *              6 = Pause
 *              7 = End
 *
 * 
 *
 * ############################
 * ROC Raw Record:
 * ############################
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
 * |    0x0F01        |  0x20  |    M    |      |
 * |_____________________________________|      |
 * | ID 1   |  0x01   |     ID len 1     |   Trigger Bank
 * |_____________________________________|      |
 * |           Event Number 1            |      |
 * |_____________________________________|      |
 * |           Timestamp 1 (?)           |      |
 * |_____________________________________|      |
 * |                  ...                |      |
 * |_____________________________________|      |
 * |                                     |      |
 * |    (One segment for each event)     |      |
 * |                                     |      |
 * |_____________________________________|      |
 * | ID M   |  0x01   |     ID len M     |      |
 * |_____________________________________|      |
 * |           Event Number M            |      |
 * |_____________________________________|      |
 * |           Timestamp M (?)           |      |
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
 * |   unless user used multiple DMAs)   |
 * |                                     |
 * |_____________________________________|
 *
 *
 *      M is the number of events.
 * 0x0F01 is the Trigger Bank identifier.
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
 *
 * ############################
 * Physics Event:
 * ############################
 *
 * MSB(31)                          LSB(0)
 * <---  32 bits ------------------------>
 * _______________________________________
 * |           Event Length              |
 * |_____________________________________|
 * | S |  Event Type  |  0x10  |    ?    |
 * |_____________________________________| ------
 * |     Built Trigger Bank Length       |      ^
 * |_____________________________________|      |
 * |    0x0F02        |  0x20  |   N+1   |      |
 * |_____________________________________|     Built
 * |        EB (Common) Segment          |   Trigger Bank
 * |_____________________________________|      |
 * |           ROC 1 Segment             |      |
 * |_____________________________________|      |
 * |                  ...                |      |
 * |_____________________________________|      |
 * |           ROC N Segment             |      V
 * |_____________________________________| ------
 * |                                     |
 * |            Data Bank 1              |
 * |     (wraps 1 or more data           |
 * |         blocks for a ROC)           |
 * |_____________________________________|
 * |                                     |
 * |                  ...                |
 * |_____________________________________|
 * |                                     |
 * |            Data Bank N              |
 * |                                     |
 * |_____________________________________|
 *
 *
 *      N is the number of ROCs.
 * 0x0F02 is the Built Trigger Bank identifier.
 *
 *
 *
 * ####################################
 * Physics Event's Built Trigger Bank:
 * ####################################
 *
 * MSB(31)                          LSB(0)
 * <---  32 bits ------------------------>
 * _______________________________________
 * |        Trigger Bank Length          |
 * |_____________________________________|
 * |    0x0F02        |  0x20  |   N+1   |
 * |_____________________________________| --------
 * | EB id  |  0x05   |       Len        |    ^
 * |_____________________________________|    |
 * |   Event Type 1   | Event Number 1   |  Common Data
 * |_____________________________________|    |
 * |                  ...                |    |
 * |_____________________________________|    |
 * |   Event Type M   | Event Number M   |    V
 * |_____________________________________| -------
 * |roc1 id |  0x05   |        Len       |    ^
 * |_____________________________________|    |
 * |             Timestamp (?)           |    |
 * |_____________________________________|    |
 * |              Misc. (?)              |  ROC Data
 * |_____________________________________|    |
 * |                                     |    |
 * |                  ...                |    |
 * |_____________________________________|    |
 * |rocN id |  0x05   |        Len       |    |
 * |_____________________________________|    |
 * |             Timestamp (?)           |    |
 * |_____________________________________|    |
 * |              Misc. (?)              |    V
 * |_____________________________________| -------
 *
 *
 *      N is the number of ROCs.
 *      M is the number of events.
 * 0x0F02 is the Built Trigger Bank identifier.
 *
 *
 *
 * ############################
 * Physics Event's Data Bank:
 * ############################
 *
 * MSB(31)                          LSB(0)
 * <---  32 bits ------------------------>
 * _______________________________________
 * |         Data Bank Length            |
 * |_____________________________________|
 * | S |   ROC id     |  0x10  |    M    |
 * |_____________________________________|
 * |                                     |
 * |        Data Block Bank 1            |
 * |                                     |
 * |_____________________________________|
 * |                                     |
 * |                  ...                |
 * |_____________________________________|
 * |                                     |
 * |           Data Bank Last            |
 * |                                     |
 * |_____________________________________|
 *
 *
 *      M is the number of events.
 *
 * </pre></code>
 *
 *
 * @author timmer
 * Oct 16, 2009
 */
public class Evio {

    /** In ROC Raw Record, mask to get roc ID from tag.
     *  In Physics Event, mask to get event type from tag.
     *  In DTR, mask to get source ID from tag. */
    private static final int ID_BIT_MASK     = 0x0fff;
    /** In ROC Raw Record and Physics Events, mask to get 4 status bits from tag.
     *  In DTR, mask to get type of event contained in payload bank. */
    private static final int STATUS_BIT_MASK = 0xf000;

    /** In ROC Raw Record, mask to get sync-event status bit from tag. */
    private static final int SYNC_BIT_MASK               = 0x1000;
    /** In ROC Raw Record, mask to get has-error status bit from tag. */
    private static final int ERROR_BIT_MASK              = 0x2000;
    /** In ROC Raw Record, mask to get reserved status bit from tag. */
    private static final int RESERVED_BIT_MASK           = 0x4000;
    /** In ROC Raw Record, mask to get single-event status bit from tag. */
    private static final int SINGLE_EVENT_MODE_BIT_MASK  = 0x8000;

    /** ID number designating a Record ID bank. */
    public static final int RECORD_ID_BANK     = 0x0F00;
    /** ID number designating a Trigger bank. */
    public static final int TRIGGER_BANK       = 0x0F01;
    /** ID number designating a Built Trigger bank. */
    public static final int BUILT_TRIGGER_BANK = 0x0F02;


    /** Object for parsing evio data. */
    private static ByteParser parser = new ByteParser();  // TODO: not good idea to have static parser


    /**
     * Private constructor since everything is static.
     */
    private Evio() { }

    /**
     * Create a 16-bit, CODA-format tag for a ROC Raw Record or Physics Event
     * out of 4 status bits and a 12-bit id.
     *
     * @param sync is sync event
     * @param error is error
     * @param reserved reserved for future use
     * @param singleEventMode is single event mode
     * @param id lowest 12 bits are id
     * @return a 16-bit tag for a ROC Raw Record or Physics Event out of 4 status bits and a 12-bit id
     */
    public static int createCodaTag(boolean sync,     boolean error,
                                    boolean reserved, boolean singleEventMode, int id) {
        int status = 0;
        
        if (sync)            status |= SYNC_BIT_MASK;
        if (error)           status |= ERROR_BIT_MASK;
        if (reserved)        status |= RESERVED_BIT_MASK;
        if (singleEventMode) status |= SINGLE_EVENT_MODE_BIT_MASK;

        return ( status | (id & ID_BIT_MASK) );
    }

    /**
     * Create a 16-bit, CODA-format tag for a ROC Raw Record, Physics Event,
     * or Data Transport Record out of 4-bit status/type and a 12-bit id.
     *
     * @param status lowest 4 bits are status/type
     * @param id     lowest 12 bits are id
     * @return a 16-bit tag for a ROC Raw Record, Physics Event, or DTR out of 4-bit status/type and a 12-bit id
     */
    public static int createCodaTag(int status, int id) {
        return ( (status << 12) | (id & ID_BIT_MASK) );
    }

    /**
     * Get the id which is the lower 12 bits of the CODA-format tag.
     *
     * @param codaTag tag from evio bank.
     * @return the id of the CODA-format tag.
     */
    public static int getCodaId(int codaTag) {
        return ID_BIT_MASK & codaTag;
    }

    /**
     * See if the given CODA-format tag indicates the ROC is in single event mode.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @param codaTag tag from evio bank.
     * @return <code>true</code> if the ROC is in single event mode.
     */
    public static boolean isSingleEventMode(int codaTag) {
        return (codaTag & SINGLE_EVENT_MODE_BIT_MASK) != 0;
    }

    /**
     * See if the given CODA-format tag indicates it is a sync event.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @param codaTag tag from evio bank.
     * @return <code>true</code> if is a sync event.
     */
    public static boolean isSyncEvent(int codaTag) {
        return (codaTag & SYNC_BIT_MASK) != 0;
    }

    /**
     * See if the given CODA-format tag indicates the ROC/EB has an error.
     * This condition is set by the ROC/EB and it is only read here - never set.
     *
     * @param codaTag tag from evio bank.
     * @return <code>true</code> if the ROC/EB has an error.
     */
    public static boolean hasError(int codaTag) {
        return (codaTag & ERROR_BIT_MASK) != 0;
    }


    /**
     * Get the event type's numerical value in Data Transport Records or the status in
     * other types of records/events which is the upper 4 bits of the CODA-format tag.
     *
     * @param codaTag tag from evio bank.
     * @return the event type or status from various records/events.
     */
    public static int getEventTypeOrStatus(int codaTag) {
        return (codaTag >>> 12);
    }

    
    /**
     * Get the event type of a bank for Data Transport Records.
     * 
     * @param bank bank to analyze
     * @return event type for bank, null if none found
     */
    public static EventType getEventType(EvioBank bank) {
        if (bank == null) return null;
        
        int type = getEventTypeOrStatus(bank.getHeader().getTag());
        return EventType.getEventType(type);
    }


    /**
     * Determine whether a Data Transport Record format bank contains control event(s) or not.
     *
     * @param bank input Data Transport Record format bank
     * @return <code>true</code> if arg contains control event(s), else <code>false</code>
     */
    public static boolean isControlEvent(EvioBank bank) {
        if (bank == null)  return false;

        int num = bank.getHeader().getNumber();
        EventType eventType = getEventType(bank);
        if (eventType == null) return false;
   
        return (num == 0xCC && eventType.isControl());
    }


    /**
     * Determine whether a Data Transport Record format bank contains physics event(s) or not.
     *
     * @param bank input Data Transport Record format bank
     * @return <code>true</code> if arg contains physics event(s), else <code>false</code>
     */
    public static boolean isPhysicsEvent(EvioBank bank) {
        if (bank == null)  return false;

        EventType eventType = getEventType(bank);
        if (eventType == null) return false;

        return (eventType.isPhysics());
    }


    /**
     * Determine whether a Data Transport Record format bank contains ROC raw record(s) or not.
     *
     * @param bank input Data Transport Record format bank
     * @return <code>true</code> if arg contains ROC raw record(s), else <code>false</code>
     */
    public static boolean isRocRawRecord(EvioBank bank) {
        if (bank == null)  return false;

        EventType eventType = getEventType(bank);
        if (eventType == null) return false;

        return (eventType.isROCRaw());
    }


    /**
     * Determine whether a Data Transport Record format bank contains banks which should be
     * on the payload bank queue (ie is a physics event, control event, or ROC raw record).
     *
     * @param bank input bank
     * @return <code>true</code> if arg is physics, control or ROC raw bank, else <code>false</code>
     */
    private static boolean isForPayloadQueue(EvioBank bank) {
        if (bank == null)  return false;

        int num = bank.getHeader().getNumber();
        EventType eventType = getEventType(bank);
        if (eventType == null) return false;
//System.out.println("isForPayloadQueue: event type = " + eventType);

        return ( eventType.isROCRaw() || (num == 0xCC && eventType.isControl()) || eventType.isPhysics() );
    }


    /**
     * Determine whether a bank is a record ID bank or not.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is record ID bank, else <code>false</code>
     */
    public static boolean isRecordIdBank(EvioBank bank) {
        if (bank == null)  return false;
        return (bank.getHeader().getTag() == RECORD_ID_BANK);
    }


    /**
     * Determine whether a bank is a trigger bank or not.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is trigger bank, else <code>false</code>
     */
    public static boolean isTriggerBank(EvioBank bank) {
        if (bank == null)  return false;
        return (bank.getHeader().getTag() == TRIGGER_BANK);
    }


    /**
     * Determine whether a bank is a built trigger bank or not.
     * In other words, has it been built by an event builder.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is a built trigger bank, else <code>false</code>
     */
    public static boolean isBuiltTriggerBank(EvioBank bank) {
        if (bank == null)  return false;
        return (bank.getHeader().getTag() == BUILT_TRIGGER_BANK);
    }


    /**
     * Determine whether a bank is a Data Transport Record (DTR) format event or not.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is Data Transport Record format event, else <code>false</code>
     */
    public static boolean isDataTransportRecord(EvioBank bank) {
        if (bank == null)  return false;

        // must be bank of banks
        if (bank.getStructureType() != StructureType.BANK) {
            return false;
        }

        // First  bank - contains record ID, has event type.
        // Second bank - must be at least one data bank.
        Vector<BaseStructure> kids = bank.getChildren();
        if (kids.size() < 2) return false;

        // first bank must contain one 32 bit int - the record ID
        BaseStructure firstBank = kids.firstElement();

        // check tag
        int tag = firstBank.getHeader().getTag();
        if (tag != RECORD_ID_BANK) return false;

        // contained data must be (U)INT32
        int[] intData = firstBank.getIntData();
        if (intData == null) {
            return false;
        }

        // lower 8 bits of recordId must equal num of top bank
        int recordId = intData[0];
        int num = bank.getHeader().getNumber();
        if ( (recordId & 0xff) != num ) {
            // contradictory internal data
            return false;
        }

        // number of payload banks must equal num of recordId bank
        num = firstBank.getHeader().getNumber();
        if (num != kids.size() - 1) {
            // contradictory internal data
            return false;
        }

        return true;
    }


    /**
     * Is the source ID in the bank (a Data Transport Record) identical to the given id?
     *
     * @param bank Data Transport Record object
     * @param id id of an input channel
     * @return <code>true</code> if bank arg's source ID is the same as the id arg, else <code>false</code>
     */
    public static boolean idsMatch(EvioBank bank, int id) {
        return (bank != null && getCodaId(bank.getHeader().getTag()) == id);
    }


    /**
     * Extract certain payload banks (physics or ROC raw format evio banks) from a
     * Data Transport Record (DTR) format event and place onto the specified queue.
     * The DTR is a bank of banks with the first bank containing the record ID.
     * The rest are payload banks which are physics events, ROC raw events,
     * run control events, or user events.<p>
     * Events which do <b>not</b> get built and which may not come simultaneously
     * from all ROCs (ie user events), are placed on the given output queue directly.<p>
     *
     * No checks done on arguments or dtrBank format as {@link #isDataTransportRecord} is assumed to
     * have already been called. However, format of payload banks is checked here for
     * the first time.<p>
     *
     * @param dtrBank input bank assumed to be in Data Transport Record format
     * @param payloadQueue queue on which to place extracted payload banks
     * @param outputQueue queue on which to place banks that do not get built/analyzed (user & unknown events)
     * @throws EmuException if dtrBank contains no data banks or record ID is out of sequence
     * @throws InterruptedException if blocked whileh putting bank on full output queue
     */
    public static void extractPayloadBanks(EvioBank dtrBank, PayloadBankQueue<PayloadBank> payloadQueue,
                                           BlockingQueue<EvioBank> outputQueue)
            throws EmuException, InterruptedException {

        // get sub banks
        Vector<BaseStructure> kids = dtrBank.getChildren();

        // check to make sure record ID is sequential
        BaseStructure firstBank = kids.firstElement();
        int recordId = firstBank.getIntData()[0];
        boolean nonFatalError;
        boolean nonFatalRecordIdError = false;

        // See what type of event DTR bank is wrapping.
        // Only interested in physics, roc raw, and control events.
        EventType eventType = getEventType(dtrBank);
        if ( !(eventType.isROCRaw() || eventType.isControl() || eventType.isPhysics()) ) {
            System.out.println("extractPayloadBanks: DTR is NOT for payload Q !!!, put on output Q !!!");
            outputQueue.put(dtrBank);
            return;
        }

        // initial recordId stored is 0, ignore that
        if (payloadQueue.getRecordId() > 0 && recordId != payloadQueue.getRecordId() + 1) {
System.out.println("extractPayloadBanks: record ID out of sequence, got " + recordId + " but expecting " + (payloadQueue.getRecordId() + 1));
            nonFatalRecordIdError = true;
        }
        payloadQueue.setRecordId(recordId);

        // store all banks except the first one containing record ID
        int numKids  = kids.size();
        int sourceId = Evio.getCodaId(dtrBank.getHeader().getTag()); // get 12 bit id from tag

        int tag;
        PayloadBank payloadBank;
        BaseStructureHeader header;

        for (int i=1; i < numKids; i++) {
            try {
                payloadBank = new PayloadBank((EvioBank) kids.get(i));
            }
            catch (ClassCastException e) {
                // dtrBank does not contain data banks and thus is not in the proper format
                throw new EmuException("DTR bank contains things other than banks");
            }

            payloadBank.setType(eventType);
            payloadBank.setRecordId(recordId);
            payloadBank.setSourceId(sourceId);
            nonFatalError = false;
            if (sourceId != getCodaId(payloadBank.getHeader().getTag())) {
System.out.println("extractPayloadBanks: DTR bank source Id conflicts with payload bank's roc id");
                nonFatalError = true;
            }
            
            header = payloadBank.getHeader();
            tag    = header.getTag();
            // pick this bank apart a little here
            if (header.getDataTypeEnum() != DataType.BANK &&
                header.getDataTypeEnum() != DataType.ALSOBANK) {
                throw new EmuException("ROC raw record not in proper format");
            }
            
            payloadBank.setSync(Evio.isSyncEvent(tag));
            payloadBank.setError(Evio.hasError(tag));
            payloadBank.setSingleEventMode(Evio.isSingleEventMode(tag));
            payloadBank.setNonFatalBuildingError(nonFatalError || nonFatalRecordIdError);
            
            // Put ROC raw event on queue.
            payloadQueue.add(payloadBank);
        }
    }


    /**
     * For each event, compare the timestamps from all ROCs to make sure they're within
     * the allowed difference.
     * 
     * @param triggerBank combined trigger bank containing timestamps to check for consistency
     * @return <code>true</code> if timestamps are OK, else <code>false</code>
     */
    public static boolean timeStampsOk(EvioBank triggerBank) {
        return true;
    }


    /**
     * Combine the trigger banks of all input payload banks of Physics event format (from previous
     * event builder) into a single trigger bank which will be used in the final built event.
     * Any error which occurs but allows the build to continue will be noted in the return value.
     * Errors which stop the event building cause an exception to be thrown.
     *
     * @param inputPayloadBanks array containing a bank (Physics event) from each channel's
     *                          payload bank queue that will be built into one event
     * @param builder object used to build trigger bank
     * @param ebId id of event builder calling this method
     * @return <code>true</code> if non fatal error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean makeTriggerBankFromPhysics(PayloadBank[] inputPayloadBanks,
                                                     EventBuilder builder, int ebId)
            throws EmuException {

        int index;
        int rocCount;
        int totalRocCount = 0;
        int numPayloadBanks = inputPayloadBanks.length;
        EvioBank[] triggerBanks = new EvioBank[numPayloadBanks];
        boolean nonFatalError = false;

        // In each payload bank (of banks) is a built trigger bank. Extract them all.
        for (int i=0; i < numPayloadBanks; i++) {

            // find the built trigger bank (should be first one)
            index = 0;
            do {
                try {
                    triggerBanks[i] = (EvioBank)inputPayloadBanks[i].getChildAt(index++);
                }
                catch (Exception e) {
                    throw new EmuException("No built trigger bank in physics event", e);
                }
            } while (!Evio.isBuiltTriggerBank(triggerBanks[i])); // check to see if it really is a built trigger bank

            // right now we don't know how many events are contained in each bank

            // number of trigger bank children must = # rocs + 1
            rocCount = triggerBanks[i].getHeader().getNumber() - 1;
            if (triggerBanks[i].getChildCount() != rocCount + 1) {
                throw new EmuException("Trigger bank does not have correct number of segments (" +
                        (rocCount + 1) + "), it has " + triggerBanks[i].getChildCount());
            }

            // track total number of rocs
            totalRocCount += rocCount;
        }

        // 1) The first segment in each built trigger bank contains common data
        //    in the format given below. Merging built banks should NOT
        //    change anything in the bank. However, all merging built trigger
        //    banks must have identical contents (which will be checked). The
        //    only change necessary is to update the EB id info in the header.
        //
        //    MSB(31)                    LSB(0)    Big Endian,  higher mem  -->
        //    _________________________________
        //    | event1 type  |  event1 number |
        //    | event2 type  |  event2 number |
        //    |       .      |        .       |
        //    |       .      |        .       |
        //    |       .      |        .       |
        //    | eventN type  |  eventN number |
        //    _________________________________
        //

        // check consistency of common data across events
        short[] commonData, firstCommonData;
        EvioSegment ebSeg = (EvioSegment)triggerBanks[0].getChildAt(0);
        firstCommonData = ebSeg.getShortData();

        for (int i=1; i < numPayloadBanks; i++) {
            commonData = ((EvioSegment)triggerBanks[i].getChildAt(0)).getShortData();
            if (firstCommonData.length != commonData.length) {
                throw new EmuException("Trying to merge records with different numbers of events");
            }
            for (int j=0; j < firstCommonData.length; j++) {
                if (firstCommonData[j] != commonData[j]) {
                    throw new EmuException("Trying to merge records with different event types or numbers");
                }
            }
        }

        // Bank we are trying to build. Need to update the num which = (# rocs + 1)
        EvioEvent combinedTrigger = builder.getEvent();
        combinedTrigger.getHeader().setNumber(totalRocCount + 1);

        // create common data segment, actually just grab first one and change id
        ebSeg.getHeader().setTag(ebId);
        try {
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }


        // 2) now put all ROC-specific segments into bank

        // for each event (from previous level EB) ...
        for (int i=0; i < triggerBanks.length; i++) {
            // copy over each roc segment in trigger bank to combined trigger bank
            for (int j=1; j < triggerBanks[i].getChildCount(); j++) {
                EvioSegment rocSeg = (EvioSegment)triggerBanks[i].getChildAt(j);
                try { builder.addChild(combinedTrigger, rocSeg); }
                catch (EvioException e) { /* never happen */ }
            }
        }

        combinedTrigger.setAllHeaderLengths();

        return nonFatalError;
    }

    

    /**
     * Combine the trigger banks of all input payload banks of ROC raw format into a single
     * trigger bank which will be used in the final built event. Any error
     * which occurs but allows the build to continue will be noted in the return value.
     * Errors which stop the event building cause an exception to be thrown.
     *
     * @param inputPayloadBanks array containing a bank (ROC Raw) from each channel's
     *                          payload bank queue that will be built into one event
     * @param builder object used to build trigger bank
     * @param ebId id of event builder calling this method
     * @return <code>true</code> if non fatal error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean makeTriggerBankFromRocRaw(PayloadBank[] inputPayloadBanks,
                                                    EventBuilder builder, int ebId)
            throws EmuException {

        int index;
        int numPayloadBanks = inputPayloadBanks.length;
        int numEvents = inputPayloadBanks[0].getHeader().getNumber();
        EvioBank[] triggerBanks = new EvioBank[numPayloadBanks];
        boolean nonFatalError = false;

        // In each payload bank (of banks) is a trigger bank. Extract them all.
        for (int i=0; i < numPayloadBanks; i++) {

            // find the trigger bank (should be first one)
            index = 0;
            do {
                try {
                    triggerBanks[i] = (EvioBank)inputPayloadBanks[i].getChildAt(index++);
                }
                catch (Exception e) {
                    throw new EmuException("No trigger bank in ROC raw record", e);
                }
            } while (!Evio.isTriggerBank(triggerBanks[i])); // check to see if it really is a trigger bank

            // check to see if all payload banks think they have same # of events
            if (numEvents != inputPayloadBanks[i].getHeader().getNumber()) {
                throw new EmuException("Data blocks contain different numbers of events");
            }

            // check if all trigger banks think they have the same number of events
            if (numEvents != triggerBanks[i].getHeader().getNumber()) {
                throw new EmuException("Data blocks contain different numbers of events");
            }
            inputPayloadBanks[i].setEventCount(numEvents);

            // number of trigger bank children must = # events
            if (triggerBanks[i].getChildCount() != numEvents) {
                throw new EmuException("Trigger bank does not have correct number of segments");
            }
        }

        // bank we are trying to build
        EvioEvent combinedTrigger = builder.getEvent();

        // 1) No sense duplicating data for each ROC/EB. Get event(trigger) types & event numbers
        //    and put into 1 common bank with each value being a short (use least significant 16
        //    bits of event) in the format:
        //
        //    MSB(31)                    LSB(0)    Big Endian,  higher mem  -->
        //    _________________________________
        //    | event1 type  |  event1 number |
        //    | event2 type  |  event2 number |
        //    |       .      |        .       |
        //    |       .      |        .       |
        //    |       .      |        .       |
        //    | eventN type  |  eventN number |
        //    _________________________________
        //

        EvioSegment segment;
        short[] evData = new short[2*numEvents];
        for (int i=0; i < numEvents; i++) {
            segment       = (EvioSegment) (triggerBanks[0].getChildAt(i));
            evData[2*i]   = (short) (segment.getHeader().getTag());  // event type
            evData[2*i+1] = (short) (segment.getIntData()[0]);       // event number
//System.out.println("for event #" + i + ": ev type = " + evData[2*i] +  ", ev # = " + evData[2*i+1]);
        }

        EvioSegment ebSeg = new EvioSegment(ebId, DataType.USHORT16);
        try {
            ebSeg.appendShortData(evData);
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }

        // It is convenient at this point to check and see if for a given place,
        // across all ROCs, the event number & type is the same.
        for (int i=0; i < numEvents; i++) {
//System.out.println("event type ROC1 = " + evData[2*i]);
            for (int j=1; j < triggerBanks.length; j++) {
                segment = (EvioSegment) (triggerBanks[j].getChildAt(i));
//System.out.println("event type next ROC = " + ((short) (segment.getHeader().getTag())));
                if (evData[2*i] != (short) (segment.getHeader().getTag())) {
                    nonFatalError = true;
                }
                if (evData[2*i+1] != (short) (segment.getIntData()[0])) {
                    nonFatalError = true;
                }
            }
        }


        // 2) now add one segment for each ROC with ROC-specific data in it
        int intCount, dataLenFromEachSeg;
        EvioSegment newRocSeg, oldRocSeg;

        // for each ROC ...
        for (int i=0; i < triggerBanks.length; i++) {
            newRocSeg = new EvioSegment(inputPayloadBanks[i].getSourceId(), DataType.INT32);
            // copy over all ints except the first which is the event Number and is stored in common

            // assume, for now, that each ROC segment in the trigger bank has the same amount of data
            oldRocSeg = (EvioSegment)triggerBanks[i].getChildAt(0);
            // (- 1) forget about event # that we already took care of
            dataLenFromEachSeg = oldRocSeg.getHeader().getLength() - 1;

            // total amount of new data for a new (ROC) segment
            intCount = numEvents * dataLenFromEachSeg;
            int[] newData = new int[intCount];

            int[] oldData;
            int position = 0;
            for (int j=0; j < numEvents; j++) {
                oldData = ((EvioSegment)triggerBanks[i].getChildAt(j)).getIntData();
                if (oldData.length != dataLenFromEachSeg + 1) {
                    throw new EmuException("Trigger segments contain different amounts of data");
                }
                System.arraycopy(oldData, 1, newData, position, dataLenFromEachSeg);
                position += dataLenFromEachSeg;
            }

            try {
                newRocSeg.appendIntData(newData);
                builder.addChild(combinedTrigger, newRocSeg);
            }
            catch (EvioException e) { /* never happen */ }
        }

        combinedTrigger.setAllHeaderLengths();

        return nonFatalError;
    }



    /**
     * Create a trigger bank from all input payload banks of ROC raw format (which are in
     * single event mode and therefore have no trigger bank) which will be used in the final
     * built event. Any error which occurs but allows the build to continue will be noted in
     * the return value. Errors which stop the event building cause an exception to be thrown.
     *
     * @param inputPayloadBanks array containing a bank (ROC Raw) from each channel's
     *                          payload bank queue that will be built into one event
     * @param builder object used to build trigger bank
     * @param ebId id of event builder calling this method
     * @return <code>true</code> if non fatal error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean makeTriggerBankFromSemRocRaw(PayloadBank[] inputPayloadBanks,
                                                       EventBuilder builder, int ebId)
            throws EmuException {

        int numPayloadBanks = inputPayloadBanks.length;
        boolean nonFatalError = false;

        for (int i=0; i < numPayloadBanks; i++) {
            // check to see if all payload banks think they have 1 event
            if (inputPayloadBanks[i].getHeader().getNumber() != 1) {
                throw new EmuException("Data blocks contain different numbers of events");
            }

            inputPayloadBanks[i].setEventCount(1);
        }

        // event we are trying to build
        EvioEvent combinedTrigger = builder.getEvent();

        // 1) No sense duplicating data for each ROC/EB. Get event(trigger) types & event numbers
        //    and put into 1 common bank with each value being a short (use least significant 16
        //    bits of event) in the format:
        //
        //    MSB(31)                    LSB(0)    Big Endian,  higher mem  -->
        //    _________________________________
        //    | event1 type  |  event1 number |
        //    _________________________________
        //

        // In each payload bank (of banks) there is no trigger bank.
        // Extract needed info from Data Block banks (pick first in list).
        EvioBank blockBank = (EvioBank)inputPayloadBanks[0].getChildAt(0);
        short[] evData = new short[2];
        evData[0] = (short) (blockBank.getHeader().getNumber());  // event type
        evData[1] = (short) (blockBank.getIntData()[0]);          // event number


        EvioSegment ebSeg = new EvioSegment(ebId, DataType.USHORT16);
        try {
            ebSeg.appendShortData(evData);
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }

        // It is convenient at this point to check and see if
        // across all ROCs, the event number & type are the same.
        // We are not checking info from additional Data Block banks
        // if more than one from a ROC.
System.out.println("event type ROC1 = " + evData[0]);
        for (int j=1; j < numPayloadBanks; j++) {
            blockBank = (EvioBank) (inputPayloadBanks[j].getChildAt(0));
System.out.println("event type next ROC = " + ((short) (blockBank.getHeader().getNumber())));
            if (evData[0] != (short) (blockBank.getHeader().getNumber())) {
                nonFatalError = true;
            }
            if (evData[1] != (short) (blockBank.getIntData()[0])) {
                nonFatalError = true;
            }
        }

        // no segments to add for each ROC since we have no ROC-specific data

        combinedTrigger.setAllHeaderLengths();

        return nonFatalError;
    }



    /**
     * Build a single physics event with the given trigger bank and the given array of Physics events.
     *
     * @param triggerBank bank containing merged trigger info
     * @param inputPayloadBanks array containing Physics events that will be built together
     * @param builder object used to build trigger bank
     * @return bank final built event
     */
    public static EvioBank buildPhysicsEventWithPhysics(EvioBank triggerBank,
                                                        EvioBank[] inputPayloadBanks,
                                                        EventBuilder builder) {

        int childrenCount;
        int numPayloadBanks = inputPayloadBanks.length;
        EvioBank  dataBlock;
        EvioEvent finalEvent = builder.getEvent();

        try {
            // add combined trigger bank
            builder.addChild(finalEvent, triggerBank);

            // add all data banks (from payload banks) which are already wrapped properly
            for (int i=0; i < numPayloadBanks; i++) {
                childrenCount = inputPayloadBanks[i].getChildCount();
                for (int j=0; j < childrenCount; j++) {
                    dataBlock = (EvioBank)inputPayloadBanks[i].getChildAt(j);
                    // ignore the built trigger bank (should be first one)
                    if (Evio.isBuiltTriggerBank(dataBlock)) {
                        continue;
                    }
                    builder.addChild(finalEvent, dataBlock);
                }
            }
        } catch (EvioException e) {
            // never happen
        }

        finalEvent.setAllHeaderLengths();

        return null;
    }



    /**
     * Build a single physics event with the given trigger bank and the given array of ROC raw records.
     *
     * @param triggerBank bank containing merged trigger info
     * @param inputPayloadBanks array containing ROC raw records that will be built together
     * @param builder object used to build trigger bank
     * @return bank final built event
     */
    public static EvioBank buildPhysicsEventWithRocRaw(EvioBank triggerBank,
                                                       EvioBank[] inputPayloadBanks,
                                                       EventBuilder builder) {

        int childrenCount;
        int numPayloadBanks = inputPayloadBanks.length;
        BankHeader header;
        EvioBank blockBank, dataBlock;
        EvioEvent finalEvent = builder.getEvent();

        try {
            // add combined trigger bank
            builder.addChild(finalEvent, triggerBank);

            // Wrap and add data block banks (from payload banks).
            // Use the same header to wrap data blocks as used for payload bank.
            for (int i=0; i < numPayloadBanks; i++) {
                header = (BankHeader)inputPayloadBanks[i].getHeader();
                blockBank = new EvioBank(header.getTag(), DataType.BANK, header.getNumber());
                childrenCount = inputPayloadBanks[i].getChildCount();
                for (int j=0; j < childrenCount; j++) {
                    dataBlock = (EvioBank)inputPayloadBanks[i].getChildAt(j);
                    // ignore the trigger bank (should be first one)
                    if (Evio.isTriggerBank(dataBlock)) {
                        continue;
                    }
                    builder.addChild(blockBank, dataBlock);
                }
                builder.addChild(finalEvent, blockBank);
            }
        } catch (EvioException e) {
            // never happen
        }

        finalEvent.setAllHeaderLengths();

        return null;
    }



    /**
     * Create an Evio ROC Raw record event/bank in single event mode to be placed in a Data Transport record.
     *
     * @param rocID       ROC id number
     * @param eventID     event (trigger) id number (0-15)
     * @param dataBankTag starting data bank tag
     * @param dataBankNum starting data bank num
     * @param eventNumber starting event number
     * @param timestamp   starting event's timestamp
     *
     * @return created ROC Raw Record (EvioEvent object)
     * @throws EvioException
     */
    public static EvioEvent createSingleEventModeRocRecord(int rocID,       int eventID,
                                                           int dataBankTag, int dataBankNum,
                                                           int eventNumber, int timestamp)
            throws EvioException {
        // single event mode means 1 event
        int numEvents = 1;

        // create a ROC Raw Data Record event/bank with numEvents physics events in it
        int rocTag = createCodaTag(false, false, false, true, rocID);
        EventBuilder eventBuilder = new EventBuilder(rocTag, DataType.BANK, numEvents);
        EvioEvent rocRawEvent = eventBuilder.getEvent();

        // put some data into event -- 2 ints per event. First is event #.
        int[] data = new int[2];
        data[0] = eventNumber;
        data[1] = 10000;

        // create a single data block bank (of ints)
        int eventType  = 33;
        int detectorID = 666;
        int detTag = createCodaTag(false, false, false, true, detectorID);
        EvioBank dataBank = new EvioBank(detTag, DataType.INT32, eventType);
        eventBuilder.appendIntData(dataBank, data);
        eventBuilder.addChild(rocRawEvent, dataBank);

        eventBuilder.setAllHeaderLengths();

        return rocRawEvent;
    }


    /**
     * Create an Evio ROC Raw record event/bank to be placed in a Data Transport record.
     *
     * @param rocID       ROC id number
     * @param eventID     event (trigger) id number (0-15)
     * @param dataBankTag starting data bank tag
     * @param dataBankNum starting data bank num
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param timestamp   starting event's timestamp
     *
     * @return created ROC Raw Record (EvioEvent object)
     * @throws EvioException
     */
    public static EvioEvent createRocRawRecord(int rocID,       int eventID,
                                               int dataBankTag, int dataBankNum,
                                               int eventNumber, int numEvents,
                                               int timestamp) throws EvioException {

        // create a ROC Raw Data Record event/bank with numEvents physics events in it
        int status = 0;  // TODO: may want to make status a method arg
        int rocTag = createCodaTag(status, rocID);
        EventBuilder eventBuilder = new EventBuilder(rocTag, DataType.BANK, numEvents);
        EvioEvent rocRawEvent = eventBuilder.getEvent();

        // create the trigger bank (of segments)
        EvioBank triggerBank = new EvioBank(TRIGGER_BANK, DataType.SEGMENT, numEvents);
        eventBuilder.addChild(rocRawEvent, triggerBank);

        EvioSegment segment;
        for (int i = 0; i < numEvents; i++) {
            // each segment contains eventNumber & timestamp of corresponding event in data bank
            segment = new EvioSegment(eventID, DataType.UINT32);
            eventBuilder.addChild(triggerBank, segment);
            // generate 2 segments per event
            int[] segmentData = new int[2];
            segmentData[0] = eventNumber++;
            segmentData[1] = timestamp++;
            eventBuilder.appendIntData(segment, segmentData); // copies reference only
        }

        // put some data into event -- one int per event
        int[] data = new int[numEvents];
        for (int i = 0; i < numEvents; i++) {
            data[i] = 10000 + i;
        }

        // create a single data bank (of ints) -- NOT SURE WHAT A DATA BANK LOOKS LIKE !!!
        EvioBank dataBank = new EvioBank(dataBankTag, DataType.INT32, dataBankNum);
        eventBuilder.appendIntData(dataBank, data);
        eventBuilder.addChild(rocRawEvent, dataBank);

        eventBuilder.setAllHeaderLengths();

        return rocRawEvent;
    }


    /**
     * Create an Evio Physics event/bank to be placed in a Data Transport record.
     *
     * @param ebID        EB id number
     * @param eventID     event (trigger) type/id number (0-15)
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param timestamp   starting event's timestamp
     * @param numRocs     number of ROCs
     * @param status      4 bits of status of EB & ROCs
     * @param startingRocId ROC ids start at this number
     *
     * @return created ROC Raw Record (EvioEvent object)
     * @throws EvioException
     */
    public static EvioEvent createPhysicsEvent(int ebID,        int eventID,
                                               int eventNumber, int numEvents,
                                               int timestamp,   int numRocs,
                                               int status,      int startingRocId) throws EvioException {

        // create a Physics event/bank with numEvents physics events in it from multiple ROCs
        int ebTag = createCodaTag(status, ebID);
        EventBuilder eventBuilder = new EventBuilder(ebTag, DataType.BANK, 0xCC);
        EvioEvent physicsEvent = eventBuilder.getEvent();

        //--------------------------------------
        // create the trigger bank (of segments)
        //--------------------------------------

        EvioBank triggerBank = new EvioBank(BUILT_TRIGGER_BANK, DataType.SEGMENT, numRocs+1);
        eventBuilder.addChild(physicsEvent, triggerBank);

        // first the common data segment
        EvioSegment segment;
        segment = new EvioSegment(ebID, DataType.USHORT16);
        // define common data
        int eventNum = eventNumber;
        short[] commonData = new short[2*numEvents];
        for (int i = 0; i < 2*numEvents; i+=2) {
            commonData[i]   = (short)eventID;
            commonData[i+1] = (short)eventNum++;
        }
        eventBuilder.appendShortData(segment, commonData); // copies reference only
        eventBuilder.addChild(triggerBank, segment);

        // now the ROC-specific segments, each with only timestamp info
        int rocId = startingRocId;
        // each segment contains 1 timestamp for each event in data bank
        int[] segmentData = new int[numEvents];
        for (int i=0; i < numEvents; i++) {
            segmentData[i] = timestamp + i;
        }

        for (int i=0; i < numRocs; i++) {
            segment = new EvioSegment(rocId++, DataType.UINT32);
            eventBuilder.appendIntData(segment, segmentData);
            eventBuilder.addChild(triggerBank, segment);
        }


        //----------------------------------------------------
        // create the data banks for ROCs and to physics event
        //----------------------------------------------------

        // now create one data bank per ROC, each of which has a single data block bank
        int rocTag, blockTag, detectorId=444;
        EvioBank dataBank, dataBlockBank;
        rocId = startingRocId;

        for (int i=0; i < numRocs; i++) {
            // a data block bank contains starting event number & one int of data for each event
            blockTag = createCodaTag(0, detectorId);
            dataBlockBank = new EvioBank(blockTag, DataType.UINT32, numEvents);// if SEM, num = event Type
            int[] bankData = new int[numEvents+1];
            bankData[0] = eventNumber;
            for (int j=1; j < numEvents+1; j++) {
                bankData[j] = timestamp + j - 1;
            }
            eventBuilder.appendIntData(dataBlockBank, bankData);

            // wrap block bank in data bank
            rocTag = createCodaTag(status, rocId++);
            dataBank = new EvioBank(rocTag, DataType.BANK, numEvents);

            // add block bank to data bank
            eventBuilder.addChild(dataBank, dataBlockBank);
            // add data bank to physics event
            eventBuilder.addChild(physicsEvent, dataBank);
        }

        eventBuilder.setAllHeaderLengths();

        return physicsEvent;
    }


    /**
     * Create an Evio Data Transport Record event to send to the event building EMU.
     *
     * @param rocEbId     ROC or EB id number, depending on value of "fromEB"
     * @param eventID     event (trigger) id number (0-15)
     * @param dataBankTag starting data bank tag
     * @param dataBankNum starting data bank num
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param timestamp   starting event's timestamp
     * @param recordId    record count
     * @param numPayloadBanks number of payload banks in this record
     * @param singleEventMode true if creating events in single event mode
     *
     * @return Evio Data Transport Record event
     * @throws EvioException
     */
    public static EvioEvent createDataTransportRecord(int rocEbId,     int eventID,
                                                      int dataBankTag, int dataBankNum,
                                                      int eventNumber, int numEvents,
                                                      int timestamp,   int recordId,
                                                      int numPayloadBanks,
                                                      boolean singleEventMode)
            throws EvioException {

        // create event with jevio package
        EventType type = EventType.ROC_RAW;
        int tag = createCodaTag(type.getValue(), rocEbId);
        EventBuilder eventBuilder = new EventBuilder(tag, DataType.BANK, recordId);
        EvioEvent ev = eventBuilder.getEvent();

        // add a bank with record ID in it
        EvioBank recordIdBank = new EvioBank(RECORD_ID_BANK, DataType.INT32, numPayloadBanks);
        eventBuilder.appendIntData(recordIdBank, new int[]{recordId});
        eventBuilder.addChild(ev, recordIdBank);

        if (singleEventMode) {
            numEvents = 1;
        }

        // add ROC Raw Records as payload banks
        EvioEvent event = null;
        for (int i=0; i < numPayloadBanks; i++) {

            if (singleEventMode) {
                event = createSingleEventModeRocRecord(rocEbId, eventID, dataBankTag, dataBankNum,
                                                       eventNumber, timestamp);
            }
            else {
                event = createRocRawRecord(rocEbId, eventID, dataBankTag, dataBankNum,
                                           eventNumber, numEvents, timestamp);
            }
            eventBuilder.addChild(ev, event);

            eventNumber += numEvents;
            timestamp   += numEvents;
        }

        eventBuilder.setAllHeaderLengths();

        return ev;
    }


    /**
     * Create an Evio Data Transport Record event to send to the event building EMU from an
     * event building EMU.
     *
     * @param ebId          EB id number
     * @param eventID       event (trigger) id number (0-15)
     * @param numRocs       number of ROCs to simulate
     * @param startingRocId starting ROC id number
     * @param eventNumber   starting event number
     * @param numEvents     number of physics events in created record
     * @param timestamp     starting event's timestamp
     * @param recordId      record count
     * @param status        4 bits of status of EB & ROCs
     * @param singleEventMode true if creating events in single event mode
     *
     * @return Evio Data Transport Record event
     * @throws EvioException
     */
    public static EvioEvent createDataTransportRecordFromEB(int ebId,        int eventID,
                                                            int numRocs,     int startingRocId,
                                                            int eventNumber, int numEvents,
                                                            int timestamp,   int recordId,
                                                            int status,
                                                            boolean singleEventMode)
            throws EvioException {

        // create event with jevio package
        EventType type = EventType.PHYSICS;
        int tag = createCodaTag(type.getValue(), ebId);
        EventBuilder eventBuilder = new EventBuilder(tag, DataType.BANK, recordId);
        EvioEvent ev = eventBuilder.getEvent();

        // add a bank with record ID in it
        EvioBank recordIdBank = new EvioBank(RECORD_ID_BANK, DataType.INT32, 1);
        eventBuilder.appendIntData(recordIdBank, new int[]{recordId});
        eventBuilder.addChild(ev, recordIdBank);

        if (singleEventMode) {
            numEvents = 1;
        }

        // add 1 physics event as payload bank
        EvioEvent event;

        event = createPhysicsEvent(ebId,        eventID,
                                   eventNumber, numEvents,
                                   timestamp,   numRocs,
                                   status,      startingRocId);

        eventBuilder.addChild(ev, event);
        eventBuilder.setAllHeaderLengths();

        return ev;
    }


}
