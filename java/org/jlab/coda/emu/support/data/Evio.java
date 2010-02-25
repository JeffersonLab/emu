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
    private static EvioByteParser parser = new EvioByteParser();  // TODO: not good idea to have static parser


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
     * other types of recordes/events which is the upper 4 bits of the CODA-format tag.
     * .
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
     * Determine whether a bank is a control event or not.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is control event, else <code>false</code>
     */
    public static boolean isControlEvent(EvioBank bank) {
        if (bank == null)  return false;

        int num = bank.getHeader().getNumber();
        EventType eventType = getEventType(bank);
        if (eventType == null) return false;
   
        return (num == 0xCC && eventType.isControl());
    }


    /**
     * Determine whether a bank is a physics event or not.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is physics event, else <code>false</code>
     */
    public static boolean isPhysicsEvent(EvioBank bank) {
        if (bank == null)  return false;

        EventType eventType = getEventType(bank);
        if (eventType == null) return false;

        return (eventType.isPhysics());
    }


    /**
      * Determine whether a bank is a ROC raw record or not.
      *
      * @param bank input bank
      * @return <code>true</code> if arg is a ROC raw record, else <code>false</code>
     */
    public static boolean isRocRawRecord(EvioBank bank) {
        if (bank == null)  return false;

        EventType eventType = getEventType(bank);
        if (eventType == null) return false;

        return (eventType.isROCRaw());
    }


    /**
     * Determine whether a bank should be on the payload bank queue
     * (ie is a physics event, control event, or ROC raw record).
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
        return (bank != null && bank.getHeader().getTag() == id);
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

        // check to see if DTR bank is wrapping a non-buildable, non-control event
        if (!Evio.isForPayloadQueue(dtrBank)) {
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
            payloadBank.setSingleMode(Evio.isSingleEventMode(tag));
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
     * Combine the trigger banks of all input payload banks into a single
     * trigger bank which will be used in the final built event. Any error
     * which occurs but allows the build to continue will be noted in the return value.
     * Errors which stop the event building cause an exception to be thrown.
     *
     * @param inputPayloadBanks array containing a bank from each channel's payload bank queue that will be
     *                          built into one event
     * @param builder object used to build trigger bank
     * @param ebId id of event builder calling this method
     * @return <code>true</code> if non fatal error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean combineTriggerBanks(PayloadBank[] inputPayloadBanks,
                                               EventBuilder builder, int ebId)
            throws EmuException {

        int index;
        int numPayloadBanks = inputPayloadBanks.length;
        int numEvents = inputPayloadBanks[0].getHeader().getNumber();
        EvioBank trigBank;
        EvioBank[] triggerBanks = new EvioBank[numPayloadBanks];
        boolean nonFatalError = false;
        // pre allocate a buffer based on a guess of its maximum size (100 ev ~= 1600 bytes)
        ByteBuffer bbuf = ByteBuffer.allocate(2048);

        // In each payload bank (of banks) is a trigger bank. Extract them all.
        for (int i=0; i < numPayloadBanks; i++) {

            // find the trigger bank (should be first one)
            index = 0;
            do {
                try {
                    trigBank = (EvioBank)inputPayloadBanks[i].getChildAt(index++);
                }
                catch (Exception e) {
                    throw new EmuException("No trigger bank in ROC raw record", e);
                }
            } while (!Evio.isTriggerBank(trigBank)); // check to see if it really is a trigger bank

            // check to see if all payload banks think they have same # of events
            if (numEvents != inputPayloadBanks[i].getHeader().getNumber()) {
                throw new EmuException("Data blocks contain different numbers of events");
            }

            // Only the header of this trigger bank has been parsed. This is because we only specified
            // parseDepth = 2 in the config file. Specifying parseDepth = 3 (or 0) would do the trick,
            // but would also mean parsing part (or all) of the opaque data blocks - which we do NOT want.
            //
            // So, ... now we need to fully parse this trigger bank. To do this we must first turn this
            // bank back into a ByteBufer, then we can parse the bytes into an EvioEvent object.

            if (trigBank.getTotalBytes() > bbuf.capacity()) {
                bbuf = ByteBuffer.allocate(trigBank.getTotalBytes() + 1024);
            }
            else {
                bbuf.clear();
            }
            // set endian
            bbuf.order(trigBank.getByteOrder());
            // put header info into buffer
            trigBank.getHeader().write(bbuf);
            // put raw bytes (non-header) into buffer
            bbuf.put(trigBank.getRawBytes());
            // get buffer ready to read
            bbuf.flip();

            try {
                triggerBanks[i] = parser.parseEvent(bbuf);
            }
            catch (EvioException e) {
                throw new EmuException("Data not in evio format", e);
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

        // event we are trying to build
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
        EvioSegment ebSeg = new EvioSegment(ebId, DataType.USHORT16);
        short[] evData = new short[2*numEvents];
        
        for (int i=0; i < numEvents; i++) {
            segment       = (EvioSegment) (triggerBanks[0].getChildAt(i));
            evData[2*i]   = (short) (segment.getHeader().getTag());  // event type
            evData[2*i+1] = (short) (segment.getIntData()[0]);       // event number
System.out.println("for event #" + i + ": ev type = " + evData[2*i] +  ", ev # = " + evData[2*i+1]);
        }

        try {
            ebSeg.appendShortData(evData);
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }

        // It is convenient at this point to check and see if for a given event #,
        // across all ROCs, the event or trigger type is the same.
        for (int i=0; i < numEvents; i++) {
System.out.println("event type ROC1 = " + evData[2*i]);
            for (int j=1; j < triggerBanks.length; j++) {
                segment = (EvioSegment) (triggerBanks[j].getChildAt(i));
System.out.println("event type next ROC = " + ((short) (segment.getHeader().getTag())));
                if (evData[2*i] != (short) (segment.getHeader().getTag())) {
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
     * Combine the trigger banks of all input Data Transport Record events into a single
     * trigger bank which will be used in the built event.
     *
     * @param triggerBank bank containing merged trigger info
     * @param inputPayloadBanks array containing events that will be built together
     * @param builder object used to build trigger bank
     * @return bank final built event
     * @throws EmuException for major error in event building
     */
    public static EvioBank buildPhysicsEvent(EvioBank triggerBank,
                                             EvioBank[] inputPayloadBanks,
                                             EventBuilder builder)
            throws EmuException {

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
     * Create an Evio Data Transport Record event to send to the event building EMU.
     *
     * @param rocID       ROC id number
     * @param eventID     event (trigger) id number (0-15)
     * @param dataBankTag starting data bank tag
     * @param dataBankNum starting data bank num
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param timestamp   starting event's timestamp
     * @param recordId    record count
     * @param numPayloadBanks number of payload banks in this record
     *
     * @return Evio Data Transport Record event
     * @throws EvioException
     */
    public static EvioEvent createDataTransportRecord(int rocID,       int eventID,
                                                      int dataBankTag, int dataBankNum,
                                                      int eventNumber, int numEvents,
                                                      int timestamp,   int recordId,
                                                      int numPayloadBanks) throws EvioException {
        // create event with jevio package
        int tag = createCodaTag(EventType.ROC_RAW.getValue(), rocID);
        EventBuilder eventBuilder = new EventBuilder(tag, DataType.BANK, recordId);
        EvioEvent ev = eventBuilder.getEvent();

        // add a bank with record ID in it
        EvioBank recordIdBank = new EvioBank(RECORD_ID_BANK, DataType.INT32, numPayloadBanks);
        eventBuilder.appendIntData(recordIdBank, new int[]{recordId});
        eventBuilder.addChild(ev, recordIdBank);

        // add ROC Raw Records as payload banks
        EvioEvent rocRawRecord;
        for (int i=0; i < numPayloadBanks; i++) {
            rocRawRecord = createRocRawRecord(rocID, eventID, dataBankTag, dataBankNum,
                                              eventNumber, numEvents, timestamp);
            eventBuilder.addChild(ev, rocRawRecord);

            eventNumber += numEvents;
            timestamp   += numEvents;

            // each go round, change # of events in payload bank
            numEvents++;
        }

        eventBuilder.setAllHeaderLengths();

        return ev;
    }


}
