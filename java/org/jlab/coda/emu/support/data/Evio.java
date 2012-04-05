/*
 * Copyright (c) 2009, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.*;
import org.jlab.coda.emu.EmuException;

import java.nio.ByteOrder;
import java.util.Arrays;

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
 * | Single|   Data   |  Error |  Sync   |
 * | Event |  Endian  |        |  Event  |
 * |  Mode |          |        |         |
 * |_____________________________________|
 *
 *  Data Endian bit is set if data is big endian
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
 * |    0x0F02        |  0x20  |   N+2   |
 * |_____________________________________| --------
 * | EB id  |   0xa   |        4         |    ^
 * |_____________________________________|    |
 * |________ First Event Number _________|    |
 * |_____________________________________|    |
 * |____________ Run Number _____________|    |
 * |_____________________________________|    |
 * | EB id  |  0x05   |       Len        |    |
 * |_____________________________________|    |
 * |   Event Type 1   |  Event Type 2    |  Common Data
 * |_____________________________________|    |
 * |                  ...                |    |
 * |_____________________________________|    |
 * |  Event Type M-1  |  Event Type M    |    V
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
    /** In ROC Raw Record, mask to get endian status bit from tag. */
    private static final int ENDIAN_BIT_MASK             = 0x4000;
    /** In ROC Raw Record, mask to get single-event status bit from tag. */
    private static final int SINGLE_EVENT_MODE_BIT_MASK  = 0x8000;



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
     * @param isBigEndian data is big endian
     * @param singleEventMode is single event mode
     * @param id lowest 12 bits are id
     * @return a 16-bit tag for a ROC Raw Record or Physics Event out of 4 status bits and a 12-bit id
     */
    public static int createCodaTag(boolean sync, boolean error, boolean isBigEndian,
                                    boolean singleEventMode, int id) {
        int status = 0;
        
        if (sync)            status |= SYNC_BIT_MASK;
        if (error)           status |= ERROR_BIT_MASK;
        if (isBigEndian)     status |= ENDIAN_BIT_MASK;
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
    public static int getTagCodaId(int codaTag) {
        return ID_BIT_MASK & codaTag;
    }

    /**
     * See if the given CODA-format tag indicates the ROC is in single event mode.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @param codaTag tag from evio bank.
     * @return <code>true</code> if the ROC is in single event mode.
     */
    public static boolean isTagSingleEventMode(int codaTag) {
        return (codaTag & SINGLE_EVENT_MODE_BIT_MASK) != 0;
    }

    /**
     * See if the given CODA-format tag indicates it is a sync event.
     * This condition is set by the ROC and it is only read here - never set.
     *
     * @param codaTag tag from evio bank.
     * @return <code>true</code> if is a sync event.
     */
    public static boolean isTagSyncEvent(int codaTag) {
        return (codaTag & SYNC_BIT_MASK) != 0;
    }

    /**
     * See if the given CODA-format tag indicates data is big endian.
     * This condition is set by the ROC and may be set in emu if data is swapped.
     *
     * @param codaTag tag from evio bank.
     * @return <code>true</code> if data is big endian.
     */
    public static boolean isTagBigEndian(int codaTag) {
        return (codaTag & ENDIAN_BIT_MASK) != 0;
    }

    /**
     * Set the given CODA-format tag to indicate data is little/big endian.
     *
     * @param codaTag tag from evio bank.
     * @param isBigEndian <code>true</code> if big endian, else <code>false</code>
     * @return new codaTag with bit set to indicate data endianness.
     */
    public static int setTagEndian(int codaTag, boolean isBigEndian) {
        if (isBigEndian) {
            return (codaTag | ENDIAN_BIT_MASK);
        }
        return (codaTag & ~ENDIAN_BIT_MASK);
    }

    /**
     * See if the given CODA-format tag indicates the ROC/EB has an error.
     * This condition is set by the ROC/EB and it is only read here - never set.
     *
     * @param codaTag tag from evio bank.
     * @return <code>true</code> if the ROC/EB has an error.
     */
    public static boolean tagHasError(int codaTag) {
        return (codaTag & ERROR_BIT_MASK) != 0;
    }


    /**
     * Get the event type's numerical value in Data Transport Records or the status in
     * other types of records/events which is the upper 4 bits of the CODA-format tag.
     *
     * @param codaTag tag from evio bank.
     * @return the event type or status from various records/events.
     */
    public static int getTagEventTypeOrStatus(int codaTag) {
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

        int type = getTagEventTypeOrStatus(bank.getHeader().getTag());
        return EventType.getEventType(type);
    }


    /**
     * Determine whether a bank is an END control event or not.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is END event, else <code>false</code>
     */
    public static boolean isAnyPhysicsEvent(EvioBank bank) {
        if (bank == null)  return false;

        // check to see if bank of banks
        BaseStructureHeader header = bank.getHeader();
        if (header.getDataType() != DataType.BANK ||
            header.getDataType() != DataType.ALSOBANK) {
            return false;
        }

        // get first bank which should be built trigger bank
        try {
            EvioBank kid = (EvioBank)bank.getChildAt(0);
            if (!CODATag.isBuiltTrigger(kid.getHeader().getTag())) {
                return false;
            }
        }
        catch (Exception e) {
            return false;
        }

        return true;
    }


    /**
     * Determine whether a bank is an END control event or not.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is END event, else <code>false</code>
     */
    public static boolean isEndEvent(EvioBank bank) {
        if (bank == null)  return false;

        BaseStructureHeader header = bank.getHeader();

        // Look inside to see if it is an END event.
        return (header.getTag() == 20 &&
                header.getNumber() == 0xCC &&
                header.getDataTypeValue() == 1 &&
                header.getLength() == 4);
    }


    /**
      * Determine whether a bank is a GO control event or not.
      *
      * @param bank input bank
      * @return <code>true</code> if arg is GO event, else <code>false</code>
      */
     public static boolean isGoEvent(EvioBank bank) {
         if (bank == null)  return false;

         BaseStructureHeader header = bank.getHeader();

         // Look inside to see if it is a GO event.
         return (header.getTag() == 18 &&
                 header.getNumber() == 0xCC &&
                 header.getDataTypeValue() == 1 &&
                 header.getLength() == 4);
     }


    /**
      * Determine whether a bank is a PRESTART control event or not.
      *
      * @param bank input bank
      * @return <code>true</code> if arg is PRESTART event, else <code>false</code>
      */
     public static boolean isPrestartEvent(EvioBank bank) {
         if (bank == null)  return false;

         BaseStructureHeader header = bank.getHeader();

         // Look inside to see if it is an END event.
         return (header.getTag() == 17 &&
                 header.getNumber() == 0xCC &&
                 header.getDataTypeValue() == 1 &&
                 header.getLength() == 4);
     }


    /**
      * Determine whether a bank is a PAUSE control event or not.
      *
      * @param bank input bank
      * @return <code>true</code> if arg is PAUSE event, else <code>false</code>
      */
     public static boolean isPauseEvent(EvioBank bank) {
         if (bank == null)  return false;

         BaseStructureHeader header = bank.getHeader();

         // Look inside to see if it is an END event.
         return (header.getTag() == 19 &&
                 header.getNumber() == 0xCC &&
                 header.getDataTypeValue() == 1 &&
                 header.getLength() == 4);
     }


    /**
      * Determine whether a bank is a PAUSE control event or not.
      *
      * @param bank input bank
      * @return <code>true</code> if arg is PAUSE event, else <code>false</code>
      */
     public static boolean isControlEvent(EvioBank bank) {
         if (bank == null)  return false;

         BaseStructureHeader header = bank.getHeader();
         int tag = header.getTag();

         // Look inside to see if it is an END event.
         return ((tag == 20 || tag == 19 || tag == 18 || tag == 17) &&
                 header.getNumber() == 0xCC &&
                 header.getDataTypeValue() == 1 &&
                 header.getLength() == 4);
     }


    /**
     * Determine whether a bank is a trigger bank from a ROC or not.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is trigger bank, else <code>false</code>
     */
    public static boolean isRawTriggerBank(EvioBank bank) {
        if (bank == null)  return false;
        return CODATag.isRawTrigger(bank.getHeader().getTag());
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
        return CODATag.isBuiltTrigger(bank.getHeader().getTag());
    }


    /**
     * Check the given payload bank (physics, ROC raw, control, or user format evio banks)
     * for correct format and place onto the specified queue.
     * <b>All other banks are thrown away.</b><p>
     *
     * No checks done on arguments. However, format of payload banks is checked here for
     * the first time.<p>
     *
     * @param pBank payload bank to be examined
     * @param payloadQueue queue on which to place acceptable payload bank
     * @throws EmuException if physics or roc raw bank has improper format
     * @throws InterruptedException if blocked while putting bank on full output queue
     */
    public static void checkPayloadBank(PayloadBank pBank, PayloadBankQueue<PayloadBank> payloadQueue)
            throws EmuException, InterruptedException {

        // check to make sure record ID is sequential - already checked by EvioReader
        int tag;
        BaseStructureHeader header;
        int recordId = pBank.getRecordId();
        int sourceId = pBank.getSourceId();
        boolean nonFatalError = false;
        boolean nonFatalRecordIdError = false;

        // See what type of event this is
        // Only interested in known types such as physics, roc raw, control, and user events.
        EventType eventType = pBank.getType();
        if (eventType == null) {
System.out.println("checkPayloadBank: unknown type, dump payload bank");
            return;
        }
//System.out.println("checkPayloadBank: got bank of type " + eventType);

        // Only worry about record id if event to be built.
        // Initial recordId stored is 0, ignore that.
        if (eventType.isAnyPhysics() || eventType.isROCRaw()) {
            if (payloadQueue.getRecordId() > 0 &&
                recordId != payloadQueue.getRecordId() + 1) {
System.out.println("checkPayloadBank: record ID out of sequence, got " + recordId +
                           " but expecting " + (payloadQueue.getRecordId() + 1) + ", type = " + eventType);
                nonFatalRecordIdError = true;
            }
            payloadQueue.setRecordId(recordId);

            header = pBank.getHeader();
            tag    = header.getTag();

            if (sourceId != getTagCodaId(tag)) {
System.out.println("checkPayloadBank: DTR bank source Id (" + sourceId + ") != payload bank's id (" + getTagCodaId(tag) + ")");
                nonFatalError = true;
            }

            // pick this bank apart a little here
            if (header.getDataType() != DataType.BANK &&
                header.getDataType() != DataType.ALSOBANK) {
                throw new EmuException("ROC raw / physics record not in proper format");
            }

            pBank.setSync(Evio.isTagSyncEvent(tag));
            pBank.setError(Evio.tagHasError(tag));
            pBank.setSingleEventMode(Evio.isTagSingleEventMode(tag));
        }

        pBank.setNonFatalBuildingError(nonFatalError || nonFatalRecordIdError);

        // Put bank on queue.
        payloadQueue.put(pBank);
    }


    /**
     * Combine the trigger banks of all input payload banks of Physics event format (from previous
     * event builder) into a single trigger bank which will be used in the final built event.
     * Any error which occurs but allows the build to continue will be noted in the return value.
     * Errors which stop the event building cause an exception to be thrown.<p>
     *
     * If timestamp checking is enabled, it will only be valid here if all physics events
     * being currently built have come via previous event builders in which timestamp
     * checking was enabled. In this case, each input bank must have timestamp information
     * in its trigger bank or an exception with be thrown. The first level of
     * event builders check the timestamp drift of each ROC. This (2nd or higher) level of
     * event builder checks the average timestamp for an event from one group of ROCs against
     * another group's. The trigger bank created in this method will contain the average
     * timestamps.<p>
     *
     * If run number and run type data are included in all input banks, then they will be
     * checked for consistency. A non-fatal error will be returned if they are not.<p>
     *
     * @param inputPayloadBanks array containing a bank (Physics event) from each channel's
     *                          payload bank queue that will be built into one event
     * @param builder   object used to build trigger bank
     * @param ebId      id of event builder calling this method
     * @param runNumber run number to place in trigger bank
     * @param runType   run type to place in trigger bank
     * @param includeRunData  if <code>true</code>, add run number and run type
     * @param checkTimestamps if <code>true</code>, check timestamp consistency and
     *                        return false if inconsistent
     * @param timestampSlop maximum number of timestamp ticks that timestamps can differ
     *                      for a single event before the error bit is set in a bank's
     *                      status. Only used when checkTimestamps arg is <code>true</code>
     *
     * @return <code>true</code> if non-fatal error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean makeTriggerBankFromPhysics(PayloadBank[] inputPayloadBanks,
                                                     EventBuilder builder, int ebId,
                                                     int runNumber, int runType,
                                                     boolean includeRunData,
                                                     boolean checkTimestamps,
                                                     int timestampSlop)
            throws EmuException {

        if (builder == null || inputPayloadBanks == null || inputPayloadBanks.length < 1) {
            throw new EmuException("arguments are null or zero-length");
        }

        int index, rocCount;
        int totalRocCount = 0;
        int numInputBanks = inputPayloadBanks.length;
        // number of events in each payload bank
        int numEvents = inputPayloadBanks[0].getHeader().getNumber();
        EvioBank[] triggerBanks = new EvioBank[numInputBanks];
        CODATag[] tags = new CODATag[numInputBanks];
        boolean nonFatalError = false;
        boolean allRunDataPresent = true;
        boolean allTimestampsPresent = true;

        // In each payload bank (of banks) is a built trigger bank. Extract them all.
        for (int i=0; i < numInputBanks; i++) {

            // find the built trigger bank (should be first one)
            index = 0;
            do {
                try {
                    triggerBanks[i] = (EvioBank)inputPayloadBanks[i].getChildAt(index++);
                    tags[i] = CODATag.getTagType(triggerBanks[i].getHeader().getTag());
                }
                catch (Exception e) {
                    throw new EmuException("No built trigger bank in physics event", e);
                }
            } while (!Evio.isBuiltTriggerBank(triggerBanks[i])); // check to see if it really is a built trigger bank

            // Number of trigger bank children must = # rocs + 2
            rocCount = triggerBanks[i].getHeader().getNumber() - 2;
            if (triggerBanks[i].getChildCount() != rocCount + 2) {
                throw new EmuException("Trigger bank does not have correct number of segments (" +
                        (rocCount + 2) + "), it has " + triggerBanks[i].getChildCount());
            }

            // Track total number of rocs
            totalRocCount += rocCount;

            // Check to see if all trigger banks have timestamp data
            if (checkTimestamps) {
                allTimestampsPresent &= CODATag.hasTimestamp(tags[i].getValue());
            }

            // Check to see if all trigger banks have run number & type data
            allRunDataPresent &= CODATag.hasRunData(tags[i].getValue());
        }

        //    Merging built banks should NOT change anything in the 2 common data banks.
        //    However, all merging built trigger banks must have identical contents in
        //    these banks (which will be checked). The only change necessary is to update
        //    the EB id info in the header of each.
        //
        //
        // 1) The first segment in each built trigger bank contains common data
        //    in the format given below. This is a segment of unsigned 64 bit
        //    integers containing the first event number followed by timestamps
        //    IF the config is set for checking timestamps. That is followed by
        //    the run number (32 bits) and the run type (32 bits) If the config
        //    is set for adding runData.
        //
        //    MSB(31)                          LSB(0)    Big Endian,  higher mem  -->
        //    _______________________________________
        //    | first event number (high 32 bits)   |
        //    | first event number (low  32 bits)   |
        //    |       timestamp1   (high 32 bits)   |
        //    |       timestamp1   (low  32 bits)   |
        //    |                      ...            |
        //    |       timestampM   (high 32 bits)   |
        //    |       timestampM   (low  32 bits)   |
        //    |       run   number (high 32 bits)   |
        //    |       run   type   (low  32 bits)   |
        //    _______________________________________
        //
        //
        // 2) The second segment in each built trigger bank contains common data
        //    in the format given below. This is a segment of unsigned 16 bit
        //    integers containing the event type of each event.
        //
        //    MSB(31)                    LSB(0)    Big Endian,  higher mem  -->
        //    __________________________________
        //    |  event1 type  |  event2 type   |
        //    |        .      |        .       |
        //    |        .      |        .       |
        //    | eventN-1 type |  eventN type   |
        //    __________________________________
        //

        // check consistency of common data across events
        long[]  commonLong,  longCommonData;
        short[] commonShort, shortCommonData;
        EvioSegment ebSeg1 = (EvioSegment)triggerBanks[0].getChildAt(0);
        EvioSegment ebSeg2 = (EvioSegment)triggerBanks[0].getChildAt(1);
        longCommonData  = ebSeg1.getLongData();
        shortCommonData = ebSeg2.getShortData();
        long firstEventNumber = longCommonData[0];

        // check to see if at least event & run #s are present
//        if (longCommonData.length < 2) {
//            throw new EmuException("Common data incomplete");
//        }

        // stuff for checking timestamps
        long   ts;
        long[] timestampsAvg = null;
        long[] timestampsMax = null;
        long[] timestampsMin = null;
        if (checkTimestamps) {
            // If timestamp info is NOT all here ...
            if (!allTimestampsPresent) {
                // Set error bit
                nonFatalError = true;
                // Don't bother checking ts consistency
                checkTimestamps = false;
System.out.println("Roc(s) missing timestamp info!");
            }
            else {
                timestampsAvg = new long[numEvents];
                timestampsMax = new long[numEvents];
                timestampsMin = new long[numEvents];
                Arrays.fill(timestampsMin, Long.MAX_VALUE);
            }
        }

        // check consistency of common data across events
        for (int i=0; i < numInputBanks; i++) {
            if (i > 0) {
                // short stuff
                commonShort = ((EvioSegment)triggerBanks[i].getChildAt(1)).getShortData();
                if (shortCommonData.length != commonShort.length) {
                    throw new EmuException("Trying to merge records with different numbers of events");
                }
                for (int j=0; j < shortCommonData.length; j++) {
                    if (shortCommonData[j] != commonShort[j]) {
                        throw new EmuException("Trying to merge records with different event types");
                    }
                }

                // long stuff
                commonLong = ((EvioSegment)triggerBanks[i].getChildAt(0)).getLongData();

                // check event number
                if (firstEventNumber != commonLong[0]) {
                    throw new EmuException("Trying to merge records with different event numbers");
                }

                // check run number & type if all such data is present
                if (allRunDataPresent) {
                    if (longCommonData.length == 2+numEvents &&
                            commonLong.length == 2+numEvents)  {
                        // if run # and/or type are different ...
                        if (longCommonData[1+numEvents] != commonLong[1+numEvents]) {
                            throw new EmuException("Trying to merge records with different run numbers and/or types");
                        }
                    }
                }
            }
            else {
                commonLong = longCommonData;
            }

            // store timestamp info
            if (checkTimestamps) {
                // timestamp data is not all there even though tag says it is
                if (commonLong.length < numEvents + 1) {
                    nonFatalError = true;
                    checkTimestamps = false;
System.out.println("Timestamp data is missing!");
                }
                else {
                    // for each event find avg, max, & min
                    for (int j=0; j < numEvents; j++) {
                        ts = commonLong[j+1];
                        timestampsAvg[j] += ts;
                        timestampsMax[j]  = ts > timestampsMax[j] ? ts : timestampsMax[j];
                        timestampsMin[j]  = ts < timestampsMin[j] ? ts : timestampsMin[j];
                    }
                }
            }

            // store stuff
            inputPayloadBanks[i].setEventCount(numEvents);
        }


        // Now that we have all timestamp info, check them against each other.
        // Allow a slop of TIMESTAMP_SLOP from the max to min.
        if (checkTimestamps) {
            for (int j=0; j < numEvents; j++) {
                // finish calculation to find average
                timestampsAvg[j] /= numInputBanks;

                if (timestampsMax[j] - timestampsMin[j] > 0)  {
System.out.println("Timestamps differing by " + (timestampsMax[j] - timestampsMin[j]));
                }
                if (timestampsMax[j] - timestampsMin[j] > timestampSlop)  {
                    nonFatalError = true;
System.out.println("Timestamps are NOT consistent !!!");
                }
            }
        }

        // Bank we are trying to build. Need to update the num which = (# rocs + 2)
        EvioEvent combinedTrigger = builder.getEvent();
        combinedTrigger.getHeader().setNumber(totalRocCount + 2);

        // 1) Create common data segment of longs, with first event #,
        //    avg timestamps, and possibly run number & run type in it.
        long[] longData;
        if (!checkTimestamps) {
            if (includeRunData) {
                longData = new long[2];
                longData[0] = firstEventNumber;
                longData[1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_RUN.getValue());
            }
            else {
                longData = new long[1];
                longData[0] = firstEventNumber;
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_BANK.getValue());
            }
        }
        // Put avg timestamps in if doing timestamp checking
        else {
            if (includeRunData) {
                longData = new long[2+numEvents];
                longData[0] = firstEventNumber;
                for (int i=0; i < numEvents; i++) {
                    longData[i+1] = timestampsAvg[i];
                }
                longData[numEvents+1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_TS_RUN.getValue());
            }
            else {
                longData = new long[1+numEvents];
                longData[0] = firstEventNumber;
                for (int i=0; i < numEvents; i++) {
                    longData[i+1] = timestampsAvg[i];
                }
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_TS.getValue());
            }
        }

        // Create common data segment of shorts, actually just
        // grab one from input payload bank and change id.
        ebSeg2.getHeader().setTag(ebId);

        // Add long & short segments to trigger bank
        try {
            EvioSegment ebSeg = new EvioSegment(ebId, DataType.ULONG64);
            ebSeg.appendLongData(longData);
            builder.addChild(combinedTrigger, ebSeg);
            builder.addChild(combinedTrigger, ebSeg2);
        }
        catch (EvioException e) { /* never happen */ }


        // 2) Now put all ROC-specific segments into bank.
        //    If we're in single event mode, there will be NO such segments.

        // for each trigger bank (from previous level EBs) ...
        for (EvioBank trBank : triggerBanks) {
            // copy over each roc segment in trigger bank to combined trigger bank
            for (int j=2; j < trBank.getChildCount(); j++) {
                try { builder.addChild(combinedTrigger, (EvioSegment)trBank.getChildAt(j)); }
                catch (EvioException e) { /* never happen */ }
            }
        }

        return nonFatalError;
    }

    

    /**
     * Combine the trigger banks of all input payload banks of ROC raw format into a single
     * trigger bank which will be used in the final built event. Any error
     * which occurs but allows the build to continue will be noted in the return value.
     * Errors which stop the event building cause an exception to be thrown.<p>
     *
     * To check timestamp consistency, for each event the difference between the max and
     * min timestamps cannot exceed the argument timestampSlop. If it does for any event,
     * this method returns <code>true</code>.<p>
     *
     * @param inputPayloadBanks array containing a bank (ROC Raw) from each channel's
     *                          payload bank queue that will be built into one event
     * @param builder object used to build trigger bank
     * @param ebId id of event builder calling this method
     * @param firstEventNumber event number to place in trigger bank
     * @param runNumber run number to place in trigger bank
     * @param runType   run type to place in trigger bank
     * @param includeRunData if <code>true</code>, add run number and run type
     * @param checkTimestamps if <code>true</code>, check timestamp consistency and
     *                        return false if inconsistent
     * @param timestampSlop maximum number of timestamp ticks that timestamps can differ
     *                      for a single event before the error bit is set in a bank's
     *                      status. Only used when checkTimestamps arg is <code>true</code>
     *
     * @return <code>true</code> if non fatal error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean makeTriggerBankFromRocRaw(PayloadBank[] inputPayloadBanks,
                                                    EventBuilder builder, int ebId,
                                                    long firstEventNumber,
                                                    int runNumber, int runType,
                                                    boolean includeRunData,
                                                    boolean checkTimestamps,
                                                    int timestampSlop)
            throws EmuException {

        if (builder == null || inputPayloadBanks == null || inputPayloadBanks.length < 1) {
            throw new EmuException("arguments are null or zero-length");
        }

        int tag, index;
        int numROCs = inputPayloadBanks.length;
        int numEvents = inputPayloadBanks[0].getHeader().getNumber();
        EvioSegment segment;
        EvioBank[] triggerBanks = new EvioBank[numROCs];
        boolean nonFatalError = false;
        boolean allTimestampsPresent = true;


        // In each payload bank (of banks) is a trigger bank. Extract them all.
        for (int i=0; i < numROCs; i++) {

            // find the trigger bank (should be first one)
            index = 0;
            do {
                try {
                    triggerBanks[i] = (EvioBank)inputPayloadBanks[i].getChildAt(index++);
                }
                catch (Exception e) {
                    throw new EmuException("No trigger bank in ROC raw record", e);
                }
            } while (!Evio.isRawTriggerBank(triggerBanks[i])); // check to see if it really is a trigger bank

            // Check to see if all PAYLOAD banks think they have same # of events
            if (numEvents != inputPayloadBanks[i].getHeader().getNumber()) {
                throw new EmuException("Data blocks contain different numbers of events");
            }

            //  Check to see if all TRIGGER banks think they have the same # of events
            if (numEvents != triggerBanks[i].getHeader().getNumber()) {
                throw new EmuException("Data blocks contain different numbers of events");
            }
            inputPayloadBanks[i].setEventCount(numEvents);

            // Number of trigger bank children must = # events
            if (triggerBanks[i].getChildCount() != numEvents) {
                throw new EmuException("Trigger bank does not have correct number of segments");
            }

            // Check to see if all trigger banks have timestamp data
            if (checkTimestamps) {
                tag = triggerBanks[i].getHeader().getTag();
                allTimestampsPresent &= CODATag.hasTimestamp(tag);
            }
        }

        // bank we are trying to build
        EvioEvent combinedTrigger = builder.getEvent();

        //    No sense duplicating data for each ROC/EB.
        //    Get event(trigger) types, first event number,
        //    and run number, and put into 2 common segments as follows:
        //
        // 1) The first segment in each built trigger bank contains common data
        //    in the format given below. This is a segment of unsigned 64 bit
        //    integers containing the first event number followed by timestamps
        //    IF the config is set for checking timestamps. That is followed by
        //    the run number (32 bits) and the run type (32 bits) If the config
        //    is set for adding runData.
        //
        //    MSB(31)                          LSB(0)    Big Endian,  higher mem  -->
        //    _______________________________________
        //    | first event number (high 32 bits)   |
        //    | first event number (low  32 bits)   |
        //    |       timestamp1   (high 32 bits)   |
        //    |       timestamp1   (low  32 bits)   |
        //    |                      ...            |
        //    |       timestampM   (high 32 bits)   |
        //    |       timestampM   (low  32 bits)   |
        //    |       run   number (high 32 bits)   |
        //    |       run   type   (low  32 bits)   |
        //    _______________________________________
        //
        //
        // 2) The second segment in each built trigger bank contains common data
        //    in the format given below. This is a segment of unsigned 16 bit
        //    integers containing the event type of each event.
        //
        //    MSB(31)                    LSB(0)    Big Endian,  higher mem  -->
        //    __________________________________
        //    |  event1 type  |  event2 type   |
        //    |        .      |        .       |
        //    |        .      |        .       |
        //    | eventM-1 type |  eventM type   |
        //    __________________________________
        //

        // Find the types of events from first ROC
        short[] evData = new short[numEvents];
        for (int i=0; i < numEvents; i++) {
            segment   = (EvioSegment) (triggerBanks[0].getChildAt(i));
            evData[i] = (short) (segment.getHeader().getTag());  // event type
        }

        // Check the consistency of timestamps if desired/possible
        long ts;
        long[] timestampsAvg = null;
        long[] timestampsMax = null;
        long[] timestampsMin = null;
        if (checkTimestamps) {
            // If timestamp info is NOT all here ...
            if (!allTimestampsPresent) {
                // Set error bit
                nonFatalError = true;
                // Don't bother checking ts consistency
                checkTimestamps = false;
System.out.println("Roc(s) missing timestamp info!");
            }
            else {
                timestampsAvg = new long[numEvents];
                timestampsMax = new long[numEvents];
                timestampsMin = new long[numEvents];
                Arrays.fill(timestampsMin, Long.MAX_VALUE);
            }
        }

        // It is convenient at this point to check and see if for a given event,
        // across all ROCs, the event number & event type are the same.
        int[] triggerData;
        int firstEvNum = (int) firstEventNumber;
        for (int i=0; i < numEvents; i++) {
            for (int j=0; j < numROCs; j++) {
                segment = (EvioSegment) (triggerBanks[j].getChildAt(i));
                // check event type consistency
                if (evData[i] != (short) (segment.getHeader().getTag())) {
System.out.println("makeTriggerBankFromRocRaw: event type differs across ROCs");
                    nonFatalError = true;
                }

                // check event number consistency
                triggerData = segment.getIntData();
                if (firstEvNum + i != triggerData[0]) {
System.out.println("makeTriggerBankFromRocRaw: event number differs across ROCs");
System.out.println("                           " + (firstEvNum+i) + " != " + (triggerData[0]));
                    nonFatalError = true;
                }

                // If they exist, store all timestamp related
                // values so consistency can be checked below
                if (checkTimestamps && triggerData.length > 2) {
                    ts = (    (0xffffL & (long)triggerData[1] << 32) |
                          (0xffffffffL & (long)triggerData[2]));
                    timestampsAvg[i] += ts;
                    timestampsMax[i]  = ts > timestampsMax[i] ? ts : timestampsMax[i];
                    timestampsMin[i]  = ts < timestampsMin[i] ? ts : timestampsMin[i];
                }
            }
            if (checkTimestamps) timestampsAvg[i] /= numROCs;
        }

        // Now that we have all timestamp info, check them against each other.
        // Allow a slop of timestampSlop from the max to min.
        if (checkTimestamps) {
            for (int i=0; i < numEvents; i++) {
                if (timestampsMax[i] - timestampsMin[i] > 0)  {
System.out.println("Timestamps differing by " + (timestampsMax[i] - timestampsMin[i]));
                }
                if (timestampsMax[i] - timestampsMin[i] > timestampSlop)  {
                    nonFatalError = true;
System.out.println("Timestamps are NOT consistent!!!");
                }
            }
        }

        // 1) Add segment of long data
        long[] longData;
        if (!checkTimestamps) {
            if (includeRunData) {
                longData = new long[2];
                longData[0] = firstEventNumber;
                longData[1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_RUN.getValue());
            }
            else {
                longData = new long[1];
                longData[0] = firstEventNumber;
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_BANK.getValue());
            }
        }
        // Put avg timestamps in if doing timestamp checking
        else {
            if (includeRunData) {
                longData = new long[2+numEvents];
                longData[0] = firstEventNumber;
                for (int i=0; i < numEvents; i++) {
                    longData[i+1] = timestampsAvg[i];
                }
                longData[numEvents+1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_TS_RUN.getValue());
            }
            else {
                longData = new long[1+numEvents];
                longData[0] = firstEventNumber;
                for (int i=0; i < numEvents; i++) {
                    longData[i+1] = timestampsAvg[i];
                }
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_TS.getValue());
            }
        }

        EvioSegment ebSeg = new EvioSegment(ebId, DataType.ULONG64);
        try {
            ebSeg.appendLongData(longData);
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }

        // 2) Add segment of event types
        ebSeg = new EvioSegment(ebId, DataType.USHORT16);
        try {
            ebSeg.appendShortData(evData);
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }

        // now add one segment for each ROC with ROC-specific data in it
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

        return nonFatalError;
    }



    /**
     * Create a trigger bank from all input payload banks of ROC raw format (which are in
     * single event mode and therefore have no trigger bank) which will be used in the final
     * built event. Any error which occurs but allows the build to continue will be noted in
     * the return value. Errors which stop the event building cause an exception to be thrown.<p>
     *
     * To check timestamp consistency, the difference between the max and
     * min timestamps cannot exceed timestampSlop. If it does,
     * this method returns <code>true</code>.<p>
     *
     * @param inputPayloadBanks array containing a bank (ROC Raw) from each channel's
     *                          payload bank queue that will be built into one event
     * @param builder object used to build trigger bank
     * @param ebId id of event builder calling this method
     * @param firstEventNumber event number to place in trigger bank
     * @param runNumber run number to place in trigger bank
     * @param runType   run type to place in trigger bank
     * @param includeRunData if <code>true</code>, add run number and run type
     * @param checkTimestamps if <code>true</code>, check timestamp consistency and
     *                        return false if inconsistent
     * @param timestampSlop maximum number of timestamp ticks that timestamps can differ
     *                      for a single event before the error bit is set in a bank's
     *                      status. Only used when checkTimestamps arg is <code>true</code>
     *
     *
     * @return <code>true</code> if non fatal error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean makeTriggerBankFromSemRocRaw(PayloadBank[] inputPayloadBanks,
                                                       EventBuilder builder, int ebId,
                                                       long firstEventNumber,
                                                       int runNumber, int runType,
                                                       boolean includeRunData,
                                                       boolean checkTimestamps,
                                                       int timestampSlop)
            throws EmuException {

        int numROCs = inputPayloadBanks.length;
        boolean nonFatalError = false;

        for (int i=0; i < numROCs; i++) {
            // check to see if all payload banks think they have 1 event
            if (inputPayloadBanks[i].getHeader().getNumber() != 1) {
                throw new EmuException("Data blocks contain different numbers of events");
            }

            inputPayloadBanks[i].setEventCount(1);
        }

        // event we are trying to build
        EvioEvent combinedTrigger = builder.getEvent();

        //    No sense duplicating data for each ROC/EB.
        //    Get event(trigger) type, event number,
        //    and run number, and put into 2 common segments as follows:
        //
        // 1) The first segment in each built trigger bank contains common data
        //    in the format given below. This is a segment of unsigned 64 bit
        //    integers containing the event number followed by the timestamp
        //    IF the config is set for checking timestamps. That is followed by
        //    the run number (32 bits) and the run type (32 bits) If the config
        //    is set for adding runData.
        //
        //    MSB(31)                          LSB(0)    Big Endian,  higher mem  -->
        //    _______________________________________
        //    |       event number (high 32 bits)   |
        //    |       event number (low  32 bits)   |
        //    |       timestamp    (high 32 bits)   |
        //    |       timestamp    (low  32 bits)   |
        //    |       run   number (high 32 bits)   |
        //    |       run   type   (low  32 bits)   |
        //    _______________________________________
        //
        //
        // 2) The second segment in each built trigger bank contains common data
        //    in the format given below. This is a segment of unsigned 16 bit
        //    integers containing the event type of each event.
        //
        //    MSB(31)                    LSB(0)    Big Endian,  higher mem  -->
        //    __________________________________
        //    | event1 type  |    (nothing)    |
        //    _________________________________
        //


        // Extract needed event type from Data Block banks (pick first in list)
        // for checking type consistency.
        EvioBank blockBank = (EvioBank)inputPayloadBanks[0].getChildAt(0);
        short[] evData = new short[1];
        evData[0] = (short) (blockBank.getHeader().getNumber());  // event type

        // This method is a convenient time to check the consistency of timestamps
        long ts, timestampsAvg = 0L, timestampsMax = 0L, timestampsMin = Long.MAX_VALUE;

        // It is convenient at this point to check and see if across
        // all ROCs, the event number, event type, & timestamp are the same.
        // We are not checking info from additional Data Block banks
        // if more than one from a ROC.
        int[] data;
        for (int j=0; j < numROCs; j++) {
            blockBank = (EvioBank) (inputPayloadBanks[j].getChildAt(0));

            // check event type consistency
            if (evData[0] != (short) (blockBank.getHeader().getNumber())) {
                nonFatalError = true;
            }

            // check event number consistency
            data = blockBank.getIntData();
            if ((int)firstEventNumber != data[0]) {
                nonFatalError = true;
            }

            // store timestamp related values so consistency can be checked later
            if (checkTimestamps && data.length > 2) {
                ts = (    (0xffffL & (long)data[1] << 32) |
                      (0xffffffffL & (long)data[2]));
                timestampsAvg += ts;
                timestampsMax  = ts > timestampsMax ? ts : timestampsMax;
                timestampsMin  = ts < timestampsMin ? ts : timestampsMin;
            }
        }

        if (checkTimestamps) {
            timestampsAvg /= numROCs;
            if ((timestampsMax - timestampsMin > timestampSlop)) {
                nonFatalError = true;
System.out.println("Timestamps are NOT consistent !!!");
            }
        }

        // 1)
        long[] longData;
        if (!checkTimestamps) {
            if (includeRunData) {
                longData = new long[2];
                longData[0] = firstEventNumber;
                longData[1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_RUN.getValue());
            }
            else {
                longData = new long[1];
                longData[0] = firstEventNumber;
                // tag already set to correct value
            }
        }
        // put avg timestamps in if doing timestamp checking
        else {
            if (includeRunData) {
                longData = new long[3];
                longData[0] = firstEventNumber;
                longData[1] = timestampsAvg;
                longData[2] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_TS_RUN.getValue());
            }
            else {
                longData = new long[2];
                longData[0] = firstEventNumber;
                longData[1] = timestampsAvg;
                combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_TS.getValue());
            }
        }

        EvioSegment ebSeg = new EvioSegment(ebId, DataType.ULONG64);
        try {
            ebSeg.appendLongData(longData);
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }

        // 2)
        // Store event type in segment of shorts.
        ebSeg = new EvioSegment(ebId, DataType.USHORT16);
        try {
            ebSeg.appendShortData(evData);
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }

        // no segments to add for each ROC since we have no ROC-specific data

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

        return null;
    }


    // TODO: make data swapping optional!!!!!!!


    /**
     * Build a single physics event with the given trigger bank
     * and the given array of ROC raw records. Swap data if necessary
     * assuming it's all 32 bit integers.
     *
     * @param triggerBank bank containing merged trigger info
     * @param inputPayloadBanks array containing ROC raw records that will be built together
     * @param builder object used to build trigger bank
     * @param swap swap the data to big endian if necessary by assuming all 32 bit ints
     * @return bank final built event
     */
    public static EvioBank buildPhysicsEventWithRocRaw(EvioBank triggerBank,
                                                       EvioBank[] inputPayloadBanks,
                                                       EventBuilder builder,
                                                       boolean swap) {

        int tag, childrenCount;
        int numPayloadBanks = inputPayloadBanks.length;
        boolean isBigE;
        BankHeader header;
        EvioBank dataBank, blockBank;
        EvioEvent finalEvent = builder.getEvent();

        try {
            // Add combined trigger bank to physics event
            builder.addChild(finalEvent, triggerBank);

            // Wrap and add data block banks (from payload banks).
            // Use the same header to wrap data blocks as used for payload bank.
            for (int i=0; i < numPayloadBanks; i++) {
                // Get Roc Raw header
                header = (BankHeader)inputPayloadBanks[i].getHeader();

                // What is the endianness of the Roc data?
                // Find it by looking at status bit in tag.
                tag = header.getTag();
                isBigE = Evio.isTagBigEndian(tag);
                // Reset it to big endian if swapping
                if (swap) {
                    tag = Evio.setTagEndian(tag, true);
                }

                // Create physics data bank with same tag & num as Roc Raw bank
                dataBank = new EvioBank(tag, DataType.BANK, header.getNumber());

                // How many banks inside Roc Raw bank ?
                childrenCount = inputPayloadBanks[i].getChildCount();

                // Add Roc Raw's data block banks to our data bank
                for (int j=0; j < childrenCount; j++) {
                    blockBank = (EvioBank)inputPayloadBanks[i].getChildAt(j);

                    // Ignore the Roc Raw's trigger bank (should be first one)
                    if (Evio.isRawTriggerBank(blockBank)) {
                        continue;
                    }

                    // Here's where things can get tricky. There is a status bit
                    // telling us the data endianness which was set on the ROC
                    // (see above). However, when reading evio events over the
                    // network, the emu knows the endianness of the host it came from.
                    // It is possible these are not the same if some crazy user
                    // stored big endian data on a little endian host or vice versa.

                    // If data is big endian or we don't want to swap, just add bank
                    if (isBigE || !swap) {
                        if (blockBank.getByteOrder() != ByteOrder.BIG_ENDIAN) {
                            blockBank.setByteOrder(ByteOrder.BIG_ENDIAN);
                        }
                        builder.addChild(dataBank, blockBank);
                    }
                    // Otherwise, read, swap and rewrite data.
                    else {
                        if (blockBank.getByteOrder() != ByteOrder.LITTLE_ENDIAN) {
                            blockBank.setByteOrder(ByteOrder.LITTLE_ENDIAN);
                        }
                        // Get the data (swapped in this method call)
                        int[] dat = blockBank.getIntData();
                        // Create a new bank to put it in
                        header = (BankHeader)blockBank.getHeader();
                        tag = header.getTag();
                        // make it say we have big endian data
                        tag = Evio.setTagEndian(tag, true);
                        blockBank = new EvioBank(tag, DataType.UINT32, header.getNumber());
                        blockBank.appendIntData(dat);
                        // Add new block bank with swapped data to data bank
                        builder.addChild(dataBank, blockBank);
                        // Now we must change the status bit saying we have big endian data.
                        // Do this for physics event (as we just did for the data & block banks).
                        BankHeader bh = (BankHeader)builder.getEvent().getHeader();
                        tag = bh.getTag();
                        tag = Evio.setTagEndian(tag, true);
                        bh.setTag(tag);
                    }
                }

                // Add our data bank to physics event
                builder.addChild(finalEvent, dataBank);
            }
        } catch (EvioException e) {
            // never happen
        }

        return null;
    }





    /**
     * Create a Control event for testing purposes.
     * An end event (type = 20) is shown below.
     * <code><pre>
     * _______________________________________
     * |        Event Length = 4             |
     * |_____________________________________|
     * |    type = 20     |  0x1   |  0xCC   |
     * |_____________________________________|
     * |                Time                 |
     * |_____________________________________|
     * |              (reserved)             |
     * |_____________________________________|
     * |      number of events in run        |
     * |_____________________________________|
     * </pre></code>
     *
     * @param type event type, must be PRESTART, GO, PAUSE, or END
     * @param runNumber run number used if generating prestart event
     *
     * @return created Control event (EvioEvent object)
     * @throws EvioException if bad event type
     */
    public static EvioEvent createControlEvent(EventType type, int runNumber)
            throws EvioException {

        EventBuilder eventBuilder;

        // put some data into event
        int[] data = new int[3];
        data[0] = (int) (System.currentTimeMillis()); // time in seconds from Jan 1, 1970 GMT
        data[1] = runNumber; // run number for prestart, else reserved
        data[2] = 0;         // # events in run

        // create a single bank of integers which is the user bank
        switch (type) {
            case PRESTART:
                eventBuilder = new EventBuilder(17, DataType.UINT32, 0xcc); break;
            case GO:
                eventBuilder = new EventBuilder(18, DataType.UINT32, 0xcc); break;
            case PAUSE:
                eventBuilder = new EventBuilder(19, DataType.UINT32, 0xcc); break;
            case END:
                eventBuilder = new EventBuilder(20, DataType.UINT32, 0xcc); break;
            default:
                throw new EvioException("bad EventType arg");
        }
        EvioEvent ev = eventBuilder.getEvent();
        eventBuilder.appendIntData(ev, data);

        return ev;
    }


    /**
     * Generate a single data block bank of entangled FADC250 fake data.
     * Try for about 150 bytes of data per event (not counting headers & trailers).
     *
     * @param firstEvNum    starting event number
     * @param numEvents     number of physics events in created record
     * @param recordId      number of ROC raw event from ROC
     * @param numberModules number of modules read by ROC
     * @param isSEM         in single event mode if <code>true</code>
     * @param timestamp     48-bit timestamp used only in single event mode
     */
    private static int[] generateEntangledDataFADC250(int firstEvNum, int numEvents,
                                                      int recordId, int numberModules,
                                                      boolean isSEM, long timestamp) {

        int index=0, moduleType=0, startIndex, eventNumber;
        if (numberModules > 16) numberModules = 16;
        int wordsPerModule = (int) (38./numberModules + .5); // round up


        int[] data;
        if (isSEM) {
            numEvents = 1;
            data = new int[3 + (2 + (1 + wordsPerModule)*numEvents)*numberModules];
        }
        else {
            data = new int[1 + (2 + (1 + wordsPerModule)*numEvents)*numberModules];
        }

        int blockHdr = (1 << 31) | ((numEvents & 0xFF) << 14) |
                       ((moduleType & 0x3) << 12) | (recordId & 0xFFF);
        int blockTlr = (1 << 31) | (1 << 27);
        int eventHdr = (1 << 31) | (2 << 27) | ((numEvents & 0xFF) << 22) |
                       ((moduleType & 0x3) << 20);
        int pulseInt = (1 << 31) | (7 << 27) | 0x7FFFF;

        // First put in starting event # (32 bits)
        data[index++] = firstEvNum;

        // if single event mode, put in timestamp
        if (isSEM) {
            data[index++] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
            data[index++] = (int)  timestamp; // low 32 bits
        }

        // Put in data module-by-module
        for (int j=0; j < numberModules; j++) {

            startIndex  = index;
            eventNumber = firstEvNum;

            // block header
            data[index++] = blockHdr | ((j & 0x1F) << 22);

            for (int k=0; k < numEvents; k++) {
                // event header
                data[index++] = eventHdr | ((j & 0x1F) << 22) | (eventNumber++ & 0xFFFFF);
                // raw data (pulse integrals)
                for (int l=0; l < wordsPerModule; l++) {
                    data[index++] = pulseInt;
                }
            }

            // block trailer
            data[index] = blockTlr | ((j & 0x1F) << 22) | (index - startIndex + 1);
            index++;
        }

        return data;
    }


    /**
     * Create an Evio ROC Raw record event/bank in single event mode to be placed in a Data Transport record.
     *
     * @param rocID       ROC id number
     * @param detectorId  id of detector producing data in data block bank
     * @param status      4-bit status associated with data
     * @param eventNumber event number
     * @param recordId    number of ROC raw event from ROC
     * @param timestamp   event's timestamp
     *
     * @return created ROC Raw Record (EvioEvent object)
     * @throws EvioException
     */
    public static EvioEvent createSingleEventModeRocRecord(int rocID,  int detectorId,
                                                           int status, int eventNumber,
                                                           int recordId, long timestamp)
            throws EvioException {
        // single event mode means 1 event
        int numEvents = 1;

        // create a ROC Raw Data Record event/bank with numEvents physics events in it
        int rocTag = createCodaTag(status, rocID);
        EventBuilder eventBuilder = new EventBuilder(rocTag, DataType.BANK, numEvents);
        EvioEvent rocRawEvent = eventBuilder.getEvent();

//        // Put some data into event -- 4 ints per event.
//        // First is event #, then timestamp, then data
//        int[] data = new int[4];
//        data[0] = eventNumber;
//        data[1] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
//        data[2] = (int) timestamp; // low 32 bits
//        data[3] = 10000;

        // Put some data into event (10 modules worth)
        int []data = generateEntangledDataFADC250(eventNumber, numEvents, recordId,
                                                  10, true, timestamp);

        // create a single data block bank (of ints)
        int eventType = 33;
        int dataTag = createCodaTag(status, detectorId);
        EvioBank dataBank = new EvioBank(dataTag, DataType.INT32, eventType);
        eventBuilder.appendIntData(dataBank, data);
        eventBuilder.addChild(rocRawEvent, dataBank);

        return rocRawEvent;
    }


//    static boolean onlyOnce = true;
    /**
     * Create an Evio ROC Raw record event/bank to be placed in a Data Transport record.
     *
     * @param rocID       ROC id number
     * @param triggerType trigger type id number (0-15)
     * @param detectorId  id of detector producing data in data block bank
     * @param status      4-bit status associated with data
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param recordId    number of ROC raw event from ROC
     * @param timestamp   starting event's timestamp
     *
     * @return created ROC Raw Record (EvioEvent object)
     * @throws EvioException
     */
    public static EvioEvent createRocRawRecord(int rocID,       int triggerType,
                                               int detectorId,  int status,
                                               int eventNumber, int numEvents,
                                               int recordId,    long timestamp)
            throws EvioException {

        // Create a ROC Raw Data Record event/bank with numEvents physics events in it
        int firstEvNum = eventNumber;
        int rocTag = createCodaTag(status, rocID);
        EventBuilder eventBuilder = new EventBuilder(rocTag, DataType.ALSOBANK, numEvents);
        EvioEvent rocRawEvent = eventBuilder.getEvent();

        // Create the trigger bank (of segments)
        EvioBank triggerBank = new EvioBank(CODATag.RAW_TRIGGER_TS.getValue(),
                                            DataType.ALSOSEGMENT, numEvents);
        eventBuilder.addChild(rocRawEvent, triggerBank);

        EvioSegment segment;
        for (int i = 0; i < numEvents; i++) {
            // Each segment contains eventNumber & timestamp of corresponding event in data bank
            segment = new EvioSegment(triggerType, DataType.UINT32);
            eventBuilder.addChild(triggerBank, segment);
            // Generate 3 segments per event (no miscellaneous data)
            int[] segmentData = new int[3];
            segmentData[0] = eventNumber++;
            segmentData[1] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
            segmentData[2] = (int)  timestamp; // low 32 bits
//            if (rocID == 1 && onlyOnce) {
//                // bad timestamp for Roc1
//                timestamp += 9;
//                onlyOnce = false;
//            }
//            else {
                timestamp += 4;
//            }

            eventBuilder.appendIntData(segment, segmentData); // copies reference only
        }

        // Create a single data block bank for 10 modules
        int []data = generateEntangledDataFADC250(eventNumber, numEvents, recordId,
                                                  10, false, 0L);

//        // put some data into event -- one int per event
//        int[] data = new int[numEvents+1];
//        data[0] = firstEvNum;
//        for (int i = 1; i < numEvents; i++) {
//            data[i] = 10000 + i;
//        }

        int dataTag = createCodaTag(status, detectorId);
        EvioBank dataBank = new EvioBank(dataTag, DataType.UINT32, numEvents);
        eventBuilder.appendIntData(dataBank, data);

        eventBuilder.addChild(rocRawEvent, dataBank);

        return rocRawEvent;
    }


    /**
     * Create an array of Evio events with simulated ROC data
     * to send to the event building EMU. <b>No</b>
     * Data Transport Record wrapping is included.
     *
     * @param rocId       ROC id number
     * @param triggerType trigger type id number (0-15)
     * @param detectorId  id of detector producing data in data block bank
     * @param status      4-bit status associated with data
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created payload bank
     * @param timestamp   starting event's timestamp
     * @param recordId    record count
     * @param numPayloadBanks number of payload banks to generate
     * @param singleEventMode true if creating events in single event mode
     *
     * @return number of events in the generated Data Transport Record event
     * @throws EvioException
     */
    public static PayloadBank[] createRocDataEvents(int rocId, int triggerType,
                                                    int detectorId, int status,
                                                    int eventNumber, int numEvents,
                                                    long timestamp, int recordId,
                                                    int numPayloadBanks,
                                                    boolean singleEventMode)
            throws EvioException {

        if (singleEventMode) {
            numEvents = 1;
        }

        EvioEvent ev;
        PayloadBank[] pBanks =  new PayloadBank[numPayloadBanks];

        // now add the rest of the records
        for (int i=0; i < numPayloadBanks; i++)  {
            // add ROC Raw Records as payload banks
            if (singleEventMode) {
                ev = createSingleEventModeRocRecord(rocId, detectorId, status,
                                                       eventNumber, recordId, timestamp);
            }
            else {
                ev = createRocRawRecord(rocId, triggerType, detectorId, status,
                                           eventNumber, numEvents, recordId, timestamp);
            }
            pBanks[i] = new PayloadBank(ev);
            pBanks[i].setType(EventType.ROC_RAW);

            eventNumber += numEvents;
            timestamp   += 4*numEvents;
        }

        return pBanks;
    }



}
