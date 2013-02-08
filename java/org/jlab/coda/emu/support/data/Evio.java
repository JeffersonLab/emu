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
 * The EMU will received evio data in standard CODA3 output (same as file format)
 * which contains banks - in this case, ROC Raw Records and Physics Events all of
 * which are in formats given below.<p>
 *
 * <code><pre>
 * ####################################
 * Network Transfer Evio Output Format:
 * ####################################
 *
 * MSB(31)                          LSB(0)
 * <---  32 bits ------------------------>
 * _______________________________________
 * |           Block Length              |
 * |_____________________________________|
 * |           Block Number              |
 * |_____________________________________|
 * |         Header Length = 8           |
 * |_____________________________________|
 * |            Event Count              |
 * |_____________________________________|
 * |             Reserved 1              |
 * |_____________________________________|
 * |        Bit Info          | Version  |
 * |_____________________________________|
 * |             Reserved 2              |
 * |_____________________________________|
 * |            Magic Number             |
 * |_____________________________________|
 * |                                     |
 * |           Payload Bank              |
 * |       (ROC Raw, Physics,            |
 * |        Control or User event)       |
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
 * |    0xFF1X        |  0x20  |    M    |      |
 * |_____________________________________|      |
 * | ID 1   |  0x01   |     ID len 1     |   Trigger Bank
 * |_____________________________________|      |
 * |           Event Number 1            |      |
 * |_____________________________________|      |
 * |       Timestamp1 (bits 31-0)        |      |
 * |_____________________________________|      |
 * |       Timestamp1 (bits 47-32)       |      |
 * |_____________________________________|      |
 * |             Misc. 1 (?)             |      |
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
 * |________   Timestamp M (?)   ________|      |
 * |_____________________________________|      |
 * |             Misc. M (?)             |      V
 * |_____________________________________| ------
 * |                                     |
 * |         Data Block Bank 1           |
 * |        (data of M events from       |
 * |         1 to multiple modules)      |
 * |                                     |
 * |_____________________________________|
 * |                  ...                |
 * |_____________________________________|
 * |                                     |
 * |         Data Block Last             |
 * |   (there will be only 1 block       |
 * |   unless user used multiple DMAs)   |
 * |                                     |
 * |_____________________________________|
 *
 *
 * M = number of events (0 = user event).
 * 0x0F01 is the Trigger Bank identifier.
 *
 * S is the 4-bit status:
 * |_____________________________________|
 * | Single|   Big    |  Error |  Sync   |
 * | Event |  Endian  |        |  Event  |
 * |  Mode |          |        |         |
 * |_____________________________________|
 *
 *  Endian bit is set if data is big endian
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
 * | builder = 0xFFXX |  0x10  |    M    |
 * |_____________________________________| ------
 * |     Built Trigger Bank Length       |      ^
 * |_____________________________________|      |
 * |    0xFF2X        |  0x20  |    N    |      |
 * |_____________________________________|     Built
 * |     2  EB (Common) Segments         |   Trigger Bank
 * |_____________________________________|   (see below)
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
 * |      (One bank for each roc)        |
 * |_____________________________________|
 *
 *
 *      M = number of events.
 *      N is the number of ROCs.
 * 0xFFXX is the id of the event builder
 * 0xFF2X is the Built Trigger Bank identifier
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
 * |     Built Trigger Bank Length       |
 * |_____________________________________|
 * |    0xFF2X        |  0x20  |    N    |
 * |_____________________________________| --------
 * | EB id  |   0xa   |        4         |    ^
 * |_____________________________________|    |
 * |________ First Event Number _________|    |
 * |_____________________________________|    |
 * |__________ Avg Timestamp 1 __________|    |
 * |_____________________________________|    |
 * |__________       ...       __________|    |
 * |_____________________________________|    |
 * |__________ Avg Timestamp M __________|    |
 * |_____________________________________|    |
 * |______ Run Number & Run Type ________|    |
 * |_____________________________________|    |
 * | EB id  |  0x05   |       Len        |    |
 * |_____________________________________|    |
 * |   Event Type 2   |  Event Type 1    |  Common Data
 * |_____________________________________|    |
 * |                  ...                |    |
 * |_____________________________________|    |
 * |  Event Type M    |  Event Type M-1  |    V
 * |_____________________________________| -------
 * |roc1 id |  0x01   |        Len       |    ^
 * |_____________________________________|    |
 * |         Timestamp for ev 1          |    |
 * |_____________________________________|    |
 * |           Misc. 1 for ev 1          |  ROC Data
 * |_____________________________________|  (missing if single event mode,
 * |                ...                  |   or sparsified & no timestamp
 * |_____________________________________|   data available)
 * |             Timestamp M             |    |
 * |_____________________________________|    |
 * |               Misc. M               |    |
 * |_____________________________________|    |
 * |                                     |    |
 * |     (one for each ROC, to ROC N)    |    |
 * |_____________________________________|    V
 *
 *
 *      N is the number of ROCs.
 *      M is the number of events.
 * 0xFF2X is the Built Trigger Bank identifier.
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
     * Get the given CODA-format tag's status which is the upper 4 bits.
     *
     * @param codaTag tag from evio bank.
     * @return the status from various records/events/banks.
     */
    public static int getTagStatus(int codaTag) {
        return (codaTag >>> 12);
    }

    // TODO: this method may need to disappear
    /**
     * Get the given bank's CODA-format tag's status which is the upper 4 bits.
     * 
     * @param bank bank to analyze
     * @return the status from given bank or -1 if bank arg is null.
     */
    public static int getTagStatus(EvioBank bank) {
        if (bank == null) return -1;
        return getTagStatus(bank.getHeader().getTag());
    }


    /**
     * Determine whether a bank is a physics event or not.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is a physics event, else <code>false</code>
     */
    public static boolean isAnyPhysicsEvent(EvioBank bank) {
        if (bank == null)  return false;

        // Tag of fully built event is in range of (0xFF50 - 0xFF8F) inclusive
        int tag = bank.getHeader().getTag();
        if (tag >= 0xFF50 && tag <= 0xFF8F) return true;

        // Partially built event is best identified by its trigger bank

        // Must be bank of banks
        BaseStructureHeader header = bank.getHeader();
        if (header.getDataType() != DataType.BANK ||
            header.getDataType() != DataType.ALSOBANK) {
            return false;
        }

        // Get first bank, which should be built trigger bank, and look at the tag
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
        return (header.getTag() == ControlType.END.getValue() &&
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
         return (header.getTag() == ControlType.GO.getValue() &&
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
         return (header.getTag() == ControlType.PRESTART.getValue() &&
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
         return (header.getTag() == ControlType.PAUSE.getValue() &&
                 header.getNumber() == 0xCC &&
                 header.getDataTypeValue() == 1 &&
                 header.getLength() == 4);
     }


    /**
      * Determine whether a bank is a SYNC control event or not.
      *
      * @param bank input bank
      * @return <code>true</code> if arg is SYNC event, else <code>false</code>
      */
     public static boolean isSyncEvent(EvioBank bank) {
         if (bank == null)  return false;

         BaseStructureHeader header = bank.getHeader();

         // Look inside to see if it is an END event.
         return (header.getTag() == ControlType.SYNC.getValue() &&
                 header.getNumber() == 0xCC &&
                 header.getDataTypeValue() == 1 &&
                 header.getLength() == 4);
     }


    /**
      * Determine whether a bank is a control event or not.
      *
      * @param bank input bank
      * @return <code>true</code> if arg is control event, else <code>false</code>
      */
     public static boolean isControlEvent(EvioBank bank) {
         if (bank == null)  return false;

         BaseStructureHeader header = bank.getHeader();
         int tag = header.getTag();

         // Look inside to see if it is an END event.
         return (ControlType.isControl(tag) &&
                 header.getNumber() == 0xCC &&
                 header.getDataTypeValue() == 1 &&
                 header.getLength() == 4);
     }


    /**
      * If the given bank is a control event, return its ControlType object, else null.
      *
      * @param bank input bank
      * @return corresponding ControlType object if arg is control event, else null
      */
     public static ControlType getControlType(EvioBank bank) {
         if (bank == null)  return null;
         return ControlType.getControlType(bank.getHeader().getTag());
     }


    /**
     * Determine whether a bank is a USER event or not.
     * Only called on banks that were originally from the
     * ROC and labeled as ROC Raw type.
     *
     * @param bank input bank
     * @return <code>true</code> if arg is USER event, else <code>false</code>
     */
     public static boolean isUserEvent(EvioBank bank) {
         if (bank == null)  return false;

         // Look inside to see if it is a USER event.
         return (bank.getHeader().getNumber() == 0);
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
        EventType eventType = pBank.getEventType();
        if (eventType == null || !eventType.isEbFriendly()) {
System.out.println("checkPayloadBank: unknown type, dump payload bank");
            return;
        }
//System.out.println("checkPayloadBank: got bank of type " + eventType);

        // Only worry about record id if event to be built.
        // Initial recordId stored is 0, ignore that.
        if (eventType.isAnyPhysics() || eventType.isROCRaw()) {
            // The recordId associated with each bank is taken from the first
            // evio block header in a single ET data buffer. For a physics or
            // ROC raw type, it should start at zero and increase by one in the
            // first evio block header of the next ET data buffer.
            // NOTE: There may be multiple banks from the same ET buffer and
            // they will all have the same recordId.
            if (recordId != payloadQueue.getRecordId() &&
                recordId != payloadQueue.getRecordId() + 1) {
System.out.println("checkPayloadBank: record ID out of sequence, got " + recordId +
                   " but expecting " + payloadQueue.getRecordId() + " or " +
                  (payloadQueue.getRecordId()+1) + ", type = " + eventType);
                nonFatalRecordIdError = true;
            }
            // Store the current value here as a convenience for the next comparison
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
     * Check each payload bank - one from each input channel - for a number of issues:<p>
     * <ol>
     * <li>if there are any sync bits set, all must be sync banks
     * <li>the ROC ids of the banks must be unique
     * <li>if any banks are in single-event-mode, all need to be in that mode
     * <li>at this point all banks are either physics events or ROC raw records,
     *     but must be identical types
     * <li>there are the same number of events in each bank
     * </ol>
     *
     * @param buildingBanks array containing banks that will be built together
     * @return <code>true</code> if non-fatal error occurred, else <code>false</code>
     * @throws EmuException if some events are in single event mode and others are not;
     *                      if some physics and others ROC raw event types;
     *                      if there are a differing number of events in each payload bank
     */
    public static boolean checkConsistency(PayloadBank[] buildingBanks) throws EmuException {
        boolean nonFatalError = false;

        // For each ROC raw data record check the sync bit
        int syncBankCount = 0;

        // For each ROC raw data record check the single-event-mode bit
        int singleEventModeBankCount = 0;

        // By the time this method is run, all input banks are either (partial)physics or ROC raw.
        // Just make sure they're all identical.
        int physicsEventCount = 0;

        // Number of events contained payload bank
        int numEvents = buildingBanks[0].getHeader().getNumber();

        for (int i=0; i < buildingBanks.length; i++) {
            if (buildingBanks[i].isSync()) {
                syncBankCount++;
            }

            if (buildingBanks[i].isSingleEventMode()) {
                singleEventModeBankCount++;
            }

            if (buildingBanks[i].getEventType().isAnyPhysics()) {
                physicsEventCount++;
            }

            for (int j=i+1; j < buildingBanks.length; j++) {
                if ( buildingBanks[i].getSourceId() == buildingBanks[j].getSourceId()  ) {
                    // ROCs have duplicate IDs
                    nonFatalError = true;
                }
            }

            // Check that there are the same # of events are contained in each payload bank
            if (numEvents != buildingBanks[i].getHeader().getNumber()) {
                throw new EmuException("differing # of events sent by each ROC");
            }
        }

        // If one is a sync, all must be syncs
        if (syncBankCount > 0 && syncBankCount != buildingBanks.length) {
            // Some banks are sync banks and some are not
            nonFatalError = true;
        }

        // If one is a single-event-mode, all must be
        if (singleEventModeBankCount > 0 && singleEventModeBankCount != buildingBanks.length) {
            // Some banks are single-event-mode and some are not, so we cannot build at this point
            throw new EmuException("not all events are in single event mode");
        }

        // All must be physics or all must be ROC raw
        if (physicsEventCount > 0 && physicsEventCount != buildingBanks.length) {
            // Some banks are physics and some ROC raw
            throw new EmuException("not all events are physics or not all are ROC raw");
        }

        return nonFatalError;
    }


    /**
     * When this is called all channels had control events.
     * Check to see if all are identical.
     *
     * @param buildingBanks array containing events that will be built together
     * @param runNumber check this (correct) run # against the one in the control event
     * @param runType   check this (correct) run type against the one in the control event
     * @throws EmuException if events contain mixture of different control types;
     *                      if prestart events contain bad data
     */
    public static void gotConsistentControlEvents(PayloadBank[] buildingBanks,
                                                  int runNumber, int runType)
            throws EmuException {

        boolean debug = false;

        // Make sure all are the same type of control event
        ControlType firstControlType = buildingBanks[0].getControlType();
        for (PayloadBank bank : buildingBanks) {
            if (bank.getControlType() != firstControlType) {
                throw new EmuException("different type control events on each channel");
            }
        }

        // Prestart events require an additional check,
        // run #'s and run types must be identical
        if (firstControlType == ControlType.PRESTART) {
            int[] prestartData;
            for (PayloadBank bank : buildingBanks) {
                prestartData = bank.getIntData();
                if (prestartData == null) {
                    throw new EmuException("PRESTART event does not have data");

                }

                if (prestartData[1] != runNumber) {
                    if (debug) System.out.println("gotValidControlEvents: warning, PRESTART event bad run #, " +
                                                          prestartData[1] + ", should be " + runNumber);
                    throw new EmuException("PRESTART event bad run # = " + prestartData[1] +
                                                   ", should be " + runNumber);
                }

                if (prestartData[2] != runType) {
                    if (debug) System.out.println("gotValidControlEvents: warning, PRESTART event bad run type, " +
                                                          prestartData[2] + ", should be " + runType);
                    throw new EmuException("PRESTART event bad run type = " + prestartData[2]+
                                                   ", should be " + runType);
                }
            }
        }
if (debug) System.out.println("gotValidControlEvents: found control event of type " +
                                      firstControlType.name());

    }


    /**
     * Check each payload bank - one from each input channel - to see if there are any
     * control events. A valid control event requires all channels to have identical
     * control events. If only some are control events, throw exception as it must
     * be all or none. If none are control events, do nothing as the banks will be built
     * into a single event momentarily.
     *
     * @param buildingBanks array containing events that will be built together
     * @param runNumber check this (correct) run # against the one in the control event
     * @param runType   check this (correct) run type against the one in the control event
     * @return <code>true</code> if a proper control events found, else <code>false</code>
     * @throws EmuException if events contain mixture of control/data or control types
     */
    public static boolean gotValidControlEvents(PayloadBank[] buildingBanks, int runNumber, int runType)
            throws EmuException {

        int controlEventCount = 0;
        int numberOfBanks = buildingBanks.length;
        EventType eventType;
        ControlType[] types = new ControlType[numberOfBanks];
        boolean debug = false;

        // Count control events
        for (PayloadBank bank : buildingBanks) {
            // Might be a ROC Raw, Physics, or Control Event
            eventType = bank.getEventType();
            if (eventType.isControl()) {
                types[controlEventCount++] = ControlType.getControlType(bank.getHeader().getTag());
            }
        }

        // If one is a control event, all must be identical control events.
        if (controlEventCount > 0) {
            // All events must be control events
            if (controlEventCount != numberOfBanks) {
if (debug) System.out.println("gotValidControlEvents: got " + controlEventCount +
                             " control events, but have " + numberOfBanks + " banks!");
                throw new EmuException("not all channels have control events");
            }

            // Make sure all are the same type of control event
            ControlType controlType = types[0];
            for (int i=1; i < types.length; i++) {
                if (controlType != types[i]) {
                    throw new EmuException("different type control events on each channel");
                }
            }

            // Prestart events require an additional check,
            // run #'s and run types must be identical
            if (controlType == ControlType.PRESTART) {
                int[] prestartData;
                for (PayloadBank bank : buildingBanks) {
                    prestartData = bank.getIntData();
                    if (prestartData == null) {
                        throw new EmuException("PRESTART event does not have data");

                    }

                    if (prestartData[1] != runNumber) {
if (debug) System.out.println("gotValidControlEvents: warning, PRESTART event bad run #, " +
                                      prestartData[1] + ", should be " + runNumber);
                        throw new EmuException("PRESTART event bad run # = " + prestartData[1] +
                                                       ", should be " + runNumber);
                    }

                    if (prestartData[2] != runType) {
if (debug) System.out.println("gotValidControlEvents: warning, PRESTART event bad run type, " +
                                      prestartData[2] + ", should be " + runType);
                        throw new EmuException("PRESTART event bad run type = " + prestartData[2]+
                                                       ", should be " + runType);
                    }
                }
            }
if (debug) System.out.println("gotValidControlEvents: found control event of type " + controlType.name());

            return true;
        }

        return false;
    }


    /**
     * This method takes an existing control event and updates its data.
     * This is useful for the event builder which receives control events
     * from ROCs or other EBs and needs to pass it "updated" further
     * downstream.
     *
     * @param controlEvent    control event to be updated; if not a valid type, false is returned
     * @param runNumber       current run number for prestart event
     * @param runType         current run type for prestart event
     * @param eventsInRun     number of events so far in run for all except prestart event
     * @param eventsSinceSync number of events since last sync for sync event
     *
     * @return <code>true</code> if a proper control event found and updated,
     *         else <code>false</code>
     */
    public static boolean updateControlEvent(EvioEvent controlEvent, int runNumber, int runType,
                                      int eventsInRun, int eventsSinceSync) {

        // Current time in seconds since Jan 1, 1970 GMT
        int time = (int) (System.currentTimeMillis()/1000L);

        ControlType type = getControlType(controlEvent);
        if (type == null) return false;

        int[] newData;

        try {
            switch (type) {
                case SYNC:
                    newData = new int[] {time, eventsSinceSync, eventsInRun};
                    controlEvent.setIntData(newData);
                    break;
                case PRESTART:
                    newData = new int[] {time, runNumber, runType};
                    controlEvent.setIntData(newData);
                    break;
                case GO:
                case PAUSE:
                case END:
                    newData = new int[] {time, 0, eventsInRun};
                    controlEvent.setIntData(newData);
                    break;
                default:
                    return false;
            }
        }
        catch (EvioException e) {
            return false;
        }

        return true;
    }


    /**
     * Create a Control event for testing purposes.
     *
     * <code><pre>
     * Sync event:
     * _______________________________________
     * |          Event Length = 4           |
     * |_____________________________________|
     * |   type = 0xFFD0  |  0x1   |  0xCC   |
     * |_____________________________________|
     * |                time                 |
     * |_____________________________________|
     * |   number of events since last sync  |
     * |_____________________________________|
     * |      number of events in run        |
     * |_____________________________________|
     *
     * Prestart event:
     * _______________________________________
     * |          Event Length = 4           |
     * |_____________________________________|
     * |   type = 0xFFD1  |  0x1   |  0xCC   |
     * |_____________________________________|
     * |                time                 |
     * |_____________________________________|
     * |              Run Number             |
     * |_____________________________________|
     * |               Run Type              |
     * |_____________________________________|
     *
     *
     * Go  (type = 0xFFD2), Pause (type = 0xFFD3) or
     * End (type = 0xFFD4) event:
     * _______________________________________
     * |          Event Length = 4           |
     * |_____________________________________|
     * |       type       |  0x1   |  0xCC   |
     * |_____________________________________|
     * |                time                 |
     * |_____________________________________|
     * |              (reserved)             |
     * |_____________________________________|
     * |      number of events in run        |
     * |_____________________________________|
     * </pre></code>
     *
     * @param type            control type, must be SYNC, PRESTART, GO, PAUSE, or END
     * @param runNumber       current run number for prestart event
     * @param runType         current run type for prestart event
     * @param eventsInRun     number of events so far in run for all except prestart event
     * @param eventsSinceSync number of events since last sync for sync event
     *
     * @return created Control event (EvioEvent object)
     * @throws EvioException if bad event type
     */
    public static EvioEvent createControlEvent(ControlType type, int runNumber, int runType,
                                               int eventsInRun, int eventsSinceSync)
            throws EvioException {

        int[] data;
        EventBuilder eventBuilder;

        // Current time in seconds since Jan 1, 1970 GMT
        int time = (int) (System.currentTimeMillis()/1000L);

        // create a single bank of integers which is the user bank
        switch (type) {
            case SYNC:
                data = new int[] {time, eventsSinceSync, eventsInRun};
                eventBuilder = new EventBuilder(type.getValue(), DataType.UINT32, 0xcc);
                break;
            case PRESTART:
                data = new int[] {time, runNumber, runType};
                eventBuilder = new EventBuilder(type.getValue(), DataType.UINT32, 0xcc);
                break;
            case GO:
            case PAUSE:
            case END:
                data = new int[] {time, 0, eventsInRun};
                eventBuilder = new EventBuilder(type.getValue(), DataType.UINT32, 0xcc);
                break;
            default:
                throw new EvioException("bad ControlType arg");
        }

        EvioEvent ev = eventBuilder.getEvent();
        eventBuilder.appendIntData(ev, data);

        return ev;
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
     * If sparsify flag is <code>true</code>, then no roc-specific data is included.
     * The trigger bank is not sparsified if timestamps exist.
     * Note that if incoming trigger banks are from events built in single event mode,
     * then there will be no roc-specific data in that case as well. If roc-specific
     * data is missing from any of the trigger banks, then it will be missing in the
     * final trigger bank as well.<p>
     *
     * @param inputPayloadBanks array containing a bank (Physics event) from each channel's
     *                          payload bank queue that will be built into one event
     * @param builder         object used to build trigger bank
     * @param ebId            id of event builder calling this method
     * @param runNumber       run number to place in trigger bank
     * @param runType         run type to place in trigger bank
     * @param includeRunData  if <code>true</code>, add run number and run type
     * @param eventsInSEM     if <code>true</code>, events are in single event mode
     * @param sparsify        if <code>true</code>, do not add roc specific segments if no relevant data
     * @param checkTimestamps if <code>true</code>, check timestamp consistency and
     *                        return false if inconsistent, include them in trigger bank
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
                                                     boolean eventsInSEM,
                                                     boolean sparsify,
                                                     boolean checkTimestamps,
                                                     int timestampSlop)
            throws EmuException {

        if (builder == null || inputPayloadBanks == null || inputPayloadBanks.length < 1) {
            throw new EmuException("arguments are null or zero-length");
        }

        int index, rocCount;
        int totalRocCount = 0;
        int numInputBanks = inputPayloadBanks.length;
        EvioBank[] triggerBanks = new EvioBank[numInputBanks];
        CODATag[] tags = new CODATag[numInputBanks];
        boolean nonFatalError = false;
        boolean haveRunData = true;
        boolean haveTimestamps=true, isTimstamped;
        boolean haveTrigWithNoRocSpecificData=false, hasRocSpecificData;

        // If in single event mode ...
        if (eventsInSEM) {
            haveTrigWithNoRocSpecificData = true;
        }

        //-------------------------------------
        // Parameters of the first trigger bank
        //-------------------------------------
        // Number of events in each payload bank
        int numEvents = inputPayloadBanks[0].getHeader().getNumber();
        boolean firstTrigTimestamped = CODATag.hasTimestamp(inputPayloadBanks[0].getHeader().getTag());


        // In each payload bank (of banks) is a built trigger bank. Extract them all.
        for (int i=0; i < numInputBanks; i++) {

            // find the built trigger bank (should be first one)
            index = 0;
            do {
                try {
                    triggerBanks[i] = (EvioBank)inputPayloadBanks[i].getChildAt(index++);
                    tags[i] = CODATag.getTagType(triggerBanks[i].getHeader().getTag());
//System.out.println("makeTriggerBankFromPhysics: tag from trig bank " + i + " = " + tags[i]);
                }
                catch (Exception e) {
                    throw new EmuException("No built trigger bank in physics event", e);
                }
            } while (!Evio.isBuiltTriggerBank(triggerBanks[i])); // check to see if it really is a built trigger bank


            // Number of rocs in this trigger bank
            rocCount = triggerBanks[i].getHeader().getNumber();

            // Total number of rocs
            totalRocCount += rocCount;

            //--------------------------------
            // run specific and timestamp data
            //--------------------------------

            // Do all trigger banks have run number & type data?
            haveRunData = haveRunData && tags[i].hasRunData();

            // Does this trigger have timestamps?
            isTimstamped = tags[i].hasTimestamp();

            // Do all trigger banks have timestamp data?
            haveTimestamps = haveTimestamps && isTimstamped;

            // If one has a timestamp, all must
            if (firstTrigTimestamped != isTimstamped) {
                throw new EmuException("If 1 trigger bank has timestamps, all must");
            }

            //--------------------------------
            // roc specific data
            //--------------------------------
            if (!eventsInSEM) {
                // Does this trigger have roc-specific data?
                hasRocSpecificData = tags[i].hasRocSpecificData();

//                System.out.println("makeTriggerBankFromPhysics: # Rocs = " + rocCount +
//                                           ", haveRunData = " + haveRunData +
//                                           ", hasRocSpecific data = " + hasRocSpecificData +
//                                   ", isTimeStamped = " + isTimstamped);
//
                // Is there at least one trigger bank without run specific data?
                if (!hasRocSpecificData) haveTrigWithNoRocSpecificData = true;

                // Sanity check
                if (hasRocSpecificData) {
                    // Number of trigger bank children must = # rocs + 2
                    if (triggerBanks[i].getChildCount() != rocCount + 2) {
                        throw new EmuException("Trigger bank does not have correct # of segments (" +
                                (rocCount + 2) + "), it has " + triggerBanks[i].getChildCount());
                    }
                }
                else {
                    // If sparsified, there are no segments for roc-specific data
                    // so the number of trigger bank children must = 2
                    if (triggerBanks[i].getChildCount() != 2) {
                        throw new EmuException("Trigger bank does not have correct # of segments (2), it has " +
                                                       triggerBanks[i].getChildCount());
                    }
                }
            }
            else {
                // If single event mode, # of trigger bank children must = 2
                if (triggerBanks[i].getChildCount() != 2) {
                    throw new EmuException("Trigger bank does not have correct # of segments (2), it has " +
                                                   triggerBanks[i].getChildCount());
                }
            }

        }


        // Can only include run data if you got it
        if (includeRunData && !haveRunData) {
            nonFatalError   = true;
            includeRunData = false;
//System.out.println("Roc(s) missing run # and run type info!");
        }

        // Can only check timestamps if you got 'em
        if (checkTimestamps && !haveTimestamps) {
            nonFatalError   = true;
            checkTimestamps = false;
//System.out.println("Roc(s) missing timestamp info!");
        }

        // If we've been told to sparsify ...
        if (sparsify) {
            // If there is timestamp data, ignore it
            if (haveTimestamps) sparsify = false;
        }
        // else we've been told NOT to sparsify ...
        else {
            // If sparsified trigger banks are coming in, ignore it
            if (haveTrigWithNoRocSpecificData) sparsify = true;
        }

        // Don't worry about sparsification if in single event mode
        // since it is already sparsified.
        if (eventsInSEM) sparsify = false;

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
        //    the run number (high 32 bits) and the run type (low 32 bits) IF
        //    the config is set for adding runData.
        //
        //    MSB(64)                LSB(0)
        //    _____________________________
        //    |     first event number    |
        //    |        timestamp1         |
        //    |            ...            |
        //    |        timestampM         |
        //    | run number  |  run type   |
        //    _____________________________
        //
        //
        // 2) The second segment in each built trigger bank contains common data
        //    in the format given below. This is a segment of unsigned 16 bit
        //    integers containing the event type of each event.
        //
        //                        <-- higher mem
        //                                     |
        //                                     V
        //    __________________________________
        //    |  event2 type  |  event1 type   |
        //    |        .      |        .       |
        //    |        .      |        .       |
        //    |  eventM type  | eventM-1 type  |
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
            timestampsAvg = new long[numEvents];
            timestampsMax = new long[numEvents];
            timestampsMin = new long[numEvents];
            Arrays.fill(timestampsMin, Long.MAX_VALUE);
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
                if (haveRunData) {
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

        // Bank we are trying to build. Need to update the num which = # rocs
        EvioEvent combinedTrigger = builder.getEvent();
        combinedTrigger.getHeader().setNumber(totalRocCount);

        // 1) Create common data segment of longs, with first event #,
        //    avg timestamps, and possibly run number & run type in it.
        long[] longData;
        if (!checkTimestamps) {
            if (includeRunData) {
                longData = new long[2];
                longData[0] = firstEventNumber;
                longData[1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                if (sparsify) {
                    combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_RUN_NRSD.getValue());
                }
                else {
                    combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_RUN.getValue());
                }
            }
            else {
                longData = new long[1];
                longData[0] = firstEventNumber;
                if (sparsify) {
                    combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_NRSD.getValue());
                }
                else {
                    combinedTrigger.getHeader().setTag(CODATag.BUILT_TRIGGER_BANK.getValue());
                }
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
        //    If we're sparsifying, do NOT include such segments.

        if (!sparsify) {
            // for each trigger bank (from previous level EBs) ...
            for (EvioBank trBank : triggerBanks) {
                // copy over each roc segment in trigger bank to combined trigger bank
                for (int j=2; j < trBank.getChildCount(); j++) {
                    try { builder.addChild(combinedTrigger, (EvioSegment)trBank.getChildAt(j)); }
                    catch (EvioException e) { /* never happen */ }
                }
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
     * If sparsify flag is <code>true</code>, then no roc-specific data is included.
     * The trigger bank is not sparsified if timestamps and/or roc misc data exist.<p>
     *
     * @param inputPayloadBanks array containing a bank (ROC Raw) from each channel's
     *                          payload bank queue that will be built into one event
     * @param builder object used to build trigger bank
     * @param ebId id of event builder calling this method
     * @param firstEventNumber event number to place in trigger bank
     * @param runNumber run number to place in trigger bank
     * @param runType   run type to place in trigger bank
     * @param includeRunData if <code>true</code>, add run number and run type
     * @param sparsify if <code>true</code>, do not add roc specific segments if no such data
     * @param checkTimestamps if <code>true</code>, check timestamp consistency and
     *                        return false if inconsistent, include them in trigger bank
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
                                                    boolean sparsify,
                                                    boolean checkTimestamps,
                                                    int timestampSlop)
            throws EmuException {

        if (builder == null || inputPayloadBanks == null || inputPayloadBanks.length < 1) {
            throw new EmuException("arguments are null or zero-length");
        }

        int tag, firstTrigTag=0, index;
        int numROCs = inputPayloadBanks.length;
        int numEvents = inputPayloadBanks[0].getHeader().getNumber();
        EvioSegment segment;
        EvioBank[] triggerBanks = new EvioBank[numROCs];
        boolean haveTimestamps, haveMiscData=false, nonFatalError=false;
        CODATag trigTag;


        // In each payload bank (of banks) is a trigger bank. Extract them all.
        for (int i=0; i < numROCs; i++) {

            // find the trigger bank (should be first one)
            index = 0;
            do {
                try {
                    triggerBanks[i] = (EvioBank)inputPayloadBanks[i].getChildAt(index);
                    // Get the first trigger bank's tag
                    if (index == 0) {
                        firstTrigTag = triggerBanks[i].getHeader().getTag();
                    }
                    index++;
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

            // Check to see if all trigger bank tags are the same
            tag = triggerBanks[i].getHeader().getTag();
            if (tag != firstTrigTag) {
                throw new EmuException("Trigger banks have different tags");
            }
        }

        // We check timestamps if told to in configuration AND if timestamps are present
        haveTimestamps = CODATag.hasTimestamp(firstTrigTag);
        if (checkTimestamps && !haveTimestamps) {
            nonFatalError   = true;
            checkTimestamps = false;
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
        //    the run number (high 32 bits) and the run type (low 32 bits) IF
        //    the config is set for adding runData.
        //
        //    MSB(64)                LSB(0)
        //    _____________________________
        //    |     first event number    |
        //    |        timestamp1         |
        //    |            ...            |
        //    |        timestampM         |
        //    | run number  |  run type   |
        //    _____________________________
        //
        //
        // 2) The second segment in each built trigger bank contains common data
        //    in the format given below. This is a segment of unsigned 16 bit
        //    integers containing the event type of each event.
        //
        //                        <-- higher mem
        //                                     |
        //                                     V
        //    __________________________________
        //    |  event2 type  |  event1 type   |
        //    |        .      |        .       |
        //    |        .      |        .       |
        //    |  eventM type  | eventM-1 type  |
        //    __________________________________
        //

        // Find the types of events from first ROC
        short[] evData = new short[numEvents];
        for (int i=0; i < numEvents; i++) {
            segment   = (EvioSegment) (triggerBanks[0].getChildAt(i));
            evData[i] = (short) (segment.getHeader().getTag());  // event type
        }

        // Check the consistency of timestamps if desired/possible
        long   ts;
        long[] timestampsAvg = null;
        long[] timestampsMax = null;
        long[] timestampsMin = null;
        if (checkTimestamps) {
            timestampsAvg = new long[numEvents];
            timestampsMax = new long[numEvents];
            timestampsMin = new long[numEvents];
            Arrays.fill(timestampsMin, Long.MAX_VALUE);
        }

        // It is convenient at this point to check and see if for a given event,
        // across all ROCs, the event number & event type are the same.
        int[] triggerData;
        int firstEvNum = (int) firstEventNumber;
        for (int i=0; i < numEvents; i++) {
            for (int j=0; j < numROCs; j++) {
                segment = (EvioSegment) (triggerBanks[j].getChildAt(i));
                // Check event type consistency
                if (evData[i] != (short) (segment.getHeader().getTag())) {
System.out.println("makeTriggerBankFromRocRaw: event type differs across ROCs");
                    nonFatalError = true;
                }

                // Check event number consistency
                triggerData = segment.getIntData();
                if (firstEvNum + i != triggerData[0]) {
System.out.println("makeTriggerBankFromRocRaw: event number differs across ROCs");
System.out.println("                           " + (firstEvNum+i) + " != " + (triggerData[0]));
                    nonFatalError = true;
                }

                // Check for misc data (if we have no timestamps) in at least one place
                // so we know if we can sparsify or not.
                if (!haveMiscData && !haveTimestamps && triggerData.length > 1) {
                    haveMiscData = true;
                }

                // If they exist, store all timestamp related
                // values so consistency can be checked below
                if (checkTimestamps && triggerData.length > 2) {
                    ts = (    (0xffffL & (long)triggerData[2] << 32) |
                          (0xffffffffL & (long)triggerData[1]));
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

        //----------------------------
        // 1) Add segment of long data
        //----------------------------
        long[] longData;
        if (!checkTimestamps) {
            if (includeRunData) {
                longData = new long[2];
                longData[0] = firstEventNumber;
                longData[1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                trigTag = CODATag.BUILT_TRIGGER_RUN;
            }
            else {
                longData = new long[1];
                longData[0] = firstEventNumber;
                trigTag = CODATag.BUILT_TRIGGER_BANK;
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
                trigTag = CODATag.BUILT_TRIGGER_TS_RUN;
            }
            else {
                longData = new long[1+numEvents];
                longData[0] = firstEventNumber;
                for (int i=0; i < numEvents; i++) {
                    longData[i+1] = timestampsAvg[i];
                }
                trigTag = CODATag.BUILT_TRIGGER_TS;
            }
        }

        EvioSegment ebSeg = new EvioSegment(ebId, DataType.ULONG64);
        try {
            ebSeg.appendLongData(longData);
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }

        //----------------------------
        // 2) Add segment of event types
        //----------------------------
        ebSeg = new EvioSegment(ebId, DataType.USHORT16);
        try {
            ebSeg.appendShortData(evData);
            builder.addChild(combinedTrigger, ebSeg);
        }
        catch (EvioException e) { /* never happen */ }

        //-------------------------------------------------------
        // 3) Add ROC-specific segments if not sparsifying
        //-------------------------------------------------------

        // We sparsify if told to in configuration AND
        // if timestamps & roc misc. data do not exist.
        sparsify = sparsify && !haveTimestamps && !haveMiscData;

        if (sparsify) {
            // Reset the trigger bank's tag if sparsifying
            if (trigTag.hasRunData()) {
                trigTag = CODATag.BUILT_TRIGGER_RUN_NRSD;
            }
            else {
                trigTag = CODATag.BUILT_TRIGGER_NRSD;
            }
        }
        else {
            // now add one segment for each ROC with ROC-specific data in it
            int intCount, dataLenFromEachSeg;
            EvioSegment newRocSeg, oldRocSeg;

            // for each ROC ...
            for (int i=0; i < triggerBanks.length; i++) {
                newRocSeg = new EvioSegment(inputPayloadBanks[i].getSourceId(), DataType.UINT32);
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
        }

        // Finally set the trigger bank's tag
        combinedTrigger.getHeader().setTag(trigTag.getValue());

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
     *                        return false if inconsistent, include them in trigger bank
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
        //    the run number (high 32 bits) and the run type (low 32 bits) IF
        //    the config is set for adding runData.
        //
        //    MSB(64)                LSB(0)
        //    _____________________________
        //    |       event number        |
        //    |        timestamp          |
        //    | run number  |  run type   |
        //    _____________________________
        //
        //
        // 2) The second segment in each built trigger bank contains common data
        //    in the format given below. This is a segment of unsigned 16 bit
        //    integers containing the event type of the event.
        //
        //                       <--  higher mem
        //    __________________________________
        //    |   (padding)    |  event1 type  |
        //    __________________________________
        //

        // Extract needed event type from Data Block banks (pick first in list)
        // for checking type consistency.
        EvioBank blockBank = (EvioBank)inputPayloadBanks[0].getChildAt(0);
        short[] evData = new short[1];
        evData[0] = (short) (blockBank.getHeader().getNumber());  // event type

        // Extract tag too. Low 8 bits = roc id, 9th bit = yes/no timestamps
        int firstTag = blockBank.getHeader().getTag();
        boolean haveTs, haveTimestamps = (0x100 & firstTag) > 0;

        // Can only check timestamps if we have any
        if (checkTimestamps && !haveTimestamps) {
            nonFatalError   = true;
            checkTimestamps = false;
        }

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

            // check timestamp consistency
            haveTs = (blockBank.getHeader().getTag() & 0x100) > 0;
            if (haveTs != haveTimestamps) {
                throw new EmuException("Some rocs have timestamps, some don't");
            }

            // store timestamp related values so consistency can be checked later
            if (checkTimestamps && data.length > 2) {
                ts = (    (0xffffL & (long)data[2] << 32) |
                      (0xffffffffL & (long)data[1]));
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
            data[index++] = (int)  timestamp; // low 32 bits
            data[index++] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
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
//        data[1] = (int) timestamp; // low 32 bits
//        data[2] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
//        data[3] = 10000;

        // Put some data into event (10 modules worth)
        int []data = generateEntangledDataFADC250(eventNumber, numEvents, recordId,
                                                  10, true, timestamp);

        // create a single data block bank (of ints)
        int eventType = 33;
        int dataTag = createCodaTag(status, detectorId);
        EvioBank dataBank = new EvioBank(dataTag, DataType.UINT32, eventType);
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
            segmentData[1] = (int)  timestamp; // low 32 bits
            segmentData[2] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
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
            pBanks[i].setEventType(EventType.ROC_RAW);

            eventNumber += numEvents;
            timestamp   += 4*numEvents;
        }

        return pBanks;
    }



}
