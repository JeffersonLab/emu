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

import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;
import org.jlab.coda.emu.EmuException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
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
      * Determine whether a bank is a SYNC control event or not.
      *
      * @param node object corresponding to evio structure
      * @return <code>true</code> if arg is SYNC event, else <code>false</code>
      */
     public static boolean isSyncEvent(EvioNode node) {
         if (node == null)  return false;

         // Look inside to see if it is an END event.
         return (node.getTag() == ControlType.SYNC.getValue() &&
                 node.getNum() == 0 &&
                 node.getDataType() == 1 &&
                 node.getLength() == 4);
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
        return bank != null && (bank.getHeader().getNumber() == 0);
    }


    /**
     * Determine whether an event is a USER event or not.
     * Only called on events that were originally from the
     * ROC and labeled as ROC Raw type.
     *
     * @param node input node
     * @return <code>true</code> if arg is USER event, else <code>false</code>
     */
    public static boolean isUserEvent(EvioNode node) {
        return node != null && (node.getNum() == 0);
    }


    /**
     * Determine whether a bank is a trigger bank from a ROC or not.
     *
     * @param node input node
     * @return <code>true</code> if arg is trigger bank, else <code>false</code>
     */
    public static boolean isRawTriggerBank(EvioNode node) {
        return node != null && CODATag.isRawTrigger(node.getTag());
    }


    /**
     * Determine whether a bank is a built trigger bank or not.
     * In other words, has it been built by an event builder.
     *
     * @param node input node
     * @return <code>true</code> if arg is a built trigger bank, else <code>false</code>
     */
    public static boolean isBuiltTriggerBank(EvioNode node) {
        return node != null && CODATag.isBuiltTrigger(node.getTag());
    }


    /**
     * Check the given payload buffer for correct format
     * (physics, ROC raw, control, or user).
     * <b>All other buffers are ignored.</b><p>
     *
     * No checks done on arguments. However, format of payload buffers
     * is checked here for the first time.<p>
     *
     * @param pBuf payload buffer to be examined
     * @param channel input channel
     * @throws EmuException if physics or roc raw bank has improper format
     */
    public static void checkPayload(PayloadBuffer pBuf, DataChannel channel)
            throws EmuException {

        // check to make sure record ID is sequential - already checked by EvioCompactReader
        int tag;
        int recordId  = pBuf.getRecordId();
        int sourceId  = pBuf.getSourceId();
        EvioNode node = pBuf.getNode();
        boolean nonFatalError = false;
        boolean nonFatalRecordIdError = false;

        // See what type of event this is
        // Only interested in known types such as physics, roc raw, control, and user events.
        EventType eventType = pBuf.getEventType();
        if (eventType == null || !eventType.isEbFriendly()) {
System.out.println("checkPayload: unknown type, dump payload buffer");
            return;
        }
//System.out.println("checkPayloadBuffer: got bank of type " + eventType);

        // Only worry about record id if event to be built.
        // Initial recordId stored is 0, ignore that.
        if (eventType.isAnyPhysics() || eventType.isROCRaw()) {
            // The recordId associated with each bank is taken from the first
            // evio block header in a single ET/cMsg-msg data buffer. For a physics or
            // ROC raw type, it should start at zero and increase by one in the
            // first evio block header of the next ET/cMsg-msg data buffer.
            // NOTE: There may be multiple banks from the same buffer and
            // they will all have the same recordId.
            if (recordId != channel.getRecordId() &&
                recordId != channel.getRecordId() + 1) {
System.out.println("checkPayload: record ID out of sequence, got " + recordId +
                   ", expecting " + channel.getRecordId() + " or " +
                  (channel.getRecordId()+1) + ", type = " + eventType +
                   ", name = " + channel.name());
                nonFatalRecordIdError = true;
            }
            // Store the current value here as a convenience for the next comparison
            channel.setRecordId(recordId);

            tag = node.getTag();

            if (sourceId != getTagCodaId(tag)) {
                System.out.println("checkPayload: buf source Id (" + sourceId + ") != buf's id from tag (" + getTagCodaId(tag) + ")");
                nonFatalError = true;
            }

            // pick this bank apart a little here
            if (node.getDataTypeObj() != DataType.BANK &&
                    node.getDataTypeObj() != DataType.ALSOBANK) {
                throw new EmuException("ROC raw / physics record not in proper format");
            }

            pBuf.setSync(Evio.isTagSyncEvent(tag));
            pBuf.setError(Evio.tagHasError(tag));
        }

        // Check source ID of bank to see if it matches channel id
        if (!pBuf.matchesId()) {
System.out.println("checkPayload: buf source id = " + pBuf.getSourceId() +
                           " != input channel id = " + channel.getID());
            nonFatalError = true;
        }

        pBuf.setNonFatalBuildingError(nonFatalError || nonFatalRecordIdError);
    }


    /**
     * Check each payload bank - one from each input channel - for a number of issues:<p>
     * <ol>
     * <li>if there are any sync bits set, all must be sync banks
     * <li>the ROC ids of the banks must be unique
     * <li>at this point all banks are either physics events or ROC raw records,
     *     but must be identical types
     * <li>there are the same number of events in each bank
     * </ol>
     *
     * @param buildingBanks array containing banks that will be built together
     * @param eventNumber   first event number in each bank (used for diagnostic output)
     *
     * @return <code>true</code> if non-fatal error occurred, else <code>false</code>
     * @throws EmuException if some physics and others ROC raw event types;
     *                      if there are a differing number of events in each payload bank;
     *                      if some events have a sync bit set and others do not.
     */
    public static boolean checkConsistency(PayloadBuffer[] buildingBanks, long eventNumber)
            throws EmuException {
        boolean nonFatalError = false;

        // For each ROC raw data record check the sync bit
        int syncBankCount = 0;

        // By the time this method is run, all input banks are either (partial)physics or ROC raw.
        // Just make sure they're all identical.
        int physicsEventCount = 0;

        // Number of events contained in payload bank
        int numEvents = buildingBanks[0].getNode().getNum();

        for (int i=0; i < buildingBanks.length; i++) {
            if (buildingBanks[i].isSync()) {
                syncBankCount++;
            }

            if (buildingBanks[i].getEventType().isAnyPhysics()) {
                physicsEventCount++;
            }

            for (int j=i+1; j < buildingBanks.length; j++) {
                if ( buildingBanks[i].getSourceId() == buildingBanks[j].getSourceId()  ) {
                    // ROCs have duplicate IDs
                    nonFatalError = true;
System.out.println("  EB mod: events have duplicate source ids");
                }
            }

            // Check that there are the same # of events are contained in each payload bank
            if (numEvents != buildingBanks[i].getNode().getNum()) {
                System.out.println("Differing # of events sent by each ROC:\n");
                System.out.println("numEvents       name      codaID");
                for (PayloadBuffer bank : buildingBanks) {
                    System.out.println("   " +
                                       bank.getNode().getNum() + "        " +
                                       bank.getSourceName() + "        " +
                                       bank.getSourceId());
                }
                throw new EmuException("differing # of events sent by each ROC");
            }
        }

        int numBanks = buildingBanks.length;

        // If one is a sync, all must be syncs
        if (syncBankCount > 0 && syncBankCount != numBanks) {
            // Some banks are sync banks and some are not
            System.out.print("  EB mod: these channels have NO sync at event " + eventNumber + ": ");
            for (PayloadBuffer buildingBank : buildingBanks) {
                if (!buildingBank.isSync()) {
                    System.out.print(buildingBank.getSourceName() + ", ");
                }
            }
            System.out.println();

            throw new EmuException("events out of sync at event " + eventNumber);
        }

        // All must be physics or all must be ROC raw
        if (physicsEventCount > 0 && physicsEventCount != numBanks) {
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
    public static void gotConsistentControlEvents(PayloadBuffer[] buildingBanks,
                                                  int runNumber, int runType)
            throws EmuException {

        boolean debug = false;

        // Make sure all are the same type of control event
        ControlType firstControlType = buildingBanks[0].getControlType();
        for (PayloadBuffer buf : buildingBanks) {
            if (buf.getControlType() != firstControlType) {
                throw new EmuException("different type control events on each channel");
            }
        }

        // Prestart events require an additional check,
        // run #'s and run types must be identical
        if (firstControlType == ControlType.PRESTART) {
            for (PayloadBuffer buf : buildingBanks) {
                IntBuffer prestartData = buf.getNode().getByteData(false).asIntBuffer();
                if (prestartData.remaining() < 1) {
                    throw new EmuException("PRESTART event does not have data");

                }

                int runN = prestartData.get(1), runT = prestartData.get(2);
                if (runN != runNumber) {
                    if (debug) System.out.println("gotValidControlEvents: warning, PRESTART event bad run #, " +
                                                  runN + ", should be " + runNumber);
                    throw new EmuException("PRESTART event bad run # = " + runN +
                                                   ", should be " + runNumber);
                }

                if (runT != runType) {
                    if (debug) System.out.println("gotValidControlEvents: warning, PRESTART event bad run type, " +
                                                  runT + ", should be " + runType);
                    throw new EmuException("PRESTART event bad run type = " + runT +
                                                   ", should be " + runType);
                }
            }
        }
if (debug) System.out.println("gotValidControlEvents: found control event of type " +
                                      firstControlType.name());

    }


    /**
     * Create a Control event.
     *
     * <code><pre>
     * Sync event:
     * _______________________________________
     * |          Event Length = 4           |
     * |_____________________________________|
     * |   type = 0xFFD0  |  0x1   |    0    |
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
     * |   type = 0xFFD1  |  0x1   |    0    |
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
     * |       type       |  0x1   |    0    |
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
     * @param error           END control event sent due to error condition
     *
     * @return created Control event (EvioEvent object)
     */
    public static EvioEvent createControlEvent(ControlType type, int runNumber, int runType,
                                               int eventsInRun, int eventsSinceSync,
                                               boolean error) {

        try {
            int[] data;
            EventBuilder eventBuilder;

            // Current time in seconds since Jan 1, 1970 GMT
            int time = (int) (System.currentTimeMillis()/1000L);

            // create a single bank of integers which is the user bank
            switch (type) {
                case SYNC:
                    data = new int[] {time, eventsSinceSync, eventsInRun};
                    eventBuilder = new EventBuilder(type.getValue(), DataType.UINT32, 0);
                    break;
                case PRESTART:
                    data = new int[] {time, runNumber, runType};
                    eventBuilder = new EventBuilder(type.getValue(), DataType.UINT32, 0);
                    break;
                case GO:
                case PAUSE:
                case END:
                default:
                    if (error) {
                        data = new int[]{time, 0xffffffff, eventsInRun};
                    }
                    else {
                        data = new int[]{time, 0, eventsInRun};
                    }
                    eventBuilder = new EventBuilder(type.getValue(), DataType.UINT32, 0);
                    break;
            }

            EvioEvent ev = eventBuilder.getEvent();
            eventBuilder.appendIntData(ev, data);
            return ev;
        }
        catch (EvioException e) {/* never happen */}

        return null;
    }


    /**
     * Create a Control event with a ByteBuffer which is ready to read.
     *
     * @param type            control type, must be SYNC, PRESTART, GO, PAUSE, or END
     * @param runNumber       current run number for prestart event
     * @param runType         current run type for prestart event
     * @param eventsInRun     number of events so far in run for all except prestart event
     * @param eventsSinceSync number of events since last sync for sync event
     * @param order           byte order in which to write event into buffer
     * @param error           END control event sent due to error condition
     *
     * @return created PayloadBuffer object containing Control event in byte buffer
     */
    public static PayloadBuffer createControlBuffer(ControlType type, int runNumber,
                                                    int runType, int eventsInRun,
                                                    int eventsSinceSync, ByteOrder order,
                                                    boolean error) {

        try {
            int[] data;
            CompactEventBuilder builder = new CompactEventBuilder(20, order);

            // Current time in seconds since Jan 1, 1970 GMT
            int time = (int) (System.currentTimeMillis()/1000L);

            // create a single bank of integers which is the user bank
            switch (type) {
                case SYNC:
                    data = new int[] {time, eventsSinceSync, eventsInRun};
                    break;
                case PRESTART:
                    data = new int[] {time, runNumber, runType};
                    break;
                case GO:
                case PAUSE:
                case END:
                default:
                    if (error) {
                        data = new int[]{time, 0xffffffff, eventsInRun};
                    }
                    else {
                        data = new int[]{time, 0, eventsInRun};
                    }
            }

            builder.openBank(type.getValue(), 0, DataType.UINT32);
            builder.addIntData(data);
            builder.closeStructure();
            PayloadBuffer pBuf = new PayloadBuffer(builder.getBuffer());  // Ready to read buffer
            pBuf.setEventType(EventType.CONTROL);
            pBuf.setControlType(type);

            return pBuf;
        }
        catch (EvioException e) {/* never happen */}

        return null;
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
     *                          payload buffer queue that will be built into one event
     * @param builder         object used to build trigger bank
     * @param ebId            id of event builder calling this method
     * @param runNumber       run number to place in trigger bank
     * @param runType         run type to place in trigger bank
     * @param includeRunData  if <code>true</code>, add run number and run type
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
    public static boolean makeTriggerBankFromPhysics(PayloadBuffer[] inputPayloadBanks,
                                                     CompactEventBuilder builder, int ebId,
                                                     int runNumber, int runType,
                                                     boolean includeRunData,
                                                     boolean sparsify,
                                                     boolean checkTimestamps,
                                                     int timestampSlop)
            throws EmuException {

        if (builder == null || inputPayloadBanks == null || inputPayloadBanks.length < 1) {
            throw new EmuException("arguments are null or zero-length");
        }

        int rocCount;
        int totalRocCount = 0;
        int numInputBanks = inputPayloadBanks.length;
        EvioNode[] triggerBanks = new EvioNode[numInputBanks];
        CODATag[] tags = new CODATag[numInputBanks];
        boolean nonFatalError = false;
        boolean allHaveRunData = true;
        boolean haveTimestamps=true, isTimstamped;
        boolean haveTrigWithNoRocSpecificData=false, hasRocSpecificData;


        //-------------------------------------
        // Parameters of the first trigger bank
        //-------------------------------------
        // Number of events in each payload bank
        int numEvents = inputPayloadBanks[0].getNode().getNum();
        boolean firstTrigTimestamped = false;


        // In each payload bank (of banks) is a built trigger bank. Extract them all.
        for (int i=0; i < numInputBanks; i++) {

            // Find the built trigger bank - first child bank
            triggerBanks[i] = inputPayloadBanks[i].getNode().getChildNodes().get(0);
            tags[i] = CODATag.getTagType(triggerBanks[i].getTag());
            if (!Evio.isBuiltTriggerBank(triggerBanks[i])) {
                throw new EmuException("No built trigger bank in physics event");
            }

            if (i==0) firstTrigTimestamped = tags[0].hasTimestamp();

            // Number of rocs in this trigger bank
            rocCount = triggerBanks[i].getNum();

            // Total number of rocs
            totalRocCount += rocCount;

            //--------------------------------
            // run specific and timestamp data
            //--------------------------------

            // Do all trigger banks have run number & type data?
            allHaveRunData = allHaveRunData && tags[i].hasRunData();

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
        EvioNode ebSeg1 = triggerBanks[0].getChildNodes().get(0);
        EvioNode ebSeg2 = triggerBanks[0].getChildNodes().get(1);
        longCommonData  = ByteDataTransformer.toLongArray(ebSeg1.getByteData(false));
        shortCommonData = ByteDataTransformer.toShortArray(ebSeg2.getByteData(false));
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
                commonShort = ByteDataTransformer.toShortArray(
                                triggerBanks[i].getChildNodes().get(1).getByteData(false));
                if (shortCommonData.length != commonShort.length) {
                    throw new EmuException("Trying to merge records with different numbers of events");
                }
                for (int j=0; j < shortCommonData.length; j++) {
                    if (shortCommonData[j] != commonShort[j]) {
                        throw new EmuException("Trying to merge records with different event types");
                    }
                }

                // long stuff
                commonLong = ByteDataTransformer.toLongArray(
                        triggerBanks[i].getChildNodes().get(0).getByteData(false));

                // check event number
                if (firstEventNumber != commonLong[0]) {
                    throw new EmuException("Trying to merge records with different event numbers");
                }

                // check run number & type if all such data is present
                if (allHaveRunData) {
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
//System.out.println("Timestamps differing by " + (timestampsMax[j] - timestampsMin[j]));
                }
                if (timestampsMax[j] - timestampsMin[j] > timestampSlop)  {
                    nonFatalError = true;
System.out.println("Timestamp NOT consistent: ev #" + (firstEventNumber + j) + ", diff = " +
                  (timestampsMax[j] - timestampsMin[j]) + ", allowed = " + timestampSlop );
                }
            }
        }

        int triggerTag;

        // 1) Create common data segment of longs, with first event #,
        //    avg timestamps, and possibly run number & run type in it.
        long[] longData;
        if (!checkTimestamps) {
            if (includeRunData) {
                longData = new long[2];
                longData[0] = firstEventNumber;
                longData[1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                if (sparsify) {
                    triggerTag = CODATag.BUILT_TRIGGER_RUN_NRSD.getValue();
                }
                else {
                    triggerTag = CODATag.BUILT_TRIGGER_RUN.getValue();
                }
            }
            else {
                longData = new long[1];
                longData[0] = firstEventNumber;
                if (sparsify) {
                    triggerTag = CODATag.BUILT_TRIGGER_NRSD.getValue();
                }
                else {
                    triggerTag = CODATag.BUILT_TRIGGER_BANK.getValue();
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
                triggerTag = CODATag.BUILT_TRIGGER_TS_RUN.getValue();
            }
            else {
                longData = new long[1+numEvents];
                longData[0] = firstEventNumber;
                for (int i=0; i < numEvents; i++) {
                    longData[i+1] = timestampsAvg[i];
                }
                triggerTag = CODATag.BUILT_TRIGGER_TS.getValue();
            }
        }

        // TODO: check space requirements first, expand if necessary
        try {
            // Start trigger bank
            builder.openBank(triggerTag, totalRocCount, DataType.SEGMENT);

            // Create common data segment of shorts, actually just
            // grab one from input payload bank and change id.
            ebSeg2.updateTag(ebId);

            // Add long & short segments to trigger bank
            builder.openSegment(ebId, DataType.ULONG64);
            builder.addLongData(longData);
            builder.closeStructure();

            builder.addEvioNode(ebSeg2);

            // 2) Now put all ROC-specific segments into bank.
            //    If we're in single event mode, there will be NO such segments.
            //    If we're sparsifying, do NOT include such segments.
            if (!sparsify) {
                // For each trigger bank (from previous level EBs) ...
                for (EvioNode trBank : triggerBanks) {
                    // Copy over each roc segment in trigger bank to combined trigger bank
                    for (int j=2; j < trBank.getChildCount(); j++) {
                        builder.addEvioNode(trBank.getChildNodes().get(j));
                    }
                }
            }

            builder.closeStructure();
        }
        catch (EvioException e) {
            // not enough room in buffer
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
    public static boolean makeTriggerBankFromRocRaw(PayloadBuffer[] inputPayloadBanks,
                                                    CompactEventBuilder builder, int ebId,
                                                    long firstEventNumber,
                                                    int runNumber, int runType,
                                                    boolean includeRunData,
                                                    boolean sparsify,
                                                    boolean checkTimestamps,
                                                    int timestampSlop,
                                                    int buildThreadOrder)
            throws EmuException {

        if (builder == null || inputPayloadBanks == null || inputPayloadBanks.length < 1) {
            throw new EmuException("arguments are null or zero-length");
        }

        boolean turnOffChecks = false;

        int tag, firstTrigTag=0;
        int numROCs = inputPayloadBanks.length;
        int numEvents = inputPayloadBanks[0].getNode().getNum();

        EvioNode[][]triggerSegments = new EvioNode[numROCs][numEvents];

        EvioNode rocNode;
        EvioNode[] triggerBanks = new EvioNode[numROCs];
        boolean haveTimestamps, haveMiscData=false, nonFatalError=false;
        CODATag trigTag;
// TODO: allow for 32 bit timestamps & check differences between ROCs' TS info

        if (turnOffChecks) {
            checkTimestamps = false;
        }

        // In each payload bank (of banks) is a trigger bank. Extract them all.
        for (int i=0; i < numROCs; i++) {

            // Find the trigger bank - first child bank
            rocNode = inputPayloadBanks[i].getNode();
            triggerBanks[i] = rocNode.getChildAt(0);
            if (!Evio.isRawTriggerBank(triggerBanks[i])) {
                throw new EmuException("No trigger bank in ROC raw record in roc " +
                       inputPayloadBanks[i].getSourceName() + ", tag = 0x" +
                       Integer.toHexString(triggerBanks[i].getTag()) +
                       ", first event # " + firstEventNumber);
            }

            // Get the first trigger bank's tag
            if (i == 0) {
                // For hardware bug work-around mask off last 4 bits of tag
                firstTrigTag = triggerBanks[i].getTag() & 0xfff0;
            }

            inputPayloadBanks[i].setEventCount(numEvents);

            // Store for future use
            for (int j=0; j < numEvents; j++) {
                triggerSegments[i][j] = triggerBanks[i].getChildAt(j);
            }

            if (!turnOffChecks) {
                //  Check to see if all TRIGGER banks think they have the same # of events
                if (numEvents != triggerBanks[i].getNum()) {
                    throw new EmuException("Data blocks contain different numbers of events, " +
                        numEvents + " != " + triggerBanks[i].getNum() + " from roc " +
                        inputPayloadBanks[i].getSourceName());
                }

                // Check to see if all PAYLOAD banks think they have same # of events
                if (numEvents != rocNode.getNum()) {
                    throw new EmuException("Data blocks contain different numbers of events, " +
                    numEvents + " != " + rocNode.getNum() + " from roc " +
                    inputPayloadBanks[i].getSourceName());
                }

                // Check that number of trigger bank children = # events
                if (triggerBanks[i].getChildCount() != numEvents) {
                    throw new EmuException("Trigger bank does not have correct number of segments, " +
                        numEvents + " != " + triggerBanks[i].getChildCount() + " from roc " +
                        inputPayloadBanks[i].getSourceName());
                }

                // Check to see if all trigger bank tags are 0xff1x
                tag = triggerBanks[i].getTag() & 0xfff0;
                if (tag != 0xff10) {
                    throw new EmuException("Trigger bank has bad tag 0x" +
                        Integer.toHexString(triggerBanks[i].getTag()) +
                        " from roc " + inputPayloadBanks[i].getSourceName());
                }

                // Check to see if all trigger bank tags are the same
                if (tag != firstTrigTag) {
                    throw new EmuException("Trigger banks have different tags, " +
                    Integer.toHexString(firstTrigTag) + " != " + Integer.toHexString(tag) +
                    " from roc " + inputPayloadBanks[i].getSourceName());
                }
            }
        }

        // We check timestamps if told to in configuration AND if timestamps are present
        haveTimestamps = CODATag.hasTimestamp(triggerBanks[0].getTag());
        if (checkTimestamps && !haveTimestamps) {
            nonFatalError   = true;
            checkTimestamps = false;
        }

        // Start building combined trigger bank

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
            evData[i] = (short) (triggerSegments[0][i].getTag());  // event type
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
        int[] triggerData = null;
        int firstEvNum = (int) firstEventNumber;
        int[][][] trigIntArrays = new int[numROCs][numEvents][];

        for (int i=0; i < numEvents; i++) {
            for (int j=0; j < numROCs; j++) {
                if (!turnOffChecks) {
                    // Check event type consistency
                    if (evData[i] != (short) (triggerSegments[j][i].getTag())) {
System.out.println("makeTriggerBankFromRocRaw: event type differs across ROCs, first has " + evData[i] +
" #" + j + " ROC has " + (short) (triggerSegments[j][i].getTag()));
                        nonFatalError = true;
                    }

                    // Check event number consistency, but make sure we take
                    // endianness into account when looking at the data.
                    trigIntArrays[j][i] = triggerData = triggerSegments[j][i].getIntData();
                    if (firstEvNum + i != triggerData[0]) {
System.out.println("makeTriggerBankFromRocRaw: event # differs (in Bt# " + buildThreadOrder + ") for ROC id#" +
                        getTagCodaId(inputPayloadBanks[j].getNode().getTag()) + ", expected " +
                        (firstEvNum+i) + " got " + (triggerData[0]) + " (0x" + Integer.toHexString(triggerData[0]) + ")");
                        nonFatalError = true;
                    }

                    // Check for misc data (if we have no timestamps) in at least one place
                    // so we know if we can sparsify or not.
                    if (!haveMiscData && !haveTimestamps && triggerData.length > 1) {
                        haveMiscData = true;
                    }
                }

                // If they exist, store all timestamp related
                // values so consistency can be checked below
                if (checkTimestamps && triggerData.length > 2) {
                    ts = (   ((0xffffL & (long)triggerData[2]) << 32) |
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
            //                if (timestampsMax[i] - timestampsMin[i] > 0)  {
//System.out.println("Timestamps differing by " + (timestampsMax[i] - timestampsMin[i]));
//                }
            for (int i=0; i < numEvents; i++)
                if (timestampsMax[i] - timestampsMin[i] > timestampSlop) {
                    nonFatalError = true;
                    System.out.println("Timestamp NOT consistent: ev #" + (firstEvNum + i) + ", diff = " +
                                               (timestampsMax[i] - timestampsMin[i]) + ", allowed = " + timestampSlop);

                    // Print out beginning of all rocs' buffers
                    for (PayloadBuffer inputPayloadBank : inputPayloadBanks) {
                        Utilities.printBuffer(inputPayloadBank.getBuffer(), 0, 10,
                                              "Data from roc " + inputPayloadBanks[i].getSourceName());
                    }
                }
        }

        //----------------------------
        // Calculate segment of long data
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


        try {
            // TODO: check space requirements first, expand if necessary

            // Start trigger bank
            builder.openBank(trigTag.getValue(), inputPayloadBanks.length,
                             DataType.SEGMENT);

            //----------------------------
            // 1) Add segment of long data
            //----------------------------
            builder.openSegment(ebId, DataType.ULONG64);
            builder.addLongData(longData);
            builder.closeStructure();

            //----------------------------
            // 2) Add segment of event types
            //----------------------------
            builder.openSegment(ebId, DataType.USHORT16);
            builder.addShortData(evData);
            builder.closeStructure();

            //-------------------------------------------------------
            // 3) Add ROC-specific segments if not sparsifying
            //-------------------------------------------------------
            if (!sparsify) {
                // now add one segment for each ROC with ROC-specific data in it
                int intCount, dataLenFromEachSeg;
                EvioNode oldRocSeg;

                // for each ROC ...
                for (int i=0; i < numROCs; i++) {
                    builder.openSegment(inputPayloadBanks[i].getSourceId(), DataType.UINT32);

                    // Copy over all ints except the first which is the event Number
                    // and is stored in common. Assume, for now, that each ROC segment
                    // in the trigger bank has the same amount of data.

                    oldRocSeg = triggerSegments[i][0];
                    // (- 1) forget about event # that we already took care of
                    dataLenFromEachSeg = oldRocSeg.getLength() - 1;

                    // total amount of new data for a new (ROC) segment
                    intCount = numEvents * dataLenFromEachSeg;
                    int[] newData = new int[intCount];

                    int[] oldData;
                    int position = 0;
                    for (int j=0; j < numEvents; j++) {
                        oldData = trigIntArrays[i][j];
                        // = ByteDataTransformer.toIntArray(triggerSegments[i][j].getByteData(false));
                        if (oldData.length != dataLenFromEachSeg + 1) {
                            throw new EmuException("Trigger segments contain different amounts of data");
                        }
                        System.arraycopy(oldData, 1, newData, position, dataLenFromEachSeg);
                        position += dataLenFromEachSeg;
                    }

                    builder.addIntData(newData);
                    builder.closeStructure();
                }
            }

            builder.closeStructure();

        }
        catch (EvioException e) {
            // not enough room
            e.printStackTrace();
        }

        return nonFatalError;
    }


    /**
     * Build a single physics event with the given trigger bank and the given array of Physics events.
     *
     * @param inputPayloadBanks array containing Physics events that will be built together
     * @param builder object used to build event
     */
    public static void buildPhysicsEventWithPhysics(PayloadBuffer[] inputPayloadBanks,
                                                    CompactEventBuilder builder) {

        int childrenCount;
        EvioNode dataBlock;

        try {
            // add all data banks (from payload banks) which are already wrapped properly
            for (PayloadBuffer inputBank : inputPayloadBanks) {
                childrenCount = inputBank.getNode().getChildCount();

                for (int j = 0; j < childrenCount; j++) {
                    dataBlock = inputBank.getNode().getChildNodes().get(j);
                    // ignore the built trigger bank (should be first one)
                    if (Evio.isBuiltTriggerBank(dataBlock)) {
                        continue;
                    }
                    builder.addEvioNode(dataBlock);
                }
            }
        } catch (EvioException e) {
            // not enough room in buffer
        }
    }


    /**
     * Build a single physics event with the given trigger bank
     * and the given array of ROC raw records.
     * Currently, no swapping is done since the bank in question may
     * contain anything.
     *
     * @param inputPayloadBanks array containing ROC raw records that will be built together
     * @param builder object used to build trigger bank
     * @return bank final built event
     */
    public static EvioBank buildPhysicsEventWithRocRaw(PayloadBuffer[] inputPayloadBanks,
                                                       CompactEventBuilder builder) {

        int childrenCount, i=0;
        EvioNode blockBank, inputBank;

        try {
            // Wrap and add data block banks (from payload banks).
            // Use the same header to wrap data blocks as used for payload bank.
            for (PayloadBuffer inputPayloadBank : inputPayloadBanks) {
                // Get Roc Raw node
                inputBank = inputPayloadBank.getNode();

                // Create physics data bank with same tag & num as Roc Raw bank
                // in order to wrap the data block bank from Roc.
                builder.openBank(inputBank.getTag(), inputBank.getNum(), DataType.BANK);
//System.out.println("    created a wrapping data-bank of tag " + inputBank.getTag() +
//", num " + inputBank.getNum());

                // How many banks inside Roc Raw bank ?
                childrenCount = inputBank.getChildCount();

//System.out.println("    roc raw ev #" + (i++) + " has " + childrenCount + " banks");
                // Add Roc Raw's data block banks to our data bank.
                // Ignore any trigger bank. If in SEM, there is no trigger bank.
                // The trigger bank is always the first one.
                int j = 1;

                for (; j < childrenCount; j++) {
                    blockBank = inputBank.getChildNodes().get(j);
                    // -------------------- NOTE -----------------------------
                    // If blockBank has data of opposite endian to our buffer,
                    // we can still add it (although less efficient).
                    // However, NO data swapping is done, only headers.
                    // -------------------------------------------------------

                    // Don't mess with user data banks since we don't know what's inside.
                    // Just copy whole thing over (with swapped headers if necessary).
//System.out.println("    try adding data-block-bank to wrapping data-bank");
                    builder.addEvioNode(blockBank);
                }

                builder.closeStructure();
            }

        } catch (EvioException e) {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * Generate a single data block bank of fake data.
     *
     * @param firstEvNum    starting event number
     * @param words         amount of data in 32bit words besides event # & timestamp
     * @param isSEM         in single event mode if <code>true</code>
     * @param timestamp     48-bit timestamp used only in single event mode
     */
    public static int[] generateData(int firstEvNum, int words,
                                      boolean isSEM, long timestamp) {

        int index=0;

        int[] data;
        if (isSEM) {
            data = new int[3 + words];
        }
        else {
            data = new int[1 + words];
        }

        // First put in starting event # (32 bits)
        data[index++] = firstEvNum;

        // if single event mode, put in timestamp
        if (isSEM) {
            data[index++] = (int)  timestamp; // low 32 bits
            data[index++] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
        }

        for (int i=0; i < words; i++) {
            data[index+i] = i;
        }

        return data;
    }


    /**
     * Create an Evio ROC Raw record event/bank to be placed in a Data Transport record.
     *
     * @param rocID       ROC id number
     * @param triggerType trigger type id number (0-15)
     * @param detectorId  id of detector producing data in data block bank
     * @param status      4-bit status associated with data
     * @param eventNumber starting event number
     * @param numEvents   number of physics events in created record
     * @param timestamp   starting event's timestamp
     * @param buf         ByteBuffer in which to write generated event
     * @param builder     used to build evio events in buffer acquired from bbSupply
     *
     * @return created ROC Raw Record (EvioEvent object)
     * @throws EvioException
     */
    public static void createRocRawRecordFast(int rocID,       int triggerType,
                                              int detectorId,  int status,
                                              int eventNumber, int numEvents,
                                              long timestamp,  ByteBuffer buf,
                                              CompactEventBuilder builder)
            throws EvioException {

        // Create a ROC Raw Data Record event/bank with numEvents physics events in it
        builder.setBuffer(buf);

        int rocTag = createCodaTag(status, rocID);
        builder.openBank(rocTag, numEvents, DataType.BANK);

        // Create the trigger bank (of segments)
        builder.openBank(CODATag.RAW_TRIGGER_TS.getValue(), numEvents, DataType.SEGMENT);

        int[] segmentData = new int[3];
        for (int i = 0; i < numEvents; i++) {
            // Each segment contains eventNumber & timestamp of corresponding event in data bank
            builder.openSegment(triggerType, DataType.UINT32);
            // Generate 3 segments per event (no miscellaneous data)
            segmentData[0] = eventNumber + i;
            segmentData[1] = (int)  timestamp; // low 32 bits
            segmentData[2] = (int) (timestamp >>> 32 & 0xFFFF); // high 16 of 48 bits
            timestamp += 4;

            builder.addIntData(segmentData);
            builder.closeStructure();
        }
        // Close trigger bank
        builder.closeStructure();

        // Create a single data block bank
//        int []data = generateData(eventNumber, 5, false, timestamp);
        int []data = generateData(eventNumber, numEvents * 40, false, timestamp);
//        int []data = generateData(eventNumber, 0, false, timestamp);

        int dataTag = createCodaTag(status, detectorId);
        builder.openBank(dataTag, numEvents, DataType.UINT32);
        builder.addIntData(data);

        builder.closeAll();
    }


}
