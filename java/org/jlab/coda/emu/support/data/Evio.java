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
import java.util.ArrayList;

/**
 * <p>This class is used as a layer on top of evio to handle CODA3 specific details.
 * The EMU will received evio data in standard CODA3 output (same as file format)
 * which contains banks - in this case, ROC Raw Records and Physics Events all of
 * which are in formats given below.</p>
 *
 * <pre><code>
 * ####################################
 * Network Transfer Evio 4 Output Format:
 * ####################################
 *
 * MSB(31)                          LSB(0)
 * &lt;---  32 bits ------------------------&gt;
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
 * &lt;---  32 bits ------------------------&gt;
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
 * &lt;---  32 bits ------------------------&gt;
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
 * &lt;---  32 bits ------------------------&gt;
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
 * |______ Run Number &amp; Run Type ________|    |
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
 * |                ...                  |   or sparsified and no timestamp
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
 * &lt;---  32 bits ------------------------&gt;
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
 * </code></pre>
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
                System.out.println("checkPayload: buf source Id (" + sourceId +
                                           ") != buf's id from tag (" + getTagCodaId(tag) + ')');
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
     * Check the given payload buffer for correct record id, source id.
     * Store error info in payload buffer.
     *
     * @param pBuf          payload buffer to be examined
     * @param channel       input channel buffer is from
     * @param eventType     type of input event in buffer
     * @param inputNode     EvioNode object representing event
     * @param recordIdError non-fatal record id error found in
     *                      {@link #checkInputType(int, DataChannel, EventType, EvioNode)}.
     */
    public static void checkStreamInput(PayloadBuffer pBuf, DataChannel channel,
                                        EventType eventType, EvioNode inputNode,
                                        boolean recordIdError) {

        int sourceId = pBuf.getSourceId();
        boolean nonFatalError = false;

        // Only worry about record id if event to be built.
        // Initial recordId stored is 0, ignore that.
        if (eventType != null && eventType.isROCRawStream()) {
            if (sourceId != inputNode.getTag()) {
                System.out.println("checkInput: buf source Id (" + sourceId +
                        ") != buf's id from tag (0x" + Integer.toHexString(inputNode.getTag()) + ')');
                nonFatalError = true;
            }
        }

        // Check source ID of bank to see if it matches channel id
        if (!pBuf.matchesId()) {
            System.out.println("checkInput: buf source id = " + sourceId +
                    " != input channel id = " + channel.getID());
            nonFatalError = true;
        }

        pBuf.setNonFatalBuildingError(nonFatalError || recordIdError);
    }


    /**
      * Check the given payload buffer for correct record id, source id.
      * Store sync and error info in payload buffer.
      *
      * @param pBuf          payload buffer to be examined
      * @param channel       input channel buffer is from
      * @param eventType     type of input event in buffer
      * @param inputNode     EvioNode object representing event
      * @param recordIdError non-fatal record id error found in
      *                      {@link #checkInputType(int, DataChannel, EventType, EvioNode)}.
      */
     public static void checkInput(PayloadBuffer pBuf, DataChannel channel,
                                   EventType eventType, EvioNode inputNode,
                                   boolean recordIdError) {

         int sourceId = pBuf.getSourceId();
         boolean nonFatalError = false;

         // Only worry about record id if event to be built.
         // Initial recordId stored is 0, ignore that.
         if (eventType != null && eventType.isBuildable()) {
             int tag = inputNode.getTag();

             if (sourceId != getTagCodaId(tag)) {
                 System.out.println("checkInput: buf source Id (" + sourceId +
                                            ") != buf's id from tag (" + getTagCodaId(tag) + ')');
                 nonFatalError = true;
             }

             pBuf.setSync(Evio.isTagSyncEvent(tag));
             pBuf.setError(Evio.tagHasError(tag));
         }

         // Check source ID of bank to see if it matches channel id
         if (!pBuf.matchesId()) {
 System.out.println("checkInput: buf source id = " + sourceId +
                            " != input channel id = " + channel.getID());
             nonFatalError = true;
         }

         pBuf.setNonFatalBuildingError(nonFatalError || recordIdError);
     }


    /**
     * Check the data coming over a channel for sequential record ids.
     * Send warning as printout.
     *
     * @param recordId  id associated with an incoming evio event
     *                  (each of which may contain several physics events)
     * @param expectedRecordId  expected record id
     * @param print if true, print out when record id is out-of-seq.
     * @param eventType type of input event
     * @param channel   input channel data is from
     * @return next expected record id
     *
     */
    public static int checkRecordIdSequence(int recordId, int expectedRecordId, boolean print,
                                            EventType eventType, DataChannel channel) {

        if (eventType.isBuildable()) {
            // The recordId associated with each bank is taken from the first
            // evio block header in a single ET/emu-socket/cMsg-msg data buffer.
            // For a physics or ROC raw type, it should start at zero and increase
            // by one in the first evio block header of the next ET/cMsg-msg data buffer.
            // NOTE: There may be multiple banks from the same buffer and
            // they will all have the same recordId.

            if (recordId != expectedRecordId) {
                if (print) {
                    System.out.println("checkRecordIdSequence: record ID out of sequence, got " + recordId +
                                               ", expecting " + expectedRecordId + ", type = " + eventType +
                                               ", name = " + channel.name());
                }
                // Reset the next expected id so we don't get a shower of printout
                return recordId + 1;
            }
            else {
                return expectedRecordId + 1;
            }
        }
        return expectedRecordId;
    }


     /**
      * Check the given payload buffer for event type
      * (physics, ROC raw, control, or user) as well as evio structure type.
      *
      * @param recordId  id associated with an incoming evio event
      *                  (each of which may contain several physics events)
      * @param channel   input channel buffer is from
      * @param eventType type of input event
      * @param inputNode EvioNode object representing event
      * @return true if there is a non-fatal error with mismatched record ids, else false
      * @throws EmuException if event is unknown type;
      *                      if physics or roc raw bank has improper format
      *
      */
     public static boolean checkInputType(int recordId, DataChannel channel,
                                          EventType eventType, EvioNode inputNode)
             throws EmuException {

         // Only interested in physics, roc raw, control, and user events.
         if (eventType == null || !eventType.isEbFriendly()) {
             throw new EmuException("unknown event type");
         }

         boolean nonFatalRecordIdError = false;

         if (eventType.isBuildable()) {
             // The recordId associated with each bank is taken from the first
             // evio block header in a single ET/emu-socket/cMsg-msg data buffer.
             // For a physics or ROC raw type, it should start at zero and increase
             // by one in the first evio block header of the next ET/cMsg-msg data buffer.
             // NOTE: There may be multiple banks from the same buffer and
             // they will all have the same recordId.
             int chanRecordId = channel.getRecordId();

             if (recordId != chanRecordId &&
                 recordId != chanRecordId + 1) {
                 System.out.println("checkInputType: record ID out of sequence, got " + recordId +
                                            ", expecting " + chanRecordId + " or " +
                                            (chanRecordId+1) + ", type = " + eventType +
                                            ", name = " + channel.name());
                 nonFatalRecordIdError = true;
             }
             // Store the current value here as a convenience for the next comparison
             channel.setRecordId(recordId);

             // Pick this event apart a little
             if (!inputNode.getDataTypeObj().isBank()) {
                 DataType eventDataType = inputNode.getDataTypeObj();
                 throw new EmuException("ROC raw / physics record contains " + eventDataType +
                                                " instead of banks (data corruption?)");
             }
         }

         return nonFatalRecordIdError;
      }


    /**
     * <p>Check each payload bank - one from each input channel - for a number of issues:</p>
     * <ol>
     * <li>if there are any sync bits set, all must be sync banks
     * <li>the ROC ids of the banks must be unique
     * <li>at this point all banks are either physics events or ROC raw records,
     *     but must be identical types
     * <li>there are the same number of events in each bank
     * </ol>
     *
     * @param buildingBanks array containing banks that will be built together
     * @param eventNumber   first event number in each bank (used for diagnostic output).
     *                      Currently event # will not be valid for SEB with multiple streams.
     * @param entangledCount also called blockLevel, it's the number of events entangled together
     *                       in one data bank.
     * @return <code>true</code> if non-fatal error occurred, else <code>false</code>
     * @throws EmuException if some physics and others ROC raw event types;
     *                      if there are a differing number of events in each payload bank;
     *                      if some events have a sync bit set and others do not.
     */
    public static boolean checkConsistency(PayloadBuffer[] buildingBanks, long eventNumber, int entangledCount)
            throws EmuException {
        boolean nonFatalError = false;

        // For each data record check the sync bit
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
        if (syncBankCount > 0) {
//            System.out.println("  EB mod: event first 8 words: ");
//            for (PayloadBuffer buildingBank : buildingBanks) {
//                Utilities.printBufferBytes(buildingBank.getNode().getStructureBuffer(true),
//                                           0, 32, buildingBank.getSourceName() );
//                System.out.println();
//            }

            if (syncBankCount != numBanks) {
                // Some banks are sync banks and some are not, currently event # will not be valid
                // for SEB with multiple streams.
                // A single buildingBank will have "entangledCount" number of physics events mixed together.
                // In this scenario, the offending sync event will be the LAST of the bunch.
                System.out.print("  EB mod: these channels have NO sync at event " + (eventNumber + entangledCount - 1) + ": ");
                for (PayloadBuffer buildingBank : buildingBanks) {
                    if (!buildingBank.isSync()) {
                        System.out.print(buildingBank.getSourceName() + ", ");
                    }
                }
                System.out.println();
                nonFatalError = true;
                //throw new EmuException("events out of sync at event " + (eventNumber + entangledCount - 1));
            }
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
     *                      if events contain mixture of streaming and triggered DAQ sources;
     *                      if prestart events contain bad data
     */
    public static void gotConsistentControlEvents(PayloadBuffer[] buildingBanks,
                                                  int runNumber, int runType)
            throws EmuException {

        boolean debug = false;

        // Make sure all are the same type of control event
        boolean firstStreamType = buildingBanks[0].isStreaming();
        ControlType firstControlType = buildingBanks[0].getControlType();
        for (PayloadBuffer buf : buildingBanks) {
            if (buf.getControlType() != firstControlType) {
                throw new EmuException("different type control events on each channel");
            }
            if (buf.isStreaming() != firstStreamType) {
                throw new EmuException("streaming and triggered channels mixed");
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
     * <pre><code>
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
     * </code></pre>
     *
     * @param type            control type, must be SYNC, PRESTART, GO, PAUSE, or END
     * @param runNumber       current run number for prestart event
     * @param runType         current run type for prestart event
     * @param eventsInRun     number of events so far in run for all except prestart event
     * @param framesInRun     number of time frames so far in run, if streaming
     * @param eventsSinceSync number of events since last sync for sync event
     * @param error           END control event sent due to error condition
     * @param isStreaming     true if data is being streamed in time slices/frames,
     *                        false if triggered events
     *
     * @return created Control event (EvioEvent object)
     */
    public static EvioEvent createControlEvent(ControlType type, int runNumber, int runType,
                                               int eventsInRun, int framesInRun, int eventsSinceSync,
                                               boolean error, boolean isStreaming) {

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


    // TODO: For STREAMING, event count no longer makes any sense!!!

    /**
     * Create a Control event with a ByteBuffer which is ready to read.
     *
     * @param type            control type, must be SYNC, PRESTART, GO, PAUSE, or END
     * @param runNumber       current run number for prestart event
     * @param runType         current run type for prestart event
     * @param eventsInRun     number of events so far in run for all except prestart event
     * @param framesInRun     number of time frames so far in run, if streaming
     * @param eventsSinceSync number of events since last sync for sync event
     * @param sourceName      name of input source
     * @param order           byte order in which to write event into buffer
     * @param error           END control event sent due to error condition
     * @param isStreaming     true if data is being streamed in time slices/frames,
     *                        false if triggered events
     *
     * @return created PayloadBuffer object containing Control event in byte buffer
     */
    public static PayloadBuffer createControlBuffer(ControlType type, int runNumber,
                                                    int runType, int eventsInRun, int framesInRun,
                                                    int eventsSinceSync, ByteOrder order,
                                                    String sourceName,
                                                    boolean error, boolean isStreaming) {

        try {
            int[] data;
            CompactEventBuilder builder = new CompactEventBuilder(20, order);

            // Current time in seconds since Jan 1, 1970 GMT
            int time = (int) (System.currentTimeMillis()/1000L);

            // create a single bank of integers which is the user bank
            switch (type) {
                case SYNC:
                    // sync not used in streaming
                    data = new int[] {time, eventsSinceSync, eventsInRun};
                    break;
                case PRESTART:
                    data = new int[] {time, runNumber, runType};
                    break;
                case GO:
                case PAUSE:
                case END:
                default:
                    int count = eventsInRun;
                    if (isStreaming) count = framesInRun;

                    if (error) {
                        data = new int[]{time, 0xffffffff, count};
                    }
                    else {
                        data = new int[]{time, 0, count};
                    }
            }

            builder.openBank(type.getValue(), 0, DataType.UINT32);
            builder.addIntData(data);
            builder.closeStructure();
            PayloadBuffer pBuf = new PayloadBuffer(builder.getBuffer());  // Ready to read buffer
            pBuf.setEventType(EventType.CONTROL);
            pBuf.setControlType(type);
            // Do this so we can use fifo as output & get accurate stats
            pBuf.setEventCount(1);
            pBuf.setSourceName(sourceName);

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
     * Many of the inputs are externally provided arrays used here in order to avoid allocating
     * them each time this method is called. Some, like rocNodes, triggerBanks, and
     * inputBuffers, are quickly found in FastEventBuilder and easy to pass in.
     *
     * @param inputPayloadBanks array containing a bank (Physics event) from each channel's
     *                          payload buffer queue that will be built into one event.
     * @param rocNodes          array of EvioNodes of input banks.
     * @param triggerBanks      array of EvioNodes of input trigger banks.
     * @param inputBuffers      array of ByteBuffers of input banks.
     * @param evBuf             ByteBuffer in which event is being built.
     * @param ebId              id of event builder calling this method.
     * @param runNumber         run number to place in trigger bank.
     * @param runType           run type to place in trigger bank.
     * @param includeRunData    if <code>true</code>, add run number and run type.
     * @param sparsify          if <code>true</code>, do not add roc specific segments if no relevant data.
     * @param checkTimestamps   if <code>true</code>, check timestamp consistency and
     *                          return false if inconsistent, include them in trigger bank
     * @param fastCopyReady     if <code>true</code>, ...
     * @param timestampSlop     maximum number of timestamp ticks that timestamps can differ
     *                          for a single event before the error bit is set in a bank's
     *                          status. Only used when checkTimestamps arg is <code>true</code>.
     * @param returnLen         array in which to return index in evBuf just past trigger bank.
     * @param longData          array used to store long data of trigger bank.
     * @param commonLong        array used to hold long trigger data for comparison of inputs.
     * @param firstInputCommonLong  array used to hold long trigger data from first input.
     * @param timestampsMin     array used to store min timestamp for each event.
     * @param timestampsMax     array used to store max timestamp for each event.
     * @param eventTypes        array used to hold short trigger data for comparison of inputs.
     * @param eventTypesRoc1    array used to store short trigger data from first input.
     *
     * @return <code>true</code> if non-fatal error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean makeTriggerBankFromPhysics(PayloadBuffer[] inputPayloadBanks,
                                                     EvioNode[] rocNodes,
                                                     EvioNode[] triggerBanks,
                                                     ByteBuffer[] inputBuffers,
                                                     ByteBuffer evBuf,
                                                     int ebId,
                                                     int runNumber, int runType,
                                                     boolean includeRunData,
                                                     boolean sparsify,
                                                     boolean checkTimestamps,
                                                     boolean fastCopyReady,
                                                     int timestampSlop,
                                                     int[]   returnLen,
                                                     long[]  longData,
                                                     long[]  commonLong,
                                                     long[]  firstInputCommonLong,
                                                     long[]  timestampsMin,
                                                     long[]  timestampsMax,
                                                     short[] eventTypes,
                                                     short[] eventTypesRoc1)
            throws EmuException {

        int rocCount;
        int totalRocCount = 0, validLongs = 0, validShorts = 0, validShortsRoc1 = 0;
        int numInputBanks = inputPayloadBanks.length;

        CODATag tag;
        EvioNode triggerBank;

        boolean nonFatalError = false;
        boolean allHaveRunData = true;
        boolean haveTimestamps=true, isTimestamped;
        boolean haveTrigWithNoRocSpecificData=false;
        boolean firstTrigTimestamped = false;

        EvioNode eventTypesSeg = null;
        long firstEventNumber = 0L;

        // Number of events in each payload bank
        int numEvents = rocNodes[0].getNum();

        // In each payload bank (of banks) is a built trigger bank. Extract them all.
        for (int i=0; i < numInputBanks; i++) {

            // store stuff
            inputPayloadBanks[i].setEventCount(numEvents);

            // Find the built trigger bank - first child bank
            triggerBank = triggerBanks[i];

            tag = CODATag.getTagType(triggerBank.getTag());
            if (tag == null || !Evio.isBuiltTriggerBank(triggerBank)) {
                throw new EmuException("No built trigger bank or bad tag in physics event");
            }

            if (i==0) {
                eventTypesSeg   = triggerBanks[0].getChildAt(1);
                eventTypesRoc1  = eventTypesSeg.getShortData(eventTypesRoc1, returnLen);
                validShortsRoc1 = returnLen[0];
                firstInputCommonLong  = triggerBanks[0].getChildAt(0).getLongData(firstInputCommonLong, returnLen);
                validLongs      = returnLen[0];
                firstEventNumber     = firstInputCommonLong[0];
                firstTrigTimestamped = tag.hasTimestamp();
            }
            else {
                // Event-type stuff
                eventTypes  = triggerBank.getChildAt(1).getShortData(eventTypes, returnLen);
                validShorts = returnLen[0];
                if (validShortsRoc1 != validShorts) {
                    throw new EmuException("Trying to merge records with different numbers of events");
                }
                for (int j=0; j < validShortsRoc1; j++) {
                    if (eventTypesRoc1[j] != eventTypes[j]) {
                        throw new EmuException("Trying to merge records with different event types");
                    }
                }
            }

            // Number of rocs in this trigger bank
            rocCount = triggerBank.getNum();

            // Total number of rocs
            totalRocCount += rocCount;

            //--------------------------------
            // run specific and timestamp data
            //--------------------------------

            // Do all trigger banks have run number & type data?
            allHaveRunData = allHaveRunData && tag.hasRunData();

            // Does this trigger have timestamps?
            isTimestamped = tag.hasTimestamp();

            // Do all trigger banks have timestamp data?
            haveTimestamps = haveTimestamps && isTimestamped;

            // If one has a timestamp, all must
            if (firstTrigTimestamped != isTimestamped) {
                throw new EmuException("If 1 trigger bank has timestamps, all must");
            }

            //--------------------------------
            // roc specific data
            //--------------------------------

//                System.out.println("makeTriggerBankFromPhysics: # Rocs = " + rocCount +
//                                           ", haveRunData = " + tag.hasRunData() +
//                                           ", hasRocSpecific data = " + tag.hasRocSpecificData() +
//                                   ", isTimeStamped = " + isTimestamped);


            // Sanity check:  Does this trigger have roc-specific data?
            if (tag.hasRocSpecificData()) {
                // Number of trigger bank children must = # rocs + 2
                if (triggerBank.getChildCount() != rocCount + 2) {
                    throw new EmuException("Trigger bank does not have correct # of segments (" +
                                                   (rocCount + 2) + "), it has " + triggerBank.getChildCount());
                }
            }
            else {
                // Is there at least one trigger bank without run specific data?
                haveTrigWithNoRocSpecificData = true;

                // If sparsified, there are no segments for roc-specific data
                // so the number of trigger bank children must = 2
                if (triggerBank.getChildCount() != 2) {
                    throw new EmuException("Trigger bank does not have correct # of segments (2), it has " +
                                                   triggerBank.getChildCount());
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
        //    MSB(63)                LSB(0)
        //    _____________________________
        //    |     first event number    |
        //    |       avg timestamp1      |
        //    |            ...            |
        //    |       avg timestampM      |
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

        int triggerTag;

        // 1) Create common data segment of longs, with first event #,
        //    avg timestamps, and possibly run number & run type in it.
        int longDataLen;
        if (!checkTimestamps) {
            if (includeRunData) {
                longData[0] = firstEventNumber;
                longData[1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                longDataLen = 2;
                if (sparsify) {
                    triggerTag = CODATag.BUILT_TRIGGER_RUN_NRSD.getValue();
                }
                else {
                    triggerTag = CODATag.BUILT_TRIGGER_RUN.getValue();
                }
            }
            else {
                longData[0] = firstEventNumber;
                longDataLen = 1;
                if (sparsify) {
                    triggerTag = CODATag.BUILT_TRIGGER_NRSD.getValue();
                }
                else {
                    triggerTag = CODATag.BUILT_TRIGGER_BANK.getValue();
                }
            }
        }
        // Put in avg timestamps if doing timestamp checking in loop below
        else {
            if (includeRunData) {
                longData[0] = firstEventNumber;
                longData[numEvents+1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                longDataLen = numEvents + 2;
                triggerTag = CODATag.BUILT_TRIGGER_TS_RUN.getValue();
            }
            else {
                longData[0] = firstEventNumber;
                longDataLen = numEvents + 1;
                triggerTag = CODATag.BUILT_TRIGGER_TS.getValue();
            }
        }

        // Checking timestamps
        long ts;
        long[] longArray;

        // Check consistency of common data across events
        for (int i=0; i < numInputBanks; i++) {
            if (i > 0) {
                // long stuff
                longArray = triggerBanks[i].getChildAt(0).getLongData(commonLong, returnLen);
                int len = returnLen[0];

                // check event number
                if (firstEventNumber != commonLong[0]) {
                    throw new EmuException("Trying to merge records with different event numbers: 1st bank = " +
                    firstEventNumber + ", bank from chan " + i + " = " + commonLong[0]);
                }

                // check run number & type if all such data is present
                if (allHaveRunData) {
                    if (validLongs == 2+numEvents &&
                               len == 2+numEvents)  {
                        // if run # and/or type are different ...
                        if (firstInputCommonLong[1+numEvents] != commonLong[1+numEvents]) {
                            throw new EmuException("Trying to merge records with different run numbers and/or types");
                        }
                    }
                }
            }
            else {
                longArray = firstInputCommonLong;
            }

            // store timestamp info
            if (checkTimestamps) {
                // timestamp data is not all there even though tag says it is
                if (validLongs < numEvents + 1) {
                    nonFatalError = true;
                    checkTimestamps = false;
System.out.println("Timestamp data is missing!");
                }
                else {
                    // for each event find avg, max, & min
                    for (int j=0; j < numEvents; j++) {
                        ts = longArray[j+1];
                        longData[j+1] += ts;
                        timestampsMax[j] = (ts >= timestampsMax[j]) ? ts : timestampsMax[j];
                        timestampsMin[j] = (ts <= timestampsMin[j]) ? ts : timestampsMin[j];
                    }
                }
            }
        }


        // Now that we have all timestamp info, check them against each other.
        // Allow a slop of TIMESTAMP_SLOP from the max to min.
        if (checkTimestamps) {
            for (int j=0; j < numEvents; j++) {
                // finish calculation to find average
                longData[j+1] /= numInputBanks;

                if (timestampsMax[j] - timestampsMin[j] > timestampSlop)  {
                    nonFatalError = true;
System.out.println("Timestamp NOT consistent: ev #" + (firstEventNumber + j) + ", diff = " +
                  (timestampsMax[j] - timestampsMin[j]) + ", allowed = " + timestampSlop );
                }
            }
        }

        // Skip over event's bank header and the trigger bank length word
        int writeIndex = 12;

        // Top bank len is not written yet. Top bank 2nd word is already written.

        //------------------------------------------------------------------
        // Trig bank header: 1) len not written yet, 2) write 2nd word now
        //------------------------------------------------------------------
        int headerWord = (triggerTag << 16) |
                         ((DataType.SEGMENT.getValue() & 0x3f) << 8) |
                         (totalRocCount & 0xff);
        evBuf.putInt(writeIndex, headerWord);
        writeIndex += 4;

        //----------------------------
        // Add segment of long data
        //----------------------------

        // Header
        headerWord = (ebId << 24) |
                     ((DataType.ULONG64.getValue() & 0x3f) << 16) |
                     (2*longDataLen & 0xffff);
        evBuf.putInt(writeIndex, headerWord);
        writeIndex += 4;

        // Data
        for (int i=0; i < longDataLen; i++) {
            evBuf.putLong(writeIndex, longData[i]);
            writeIndex += 8;
        }

        //----------------------------
        // Add segment of event types
        //----------------------------

        // Header
        int padding = eventTypesSeg.getPad();

        headerWord = (ebId << 24) |
                     (padding << 22) |
                     ((DataType.USHORT16.getValue() & 0x3f) << 16) |
                     (eventTypesSeg.getLength() & 0xffff);
        evBuf.putInt(writeIndex, headerWord);
        writeIndex += 4;

        // Data
        for (int i=0; i < numEvents; i++) {
            evBuf.putShort(writeIndex, eventTypesRoc1[i]);
            writeIndex += 2;
        }
        writeIndex += padding;

        //-------------------------------------------------------
        // 3) Add ROC-specific segments if not sparsifying
        //-------------------------------------------------------

        if (!sparsify) {
            int segBytes, srcPos;
            EvioNode trigBank, trigSeg;
            ByteBuffer inputBuffer;

            // For each trigger bank (from previous level EBs) ...
            for (int i=0; i < numInputBanks; i++) {
                trigBank    = triggerBanks[i];
                inputBuffer = inputBuffers[i];

                // Copy over each roc segment in trigger bank to combined trigger bank.
                // Skip over timestamp seg and event type seg.
                for (int j=2; j < trigBank.getChildCount(); j++) {
                    trigSeg = trigBank.getChildAt(j);

                    // Copy over data
                    if (fastCopyReady) {
                        // How many bytes to copy?
                        segBytes = trigSeg.getTotalBytes();

                        System.arraycopy(inputBuffer.array(), trigSeg.getPosition(),
                                         evBuf.array(), writeIndex, segBytes);
                    }
                    else {
                        // Write header
                        headerWord = (trigSeg.getTag() << 24) |
                                     ((trigSeg.getType() & 0x3f) << 16) |
                                     (trigSeg.getLength() & 0xffff);

                        evBuf.putInt(writeIndex, headerWord);
                        writeIndex += 4;

                        // How much data and where is it?
                        segBytes = 4*trigSeg.getDataLength();
                        srcPos = trigSeg.getDataPosition();

                        ByteBuffer duplicateBuf = inputBuffer.duplicate();
                        duplicateBuf.limit(srcPos + segBytes).position(srcPos);

                        evBuf.position(writeIndex);
                        // This method is relative to position
                        evBuf.put(duplicateBuf);
                        evBuf.position(0);
                    }

                    writeIndex += segBytes;
                }
            }
        }

        // Now go back and write trigger bank header since we have length.
        // Note, writeIndex starts at beginning of built event so skip over
        // 1st bank header and then trig bank length words.
        evBuf.putInt(8, writeIndex/4 - 3);
        returnLen[0] = writeIndex;

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
     * Many of the inputs are externally provided arrays used here in order to avoid allocating
     * them each time this method is called. Some, like rocNodes, rocRecord, and rocOffset,
     * are quickly found in FastEventBuilder and easy to pass in.
     *
     * @param inputPayloadBanks array containing a bank (ROC Raw) from each channel's
     *                          payload bank queue that will be built into one event.
     * @param builtEventBuf     ByteBuffer of event being built.
     * @param ebId              id of event builder calling this method.
     * @param firstEventNumber  event number to place in trigger bank.
     * @param runNumber         run number to place in trigger bank.
     * @param runType           run type to place in trigger bank.
     * @param includeRunData    if <code>true</code>, add run number and run type.
     * @param sparsify          if <code>true</code>, do not add roc specific segments if no such data.
     * @param checkTimestamps   if <code>true</code>, check timestamp consistency and
     *                          return false if inconsistent, include them in trigger bank.
     * @param timestampSlop     maximum number of timestamp ticks that timestamps can differ
     *                          for a single event before the error bit is set in a bank's
     *                          status. Only used when checkTimestamps arg is <code>true</code>.
     * @param buildThreadOrder  for debug printout, which build thread.
     * @param longData          long array, passed in to avoid unnecessary object creation.
     * @param evData            short array, passed in to avoid unnecessary object creation.
     * @param segmentData       int array, passed in to avoid unnecessary object creation.
     * @param returnLen         int array used to return index into builtEventBuf of where to write.
     * @param rocOffset         array of rocNodes' backing buffer offsets.
     * @param rocRecord         array of rocNodes' backing buffers.
     * @param rocNodes          array of EvioNodes of input banks.
     * @param fastCopyReady     if <code>true</code>, roc data buffer backing byte arrays and
     *                          EB's ByteBufferSupply buffers both have the same endian value.
     *
     * @return <code>true</code> if recoverable error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean makeTriggerBankFromRocRaw(PayloadBuffer[] inputPayloadBanks,
                                                    ByteBuffer builtEventBuf,
                                                    int ebId,
                                                    long firstEventNumber,
                                                    int runNumber, int runType,
                                                    boolean includeRunData,
                                                    boolean sparsify,
                                                    boolean checkTimestamps,
                                                    int timestampSlop,
                                                    int buildThreadOrder,
                                                    long[] longData,
                                                    short[] evData,
                                                    int[] segmentData,
                                                    int[] returnLen,
                                                    int[] rocOffset,   // in bytes
                                                    ByteBuffer[] rocRecord,
                                                    EvioNode[] rocNodes,
                                                    boolean fastCopyReady)
            throws EmuException {

        boolean turnOffChecks = false;

        int tag, firstTrigTag=0, rocTrigBankCount=0;
        // Number of rocs
        int numROCs = inputPayloadBanks.length;
        // Number of rocs with timestamp info
        int numRocsWithTSs;
        int numEvents = inputPayloadBanks[0].getNode().getNum();

        EvioNode rocNode, trigBank;
        boolean haveTimestamps, haveMiscData=false, nonFatalError=false;
        boolean fatalError=false, firstTagFound=false;
        EvioNode triggerSegment;

        CODATag trigTag;
        // TODO: allow for 32 bit timestamps & check differences between ROCs' TS info

        if (turnOffChecks) {
            checkTimestamps = false;
        }

//        long t1=0L, t2=0L, deltaT1=0L, deltaT2=0L, deltaT3=0L;

//        boolean measureTimes = false;
//         if (statCounter++ == 500000) {
//             measureTimes = true;
//             t1 = System.nanoTime();
//         }

        // In each payload bank (of banks) is a trigger bank. Extract them all.
         for (int i=0; i < numROCs; i++) {

             // Find the trigger bank - first child bank
             rocNode = rocNodes[i];
             trigBank = rocNode.getChildAt(0);
             if (trigBank == null) {
                 fatalError = true;
                 continue;
             }

             // For hardware bug work-around, mask off
             // last 2 bytes of raw trigger bank tag.
             tag = trigBank.getTag() & 0xff1f;

             if (!CODATag.isRawTrigger(tag)) {
                 fatalError = true;
                 continue;
             }

             // Get the first trigger bank's tag
             rocTrigBankCount++;
             if (!firstTagFound) {
                 firstTrigTag = tag;
                 firstTagFound = true;
             }

             inputPayloadBanks[i].setEventCount(numEvents);

             if (!turnOffChecks) {
                 //  Check to see if all TRIGGER banks think they have the same # of events
                 if (numEvents != trigBank.getNum()) {
                     fatalError = true;
                     continue;
                 }

                 // Check to see if all PAYLOAD banks think they have same # of events
                 if (numEvents != rocNode.getNum()) {
                     fatalError = true;
                     continue;
                 }

                 // Check that number of trigger bank children = # events
                 if (trigBank.getChildCount() != numEvents) {
                     fatalError = true;
                     continue;
                 }

                 // Check to see if all trigger bank tags are the same
                 if (tag != firstTrigTag) {
                     fatalError = true;
                     continue;
                 }
             }
         }

         // Do we have at least 1 ROC with a trigger bank?
         if (fatalError && rocTrigBankCount == 0) {
             throw new EmuException("No trigger bank found in any ROC");
         }

         // We check timestamps if told to in configuration AND if timestamps are present
         haveTimestamps = CODATag.hasTimestamp(firstTrigTag);
         if (checkTimestamps && !haveTimestamps) {
             nonFatalError   = true;
             checkTimestamps = false;
         }

//         if (measureTimes) {
//             t2 = System.nanoTime();
//             deltaT1 = t2-t1;
//             t1 = System.nanoTime();
//         }

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
         //    MSB(63)                LSB(0)
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


         //----------------------------
         // Calculate segment of long data
         //----------------------------
         int longDataLen;
         if (!checkTimestamps) {
             if (includeRunData) {
                 longData[0] = firstEventNumber;
                 longData[1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                 longDataLen = 2;
                 trigTag = CODATag.BUILT_TRIGGER_RUN;
             }
             else {
                 longData[0] = firstEventNumber;
                 longDataLen = 1;
                 trigTag = CODATag.BUILT_TRIGGER_BANK;
             }
         }
         // Put avg timestamps in if doing timestamp checking
         else {
             if (includeRunData) {
                 longData[0] = firstEventNumber;
                 // The avg timestamps will be added to longData
                 // in loop below after being calculated
                 longData[numEvents+1] = (((long)runNumber) << 32) | (runType & 0xffffffffL);
                 longDataLen = numEvents + 2;
                 trigTag = CODATag.BUILT_TRIGGER_TS_RUN;
             }
             else {
                 longData[0] = firstEventNumber;
                 longDataLen = numEvents + 1;
                 // The avg timestamps will be added to longData
                 // in loop below after being calculated
                 trigTag = CODATag.BUILT_TRIGGER_TS;
             }
         }


         // It is convenient at this point to check and see if for a given event,
         // across all ROCs, the event number & event type are the same.
         int[] triggerData = null;
         int evNum, firstEvNum = (int) firstEventNumber;
         // For checking the consistency of timestamps
         long ts, timestampsMax, timestampsMin;
         boolean haveTypeInfo;
         int bufOffset;
         ByteBuffer backingBuf;

         for (int i=0; i < numEvents; i++) {

             timestampsMax = 0;
             timestampsMin = Long.MAX_VALUE;
             evNum = firstEvNum + i;
             numRocsWithTSs = numROCs;
             haveTypeInfo = false;

             for (int j=0; j < numROCs; j++) {

                 trigBank = rocNodes[j].getChildAt(0);
                 // If Roc has no trigger bank, skip all calculations needing
                 // trigger data including finding the avg timestamp for an event.
                 if (trigBank == null) {
                     numRocsWithTSs--;
                     fatalError = true;
                     continue;
                 }

                 triggerSegment = trigBank.getChildAt(i);
                 // If Roc has a trigger bank but it's missing data for a particular event,
                 // it's going to mess things up so that we'll possibly be using data for
                 // event A when it's really for event B. I don't think there is any way to
                 // sort this out so just flag the error condition and try to stumble on.
                 // Skip all calculations needing trigger data including finding the avg
                 // timestamp for an event.
                 if (triggerSegment == null) {
                     numRocsWithTSs--;
                     fatalError = true;
                     continue;
                 }

                 if (!haveTypeInfo) {
                     // Find & store the types of events from first ROC with this info
                     evData[i] = (short) (triggerSegment.getTag());  // event type
                     haveTypeInfo = true;
                 }

                 if (!turnOffChecks) {
                     // Check event type consistency
                     if (evData[i] != (short) (triggerSegment.getTag())) {
 System.out.println("makeTriggerBankFromRocRaw: event type differs across ROCs, first has " + evData[i] +
 " #" + j + " ROC has " + (short) (triggerSegment.getTag()));
                         nonFatalError = true;
                     }

                     // Check event number consistency, but make sure we take
                     // endianness into account when looking at the data.
                     triggerData = triggerSegment.getIntData(segmentData, returnLen);
                     if (evNum != triggerData[0]) {
 //System.out.println("makeTriggerBankFromRocRaw: event # differs (in Bt# " + buildThreadOrder + ") for ROC id#" +
 //                        getTagCodaId(inputPayloadBanks[j].getNode().getTag()) + ", expected " +
 //                        evNum + " got " + (triggerData[0]) + " (0x" + Integer.toHexString(triggerData[0]) + ')');
                         nonFatalError = true;
                     }

                     // Check for misc data (if we have no timestamps) in at least one place
                     // so we know if we can sparsify or not.
                     if (!haveMiscData && !haveTimestamps && returnLen[0] > 1) {
                         haveMiscData = true;
                     }
                 }

                 // If they exist, store timestamp related
                 // values so consistency can be checked below
                 if (checkTimestamps && returnLen[0] > 2) {
                     ts = (   ((0xffffL & (long)triggerData[2]) << 32) |
                           (0xffffffffL & (long)triggerData[1]));
                     // Avg timestamp stored directly in longData[i+1].
                     // This calculation is finished a few lines further down.
                     longData[i+1] += ts;
 //System.out.println("TS = " + ts);
                     timestampsMax = (ts >= timestampsMax) ? ts : timestampsMax;
                     timestampsMin = (ts <= timestampsMin) ? ts : timestampsMin;
                 }
             }

             // If there is a fatal error, it's possible numRocsWithTSs = 0,
             // so avoid following calculation which would divide by 0.
             if (checkTimestamps && !fatalError) {
                 // Calculate avg TS now
                 longData[i+1] /= numRocsWithTSs;
 //System.out.println("avg TS = " + longData[i+1]);

                 // Now that we have the timestamp info, check them against each other,
                 // allowing a difference of timestampSlop from the max to min.
                 if (timestampsMax - timestampsMin > timestampSlop) {
                     nonFatalError = true;
System.out.println("Timestamp NOT consistent: ev #" + (firstEvNum + i) + ", diff = " +
                   (timestampsMax - timestampsMin) + ", allowed = " + timestampSlop);

                     // Go back, fish out the timestamp values, and print them
                     for (int j=0; j < numROCs; j++) {
                         trigBank = rocNodes[j].getChildAt(0);
                         if (trigBank == null) {
                             continue;
                         }

                         triggerSegment = trigBank.getChildAt(i);
                         if (triggerSegment == null) {
                             continue;
                         }

                         triggerData = triggerSegment.getIntData(segmentData, returnLen);

                         if (returnLen[0] > 2) {
                             ts = (   ((0xffffL & (long)triggerData[2]) << 32) |
                                   (0xffffffffL & (long)triggerData[1]));
System.out.println("TS = " + ts + " for " + inputPayloadBanks[j].getSourceName() );
                         }
                     }
                 }
             }

         }

//         if (measureTimes) {
//             t2 = System.nanoTime();
//             deltaT2 = t2-t1;
//             t1 = System.nanoTime();
//         }

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

         // Start trigger bank
         if (fatalError) {
             trigTag = CODATag.BUILT_TRIGGER_ROC_ERROR;
         }

         // Skip over event's bank header and the trigger bank length word
         int writeIndex = 12;
         int trigBankLen = 1; // for bank header

         // Top bank len is not written yet. Top bank 2nd word is already written.

         //------------------------------------------------------------------
         // Trig bank header: 1) len not written yet, 2) write 2nd word now
         //------------------------------------------------------------------
         int headerWord = (trigTag.getValue() << 16) |
                 ((DataType.SEGMENT.getValue() & 0x3f) << 8) |
                 (numROCs & 0xff);
         builtEventBuf.putInt(writeIndex, headerWord);
         writeIndex += 4;

         //----------------------------
         // Add segment of long data
         //----------------------------

         // Header
         headerWord = (ebId << 24) |
                 ((DataType.ULONG64.getValue() & 0x3f) << 16) |
                 (2*longDataLen & 0xffff);
         builtEventBuf.putInt(writeIndex, headerWord);
         writeIndex += 4;

         // Data
         for (int i=0; i < longDataLen; i++) {
             builtEventBuf.putLong(writeIndex, longData[i]);
             writeIndex += 8;
         }

         //----------------------------
         // Add segment of event types
         //----------------------------

         // Header
         int padding = numEvents % 2 == 0 ? 0 : 2;

         headerWord = (ebId << 24) |
                 (padding << 22) |
                 ((DataType.USHORT16.getValue() & 0x3f) << 16) |
                 ((numEvents + 1)/2 & 0xffff);
         builtEventBuf.putInt(writeIndex, headerWord);
         writeIndex += 4;

         // Data
         for (int i=0; i < numEvents; i++) {
             builtEventBuf.putShort(writeIndex, evData[i]);
             writeIndex += 2;
         }
         writeIndex += padding;

         //-------------------------------------------------------
         // 3) Add ROC-specific segments if not sparsifying
         //-------------------------------------------------------

         if (!sparsify) {
             // Add one segment for each ROC with ROC-specific data in it
             int totalSegDataWords, segWords, dataWordsFromEachSeg=0, segTotalBytes;
             int len, destHeaderPos, srcPos;

             // For each ROC ...
             for (int i=0; i < numROCs; i++) {
                 bufOffset  = rocOffset[i];
                 backingBuf = rocRecord[i];

                 // Track header position for later writing & skip over it
                 destHeaderPos = writeIndex;
                 writeIndex += 4;

                 segTotalBytes = 0;
                 totalSegDataWords = 0;

                 // Write data first while we find its length
                 for (int j=0; j < numEvents; j++) {
                     // Position of seg header in src (roc trigger bank).
                     // First time thru loop, setTotalBytes is unknown but that's OK
                     // since j=0.
                     srcPos = bufOffset + j*segTotalBytes + 16;

                     // Len in words of seg
                     segWords = backingBuf.getInt(srcPos) & 0xffff;

                     // Subtract 1 since we won't be writing event # (will put in common data)
                     int dataWords = segWords - 1;
                     totalSegDataWords += dataWords;

                     // Make sure each seg in roc's trigger bank has same amount of data
                     if (j == 0) {
                         dataWordsFromEachSeg = segWords;
                         segTotalBytes = 4*(segWords + 1);
//Utilities.printBufferBytes(backingBuf, bufOffset, 140, "Roc " + i + ", segWords = " + segWords);
                     }
                     else if (segWords != dataWordsFromEachSeg) {
System.out.println("makeTriggerBankFromRocRaw: failure for ROC " + i + ", event " + j);
System.out.println("                         : segTotalBytes = " + segTotalBytes);
System.out.println("                         : bufOffset = " + bufOffset);
System.out.println("                         : srcPos = " + srcPos);
System.out.println("                         : segWords = " + segWords);
System.out.println("                         : segWords from event 0 = " + dataWordsFromEachSeg);

                         Utilities.printBytes(backingBuf, bufOffset, 140, "Roc " + i);
                         Utilities.printBytes(backingBuf, srcPos, 20, "Roc " + i + ", bad seg " + j);
 // TODO: Bombs here for Dave A., but not me
                         throw new EmuException("Trigger segments contain different amounts of data");
                     }

                     // Skip header and event # in src (roc trigger bank)
                     srcPos += 8;

                     // Copy data to built trigger bank
                     if (fastCopyReady) {
                         len = 4*dataWords;
                         System.arraycopy(backingBuf.array(), srcPos,
                                          builtEventBuf.array(), writeIndex,len);
                         srcPos += len;
                         writeIndex += len;
                     }
                     else {
                         for (int k = 0; k < dataWords; k++) {
                             builtEventBuf.putInt(writeIndex, backingBuf.getInt(srcPos));
                             srcPos += 4;
                             writeIndex += 4;
                         }
                     }
                 }

                 // Now go back and write seg header since we have length
                 headerWord = (inputPayloadBanks[i].getSourceId() << 24) |
                         ((DataType.UINT32.getValue() & 0x3f) << 16) |
                         (totalSegDataWords & 0xffff);
 //System.out.println("write seg header 0x" + Integer.toHexString(headerWord) +
 //                  " for Roc seg " + i + " at pos = " + destHeaderPos);

                 builtEventBuf.putInt(destHeaderPos, headerWord);
             }
         }

         // Now go back and write trigger bank header since we have length.
         // Note, writeIndex starts at beginning of built event so skip over 1st bank header.
         builtEventBuf.putInt(8, (writeIndex - 8)/4 - 1);

//         if (measureTimes) {
//             t2 = System.nanoTime();
//             deltaT3 = t2-t1;
//             System.out.println("NEW T1 = " + deltaT1 + ", T2 = " + deltaT2 + ", T3 = " + deltaT3);
//         }

         // Send back position for the next write to start
         returnLen[0] = writeIndex;

         return nonFatalError || fatalError;
     }


    /**
     * Method to switch the endian of all contained evio headers (NOT the data)
     * in a ROC's time slice bank while copying to another buffer.
     *
     * @param switchEndianFastCopyReady do both buffers have a backing array, allowing for bulk copy?
     * @param builtEventBuf  buffer to write to.
     * @param ebBuffer       buffer containing data from a DC to be swapped.
     * @param writeIndex     position in builtEventBuffer to begin writing.
     * @param timeSliceBank  EvioNode object to be writtten to builtEventBuffer (backed by ebBuffer).
     *
     * @return final writeIndex value.
     */
    private static int switchTimeSliceEndian(boolean switchEndianFastCopyReady,
                                             ByteBuffer builtEventBuf,
                                             ByteBuffer ebBuffer,
                                             int writeIndex,
                                             EvioNode timeSliceBank) {

        // Note that each ROC's time slice bank has multiple internal banks and segments.
        // All headers of such evio structures need to have their endian switched.
        // The Stream Info Bank will also have its data switched.
        // NO other (actual) data is switched.

        int dataByteLen;
        EvioNode dataBank;
        int pos = timeSliceBank.getPosition();
        int childrenCount = timeSliceBank.getChildCount();

        // First the Time Slice Bank header
        builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
        writeIndex += 4; pos += 4;
        builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
        writeIndex += 4; pos += 4;

        // Now the contained SIB bank and its data
        EvioNode sibBank = timeSliceBank.getChildAt(0);

        // The SIB bank header (bank of segments)
        builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
        writeIndex += 4; pos += 4;
        builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
        writeIndex += 4; pos += 4;

        // The SIB bank's first segment (seg header and 3 ints)
        builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
        writeIndex += 4; pos += 4;
        builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
        writeIndex += 4; pos += 4;
        builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
        writeIndex += 4; pos += 4;
        builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
        writeIndex += 4; pos += 4;

        // The second segment (seg header and some shorts)
        builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
        writeIndex += 4; pos += 4;
        // Swap each short
        int shortCount = 2*sibBank.getChildAt(1).getDataLength();
        for (int i=0; i < shortCount; i++) {
            builtEventBuf.putShort(writeIndex, ebBuffer.getShort(pos));
            writeIndex += 2; pos += 2;
        }

        // Swap each Data bank header
        ArrayList<EvioNode> children = timeSliceBank.getChildNodes();
        for (int k=1; k < childrenCount; k++) {
            dataBank = children.get(k);
            dataByteLen = 4*dataBank.getDataLength();

            // Header
            builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
            writeIndex += 4; pos += 4;
            builtEventBuf.putInt(writeIndex, ebBuffer.getInt(pos));
            writeIndex += 4; pos += 4;

            // Data - will not be swapped, just copied
            if (switchEndianFastCopyReady) {
                System.arraycopy(ebBuffer.array(), pos, builtEventBuf.array(), writeIndex, dataByteLen);
            }
            else {
                // Need a duplicate here even though it produces garbage.
                // The backing buf of this evio node is still being used
                // in the communication channel to add later events.
                ByteBuffer duplicateBuf = ebBuffer.duplicate();
                duplicateBuf.limit(pos + dataByteLen).position(pos);

                // Write
                builtEventBuf.position(writeIndex);
                builtEventBuf.put(duplicateBuf);

                // Restore buffer status
                builtEventBuf.position(0);
            }
            writeIndex += dataByteLen;
            pos += dataByteLen;
        }

        return writeIndex;
    }


    /**
     * <p>Combine banks from an Aggregator output into an event
     * with a Stream Info Bank, and appended ROC Time Slice Banks. Any error
     * which occurs but allows the build to continue will be noted in the return value
     * and in error bit in bank nums.
     * Errors which stop the event building cause an exception to be thrown.
     * Called only for aggregating already aggregated streams.</p>
     *
     * <p>To check timestamp consistency, for each built slice the difference between the max and
     * min timestamps cannot exceed the argument timestampSlop. If it does for any slice,
     * this method returns <code>true</code>.</p>
     *
     * Many of the args are externally provided arrays used here in order to avoid allocating
     * them each time this method is called. Some, like inputNodes and ebRecord,
     * are quickly found in StreamAggregator and easy to pass in.
     *
     * @param inputSliceCount   number of input buffers/banks/timeslices.
     * @param inputPayloadBanks array containing all banks of the same time slice from all
     *                          input channels.
     * @param builtEventBuf     ByteBuffer of event being built.
     * @param tag               evio tag of top level bank to create.
     * @param timestampSlop     maximum number of timestamp ticks that timestamps can differ
     *                          between input slices before the exception is thrown.
     * @param frame             frame number containing given time stamps.
     * @param bankData          int array, passed in to avoid unnecessary object creation.
     * @param returnLen         int array used to return index into builtEventBuf of where to write.
     * @param ebBuf             array of inputNodes' backing buffers.
     * @param inputNodes        array of EvioNodes of input banks.
     * @param fastCopyReady     if <code>true</code>, input data buffer backing byte arrays and
     *                          this builder's ByteBufferSupply buffers both have the same endian value.
     * @param nonFatalError     if true, there is already a non fatal error in that the ROC id in the
     *                          tag does not match the expected CODA id for one of the inputs
     *
     * @return <code>true</code> if recoverable error occurred, else <code>false</code>
     * @throws EmuException for major error in aggregating which necessitates stopping the build
     */
    public static boolean combineAggregatedStreams(int inputSliceCount,
                                                   PayloadBuffer[] inputPayloadBanks,
                                                   ByteBuffer builtEventBuf,
                                                   int tag,
                                                   int timestampSlop,
                                                   long frame,
                                                   int[] bankData,
                                                   int[] returnLen,
                                                   ByteBuffer[] ebBuf,
                                                   EvioNode[] inputNodes,
                                                   boolean fastCopyReady,
                                                   boolean nonFatalError)
            throws EmuException {

        // If this method is being called, the output of streaming ROCs have been combined by
        // the combineRocStreams() or combineSingleVtpStreamsToPhysics() method,
        // thus assuring us each input has a Stream Info Bank (SIB).
        int childrenCount, byteLen, pos;
        boolean switchEndianFastCopyReady = false;
        EvioNode timeSliceBank, node, bank, seg;
        boolean switchEndian = builtEventBuf.order() != ebBuf[0].order();

        // Start building combined Stream Info Bank
        //
        //    MSB(63)                LSB(0)
        //    _____________________________
        //    |     Stream Bank Header    |
        //    |___________________________|
        //    | Time Slice Segment Header |
        //    |___________________________|
        //    |        Frame Number       |
        //    |   Avg timestamp (31-0)    |
        //    |   Avg timestamp (63-32)   |
        //    |___________________________|
        //    |  Agg Info Segment Header  |
        //    |___________________________|
        //    |  ROC 1 ID   |  ??  |  SS  |
        //    |  ROC 2 ID   |  ??  |  SS  |
        //    |  ROC N ID   |  ??  |  SS  |
        //    |___________________________|

        //----------------------------
        // Calculate bank of time data
        //----------------------------

        long ts, tsAvg = 0, frameNum, timestampsMax = Long.MIN_VALUE, timestampsMin = Long.MAX_VALUE;
        long tsTotal = 0L;
        int num, streams, sliceCount = 0;

        for (int j = 0; j < inputSliceCount; j++) {

            num = inputNodes[j].getNum();
            streams = num & 0x7f;
            sliceCount += streams;

            if (streams < 1) {
                System.out.println("combineAggStreams: streams from " + j + " = " + streams);
            }

            // Look at err bit
            if ((num & 0x80) > 0) {
                nonFatalError = true;
            }

            // SIB
            bank = inputNodes[j].getChildAt(0);
            // If input bank has no frame and timestamp info, error
            if (bank == null) {
                throw new EmuException("no Stream Info Bank found");
            }

            // Time Slice Segment
            seg = bank.getChildAt(0);
            // If input bank has no frame and timestamp info, error
            if (seg == null) {
                throw new EmuException("no Stream Info Bank found");
            }

            // Check frame number consistency
            seg.getIntData(bankData, returnLen);

            // If they exist, store timestamp related
            // values so consistency can be checked below
            if (returnLen[0] > 2) {
                // Pull out the frame number and compare with what we expect
                frameNum = bankData[0] & 0xffffffffL;
                if (frameNum != frame) {
                    // start on 20 bytes boundary for printing purposes
                    int ppos = inputNodes[j].getChildAt(0).getPosition() / 20 * 20;
                    Utilities.printBytes(ebBuf[j].array(), ppos, 256, "TSS");
                    throw new EmuException("data contains frame = " + frameNum + " but should be " + frame);
                }

                // Pull out the avg time stamp so we can find new avg
                ts = ((((long) bankData[2]) << 32) | (0xffffffffL & (long) bankData[1]));

                // The calculation of the avg is finished a few lines further down
                tsTotal += ts;
                timestampsMax = Math.max(ts, timestampsMax);
                timestampsMin = Math.min(ts, timestampsMin);
            }
            else {
                throw new EmuException("incomplete time data in event");
            }
        }

        // Calculate avg TS now
        // TODO: Should we have a weighted average here?
        tsAvg = tsTotal / inputSliceCount;
        //System.out.println("avg TS = " + tsAvg);

        // Now that we have the timestamp info, check them against each other,
        // allowing a difference of timestampSlop from the max to min.
        if (timestampsMax - timestampsMin > timestampSlop) {
            nonFatalError = true;
            System.out.println("Timestamps NOT consistent: max = " + timestampsMax + ", min = " +
                    timestampsMin + ", diff = " + (timestampsMax - timestampsMin) + ", allowed = " + timestampSlop);

            // Go back, fish out the timestamp values, and print them
            for (int i = 0; i < inputSliceCount; i++) {
                bank = inputNodes[i].getChildAt(0).getChildAt(0);
                bank.getIntData(bankData, returnLen);

                if (returnLen[0] > 2) {
                    int frNum = bankData[0];
                    ts = ((((long) bankData[2]) << 32) | (0xffffffffL & (long) bankData[1]));
                    System.out.println("Frame = " + frNum + ", TS = " + ts + " for " + inputPayloadBanks[i].getSourceName());
                }
            }
        }

        //------------------------------------------------------------------
        // Streaming Physics Event header
        //------------------------------------------------------------------

        // Skip over event's evio header length for now
        int writeIndex = 4;

        int ssNum = ((nonFatalError ? 1 : 0) << 7) | sliceCount;
        builtEventBuf.putInt(writeIndex, tag << 16 | (0x10 << 8) | ssNum);
        writeIndex += 4;
        int totalWords = 1; // (not including the first/top bank length)
        //------------------------------------------------------------------
        // SIB bank header
        //------------------------------------------------------------------
        builtEventBuf.putInt(writeIndex, 6 + sliceCount);
        writeIndex += 4;
        builtEventBuf.putInt(writeIndex, (CODATag.STREAMING_SIB_BUILT.getValue() << 16) |
                (DataType.SEGMENT.getValue() << 8) | ssNum);
        writeIndex += 4;
        //------------------------------------------------------------------
        // Time Slice Segment
        //------------------------------------------------------------------
        builtEventBuf.putInt(writeIndex, (CODATag.STREAMING_TSS_BUILT.getValue() << 24) | (1 << 16) | 3);
        writeIndex += 4;
        // Add int data
        builtEventBuf.putInt(writeIndex, (int) frame);
        writeIndex += 4;
        builtEventBuf.putInt(writeIndex, (int) (tsAvg));
        writeIndex += 4;
        builtEventBuf.putInt(writeIndex, (int) (tsAvg >>> 32L));
        writeIndex += 4;
        //------------------------------------------------------------------
        // Aggregation Info Segment
        //------------------------------------------------------------------
        builtEventBuf.putInt(writeIndex, CODATag.STREAMING_AIS_BUILT.getValue() << 24 | (1 << 16) | sliceCount);
        writeIndex += 4;

        // Copy over each event's entries for the AIS for combined AIS
        for (int i = 0; i < inputSliceCount; i++) {
            // # streams for this physics event
            streams = inputNodes[i].getNum() & 0x7f;
            // Starting position of data to be copied
            pos = inputNodes[i].getChildAt(0).getChildAt(1).getDataPosition();

            for (int j = 0; j < streams; j++) {
                builtEventBuf.putInt(writeIndex, ebBuf[i].getInt(pos));
                writeIndex += 4;
                pos += 4;
            }
        }

        // total words up to this point
        totalWords += 7 + sliceCount;

        //-------------------------------------------------------
        // Add ROC Time Slice Banks
        //-------------------------------------------------------

        if (fastCopyReady) {
            for (int i = 0; i < inputSliceCount; i++) {
                node = inputNodes[i];

                // If fast copy can be done, append all data banks with a single copy
                pos = node.getChildAt(1).getPosition();
                // Data to copy is top node's length of data minus stream info bank length
                int dataBanksBytes = 4 * node.getDataLength() - node.getChildAt(0).getTotalBytes();
                System.arraycopy(ebBuf[i].array(), pos, builtEventBuf.array(), writeIndex, dataBanksBytes);
                writeIndex += dataBanksBytes;
            }
        }
        else {
            // If switching endian, can we do fast copy of data?
            switchEndianFastCopyReady = ebBuf[0].hasArray() && builtEventBuf.hasArray();

            for (int i = 0; i < inputSliceCount; i++) {
                node = inputNodes[i];
                childrenCount = node.getChildCount();
                ArrayList<EvioNode> childNodes = node.getChildNodes();

                // Skip over first (Stream Info) bank
                for (int j = 1; j < childrenCount; j++) {
                    timeSliceBank = childNodes.get(j);
                    pos = timeSliceBank.getPosition();
                    // Get length of bank to be written
                    byteLen = timeSliceBank.getTotalBytes();
                    totalWords += byteLen / 4;

                    // If endianness not being switched, copy everything as is
                    if (!switchEndian) {
                        ByteBuffer duplicateBuf = ebBuf[i].duplicate();
                        duplicateBuf.limit(pos + byteLen).position(pos);

                        builtEventBuf.position(writeIndex);
                        builtEventBuf.put(duplicateBuf);
                        builtEventBuf.position(0);
                        writeIndex += byteLen;
                    }
                    // If endianness IS being switched
                    else {
                        writeIndex = switchTimeSliceEndian(
                                switchEndianFastCopyReady,
                                builtEventBuf,
                                ebBuf[i],
                                writeIndex,
                                timeSliceBank);
                    }
                }
            }
        }

        // Now go back and write bank header since we have length
        builtEventBuf.putInt(0, totalWords);

        // Send back position for the next write to start
        returnLen[0] = writeIndex;

        return nonFatalError;
    }


    ////////////////////////////////////////////
    // Called only if aggregating ROC streams
    ///////////////////////////////////////////


    /**
     * <p>Combine the input payload banks of streamed ROC raw format into an event
     * with a Stream Info Bank, and appended Time Slice Banks. Any error
     * which occurs but allows the build to continue will be noted in the return value.
     * Errors which stop the event building cause an exception to be thrown.
     * Called only if aggregating ROC streams.</p>
     *
     * <p>To check timestamp consistency, for each built slice the difference between the max and
     * min timestamps cannot exceed the argument timestampSlop. If it does for any slice,
     * this method returns <code>true</code>.</p>
     *
     * Many of the inputs are externally provided arrays used here in order to avoid allocating
     * them each time this method is called. Some, like rocNodes, rocRecord, and rocOffset,
     * are quickly found in StreamAggregator and easy to pass in.
     *
     * @param sliceCount        number of input buffers/banks/timeslices.
     * @param inputPayloadBanks array containing all banks of the same time slice from all
     *                          input channels.
     * @param builtEventBuf     ByteBuffer of event being built.
     * @param tag               evio tag of top level bank to create.
     * @param timestampSlop     maximum number of timestamp ticks that timestamps can differ
     *                          between input events before the error bit is set in a bank's
     *                          status.
     * @param frame             frame number containing given time stamps.
     * @param bankData          int array, passed in to avoid unnecessary object creation.
     * @param returnLen         int array used to return index into builtEventBuf of where to write.
     * @param rocBuf            array of rocNodes' backing buffers.
     * @param rocNodes          array of EvioNodes of input banks.
     * @param fastCopyReady     if <code>true</code>, input data buffer backing byte arrays and
     *                          this builder's ByteBufferSupply buffers both have the same endian value.
     * @param nonFatalError     if true, there is already a non fatal error in that the ROC id in the
     *                          tag does not match the expected CODA id for one of the inputs
     *
     * @return <code>true</code> if recoverable error occurred, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean combineRocStreams(int sliceCount,
                                            PayloadBuffer[] inputPayloadBanks,
                                            ByteBuffer builtEventBuf,
                                            int tag,
                                            int timestampSlop,
                                            long frame,
                                            int[] bankData,
                                            int[] returnLen,
                                            ByteBuffer[] rocBuf,
                                            EvioNode[] rocNodes,
                                            boolean fastCopyReady,
                                            boolean nonFatalError)
            throws EmuException {

        int pos, totalStreams = 0;
        EvioNode bank, node, sibBank, seg;
        boolean switchEndian = builtEventBuf.order() != rocBuf[0].order();
        long ts, frameNum, tsAvg=0L, tsMax=Long.MIN_VALUE, tsMin=Long.MAX_VALUE;

        // Start building Stream Info Bank
        //
        //    MSB(63)                LSB(0)
        //    _____________________________
        //    |     Stream Bank Header    |
        //    |___________________________|
        //    | Time Slice Segment Header |
        //    |___________________________|
        //    |        Frame Number       |
        //    |   Avg timestamp (31-0)    |
        //    |   Avg timestamp (63-32)   |
        //    |___________________________|
        //    |  Agg Info Segment Header  |
        //    |___________________________|
        //    |  ROC 1 ID   |  ??  |  SS  |
        //    |  ROC 2 ID   |  ??  |  SS  |
        //    |  ROC N ID   |  ??  |  SS  |
        //    |___________________________|

        //----------------------------
        // Calculate bank of time data
        //----------------------------

        for (int j=0; j < sliceCount; j++) {
            int num = rocNodes[j].getNum();
            // pick out 3 bits of total stream info
            totalStreams += (num >> 4) & 0x7;

            // Find the SIB bank - first child bank
            sibBank = rocNodes[j].getChildAt(0);
            if (sibBank == null) {
                throw new EmuException("SIB bank is missing from ROC input");
            }

            int sibTag = sibBank.getTag();
            if (!CODATag.isSIB(sibTag)) {
                throw new EmuException("SIB bank tag is wrong value (found 0x" + Integer.toHexString(sibTag) +
                        ", should be 0x" + Integer.toHexString(CODATag.STREAMING_SIB.getValue()));
            }

            seg = sibBank.getChildAt(0);
            // If Roc has no frame and timestamp info, error
            if (seg == null) {
                throw new EmuException("no Stream Info Bank's time-slice-segment found");
            }

            // Check frame number consistency
            seg.getIntData(bankData, returnLen);

            // If they exist, store timestamp related
            // values so consistency can be checked below
            if (returnLen[0] > 2) {
                // Pull out the frame number and compare with what we expect
                frameNum = bankData[0] & 0xffffffffL;
                if (frameNum != frame) {
                    throw new EmuException("data contains frame = " + frameNum + " but should be " + frame);
                }

                // Pull out the time stamp so we can find avg
                ts = (((long)bankData[2]) << 32) | (0xffffffffL & (long)bankData[1]);

                // The calculation of the avg is finished a few lines further down
                tsAvg += ts;
                tsMax = Math.max(ts, tsMax);
                tsMin = Math.min(ts, tsMin);
            }
            else {
                throw new EmuException("incomplete time data in event");
            }
        }

        // Calculate avg TS now
        tsAvg /= sliceCount;
        //System.out.println("avg TS = " + tsAvg);

        // Now that we have the timestamp info, check them against each other,
        // allowing a difference of timestampSlop from the max to min.
        if (tsMax - tsMin > timestampSlop) {
            nonFatalError = true;
            System.out.println("Timestamps NOT consistent: max = " + tsMax + ", min = " +
                    tsMin + ", diff = " + (tsMax - tsMin) + ", allowed = " + timestampSlop);

            // Go back, fish out the timestamp values, and print them
            for (int i=0; i < sliceCount; i++) {
                bank = rocNodes[i].getChildAt(0).getChildAt(0);
                bank.getIntData(bankData, returnLen);

                if (returnLen[0] > 2) {
                    int frNum = bankData[0];
                    ts = (((long)bankData[2]) << 32) | (0xffffffffL & (long)bankData[1]);
                    System.out.println("Frame = " + frNum + ", TS = " + ts +
                                       " for " + inputPayloadBanks[i].getSourceName() );
                }
            }
        }

        //------------------------------------------------------------------
        // Streaming Physics Event header
        //------------------------------------------------------------------

        // Skip over event's evio header length for now
        int writeIndex = 4;

        int ssNum = ((nonFatalError ? 1 : 0) << 7) | totalStreams;
        builtEventBuf.putInt(writeIndex, tag << 16 | (0x10 << 8) | ssNum);
        writeIndex += 4;
        int totalWords = 1; // (not including the first/top bank length)
        //------------------------------------------------------------------
        // SIB bank header
        //------------------------------------------------------------------
        builtEventBuf.putInt(writeIndex, 6 + sliceCount);
        writeIndex += 4;
        builtEventBuf.putInt(writeIndex, (CODATag.STREAMING_SIB_BUILT.getValue() << 16) | (0x20 << 8) | ssNum);
        writeIndex += 4;
        //------------------------------------------------------------------
        // Time Slice Segment
        //------------------------------------------------------------------
        builtEventBuf.putInt(writeIndex, (CODATag.STREAMING_TSS_BUILT.getValue() << 24) | (1 << 16) | 3);
        writeIndex += 4;
        // Add int data
        builtEventBuf.putInt(writeIndex, (int)frame);            writeIndex += 4;
        builtEventBuf.putInt(writeIndex, (int)(tsAvg));          writeIndex += 4;
        builtEventBuf.putInt(writeIndex, (int)(tsAvg >>> 32L));  writeIndex += 4;
        //------------------------------------------------------------------
        // Aggregation Info Segment
        //------------------------------------------------------------------
        builtEventBuf.putInt(writeIndex, CODATag.STREAMING_AIS_BUILT.getValue() << 24 | (1 << 16) | sliceCount);
        writeIndex += 4;

        // Copy over each Top bank's 2nd header word and copy it here
        for (int i=0; i < sliceCount; i++) {
            pos = rocNodes[i].getPosition(); // position of header in backing array
            builtEventBuf.putInt(writeIndex, rocBuf[i].getInt(pos + 4));
            writeIndex += 4;
        }

        // total words up to this point
        totalWords += 7 + sliceCount;

        //-------------------------------------------------------
        // Add ROC Time Slice Banks
        //-------------------------------------------------------

        boolean switchEndianFastCopyReady = false;

        if (!fastCopyReady) {
            // If switching endian, can we do fast copy of data?
            switchEndianFastCopyReady = rocBuf[0].hasArray() && builtEventBuf.hasArray();
        }

        // Add all Time Slice Banks
        for (int i=0; i < sliceCount; i++) {
            node = rocNodes[i];
            pos  = node.getPosition();
            // Get length of Time Slice Bank to be written
            int byteLen = node.getTotalBytes();
            totalWords += byteLen / 4;

            // There's nothing tricky to worry about (e.g. buffer mapped to part of backing array)
            if (fastCopyReady) {
                System.arraycopy(rocBuf[i].array(), pos, builtEventBuf.array(), writeIndex, byteLen);
                writeIndex += byteLen;
            }
            // If endianness not being switched, copy everything as is
            else if (!switchEndian) {
                // Need a duplicate here even though it produces garbage.
                // The backing buf of this evio node is still being used
                // in the communication channel to add later events.
                ByteBuffer duplicateBuf = rocBuf[i].duplicate();
                duplicateBuf.limit(pos + byteLen).position(pos);

                // Write
                builtEventBuf.position(writeIndex);
                builtEventBuf.put(duplicateBuf);

                // Restore buffer status
                builtEventBuf.position(0);

                writeIndex += byteLen;
            }
            // If endianness IS being switched
            else {
                writeIndex = switchTimeSliceEndian(
                        switchEndianFastCopyReady,
                        builtEventBuf,
                        rocBuf[i],
                        writeIndex,
                        node);
            }
        }

        // Now go back and write bank header since we have length
        builtEventBuf.putInt(0, totalWords);

        // Send back position for the next write to start
        returnLen[0] = writeIndex;

        return nonFatalError;
    }


    /**
     * <p>
     * Combine up to 4 streams of a single VTP (VXS Triggger Processor) board into one
     * ROC Streaming Physics bank. Each stream provides data from the identical time frame.
     * Since they come from the same VTP they can be combined into one ROC Time Slice Bank.
     * Called only if aggregating ROC streams.</p>
     *
     * <p>To check timestamp consistency, for each built slice the difference between the max and
     * min timestamps cannot exceed the argument timestampSlop. If it does for any slice,
     * this method returns <code>true</code>.</p>
     *
     * Some inputs are externally provided arrays used here in order to avoid allocating
     * them each time this method is called. Some, like rocNodes and rocBuf,
     * are quickly found in StreamAggregator and easy to pass in.
     *
     * @param sliceCount        number of input buffers/banks/timeslices.
     * @param builtEventBuf     ByteBuffer of event being built.
     * @param timestamps        convenience array to hold 4 long timestamps.
     * @param returnLen         int array used to return index into builtEventBuf of where to write.
     * @param rocBuf            array of rocNodes' backing buffers.
     * @param rocNodes          array of EvioNodes of input banks.
     * @param fastCopyReady     if <code>true</code>, input data buffer backing byte arrays and
     *                          this builder's ByteBufferSupply buffers both have the same endian value.
     * @param nonFatalError     if true, there is already a non fatal error in that the ROC id in the
     *                          tag does not match the expected CODA id for one of the inputs
     *
     * @return <code>true</code> if recoverable error occurred in this method, else <code>false</code>
     * @throws EmuException for major error in event building which necessitates stopping the build
     */
    public static boolean combineSingleVtpStreamsToPhysics(
            int sliceCount,
            ByteBuffer builtEventBuf,
            long[] timestamps,
            int[] returnLen,
            ByteBuffer[] rocBuf,
            EvioNode[] rocNodes,
            boolean fastCopyReady,
            boolean nonFatalError)
                                    throws EmuException {

        // First thing is to combine all Stream-Status (single) bytes into 1 status.
        // Used this as top header "num" as well as Stream Info Bank header "num".
        // Also do checks on data.
        EvioNode rocNode = rocNodes[0];
        int rocId = rocNode.getTag();

        EvioNode sibBank = rocNode.getChildAt(0);
        if (sibBank == null) {
            throw new EmuException("SIB bank is missing from ROC input");
        }

        EvioNode aggInfoSeg;
        EvioNode timeSliceSeg = sibBank.getChildAt(0);
        if (timeSliceSeg == null) {
            throw new EmuException("time slice segment is missing from ROC input");
        }

        // Don't use timeSliceSeg.getIntData()[0] to avoid creating array
        ByteBuffer rocBuf0 = rocBuf[0];

        int tframe = rocBuf0.getInt(timeSliceSeg.getDataPosition());
        int ts1=0, ts2=0;
        boolean switchEndianFastCopyReady = false;
        if (!fastCopyReady) {
            // If switching endian, can we do fast copy of data?
            switchEndianFastCopyReady = rocBuf0.hasArray() && builtEventBuf.hasArray();
        }

        int pos = 0;
        int sibTag = 0;
        int streamMask = 0;
        int totalStreams = 0;
        int payloadCount = 0;
        ByteBuffer timeSegBuf;
        // Is there an error bit set on any of roc time slice banks?
        // If so, set this to 1.
        int isError = 0;

        for (int j=0; j < sliceCount; j++) {
            rocNode = rocNodes[j];

            int num = rocNode.getNum();
            // pick out 3 bits of total stream info
            totalStreams += (num >> 4) & 0x7;
            // created combined stream mask of lowest 4 bits
            streamMask |= num & 0xf;

            // Check error bit in num of roc's top bank
            if ((num & 0x80) > 0) {
                isError = 1;
            }

            // check for identical roc ids
            if (rocNode.getTag() != rocId) {
                // Differing ROC ids (include this later in built event's stream status)
                nonFatalError = true;
            }

            sibTag = sibBank.getTag();
            if (!CODATag.isSIB(sibTag)) {
                throw new EmuException("SIB bank tag is wrong val (found 0x" + Integer.toHexString(sibTag) +
                        ", should be 0x" + Integer.toHexString(CODATag.STREAMING_SIB.getValue()));
            }

            // check for identical time frames
            timeSliceSeg = rocNode.getChildAt(0).getChildAt(0);
            if (timeSliceSeg == null) {
                throw new EmuException("time slice segment is missing from ROC input");
            }

            timeSegBuf  = timeSliceSeg.getBuffer();
            int dataPos = timeSliceSeg.getDataPosition();
            if (tframe != timeSegBuf.getInt(dataPos)) {
                // major error, we must combine the SAME time frames
                throw new EmuException("Cannot combine different time frames into 1 ROC time slice bank");
            }

            // store timestamps to check, later, for consistency
            ts1 = timeSegBuf.getInt(dataPos + 4);
            ts2 = timeSegBuf.getInt(dataPos + 8);
            timestamps[j] = ((long)ts2 << 32) | ((long)ts1 & 0xffffffffL);
            if (j > 0) {
                if (timestamps[j] != timestamps[0]) {
                    System.out.println("Time stamps differ between streams from single VTP ts[0] = " + timestamps[0] +
                            " vs  ts[" + j + "] " + timestamps[j]);
                    throw new EmuException("Time stamps differ between streams from single VTP");
                }
            }

            // Find how many shorts in final Aggregation Info Segment
            aggInfoSeg = rocNode.getChildAt(0).getChildAt(1);
            if (aggInfoSeg == null) {
                throw new EmuException("aggregation info segment is missing from ROC input");
            }

            // Data type is short, dataLen is in 4-byte words
            payloadCount += (4*aggInfoSeg.getDataLength() - aggInfoSeg.getPad())/2;
        }

        //-------------------------------------------------------------------
        // There are 7 words (top bank header, Time Info Bank header & data)
        // which will come before the ROC Time Slice Bank.
        //-------------------------------------------------------------------

        // At this point we can begin to create the output buffer
        // 1st word is top bank's total len which we'll fill in later.
        int destPos = 4;

        // 2nd word is top bank's tag/type/num.
        // Num => 1 stream & error bit
        int ssNum = ((isError | (nonFatalError ? 1 : 0)) << 7) | 1;
        builtEventBuf.putInt(destPos, CODATag.STREAMING_PHYSICS.getValue() << 16 | 0x10 << 8 | ssNum);
        destPos += 4;

        // 3rd word is Stream Info Bank's length
        builtEventBuf.putInt(destPos, 6+sliceCount);
        destPos += 4;

        // 4th word it SIB's tag/type/num (type = segments)
        builtEventBuf.putInt(destPos, CODATag.STREAMING_SIB_BUILT.getValue() << 16 | 0x20 << 8 | ssNum);
        destPos += 4;

        // 5th word it Time Slice Seg's tag/type/num (len = 3 words)
        builtEventBuf.putInt(destPos, CODATag.STREAMING_TSS_BUILT.getValue() << 24 | 1 << 16 | 3);
        destPos += 4;

        // Write TSS data. Because this is generated data, use same endian as headers
        builtEventBuf.putInt(destPos, tframe);                       destPos += 4;
        builtEventBuf.putInt(destPos, (int)timestamps[0]);           destPos += 4;
        builtEventBuf.putInt(destPos, (int)(timestamps[0] >>> 32));  destPos += 4;

        // 9th word it Aggregation Info Seg's tag/type/num (len = # rocs, 1 word/roc)
        builtEventBuf.putInt(destPos, CODATag.STREAMING_AIS_BUILT.getValue() << 24 | 1 << 16 | sliceCount);
        destPos += 4;

        // In this case, the ROCS are combined into one, so only one word is written here:
        // ROC_ID  |  ??  | SS
        int combinedRocNum = (isError << 7) | totalStreams << 4 | streamMask;
        builtEventBuf.putInt(destPos, rocId << 16 | combinedRocNum);
        destPos += 4;

        //-----------------------------------------
        // ROC Time Slice Bank
        //-----------------------------------------

        // Aggregation info seg data's byte padding
        int padding = 2 * (payloadCount % 2);
        // Aggregation info segment len
        int aggDataWords = (2 * payloadCount + padding) / 4;

        // 1st word is roc time slice (top) bank len which we'll fill in later.
        int tsbPos = destPos;
        destPos += 4;

        // 2nd word is top bank's tag/type/num
        builtEventBuf.putInt(destPos, rocId << 16 | 0x10 << 8 | combinedRocNum);
        destPos += 4;

        // 3rd word is Stream Info Bank's evio length in words.
        builtEventBuf.putInt(destPos, 6 + aggDataWords);
        destPos += 4;

        // 4th word it SIB's tag/type/num
        builtEventBuf.putInt(destPos, CODATag.STREAMING_SIB.getValue() << 16 | 0x20 << 8 | combinedRocNum);
        destPos += 4;

        //------------------------------
        // Copy Time Slice Segment (4 ints = 16 bytes) which should be same for all inputs
        //------------------------------

        // 5th word it TSS seg header tag/type/len
        builtEventBuf.putInt(destPos, CODATag.STREAMING_TSS.getValue() << 24 | 0x1 << 16 | 3);
        destPos += 4;
        // TSS data
        builtEventBuf.putInt(destPos, tframe);
        destPos += 4;
        builtEventBuf.putInt(destPos, ts1);
        destPos += 4;
        builtEventBuf.putInt(destPos, ts2);
        destPos += 4;

        //------------------------------
        // Create Aggregation Info Segment
        //------------------------------

        // 9th word = Aggregation Info Segment header
        builtEventBuf.putInt(destPos, CODATag.STREAMING_AIS.getValue() << 24 | padding << 22 | 0x5 << 16 | aggDataWords);

        // AIS data starts at 10th word
        destPos += 4;
        int destPos2 = destPos;
        byte[] backingArray = builtEventBuf.array();

        for (int i = 0; i < sliceCount; i++) {
            aggInfoSeg = rocNodes[i].getChildAt(0).getChildAt(1);
            rocBuf0 = aggInfoSeg.getBuffer();
            pos = aggInfoSeg.getDataPosition(); // position of data in array/buffer
            int dataBytes = 4 * aggInfoSeg.getDataLength() - aggInfoSeg.getPad();

            if (fastCopyReady) {
                // If same endian and 2 backing arrays
                System.arraycopy(rocBuf0.array(), pos, builtEventBuf.array(), destPos2, dataBytes);
                destPos2 += dataBytes;
            }
            else {
                for (int j = 0; j < dataBytes/2; j++) {
                    builtEventBuf.putShort(destPos2, rocBuf0.getShort(pos));
                    destPos2 += 2;
                    pos += 2;
                }
            }
        }

        destPos += 4 * aggDataWords;

        //-------------------------------------------------------
        // Add Data/Payload Banks
        //-------------------------------------------------------

        // Do a fast copy if possible
        if (fastCopyReady) {
            for (int i = 0; i < sliceCount; i++) {
                rocNode = rocNodes[i];
                int copyBytes = 4*rocNode.getDataLength() - rocNode.getChildAt(0).getTotalBytes();
                pos = rocNode.getChildAt(1).getPosition();

                System.arraycopy(rocBuf[i].array(), pos, builtEventBuf.array(), destPos, copyBytes);
                destPos += copyBytes;
            }
        }
        else {
            for (int i = 0; i < sliceCount; i++) {
                rocNode = rocNodes[i];
                rocBuf0 = rocBuf[i];

                // Number of data banks = total child banks of top bank - stream info bank
                int dataBankCount = rocNode.getChildCount() - 1;

                for (int j = 0; j < dataBankCount; j++) {
                    EvioNode node = rocNode.getChildAt(j + 1);
                    pos = node.getPosition();
                    int dataLen = 4 * node.getDataLength();

                    // To keep this as simple as possible, first copy over 2 word
                    // bank header, swapping endian automatically
                    builtEventBuf.putInt(destPos, rocBuf0.getInt(pos));
                    destPos += 4;
                    pos += 4;
                    builtEventBuf.putInt(destPos, rocBuf0.getInt(pos));
                    destPos += 4;
                    pos += 4;

                    // Now the data which is NOT swapped
                    if (switchEndianFastCopyReady) {
                        // if fast copy possible
                        System.arraycopy(rocBuf0.array(), pos, backingArray, destPos, dataLen);
                    }
                    else {
                        // Need a duplicate here even though it produces garbage.
                        // The backing buf of this evio node is still being used
                        // in the communication channel to add later events.
                        ByteBuffer duplicateBuf = rocBuf0.duplicate();
                        duplicateBuf.limit(pos + dataLen).position(pos);

                        // Write
                        builtEventBuf.position(destPos);
                        builtEventBuf.put(duplicateBuf);

                        // Restore buffer status
                        builtEventBuf.position(0);
                    }

                    destPos += dataLen;
                }
            }
        }

        // At this point, destPos is the total number of bytes written

        // Go back and write EVIO length of top bank.
        // Evio bank length (in words) does NOT include length itself
        builtEventBuf.putInt(0, destPos/4 - 1);

        // Go back and write evio length of ROC TS Bank
        builtEventBuf.putInt(tsbPos, (destPos - tsbPos)/4 - 1);

        // Send back position for the next write to start
        returnLen[0] = destPos;

        return nonFatalError;
    }


    /**
     * Build a single physics event with the given array of ROC raw records.
     * No data (only header) swapping is done.
     *
     * @param rocNodes       array containing EvioNode events that will be built together.
     * @param fastCopyReady  if <code>true</code>, roc data buffer backing byte arrays and
     *                       EB's ByteBufferSupply buffers both have the s2526
     *                       ame endian value.
     * @param numRocs        number of input channels.
     * @param writeIndex     index into evBuf to start writing.
     * @param builtEventBuf  ByteBuffer containing event being built.
     * @param returnLen      int array used to return index into evBuf of where to write.
     * @param rocRecord      array of backing buffers to inputNodes.

     * @return bank final built event
     */
    public static EvioBank buildPhysicsEventWithRocRaw(EvioNode[] rocNodes,
                                                       boolean fastCopyReady,
                                                       int numRocs,
                                                       int writeIndex,
                                                       ByteBuffer builtEventBuf,
                                                       int[] returnLen,
                                                       ByteBuffer[] rocRecord) {

        int childrenCount, secondHeaderWord, dataBankPos, pos, lim, blockPos, dataLen, totalLen;
        EvioNode blockBank, rocNode;
        ByteBuffer rocBuffer;

        // Wrap and add data block banks (from payload banks).
        // Use the same header to wrap data blocks as used for payload bank.
        for (int i=0; i < numRocs; i++) {
            // Get Roc Raw node & buffer
            rocNode = rocNodes[i];
            rocBuffer = rocRecord[i];
            pos = rocBuffer.position();
            lim = rocBuffer.limit();

            dataBankPos = writeIndex;
            writeIndex += 4;

            // Create physics data bank with same tag & num as Roc Raw bank
            // in order to wrap the data block bank from Roc.
            secondHeaderWord = rocBuffer.getInt(rocNode.getPosition() + 4);
            builtEventBuf.putInt(writeIndex, secondHeaderWord);
            writeIndex += 4;

            // How many banks inside Roc Raw bank ?
            childrenCount = rocNode.getChildCount();

            // Take data block bank header into account
            totalLen = 1;

            // Add Roc Raw's data block banks to our data bank.
            // Ignore any trigger bank.
            // The trigger bank is always the first one.
            for (int j=1; j < childrenCount; j++) {
                blockBank = rocNode.getChildNodes().get(j);
                // -------------------- NOTE -----------------------------
                // If blockBank has data of opposite endian to our buffer,
                // we can still add it (although less efficient).
                // However, NO data swapping is done, only headers.
                // -------------------------------------------------------

                // Don't mess with user data banks since we don't know what's inside.
                // Just copy whole thing over (with swapped headers if necessary).
                // Header words first ...

                blockPos = blockBank.getPosition();
                builtEventBuf.putInt(writeIndex, rocBuffer.getInt(blockPos));
                writeIndex += 4;
                builtEventBuf.putInt(writeIndex, rocBuffer.getInt(blockPos + 4));
                writeIndex += 4;

                // Then data ...

                dataLen = blockBank.getDataLength();
                // There's nothing tricky to worry about (e.g. buffer mapped to part of backing array)
                if (fastCopyReady) {
                    System.arraycopy(rocBuffer.array(), blockPos + 8, builtEventBuf.array(), writeIndex, 4*dataLen);
                }
                else {
                    ByteBuffer duplicateBuf = rocBuffer.duplicate();
                    duplicateBuf.limit(blockPos + 8 + 4 * dataLen).position(blockPos + 8);

                    builtEventBuf.position(writeIndex);
                    // This method is relative to position
                    builtEventBuf.put(duplicateBuf);
                    builtEventBuf.position(0);
                }
                // TODO: WHat if we need to switch endian??? See method below ...

                writeIndex += 4*dataLen;
                totalLen += dataLen + 2;
            }

            // Restore orig values
            rocBuffer.limit(lim).position(pos);

            // Write length of top level data bank
            builtEventBuf.putInt(dataBankPos, totalLen);
        }

        returnLen[0] = writeIndex;
//Utilities.printBufferBytes(builtEventBuf, 0, writeIndex, "NEW Built Event Buf");

        return null;
    }


    /**
     * Build a single physics event with the given array of Physics events.
     *
     * @param inputNodes     array containing EvioNode events that will be built together.
     * @param evBuf          ByteBuffer containing event being built.
     * @param inputCount     number of input channels.
     * @param writeIndex     index in evBuf's backing array to start writing.
     * @param fastCopyReady  if <code>true</code>, roc data buffer backing byte arrays and
     *                       EB's ByteBufferSupply buffers both have the same endian value.
     * @param returnLen      int array used to return index into evBuf of where to write.
     * @param rocRecord      array of backing buffers to inputNodes.
     */
    public static void buildPhysicsEventWithPhysics(EvioNode[] inputNodes,
                                                    ByteBuffer evBuf,
                                                    int inputCount,
                                                    int writeIndex,
                                                    boolean fastCopyReady,
                                                    int[] returnLen,
                                                    ByteBuffer[] rocRecord) {

        int childrenCount, byteLen, pos;
        int subChildrenCount, dataBlockByteLen;
        boolean switchEndianFastCopyReady = false;
        EvioNode dataBank, dataBlockBank, node;
        ByteBuffer rocBuffer;

        if (!fastCopyReady) {
            // If switching endian, can we do fast copy of data?
            switchEndianFastCopyReady = rocRecord[0].hasArray() && evBuf.hasArray();
        }

        // Add all data banks (from payload banks) which are already wrapped properly
        for (int i=0; i < inputCount; i++) {
            node = inputNodes[i];
            childrenCount = node.getChildCount();

            // Get buffer
            rocBuffer = rocRecord[i];

            // Skip over first (trigger) bank
            for (int j=1; j < childrenCount; j++) {
                dataBank = node.getChildNodes().get(j);
                pos = dataBank.getPosition();
                byteLen = dataBank.getTotalBytes();

                // There's nothing tricky to worry about (e.g. buffer mapped to part of backing array)
                if (fastCopyReady) {
                    System.arraycopy(rocBuffer.array(), pos, evBuf.array(), writeIndex, byteLen);
                    writeIndex += byteLen;
                }
                // If endianness not being switched, copy everything as is
                else if (evBuf.order() == rocBuffer.order()) {
                    ByteBuffer duplicateBuf = rocBuffer.duplicate();
                    duplicateBuf.limit(pos + byteLen).position(pos);

                    evBuf.position(writeIndex);
                    // This method is relative to position
                    evBuf.put(duplicateBuf);
                    evBuf.position(0);
                    writeIndex += byteLen;
                }
                // If endianness IS being switched
                else {
                    // Note that each data bank may contain multiple data block banks,
                    // so all these headers need to have endian switched.
                    // Data is NOT switched.

                    // First the data bank header
                    evBuf.putInt(writeIndex, rocBuffer.getInt(pos));
                    writeIndex += 4; pos += 4;
                    evBuf.putInt(writeIndex, rocBuffer.getInt(pos));
                    writeIndex += 4; pos += 4;

                    // Now the contained data block banks
                    subChildrenCount = dataBank.getChildCount();
                    for (int k=0; k < subChildrenCount; k++) {
                        dataBlockBank = dataBank.getChildNodes().get(k);
                        dataBlockByteLen = 4*dataBlockBank.getDataLength();

                        // Header
                        evBuf.putInt(writeIndex, rocBuffer.getInt(pos));
                        writeIndex += 4; pos += 4;
                        evBuf.putInt(writeIndex, rocBuffer.getInt(pos));
                        writeIndex += 4; pos += 4;

                        // Data
                        if (switchEndianFastCopyReady) {
                            System.arraycopy(rocBuffer.array(), pos, evBuf.array(), writeIndex, dataBlockByteLen);
                        }
                        else {
// TODO: This will switch the data's endian !!!
                            ByteBuffer duplicateBuf = rocBuffer.duplicate();
                            duplicateBuf.limit(pos + dataBlockByteLen).position(pos);

                            evBuf.position(writeIndex);
                            // This method is relative to position
                            evBuf.put(duplicateBuf);
                            evBuf.position(0);
                        }
                        writeIndex += dataBlockByteLen; pos += dataBlockByteLen;
                    }
                }
            }
        }
        returnLen[0] = writeIndex;
    }


    /**
     * Generate a single data block bank of fake data.
     *
     * @param firstEvNum    starting event number
     * @param words         amount of data in 32bit words besides event # and timestamp
     * @param isSEM         in single event mode if <code>true</code>
     * @param timestamp     48-bit timestamp used only in single event mode
     * @return array of ints containing generated data.
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
     * @throws EvioException if error in building evio event.
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
