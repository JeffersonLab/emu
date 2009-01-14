package org.jlab.coda.support.evio;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 25, 2008
 * Time: 8:39:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class DataSegmentImpl extends DataImpl implements DataSegment {

    /**
     * Constructor DataBank creates a new DataBank instance.
     * This constructor maps a DataBank onto an existing buffer db
     * starting at the offset offs.
     * <p/>
     * Typical use - mapping a DataBank object onto existing data
     *
     * @param db   of type byte[]
     * @param offs of type int
     */
    public DataSegmentImpl(byte[] db, int offs) {
        super(DataBank.WORD_SIZE, db, offs);
    }

    /**
     * Constructor DataBank creates a new DataBank instance.
     * This constructor maps a DataBank onto an existing buffer db
     * and generates the bankheader.
     * <p/>
     * Typical use - generating new banks.
     *
     * @param dc   bank content class
     * @param db   of type byte[]
     * @param offs of type int
     */
    public DataSegmentImpl(Class dc, byte[] db, int offs) {
        super(DataBank.WORD_SIZE, 0, dc, db, offs);
    }

}
