package org.jlab.coda.support.evio;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 24, 2008
 * Time: 2:13:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class DataBankImpl extends DataImpl implements DataBank {

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
    public DataBankImpl(byte[] db, int offs) {
        super(DataBank.WORD_SIZE, db, offs);

    }

    /**
     * Constructor DataBank creates a new DataBank instance.
     * This constructor maps a DataBank onto an existing buffer db
     * and generates the bankheader.
     * <p/>
     * Typical use - generating new banks.
     *
     * @param type bank type
     * @param dc   bank content class
     * @param db   of type byte[]
     * @param offs of type int
     */
    public DataBankImpl(int type, Class dc, byte[] db, int offs) {
        super(DataBank.WORD_SIZE, type, dc, db, offs);
    }

}
