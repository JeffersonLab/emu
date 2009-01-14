package org.jlab.coda.support.evio;

import org.jlab.coda.support.configurer.DataNotFoundException;

import java.util.ArrayList;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 21, 2008
 * Time: 1:53:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class DataIDBankImpl extends DataBankImpl implements DataBank {

    ArrayList<DataSegment> banks;
    private int recordID;

    /**
     * Create a new ID bank object and fill in header in buffer
     *
     * @param id
     * @param data
     * @param o
     */
    public DataIDBankImpl(int id, byte[] data, int o) {
        super(id, DataSegment.class, data, o);
        recordID = id;
    }

    /**
     * Create a new ID bank object and map it onto existing data
     *
     * @param data
     * @param o
     * @throws DataNotFoundException
     */
    public DataIDBankImpl(byte[] data, int o) throws BankFormatException {
        super(data, o);
        // Decode the bank
        int children = getN();

        int index = getStart();
        int count = 0;
        for (count = 0; count < children; count++) {
            DataSegment ds = new DataSegmentImpl(data, index);
            int sl = ds.length();
            index += (sl + WORD_SIZE);
        }
    }

}
