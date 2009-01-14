/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.support.evio;

import org.jlab.coda.support.configurer.DataNotFoundException;

import java.util.Vector;

/**
 * <pre>
 * Class <b>DataEvent </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */

public class DataEvent extends DataImpl implements DataBank {
    Vector<DataBank> banks = new Vector<DataBank>();

    /**
     * Constructor DataEvent creates a new DataEvent instance.
     *
     * @param db of type byte[]
     */
    public DataEvent(byte[] db, int offs) throws DataNotFoundException {
        super(db, offs);
        decode();
    }

    /**
     * Constructor DataEvent creates a new DataEvent instance.
     *
     * @param db of type byte[]
     */
    public DataEvent(byte[] db) throws DataNotFoundException {
        super(db);
        decode();
    }

    /**
     * Constructor DataEvent creates a new DataEvent instance.
     *
     * @param bufferLength of type int
     */
    public DataEvent(int bufferLength, int n, int id, int stat) throws DataNotFoundException, BankFormatException {
        super(bufferLength);

        setN(n);
        setId(id);
        setStat(stat);
        setPayloadClass(DataBank.class);
    }

    private void decode() throws DataNotFoundException {
        int l = length();
        int s = getDataOffset();

        if (l < s) {
            throw new DataNotFoundException("event length is too small");
        }

        int bl = getData(s);
        DataIDBankImpl idb = new DataIDBankImpl(getBuffer(), s);
        banks.add(idb);
        s += (bl + 1) * 4;

        while (s < l) {
            bl = getData(s);
            banks.add(new DataImpl(getBuffer(), s));
            s += (bl + 1) * 4;
        }

    }

}
