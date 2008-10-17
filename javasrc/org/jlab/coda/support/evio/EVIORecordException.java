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

import java.util.Vector;

/** @version $Id$ */

@SuppressWarnings({"WeakerAccess"})
public class EVIORecordException extends Exception {

    /** Field causes */
    final Vector<Throwable> causes = new Vector<Throwable>();

    /**
     * Constructor EVIORecordException creates a new EVIORecordException instance.
     *
     * @param message of type String
     */
    public EVIORecordException(String message) {
        super(message);
    }

    /**
     * Method addCause ...
     *
     * @param e of type Throwable
     */
    public void addCause(Throwable e) {
        causes.add(e);
    }

    /**
     * Method getCausesArray returns the causesArray of this EVIORecordException object.
     *
     * @return the causesArray (type Throwable[]) of this EVIORecordException object.
     */
    public Throwable[] getCausesArray() {
        return (Throwable[]) causes.toArray();
    }

    /**
     * Method getCauses returns the causes of this EVIORecordException object.
     *
     * @return the causes (type Vector<Throwable>) of this EVIORecordException object.
     */
    public Vector<Throwable> getCauses() {
        return causes;
    }

    /**
     * Method throwMe ...
     *
     * @return boolean
     */
    public boolean throwMe() {
        return !causes.isEmpty();
    }

    /**
     * Method toString ...
     *
     * @return String
     */
    public String toString() {
        String ret = "";

        for (Throwable cause1 : causes) {
            if (cause1.getClass() == EVIOHdrException.class) {
                ret += "hdr: " + ((EVIOHdrException) cause1).getHeader() + " ";
            }
            ret += cause1.getMessage() + "\n";
        }
        return ret;
    }
}
