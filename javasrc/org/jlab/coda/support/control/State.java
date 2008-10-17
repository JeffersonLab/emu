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

package org.jlab.coda.support.control;

import java.util.EnumSet;
import java.util.Vector;

/** @author heyes */
public interface State {
    /** @return the state name */
    public String name();

    /** @return the description */
    public String getDescription();

    /**
     * Method getCauses returns the causes of this CODAState object.
     *
     * @return the causes (type Vector<Throwable>) of this CODAState object.
     */
    public Vector<Throwable> getCauses();

    /**
     * Method allowed ...
     *
     * @return EnumSet
     */
    public EnumSet allowed();

}
