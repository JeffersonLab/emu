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

package org.jlab.coda.emu.support.control;

import java.util.EnumSet;
import java.util.Vector;

/** @author heyes */
public interface State {
    /**
     * Get the state name.
     * @return state name.
     */
    public String name();

    /**
     * Get the description of this state.
     * @return description of this state
     */
    public String getDescription();

    /**
     * Get the vector containing the causes of any exceptions
     * of an attempted transition from this state.
     * @return vector(type Vector<Throwable>) of causes of any execeptions
     *         of an attempted transition from this state.
     */
    public Vector<Throwable> getCauses();

    /**
     * This method returns a set of the allowed transitions from this state.
     * @return a set of the allowed transitions from this state.
     */
    public EnumSet allowed();

}
