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

import org.jlab.coda.emu.support.codaComponent.CODATransition;

import java.util.EnumSet;

/**
 * This interface defines a state of an object.
 * @author heyes
 * */
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
     * Get the set of allowed transitions from this state.
     * @return set of allowed transitions from this state.
     */
    public EnumSet<CODATransition> allowed();

}
