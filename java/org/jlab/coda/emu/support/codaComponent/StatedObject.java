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

package org.jlab.coda.emu.support.codaComponent;

/**
 * An object that implements this interface has a state.
 */
public interface StatedObject {
    /**
     * Get the state of this object.
     * @return the state of this object
     */
    public State state();
}