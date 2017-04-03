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
 * @author heyes
 * @author timmer
 */
public interface StatedObject {

    /**
     * Get the state of this object.
     * @return the state of this object
     */
    public CODAStateIF state();

    /**
     * Get any available error information.
     * @return any available error information; null if none
     */
    public String getError();

}