package org.jlab.coda.emu.support.codaComponent;

import org.jlab.coda.emu.support.control.State;

/**
 * An object that implements this interface has a name and a state.
 */
public interface StatedObject {
    /**
     * Get the name of this object.
     * @return the name of this object
     */
    public String name();

    /**
     * Get the state of this object.
     * @return the state of this Cobject
     */
    public State state();
}