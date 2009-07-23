package org.jlab.coda.support.codaComponent;

import org.jlab.coda.support.control.State;

/**
 * An CODA component that implements this interface has a name and a state.
 */
public interface StatedObject {
    /**
     * Get the name of this CODA component
     * @return the name of this CODA component
     */
    public String name();

    /**
     * Get the state of this CODA component.
     * @return the state of this CODA componen
     */
    public State state();
}