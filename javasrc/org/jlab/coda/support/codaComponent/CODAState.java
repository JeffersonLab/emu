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

package org.jlab.coda.support.codaComponent;

import org.jlab.coda.support.control.State;

import java.util.EnumSet;
import java.util.Vector;

/** @author heyes */
public enum CODAState implements State {
    /** Field UNCONFIGURED */UNCONFIGURED("codaComponent is not configured", EnumSet.noneOf(CODATransition.class)),
    /** Field CONFIGURED */CONFIGURED("configuration is loaded", EnumSet.of(CODATransition.download)),
    /** Field DOWNLOADED */DOWNLOADED("external modules loaded", EnumSet.of(CODATransition.download, CODATransition.prestart)),
    /** Field PRESTARTED */PRESTARTED("modules initialized and ready to go", EnumSet.of(CODATransition.go)),
    /** Field ACTIVE */ACTIVE("Taking data", EnumSet.of(CODATransition.pause, CODATransition.end)),
    /** Field PAUSED */PAUSED("Data taking is paused", EnumSet.of(CODATransition.go, CODATransition.end)),
    /** Field ENDED */ENDED("Data taking is ended", EnumSet.of(CODATransition.prestart)),
    /** Field ERROR */ERROR("An error has occured", EnumSet.noneOf(CODATransition.class));

    /** Field description */
    private final String description;
    /** Field causes */
    private final Vector<Throwable> causes = new Vector<Throwable>();
    /** Field allowed */
    private final EnumSet allowed;

    private boolean reported = false;

    /**
     * Constructor CODAState creates a new CODAState instance.
     *
     * @param description of type String
     * @param allowed     of type EnumSet
     */
    CODAState(String description, EnumSet allowed) {
        this.description = description;
        this.allowed = allowed;

    }

    /** @return the description */
    /* (non-Javadoc)
    * @see org.jlab.coda.support.control.State#getDescription()
    */
    public String getDescription() {
        return description;
    }

    /**
     * Method getCauses returns the causes of this CODAState object.
     *
     * @return the causes (type Vector<Throwable>) of this CODAState object.
     */
    public Vector<Throwable> getCauses() {
        return causes;
    }

    /**
     * Method allowed ...
     *
     * @return EnumSet
     */
    public EnumSet allowed() {
        return allowed;
    }

}
