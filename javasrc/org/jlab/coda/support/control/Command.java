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

/**
 * Interface describing a generic command.
 * @author heyes
 */
public interface Command {

    /**
     * Get the name of this command.
     * @return name of this command
     */
    public String name();

    /**
     * Description of this command.
     * @return the description of this command
     */
    public String description();

    /**
     * Is this command enabled?
     * @return is this command is enabled?
     */
    public boolean isEnabled();

    /** Enable this command. */
    public void enable();

    /** Disable this command. */
    public void disable();

    /**
     * Enable the given set of commands.
     * @param cmds set of commands to be enabled
     */
    public void allow(EnumSet cmds);

    /**
     * Get the object (actually a cMsgPayloadItem) associated with this tag (unique mapping).
     *
     * @param tag name
     * @return Object object associated with tag
     */
    public Object getArg(String tag);

    /**
     * Keep a set of tags each associated with a cMsgPayloadItem
     * (although Object is used instead to avoid strict dependence
     * on cMsg).
     *
     * @param tag   of type String (name of cMsgPayloadItem)
     * @param value of type Object (actually cMsgPayloadItem)
     */
    public void setArg(String tag, Object value);

    /**
     * Does this command have any associated objects (args)?
     * @return boolean
     */
    public boolean hasArgs();

    /** Clear all args. */
    public void clearArgs();

    /**
     * Get the resulting State (enum object) if command suceeded.
     * @return resulting State (enum object) if command suceeded
     */
    public State success();
}
