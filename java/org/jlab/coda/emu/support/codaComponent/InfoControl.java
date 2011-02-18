/*
 * Copyright (c) 2011, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.codaComponent;

import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.messaging.RCConstants;

import java.util.EnumSet;
import java.util.HashMap;

/**
 * This enum enumerates the possible actions of getting information.
 *
 * @author timmer
 */
public enum InfoControl implements Command {

    /** Command to get the state .*/
    GET_STATE("Get the state", RCConstants.rcGetState),
    /** Command to get the status. */
    GET_STATUS("Get the status", RCConstants.rcGetStatus),
    /** Command to get the object type. */
    GET_OBJECT_TYPE("Get the object type", RCConstants.rcGetObjectType),
    /** Command to get the coda class. */
    GET_CODA_CLASS("Get the coda class", RCConstants.rcGetCodaClass);

    /** Map of arguments contained in the message from run control (in payload). */
    private final HashMap<String, Object> args = new HashMap<String, Object>();

    /** Description of this command. */
    private String description;

    /** Type in incoming message from run control requesting this command. */
    private String messageType;

    /** Is this command enabled? */
    private boolean enabled = true;

    /** Map containing mapping of string of incoming message type from run control to an enum/command. */
    private static HashMap<String, InfoControl> messageTypeToEnumMap = new HashMap<String, InfoControl>();

    // Fill static hashmap after all enum objects created.
    static {
        for (InfoControl item : InfoControl.values()) {
            messageTypeToEnumMap.put(item.messageType, item);
        }
    }

    /**
     * Constructor that creates a new InfoControl instance.
     * @param description description of this session control command
     * @param messageType type in incoming message from run control requesting this command
     */
    InfoControl(String description, String messageType) {
        this.description = description;
        this.messageType = messageType;
    }

    /**
     * Map from type of incoming message from run control to a particular enum.
     * @param s type contained in incoming message from run control.
     * @return associated enum, else null.
     */
    public static InfoControl get(String s) {
        return messageTypeToEnumMap.get(s);
    }

    /**
     * {@inheritDoc}
     * @return {@inheritDoc}
     */
    public String description() {
        return description;
    }

    /**
     * {@inheritDoc}
     * @return {@inheritDoc}
     */
    public boolean isEnabled() {
        return enabled;
    }

    /** {@inheritDoc} */
    public void enable() {
        enabled = true;
    }

    /** {@inheritDoc} */
    public void disable() {
        enabled = false;
    }

    /**
     * {@inheritDoc}
     * @param cmds {@inheritDoc}
     */
    public void allow(EnumSet cmds) {
    }

    /**
     * {@inheritDoc}
     * @param tag {@inheritDoc}
     * @return {@inheritDoc}
     */
    public Object getArg(String tag) {
        return args.get(tag);
    }

    /**
     * {@inheritDoc}
     *
     * @param tag {@inheritDoc}
     * @param value {@inheritDoc}
     */
    public void setArg(String tag, Object value) {
        args.put(tag, value);
    }

    /**
     * {@inheritDoc}
     * @return {@inheritDoc}
     */
    public boolean hasArgs() {
        return !args.isEmpty();
    }

    /** {@inheritDoc} */
    public void clearArgs() {
        args.clear();
    }

    /**
     * {@inheritDoc}. Not used.
     * @return {@inheritDoc}
     */
    public State success() {
        return null;
    }
}