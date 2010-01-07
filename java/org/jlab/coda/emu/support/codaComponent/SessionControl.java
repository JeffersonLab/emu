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

import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.messaging.RCConstants;

import java.util.EnumSet;
import java.util.HashMap;

/**
 * This enum enumerates the possible actions of a "session object"
 * as opposed to a "run object" or "coda object". The session
 * object is aware only of the run number, run type, and reporting status.
 *
 * @author heyes
 * @author timmer
 */
public enum SessionControl implements Command {

    /** Command to set run number. */
    SET_RUN_NUMBER("Set the run number", RCConstants.setRunNumber),
    /** Command to get run number .*/
    GET_RUN_NUMBER("Get the run number", RCConstants.getRunNumber),
    /** Command to set run type. */
    SET_RUN_TYPE("Set the run type", RCConstants.setRunType),
    /** Command to get run type. */
    GET_RUN_TYPE("Get the run type", RCConstants.getRunType),
    /** Command to set start reporting. */
    START_REPORTING("start reporting", RCConstants.startReporting),
    /** Command to set stop reporting. */
    STOP_REPORTING("stop reporting", RCConstants.stopReporting);

    /** Map of arguments contained in the message from run control (in payload). */
    private final HashMap<String, Object> args = new HashMap<String, Object>();

    /** Description of this command. */
    private String description;

    /** Type in incoming message from run control requesting this command. */
    private String messageType;

    /** Is this command enabled? */
    private boolean enabled = true;

    /** Map containing mapping of string of incoming message type from run control to an enum/command. */
    private static HashMap<String, SessionControl> messageTypeToEnumMap = new HashMap<String, SessionControl>();

    // Fill static hashmap after all enum objects created.
    static {
        for (SessionControl item : SessionControl.values()) {
            messageTypeToEnumMap.put(item.messageType, item);
        }
    }

    /**
     * Constructor that creates a new SessionControl instance.
     * @param description description of this session control command
     * @param messageType type in incoming message from run control requesting this command
     */
    SessionControl(String description, String messageType) {
        this.description = description;
        this.messageType = messageType;
    }

    /**
     * Map from type of incoming message from run control to a particular enum.
     * @param s type contained in incoming message from run control.
     * @return associated enum, else null.
     */
    public static SessionControl get(String s) {
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