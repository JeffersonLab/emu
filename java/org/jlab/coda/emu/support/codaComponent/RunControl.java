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
 * This enum enumerates the possible actions of a "run object"
 * as opposed to a "session object" or "coda object". The run
 * object is aware of the run but does not respond to CODA
 * run control transition commands as a coda object would.
 *
 * @author heyes
 * @author timmer
 */

public enum RunControl implements Command {

    /** Command to set run number. */
    SET_RUN_NUMBER("Set the run number", null, RCConstants.setRunNumber),
    /** Command to get run number .*/
    GET_RUN_NUMBER("Get the run number", null, RCConstants.getRunNumber),
    /** Command to get run type. */
    SET_RUN_TYPE("Set the run type", null, RCConstants.setRunType),
    /** Command to get run type. */
    GET_RUN_TYPE("Get the run type", null, RCConstants.getRunType);

    /** Description of this command. */
    private final String description;

    /** Map of arguments contained in the message from run control (in payload). */
    private HashMap<String, Object> args = new HashMap<String, Object>();

    /** Type in incoming message from run control requesting this command. */
    private String messageType;

    private String success = null;

    /** Is this command enabled? */
    private boolean enabled = true;

    /** Map containing mapping of string of incoming message type from run control to an enum/command. */
    private static HashMap<String, RunControl> messageTypeToEnumMap = new HashMap<String, RunControl>();

    // Fill static hashmap after all enum objects created.
    static {
        for (RunControl item : RunControl.values()) {
            messageTypeToEnumMap.put(item.messageType, item);
        }
    }

    /**
     * Constructor which creates a new RunControl instance.
     *
     * @param description description of the run control command
     * @param success of the run control command
     * @param messageType type in incoming message from run control requesting this command
     */
    RunControl (String description, String success, String messageType) {
        this.description = description;
        this.success = success;
        this.messageType = messageType;
    }

    /**
     * Map from type of incoming message from run control to a particular enum.
     * @param s type contained in incoming message from run control.
     * @return associated enum, else null.
     */
    public static RunControl get(String s) {
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
     * {@inheritDoc}
     * @return {@inheritDoc}
     */
    public State success() {
        if (success != null) {
            return CODAState.valueOf(success);
        }
        return null;
    }

}