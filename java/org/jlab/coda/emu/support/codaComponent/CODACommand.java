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

import java.util.EnumSet;
import java.util.HashMap;

import static org.jlab.coda.emu.support.messaging.RCConstants.*;

/**
 * This class enumerates all the commands that Run Control can send to this Emu.
 * These objects only contain static data.
 *
 * @author timmer
 * Date: 3/14/11
 */
public enum CODACommand {

    // run/transition/

    /** Configure transition. */
    CONFIGURE("Load configuration", configure, true, 0, InputType.PAYLOAD_TEXT, configPayloadFileName),
    /** Download transition. */
    DOWNLOAD("Apply configuration and load", download, true, 0, null, null),
    /** Prestart transition. */
    PRESTART("Prepare to start", prestart, true, 0, InputType.PAYLOAD_INT, "runNumber"),
    /** Go transition. */
    GO("Start taking data", go, true, 0, null, null),
    /** End transition. */
    END("End taking data", end, true, 0, null, null),
    /** Reset transition. */
    RESET("Return to configured state", reset, true, 0, null, null),

    // coda/info/

    /** Command to get the state .*/
    GET_STATE("Get state", getState, false, -1, null, null),
    /** Command to get the status. */
    GET_STATUS("Get status", getStatus, false, -1, null, null),
    /** Command to get the object type. */
    GET_OBJECT_TYPE("Get object type", getObjectType, false, -1, null, null),
    /** Command to get the coda class. */
    GET_CODA_CLASS("Get coda class", getCodaClass, false, -1, null, null),

    // run/control/

    /** Command to set buffer level - smallest evio-event/et-buffer for ROCs. */
    SET_BUF_LEVEL("Set buffer level", setBufferLevel, false, -1, null, null),
    /** Command to get buffer level - smallest evio-event/et-buffer for ROCs. */
    GET_BUF_LEVEL("Get buffer level", getBufferLevel, false, -1, null, null),
    /** Command to enable output channels. */
    ENABLE_OUTPUT("Enable output channels", enableOutput, false, -1, null, null),
    /** Command to disable output channels. */
    DISABLE_OUTPUT("Disable output channels", disableOutput, false, -1, null, null),

    // session/control/

    /** Command to set time interval between reporting messages in seconds. */
    SET_INTERVAL("Set interval", setInterval, false, 1, InputType.USER_INT, null),
    /** Command to set start reporting. */
    START_REPORTING("Start reporting", startReporting, false, 1, null, null),
    /** Command to set stop reporting. */
    STOP_REPORTING("Stop reporting", stopReporting, false, 1, null, null),
    /** Command to exit. */
    EXIT("Shutdown coda component", exit, false, 1, null, null),
    /** Command to set session. */
    SET_SESSION("Set session", setSession, false, -1, null, null),
    /** Command to get session. */
    GET_SESSION("Get session", getSession, false, -1, null, null),
    /** Command to set run number .*/
    SET_RUN_NUMBER("Set run number", setRunNumber, false, -1, null, null),
    /** Command to get run number .*/
    GET_RUN_NUMBER("Get run number", getRunNumber, false, -1, null, null),
    /** Command to set run type. */
    SET_RUN_TYPE("Set run type", setRunType, false, -1, null, null),
    /** Command to get run type. */
    GET_RUN_TYPE("Get run type", getRunType, false, -1, null, null),
    /** Command to get config id. */
    GET_CONFIG_ID("Get config id", getConfigId, false, -1, null, null),
    ;



    /** Ways in which a command's input obtained from a cMsg message. */
    public static enum InputType {
        /** By calling msg.getText(). */
        TEXT,
        /** By calling msg.userInt(). */
        USER_INT,
        /** By getting a payload item of text. */
        PAYLOAD_TEXT,
        /** By getting a payload item of int. */
        PAYLOAD_INT,
        ;
    }


    /** String coming from Run Control specifying this command. */
    private final String cmdString;

    /** Description of this command. */
    private final String description;

    /** Does this command represent a runcontrol transition? */
    private boolean isTransition;

    /**
     * If this command is displayed in GUI, display with commands of same gui group
     * in the same toolbar.
     * There are 4 gui groups that are used, 0-3, each with one line of buttons
     * in the debug gui. All commands with other gui group values are ignored in
     * the debug gui.
     */
    private final int guiGroup;

    /** How is this command's input obtained from a cMsg message? */
    private final InputType inputType;

    /** If inputType = InputType.PAYLOAD_TEXT / INT, this is the payload name. */
    private String payloadName;


    /** Map of string of incoming message from run control to an enum/command. */
    private final static HashMap<String, CODACommand> commandTypeToEnumMap = new HashMap<>();


    // Fill static hashmap after all enum objects created.
    static {
        for (CODACommand item : CODACommand.values()) {
            commandTypeToEnumMap.put(item.getCmdString(), item);
        }
    }


    /**
     * Map from type of incoming message from run control to a particular enum.
     * @param s type contained in incoming message from run control.
     * @return associated enum, else null.
     */
    public static CODACommand get(String s) {
        return commandTypeToEnumMap.get(s);
    }


    /**
     * Constructor CODATransition creates a new CODATransition instance.
     *
     * @param description  description of command
     * @param cmdString    string from Run Control specifying this command
     * @param isTransition does this command represent a runcontrol transition?
     * @param guiGroup     in debug GUI there are 2 gui groups that are used.
     *                     Group values of 0 & 1 each have one line of buttons.
     *                     All commands with other gui group values (-1) are
     *                     ignored in debug gui.
     * @param inputType    how is this command's input obtained from cMsg message:
     *                     getText, getUserInt, payload text or payload int?
     * @param payloadName  if input type is payload, this is payload's name
     */
    CODACommand(String description, String cmdString, boolean isTransition,
                int guiGroup, InputType inputType, String payloadName) {

        this.cmdString    = cmdString;
        this.description  = description;
        this.isTransition = isTransition;
        this.guiGroup     = guiGroup;
        this.inputType    = inputType;
        this.payloadName  = payloadName;
    }


    public InputType getInputType() {
        return inputType;
    }

    public int getGuiGroup() {
        return guiGroup;
    }

    public boolean isTransition() {
        return isTransition;
    }

    public String getPayloadName() {
        return payloadName;
    }

    /**
     * This method returns an EnumSet object of all the CODACommands which
     * are of the given gui group. This is used to automatically create the
     * DebugGUI.
     *
     * @param group gui group we want the members of
     * @return EnumSet of CODACommands which are member of the given gui group.
     */
    public static EnumSet<CODACommand> getGuiGroup(int group) {
        // put all commands in the set
        EnumSet<CODACommand> enumSet = EnumSet.allOf(CODACommand.class);

        // subtract out items that do NOT have the correct gui group
        for (CODACommand item : CODACommand.values()) {
            if (item.getGuiGroup() != group) {
                enumSet.remove(item);
            }
        }

        return enumSet;
    }

    /**
     * Get the string coming from Run Control specifying this command.
     * @return string coming from Run Control specifying this command.
     */
    public String getCmdString() {
        return cmdString;
    }

    /**
     * Get this command's description.
     * @return this command's description.
     */
    public String getDescription() {
        return description;
    }


}