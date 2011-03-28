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
    CONFIGURE("Load configuration", configure, 0, InputType.PAYLOAD_TEXT, configPayloadFileName),
    /** Download transition. */
    DOWNLOAD("Apply configuration and load", download, 0, null, null),
    /** Prestart transition. */
    PRESTART("Prepare to start", prestart, 0, null, null),
    /** Go transition. */
    GO("Start taking data", go, 0, null, null),
    /** End transition. */
    END("End taking data", end, 0, null, null),
    /** Pause transition. */
    PAUSE("Pause taking data", pause, 0, null, null),
    /** Reset transition. */
    RESET("Return to configured state", reset, 0, null, null),

    // coda/info/

    /** Command to get the state .*/
    GET_STATE("Get state", getState, -1, null, null),
    /** Command to get the status. */
    GET_STATUS("Get status", getStatus, -1, null, null),
    /** Command to get the object type. */
    GET_OBJECT_TYPE("Get object type", getObjectType, -1, null, null),
    /** Command to get the coda class. */
    GET_CODA_CLASS("Get coda class", getCodaClass, -1, null, null),

    // run/control/

    /** Command to set run number. */
    SET_RUN_NUMBER("Set run number", setRunNumber, 1, InputType.PAYLOAD_INT, "RUNNUMBER"),
    /** Command to get run number .*/
    GET_RUN_NUMBER("Get run number", getRunNumber, -1, null, null),
    /** Command to set run type. */
    SET_RUN_TYPE("Set run type", setRunType, -1, null, null),
    /** Command to get run type. */
    GET_RUN_TYPE("Get run type", getRunType, -1, null, null),

    // session/setOption/    none implemented

    /** Command to set run number. */
    SET_FILE_PATH("Set config file path", setFilePath, -1, null, null),
    /** Command to get run number .*/
    SET_FILE_PREFIX("Get config file prefix", setFilePrefix, -1, null, null),
    /** Command to set run type. */
    SET_CONFIG_FILE("Set xml config file contents", setConfigFile, -1, null, null),

    // session/control/

    /** Command to set time interval between reporting messages in seconds. */
    SET_INTERVAL("Set interval", setInterval, 1, InputType.USER_INT, null),
    /** Command to set start reporting. */
    START_REPORTING("Start reporting", startReporting, 1, null, null),
    /** Command to set stop reporting. */
    STOP_REPORTING("Stop reporting", stopReporting, 1, null, null),
    /** Command to exit. */
    EXIT("Shutdown coda component", exit, 1, null, null),
    /** Command to set state. */
    SET_STATE("Set state", setState, -1, null, null),
    /** Command to set session. */
    SET_SESSION("Set session", setSession, -1, null, null),
    /** Command to get session. */
    GET_SESSION("Get session", getSession, -1, null, null),
    /** Command to release session. */
    RELEASE_SESSION("Release session", releaseSession, -1, null, null),
    /** Command to start. */
    START("Start", start, -1, null, null),
    /** Command to stop. */
    STOP("Stop", stop, -1, null, null),
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
    private final static HashMap<String, CODACommand> commandTypeToEnumMap = new HashMap<String, CODACommand>();


    // Fill static hashmap after all enum objects created.
    static {
        for (CODACommand item : CODACommand.values()) {
            commandTypeToEnumMap.put(item.getCmdString(), item);
//System.out.println("map : " + item.getCmdString() + "  ->  " + item.name());
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
     * @param description description of command
     * @param cmdString string from Run Control specifying this command
     * @param guiGroup  in debug GUI there are 4 gui groups that are used,
     *                  0-3,each with one line of buttons.
     *                  All commands with other gui group values are
     *                  ignored in debug gui.
     * @param inputType how is this command's input obtained from cMsg message:
     *                  getText, getUserInt, payload text or payload int?
     * @param payloadName if input type is payload, this is payload's name
     */
    CODACommand(String description, String cmdString,
                int guiGroup, InputType inputType, String payloadName) {

        this.cmdString   = cmdString;
        this.description = description;
        this.guiGroup    = guiGroup;
        this.inputType   = inputType;
        this.payloadName = payloadName;
    }


    public InputType getInputType() {
        return inputType;
    }

    public int getGuiGroup() {
        return guiGroup;
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