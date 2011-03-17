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
public enum EmuCommand {

    // run/transition/

    /** Configure transition. */
    CONFIGURE("Load configuration", configure, false, false, true, 0, null), // actually in session/control/
    /** Download transition. */
    DOWNLOAD("Apply configuration and load", download, false, false, true, 0, null),
    /** Prestart transition. */
    PRESTART("Prepare to start", prestart, false, false, true, 0, null),
    /** Go transition. */
    GO("Start taking data", go, false, false, true, 0, null),
    /** End transition. */
    END("End taking data", end, false, false, true, 0, null),
    /** Pause transition. */
    PAUSE("Pause taking data", pause, false, false, true, 0, null),
    /** Reset transition. */
    RESET("Return to configured state", reset, false, false, true, 0, null), // actually in session/control/

    // coda/info/

    /** Command to get the state .*/
    GET_STATE("Get state", getState, false, true, false, 1, InputType.STRING),
    /** Command to get the status. */
    GET_STATUS("Get status", getStatus, false, true, false, 1, InputType.STRING),
    /** Command to get the object type. */
    GET_OBJECT_TYPE("Get object type", getObjectType, false, true, false, 1, InputType.STRING),
    /** Command to get the coda class. */
    GET_CODA_CLASS("Get coda class", getCodaClass, false, true, false, 1, InputType.STRING),

    // run/control/

    /** Command to set run number. */
    SET_RUN_NUMBER("Set run number", setRunNumber, true, false, false, 1, null),
    /** Command to get run number .*/
    GET_RUN_NUMBER("Get run number", getRunNumber, false, true, false, 1, InputType.STRING),
    /** Command to set run type. */
    SET_RUN_TYPE("Set run type", setRunType, true, false, false, 1, null),
    /** Command to get run type. */
    GET_RUN_TYPE("Get run type", getRunType, false, true, false, 1, InputType.STRING),

    // session/control/

    /** Command to set state. */
    SET_STATE("Set state", setState, true, false, false, 3, null),
    /** Command to set session. */
    SET_SESSION("Set session", setSession, true, false, false, 3, null),
    /** Command to get session. */
    GET_SESSION("Get session", getSession, false, true, false, 3, InputType.STRING),
    /** Command to release session. */
    RELEASE_SESSION("Release session", releaseSession, false, false, false, 3, null),
    /** Command to set start reporting. */
    START_REPORTING("Start reporting", startReporting, false, false, false, 3, null),
    /** Command to set stop reporting. */
    STOP_REPORTING("Stop reporting", stopReporting, false, false, false, 3, null),
    /** Command to set interval. */
    SET_INTERVAL("Set interval", setInterval, true, false, false, 3, InputType.INT),
    /** Command to start. */
    START("Start", start, false, false, false, 3, null),
    /** Command to stop. */
    STOP("Stop", stop, false, false, false, 3, null),
    /** Command to exit. */
    EXIT("Shutdown coda component", exit, false, false, false, 3, null),

    // session/setOption/

    /** Command to set run number. */
    SET_FILE_PATH("Set config file path", setRunNumber, true, false, false, 2, null),
    /** Command to get run number .*/
    SET_FILE_PREFIX("Get config file prefix", getRunNumber, true, false, false, 2, null),
    /** Command to set run type. */
    SET_XML_FILE("Set xml config file name", setRunType, true, false, false, 2, null),
    /** Command to get run type. */
    SET_XML_STRING("Set xml config string", getRunType, true, false, false, 2, null);


    static enum InputType {
        STRING,
        DOUBLE,
        INT;
    }


    /** String coming from Run Control specifying this command. */
    private final String cmdString;

    /** Description of this command. */
    private final String description;

    /** This command requires input data from Run Control. */
    private final boolean hasInput;

    /** This command has output that Run Control needs. */
    private final boolean hasOutput;

    /** This command has a result which the emu must use (eg. changes state). */
    private final boolean hasResult;

    /** If displayed in GUI, display with commands of same gui group. */
    private final int guiGroup;

    /** If hasInput, what type of input is required? */
    private final InputType inputType;


    /** Map of string of incoming message from run control to an enum/command. */
    private final static HashMap<String, EmuCommand> commandTypeToEnumMap = new HashMap<String, EmuCommand>();


    // Fill static hashmap after all enum objects created.
    static {
        for (EmuCommand item : EmuCommand.values()) {
            commandTypeToEnumMap.put(item.getCmdString(), item);
        }
    }


    /**
     * Map from type of incoming message from run control to a particular enum.
     * @param s type contained in incoming message from run control.
     * @return associated enum, else null.
     */
    public static EmuCommand get(String s) {
        return commandTypeToEnumMap.get(s);
    }


    /**
     * Constructor CODATransition creates a new CODATransition instance.
     *
     * @param description of command
     * @param cmdString string from Run Control specifying this command
     */
    EmuCommand(String description, String cmdString,
               boolean hasInput, boolean hasOutput, boolean hasResult,
               int guiGroup, InputType inputType) {

        this.cmdString   = cmdString;
        this.description = description;
        this.hasInput    = hasInput;
        this.hasOutput   = hasOutput;
        this.hasResult   = hasResult;
        this.guiGroup    = guiGroup;
        this.inputType   = inputType;
    }

    // new

    public boolean hasInput() {
        return hasInput;
    }

    public boolean hasOutput() {
        return hasOutput;
    }

    public boolean hasResult() {
        return hasResult;
    }

    public InputType getInputType() {
        return inputType;
    }

    public int getGuiGroup() {
        return guiGroup;
    }

    /**
     * This method returns an EnumSet object of all the EmuCommands which
     * are of the given gui group. This is used to automatically create the
     * DebugGUI.
     *
     * @param group gui group we want the members of
     * @return EnumSet of EmuCommands which are member of the given gui group.
     */
    public static EnumSet<EmuCommand> getGuiGroup(int group) {
        // put all commands in the set
        EnumSet<EmuCommand> enumSet = EnumSet.allOf(EmuCommand.class);

        // subtract out items that do NOT have the correct gui group
        for (EmuCommand item : EmuCommand.values()) {
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