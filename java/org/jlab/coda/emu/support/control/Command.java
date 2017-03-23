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

package org.jlab.coda.emu.support.control;



import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.cMsg.cMsgPayloadItem;
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.codaComponent.State;

import java.util.HashMap;

/**
 * This class represents a command given to an emu by CODA's Run
 * Control. It is used to wrap a static enum CODACommand object
 * (the command itself), also allowing storage of mutable, non-static
 * data (arguments accompanying the command) which will be
 * discarded once the command is executed.
 *
 * @author timmer
 * Date: 3/14/11
 */
public class Command {

    /**
     * If this object wraps a transition command, this is
     * the state the emu is in if the transition successful.
     */
    private State success;

    /** Original cMsg message from Run Control containing command to be executed. */
    private cMsgMessage msg;

    /** Is this command from run control or from the debug gui? */
    private boolean fromDebugGui;

    /** CODACommand object to be wrapped by this class. */
    private final CODACommand codaCommand;

    /** Map of arguments contained in the message from run control (in payload). */
    private final HashMap<String, cMsgPayloadItem> args = new HashMap<>(16);



    /**
     * Constructor.
     * @param codaCommand CODACommand object to be wrapped by this class.
     */
    public Command(CODACommand codaCommand) {
        this.codaCommand = codaCommand;

        // see if this is a transition command (which has a state result)
        for (CODATransition transition : CODATransition.values()) {
            if (codaCommand.name().equalsIgnoreCase(transition.name())) {
                // it is a transition command, store it successful state
                success = transition.success();
            }
        }
    }

    /**
     * Get the static command object representing the Run Control command.
     * @return static command object representing the Run Control command.
     */
    public CODACommand getCodaCommand() {
        return codaCommand;
    }

    /**
     * Get the name of the Run Control command.
     * @return name of the Run Control command.
     */
    public String name() {
        return codaCommand.name();
    }

    /**
     * Get the description of the Run Control command.
     * @return description of the Run Control command.
     */
    public String description() {
        return codaCommand.getDescription();
    }

    /**
     * Get the cMsg message containing the Run Control command.
     * @return cMsg message containing the Run Control command.
     */
    public cMsgMessage getMessage() {
        return msg;
    }

    /**
     * Set the cMsg message containing the Run Control command.
     * @param msg cMsg message containing the Run Control command.
     */
    public void setMessage(cMsgMessage msg) {
        this.msg = msg;
    }

    /**
     * Is this command from the debug gui or run control?
     * @return <code>true</code> if command from debug gui, else <code>false</code>
     */
    public boolean isFromDebugGui() {
        return fromDebugGui;
    }

    /**
     * Set whether this command is from the debug gui or run control.
     * @param fromDebugGui <code>true</code> if command from debug gui, else <code>false</code>
     */
    public void fromDebugGui(boolean fromDebugGui) {
        this.fromDebugGui = fromDebugGui;
    }

    /**
     * Get the object (a cMsgPayloadItem object) associated
     * with this tag (unique mapping). The tag is the name of
     * a payload item in the cMsg message sent as a command by
     * Run Control.
     *
     * @param tag name
     * @return Object object associated with tag
     */
    public cMsgPayloadItem getArg(String tag) {
        return args.get(tag);
    }

    /**
     * Keep a set of tags, each associated with a cMsgPayloadItem
     * (although Object is used instead to avoid strict dependence
     * on cMsg). This method adds one such tag-payloadItem pairing.
     *
     * @param tag   of type String (name of cMsgPayloadItem)
     * @param value of type Object (actually cMsgPayloadItem)
     */
    public void setArg(String tag, cMsgPayloadItem value) {
        args.put(tag, value);
    }

    /**
     * Does this command have any associated objects (args)?
     * @return <code>true</code> if this command has any associated cMsgPayloadItems,
     *         else <code>false</code>.
     */
    public boolean hasArgs() {
        return !args.isEmpty();
    }


    /**
     * If this object is a transition command, this method
     * returns the state the Emu enters upon its success.
     * If not a transition command, it returns null.
     *
     * @return the state the Emu enters upon success of a transition command, else null.
     */
    public State success() {
        return success;
    }
}
