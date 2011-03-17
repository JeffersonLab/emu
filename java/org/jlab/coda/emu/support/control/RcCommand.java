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

import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.codaComponent.EmuCommand;

import java.util.HashMap;

/**
 * @author timmer
 * Date: 3/14/11
 */
public class RcCommand {


    String cmd;

    EmuCommand emuCommand;

    /** Map of arguments contained in the message from run control (in payload). */
    private final HashMap<String, Object> args = new HashMap<String, Object>();

    private State success = null;


    public RcCommand(EmuCommand emuCommand) {
        setEmuCommand(emuCommand);
        cmd = emuCommand.getCmdString();
    }


    public void setEmuCommand(EmuCommand emuCommand) {
        this.emuCommand = emuCommand;

        // see if this is a transition command (which has a state result)
        for (CODATransition transition : CODATransition.values()) {
            if (emuCommand.name().equalsIgnoreCase(transition.name())) {
                // it is a transition command, store it successful state
                success = transition.success();
            }
        }
    }


    public EmuCommand getEmuCommand() {
        return emuCommand;
    }

    public String name() {
        return emuCommand.name();
    }

    public String description() {
        return emuCommand.getDescription();
    }


    /**
     * Get the object (actually a cMsgPayloadItem) associated with this tag (unique mapping).
     *
     * @param tag name
     * @return Object object associated with tag
     */
    public Object getArg(String tag) {
        return args.get(tag);
    }

    /**
     * Keep a set of tags each associated with a cMsgPayloadItem
     * (although Object is used instead to avoid strict dependence
     * on cMsg).
     *
     * @param tag   of type String (name of cMsgPayloadItem)
     * @param value of type Object (actually cMsgPayloadItem)
     */
    public void setArg(String tag, Object value) {
        args.put(tag, value);
    }

    /**
     * Does this command have any associated objects (args)?
     * @return boolean
     */
    public boolean hasArgs() {
        return !args.isEmpty();
    }


    /**
     * If this object is a transition command, this method
     * returns the state the Emu enters upon its success.
     * If not, it returns null.
     *
     * @return the state the Emu enters upon success of a transition command, else null.
     */
    public State success() {
        return success;
    }
}
