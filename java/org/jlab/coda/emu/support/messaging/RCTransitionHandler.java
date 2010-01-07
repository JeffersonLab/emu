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

package org.jlab.coda.emu.support.messaging;

import org.jlab.coda.cMsg.cMsgCallbackInterface;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.control.Command;

import java.util.Set;

/**
 * <pre>
 * Class <b>transitionHandler </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 24, 2008
 */
class RCTransitionHandler extends GenericCallback implements cMsgCallbackInterface {

    /** Connection to cMsg server. */
    private CMSGPortal cmsgPortal;

    /**
     * Constructor RCTransitionHandler creates a new RCTransitionHandler instance.
     *
     * @param cmsgPortal of type CMSGPortal
     */
    public RCTransitionHandler(CMSGPortal cmsgPortal) {
        this.cmsgPortal = cmsgPortal;
    }

    /**
     * Method callback for subscription of subject = * and
     * type = run/transition/* .
     *
     * @param msg cMsgMessage being received
     * @param o   object given in subscription & passed in here (null in this case)
     */
    public void callback(cMsgMessage msg, Object o) {
System.out.println("GOT " + msg.getType() + " message");
        try {
            String type = msg.getType();
            String cmdS = (type.substring(type.lastIndexOf("/") + 1)).toUpperCase();

            // CODATransition is an enum but it implements Command so it is a Command object.
            // Examples: CONFIGURE, DOWNLOAD, PRESTART, GO, END, PAUSE, RESUME.
            // The string cmdS may not be an allowed enum value, in which case an
            // IllegalArgumentException will be thrown.
            Command cmd;
            try {
                cmd = CODATransition.valueOf(cmdS);
            } catch (IllegalArgumentException e) {
                // bug bug: do we want this printed, logged, etc ???
                System.out.println("Received an invalid transition command");
                return;
            }

            // set the args for this command
            Set<String> names = msg.getPayloadNames();
            cmd.clearArgs();
            for (String name : names) {
                cmd.setArg(name, msg.getPayloadItem(name));
            }

            // Get the EMU object and have it post this new command
            // by putting it in a Q that is periodically checked by
            // the EMU's "run" (main thread) method.
            cmsgPortal.comp.postCommand(cmd);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}