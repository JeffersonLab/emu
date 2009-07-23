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

package org.jlab.coda.support.messaging;

import org.jlab.coda.cMsg.cMsgCallbackInterface;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.support.codaComponent.RunControl;
import org.jlab.coda.support.control.Command;

import java.util.Set;

/**
 * This class defines the cMsg callback run when run control commands
 * (sub = *, type = run/control/*) are received.
 *
 * @author heyes
 *         Sep 24, 2008, 8:46:54 AM
 */
public class RCControlHandler extends GenericCallback implements cMsgCallbackInterface {
    /** The CMSGPortal object that created this object. */
    private CMSGPortal cmsgPortal;

    /**
     * Constructor RCControlHandler creates a new RCControlHandler instance.
     *
     * @param cmsgPortal of type CMSGPortal
     */
    public RCControlHandler(CMSGPortal cmsgPortal) {
        this.cmsgPortal = cmsgPortal;
    }

    /**
     * Method callback ...
     * type = run/control/* .
     *
     * @param msg cMsgMessage being received
     * @param o   object given in subscription & passed in here (null in this case)
     */
    public void callback(cMsgMessage msg, Object o) {

        try {
            String type = msg.getType();
            String cmdS = type.substring(type.lastIndexOf("/") + 1);

            // See if message's type (after last / ) is a recognized run control command.
            // Examples: reset, configure, start, stop, getsession, setsession, etc.
            // The string cmdS may not be an allowed enum value, in which case an
            // IllegalArgumentException will be thrown.
            Command cmd;
            try {
                cmd = RunControl.valueOf(cmdS);
            } catch (IllegalArgumentException e) {
                // bug bug: do we want this printed, logged, etc ???
                System.out.println("Received an invalid run control command");
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
