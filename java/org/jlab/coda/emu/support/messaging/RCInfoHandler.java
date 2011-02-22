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
import org.jlab.coda.emu.support.codaComponent.InfoControl;
import org.jlab.coda.emu.support.control.Command;

import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Sep 24, 2008
 * Time: 8:52:33 AM
 */
public class RCInfoHandler extends GenericCallback implements cMsgCallbackInterface {
    /** Field cmsgPortal */
    private CMSGPortal cmsgPortal;

    /**
     * Constructor RCInfoHandler creates a new RCInfoHandler instance.
     *
     * @param cmsgPortal of type CMSGPortal
     */
    public RCInfoHandler(CMSGPortal cmsgPortal) {
        this.cmsgPortal = cmsgPortal;
    }

    /**
     * Method callback ...
     * type = coda/info/* .
     *
     * @param msg cMsgMessage being received
     * @param o   object given in subscription & passed in here (null in this case)
     */
    public void callback(cMsgMessage msg, Object o) {
System.out.println("GOT " + msg.getType() + " message");

        try {
            // See if message's type is a recognized session-related command.
            // Examples: set/get run number, set/get run type, start/stop reporting
            Command cmd = InfoControl.get(msg.getType());
            if (cmd == null) {
System.out.println("Received an invalid session command");
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
