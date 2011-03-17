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

package org.jlab.coda.emu.support.messaging;

import org.jlab.coda.cMsg.cMsgCallbackInterface;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.control.RcCommand;

import java.util.Set;

/**
 * <pre>
 * Class <b>RcCommandHandler</b>
 * </pre>
 *
 * @author timmer
 *         Created on Mar 15, 2011
 */
class RcCommandHandler extends GenericCallback implements cMsgCallbackInterface {

    /** Connection to cMsg server. */
    private CMSGPortal cmsgPortal;

    /**
     * Constructor RCTransitionHandler creates a new RCTransitionHandler instance.
     *
     * @param cmsgPortal of type CMSGPortal
     */
    public RcCommandHandler(CMSGPortal cmsgPortal) {
        this.cmsgPortal = cmsgPortal;
    }

    /**
     * This method is a callback for subscriptions of subject = * and various types like
     * run/transition/* ., run/control/*, session/setOption/*, session/control/*,
     * and coda/info/* .
     *
     * @param msg cMsgMessage being received
     * @param o   object given in subscription & passed in here (null in this case)
     */
    public void callback(cMsgMessage msg, Object o) {
System.out.println("GOT " + msg.getType() + " message");
        try {
            String type = msg.getType();
            String cmdS = (type.substring(type.lastIndexOf("/") + 1));

            // The string cmdS may not be an allowed enum value, in which case an
            // IllegalArgumentException will be thrown.
            CODACommand emuCmd;
            try {
                emuCmd = CODACommand.valueOf(cmdS);
            } catch (IllegalArgumentException e) {
                // TODO: bug bug: do we want this printed, logged, etc ???
System.out.println("Received an invalid command");
                return;
            }

            // Get the emuCmd - which is a STATIC CODACommand enum object - and
            // wrap it with and RcCommand object which is not static and allows
            // us to attach all manner of mutable data to it. Thus we can now
            // store any extraneous data Run Control sends us and store them as
            // "args". We can also store GUI or emu-specific data.
            RcCommand rcCmd = new RcCommand(emuCmd);

            // set the args for this command
            Set<String> names = msg.getPayloadNames();
            for (String name : names) {
                rcCmd.setArg(name, msg.getPayloadItem(name));
            }

            // Get the Emu object and have it post this new command
            // by putting it in a Q that is periodically checked by
            // the Emu's "run" (main thread) method.
            cmsgPortal.comp.postCommand(rcCmd);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}