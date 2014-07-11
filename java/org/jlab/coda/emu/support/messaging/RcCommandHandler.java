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
import org.jlab.coda.emu.support.control.Command;

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
     * Constructor RcCommandHandler creates a new RcCommandHandler instance.
     *
     * @param cmsgPortal of type CMSGPortal
     */
    RcCommandHandler(CMSGPortal cmsgPortal) {
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
//System.out.println("callback: got " + msg.getType() + " message");
        try {
            String type = msg.getType();

            // The string cmdS may not be an allowed enum value, in which case an
            // IllegalArgumentException will be thrown.
            CODACommand codaCmd;
            try {
                codaCmd = CODACommand.get(type);
            } catch (IllegalArgumentException e) {
                // TODO: bug bug: do we want this printed, logged, etc ???
//System.out.println("callback: received an invalid command of type " + type);
                return;
            }

            if (codaCmd == null) {
                // TODO: bug bug: do we want this printed, logged, etc ???
//System.out.println("callback: received an invalid command of type " + type);
                return;
            }

            // RESET commands have no accompanying metadata and are of
            // the highest priority. We don't want them stuck in a Q
            // somewhere so treat them separately.
            if (codaCmd == CODACommand.RESET) {
                cmsgPortal.emu.reset();
                return;
            }

            // Get the emuCmd - which is a STATIC CODACommand enum object - and
            // wrap it with and Command object which is not static and allows
            // us to attach all manner of mutable data to it. Thus we can now
            // store any extraneous data Run Control sends us and store them as
            // "args". We can also store GUI or emu-specific data.
            Command cmd = new Command(codaCmd);

            // Save original msg. This is useful, for example,
            // if response to sendAndGet is necessary.
            cmd.setMessage(msg);

            // set the args for this command
            Set<String> names = msg.getPayloadNames();
            for (String name : names) {
                cmd.setArg(name, msg.getPayloadItem(name));
            }

            // Get the Emu object and have it post this new command
            // by putting it in a Q that is periodically checked by
            // the Emu's "run" (main thread) method.
            cmsgPortal.emu.postCommand(cmd);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}