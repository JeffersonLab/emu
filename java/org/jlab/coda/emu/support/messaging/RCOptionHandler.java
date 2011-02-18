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
import org.jlab.coda.emu.support.codaComponent.SessionControl;
import org.jlab.coda.emu.support.control.Command;

import java.util.Set;

/**
 * Currently this callback does nothing. It's reserved for future use.
 */
public class RCOptionHandler extends GenericCallback implements cMsgCallbackInterface {
    /** Field cmsgPortal */
    private CMSGPortal cmsgPortal;

    /**
     * Constructor RCSessionHandler creates a new RCSessionHandler instance.
     *
     * @param cmsgPortal of type CMSGPortal
     */
    public RCOptionHandler(CMSGPortal cmsgPortal) {
        this.cmsgPortal = cmsgPortal;
    }

    /**
     * Method callback ...
     * type = session/control/* .
     *
     * @param msg cMsgMessage being received
     * @param o   object given in subscription & passed in here (null in this case)
     */
    public void callback(cMsgMessage msg, Object o) {
System.out.println("GOT " + msg.getType() + " message");
    }
}
