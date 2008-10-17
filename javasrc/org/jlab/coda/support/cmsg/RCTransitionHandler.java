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

package org.jlab.coda.support.cmsg;

import org.jlab.coda.cMsg.cMsgCallbackInterface;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.support.component.CODATransition;
import org.jlab.coda.support.control.Command;

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
    /** Field cmsgPortal */
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
     * Method callback ...
     *
     * @param msg of type cMsgMessage
     * @param o   of type Object
     */
    public void callback(cMsgMessage msg, Object o) {

        try {
            String type = msg.getType();
            String cmdS = type.substring(type.lastIndexOf("/") + 1);
            Command cmd = CODATransition.valueOf(cmdS);
            cmd.clearArgs();
            Set<String> names = msg.getPayloadNames();
            for (String name : names) {
                cmd.setArg(name, msg.getPayloadItem(name));
            }
            cmsgPortal.comp.postCommand(cmd);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}