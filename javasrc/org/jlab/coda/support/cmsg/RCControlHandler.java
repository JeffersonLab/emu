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
import org.jlab.coda.support.component.RunControl;
import org.jlab.coda.support.control.Command;

import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Sep 24, 2008
 * Time: 8:46:54 AM
 */
public class RCControlHandler extends GenericCallback implements cMsgCallbackInterface {
    /** Field cmsgPortal */
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
     *
     * @param msg of type cMsgMessage
     * @param o   of type Object
     */
    public void callback(cMsgMessage msg, Object o) {

        try {
            String type = msg.getType();
            String cmdS = type.substring(type.lastIndexOf("/") + 1);
            Command cmd = RunControl.valueOf(cmdS);
            Set<String> names = msg.getPayloadNames();
            cmd.clearArgs();
            for (String name : names) {
                cmd.setArg(name, msg.getPayloadItem(name));
            }
            cmsgPortal.comp.postCommand(cmd);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
