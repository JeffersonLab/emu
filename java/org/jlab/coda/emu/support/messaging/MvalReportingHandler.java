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


/**
 * This class receives messages from the ET input channels containing the
 * lowest number of evio events in each ET event (or M). This callback selects
 * the lowest M value from all reporting channels in order to send that
 * number to the ROCs on the other end of the input channels. A reset can
 * be received for this value (used at prestart).<p>
 *
 * Note that M values are only reported if they've changed and at a max
 * rate of 2 Hz. This callback is only active if this emu is a DC or PEB.
 *
 * @author timmer
 * (Jul 14, 2014)
 */
class MvalReportingHandler extends GenericCallback implements cMsgCallbackInterface {

    /** Smallest # of evio events in a single ET buffer as reported by ET input channels. */
    private int lowestM = Integer.MAX_VALUE;

    /** Contains connection(s) to cMsg server. */
    private final CMSGPortal cmsgPortal;

    private final boolean debug = false;


    /**
     * Constructor.
     * @param cmsgPortal of type CMSGPortal
     */
    MvalReportingHandler(CMSGPortal cmsgPortal) {
        this.cmsgPortal = cmsgPortal;
    }


    /**
     * This method is a callback for subscriptions of subject = this emu's name.
     * This callback can get 2 types of messages, containing:<p>
     * <ol>
     * <li>For DC/PEB: M value from ET input channel,or<p>
     * <li>For DC/PEB: command to reset M value<p>
     * </ol>
     *
     * @param msg cMsgMessage being received
     * @param o   object given in subscription & passed in here (null in this case)
     */
    public void callback(cMsgMessage msg, Object o) {

        int val = msg.getUserInt();

        if (msg.getType().equalsIgnoreCase("M")) {
            // Reject invalid values
            if (val < 1) return;
if (debug) System.out.println("      MMMMMMMMMMMMMMM  GOT m = " + val);

            if (val < lowestM) {
                lowestM = val;
                // Since M changes, pass that on to the rocs
if (debug) System.out.println("      MMMMMMMMMMMMMMM  SEND roc m = " + lowestM);
                cmsgPortal.sendRocMessage(lowestM);
            }
        }
        // This message comes at prestart, so reset M
        else if (msg.getType().equalsIgnoreCase("reset")) {
if (debug) System.out.println("      MMMMMMMMMMMMMMM  GOT reset");
            lowestM = Integer.MAX_VALUE;
        }

        // This callback will receive messages meant
        // for other purposes, just ignore them.
    }

}