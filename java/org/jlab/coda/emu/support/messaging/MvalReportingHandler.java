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
 * number of evio events in each ET event (or M). This callback selects the
 * lowest M value from all reporting channels and calculates a metric which
 * gets sent to the rocs through a cMsg message. That metric is:<p>
 *
 *     n = ((total # ET events / # ET groups) - 1) * M <p>
 *
 * In plain English, the most events a single roc can have minus 1 times
 * the smallest # of evio events per et event. If a roc has this many
 * (or more) evio events in a single ET buffer, then it's time to send that
 * buffer even if it isn't full. That way the event builders are not
 * kept waiting for buffers from rocs with little data.<p>
 *
 * Note that M values are only reported if they've changed and at a max
 * rate of 10 Hz. This callback is only active if this emu is a DC.
 *
 * @author timmer
 * (Jul 14, 2014)
 */
class MvalReportingHandler extends GenericCallback implements cMsgCallbackInterface {

    /** Desired maximum # of evio events per ET buffer in rocs. */
    private int metric;

    /** (total number of ET events)/(total number of Et groups) */
    private int eventsPerRoc;

    /** Smallest # of evio events in a single ET buffer as reported bt ET input channels. */
    private int lowestM = Integer.MAX_VALUE;

    /** Connection to cMsg server. */
    private CMSGPortal cmsgPortal;


    /**
     * Constructor.
     *
     * @param cmsgPortal of type CMSGPortal
     */
    MvalReportingHandler(CMSGPortal cmsgPortal) {
        this.cmsgPortal = cmsgPortal;
    }

    /**
     * This method is a callback for subscriptions of subject = this emu's name.
     *
     * @param msg cMsgMessage being received
     * @param o   object given in subscription & passed in here (null in this case)
     */
    public void callback(cMsgMessage msg, Object o) {
        // This callback can get 2 types of messages, containing:
        //     1) m value from ET input channel, or
        //     2) (total number of ET events)/(total number of Et groups)

        int val = msg.getUserInt();

        if (msg.getType().equalsIgnoreCase("M")) {
            // Reject invalid values
            if (val < 1) return;

            System.out.println("      MMMMMMMMMMMMMMM  GOT m = " + val);

            if (val < lowestM) {
                lowestM = val;
                metric = ((eventsPerRoc) - 1) * lowestM;
                // Since M changes, pass that on to the rocs
System.out.println("      MMMMMMMMMMMMMMM  SEND roc metric = " + metric);
                cmsgPortal.sendRocMessage(metric);
            }
        }
        // This message comes before the run starts, so reset stuff
        else if (msg.getType().equalsIgnoreCase("eventsPerGroup")) {
            // Reject invalid values
            System.out.println("      MMMMMMMMMMMMMMM  GOT events/roc = " + val);
            if (val < 1) return;

            eventsPerRoc = val;
            lowestM = Integer.MAX_VALUE;
        }
        else {
            System.out.println("      MMMMMMMMMMMMMMM  GOT unknown message = " + val);
        }
    }

}