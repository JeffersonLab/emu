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
 * number of evio events in each ET event (or M). This callback selects
 * the lowest & highest M values from all reporting channels in order to send that
 * number to the ROCs on the other end of the input channels. It also sends the
 * highest safe value of M for ROCs which currently is set at 2 x lowM. This feedback
 * msg to ROCs is only sent if the highest safe value changes.
 * A reset can be received for low, high and safe high values (used at prestart).<p>
 *
 * Note that M values are reported at a max rate of 1/2 Hz.
 * This callback is only active if this emu is a DC or PEB.
 *
 * @author timmer
 * (Jul 14, 2014)
 */
class MvalReportingHandler extends GenericCallback implements cMsgCallbackInterface {

    /** Smallest # of evio events in a single ET buffer as reported by ET input channels. */
    private int lowestM = Integer.MAX_VALUE;

    /** Largest # of evio events in a single ET buffer as reported by ET input channels. */
    private int highestM = Integer.MIN_VALUE;

    /** Largest safe # of evio events in a single ET buffer as reported by ET input channels.
     * Based on the high-low difference, it is the highest M value that will allow ROCs to
     * operate without any using the timeout to send data. */
    private int highestSafeM = Integer.MIN_VALUE;

    /** The total number of events available to a single ROC,
     *  or the amount of ET events in its ET system "group". */
    private int totalEventsPerRoc;

    /** Contains connection(s) to cMsg server. */
    private final CMSGPortal cmsgPortal;

    private final boolean debug = false;

    private long timeAtLastUpdate;


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

        // Each channel reports its current M value at 2Hz
        if (msg.getType().equalsIgnoreCase("M")) {
            // Reject invalid values
            if (val < 1) return;

            // Find extreme values of m
            if (val < lowestM) {
                lowestM = val;
if (debug) System.out.println("      MMMMMMMMMMMMMMM  emu set lowest m = " + val);
            }

            if (val > highestM) {
                highestM = val;
if (debug) System.out.println("      MMMMMMMMMMMMMMM  emu set highest m = " + val);
            }

            long now = System.currentTimeMillis();

            // If it's been 2 seconds since our last feedback to the ROC,
            // send the latest values if highestSafeM has changed.
            // Then clear the high and low to find new, more recent, values
            // in 2 more seconds.
            if (now - timeAtLastUpdate >= 2000) {

                // Only update ROCs if things have changed
                if (highestSafeM != 2*lowestM) {

                    highestSafeM = 2*lowestM;
if (debug) System.out.println("      MMMMMMMMMMMMMMM  SEND roc low = " + lowestM +
                                 ", high = " + highestM + ", safe high = " + highestSafeM);
                    // Since M changes, pass that on to the rocs
                    cmsgPortal.sendRocMessage(lowestM, highestM, highestSafeM);
                }

                // Recalculate low & high over next 2 sec
                lowestM  = Integer.MAX_VALUE;
                highestM = Integer.MIN_VALUE;
                timeAtLastUpdate = now;
            }
        }

        // This message comes at prestart: resetM & get ET system info
        // TODO: This is never used??? If so, remove!
        else if (msg.getType().equalsIgnoreCase("eventsPerRoc")) {
if (debug) System.out.println("      MMMMMMMMMMMMMMM  emu GOT eventsPerRoc, initializing M");
            totalEventsPerRoc = msg.getUserInt();
            lowestM      = Integer.MAX_VALUE;
            highestM     = Integer.MIN_VALUE;
            highestSafeM = Integer.MIN_VALUE;

            // Theoretically it is possible for all input channels to come from
            // different ET systems; however, currently they will all only come
            // from one. We expect each input channel of a DC/PEB to send this msg.
            // It cannot be sent by the transport object as that only has a cmsg
            // connection IF it is locally created by that transport (not guaranteed).
        }


        // This callback will receive messages meant
        // for other purposes, just ignore them.
    }

}