/*
 * Copyright (c) 2010, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.messaging;

/**
 * This class holds cMsg message types that are used for both subscriptions and messages.
 * @author timmer
 * @date 1/6/2010
 */
public class RCConstants {

    // Subscriptions

    /** String used as type in subscription that receives transition commands. */
    static public String transitionCommandType = "run/transition/*";

    /** String used as type in subscription that receives run commands. */
    static public String runCommandType        = "run/control/*";

    /** String used as type in subscription that receives session commands. */
    static public String sessionCommandType    = "session/control/*";

    // Types in messages sent

    /** String used as type of status message periodically sent to rcServer. */
    static public String reportStatus     = "rc/report/status";

    // Types in messages received in session/control/* callback

    /** String used as type in message for the start-reporting session command. */
    static public String startReporting   = "session/control/startReporting";

    /** String used as type in message for the stop-reporting session command. */
    static public String stopReporting    = "session/control/stopReporting";

    /** String used as type in message for the get-run-type session command. */
    static public String getRunType       = "session/control/getRunType";

    /** String used as type in message for the set-run-type session command. */
    static public String setRunType       = "session/control/setRunType";

    /** String used as type in message for the get-run-number session command. */
    static public String getRunNumber     = "session/control/getRunNumber";

    /** String used as type in message for the set-run-number session command. */
    static public String setRunNumber     = "session/control/setRunNumber";

    // Types in messages received in run/control/* callback

    /** String used as type in message for the set state run command. */
    static public String setState         = "run/control/setState";

    /** String used as type in message for the set session run command. */
    static public String setSession       = "run/control/setSession";

    /** String used as type in message for the get session run command. */
    static public String getSession       = "run/control/getSession";

    /** String used as type in message for the release session run command. */
    static public String releaseSession   = "run/control/releaseSession";

    /** String used as type in message for the set interval run command. */
    static public String setInterval      = "run/control/setInterval";

    /** String used as type in message for the start run command. */
    static public String start            = "run/control/start";

    /** String used as type in message for the stop run command. */
    static public String stop             = "run/control/stop";

    /** String used as type in message for the reset run command. */
    static public String reset            = "run/control/reset";

    /** String used as type in message for the exit run command. */
    static public String exit             = "run/control/exit";

  }
