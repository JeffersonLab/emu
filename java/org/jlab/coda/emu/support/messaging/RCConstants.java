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
 * 1/6/2010
 */
public class RCConstants {

    // Subscriptions

    /** String used as type in subscription that receives transition commands. */
    public static final String transitionCommandType = "run/transition/*";
    /** String used as type in subscription that receives run commands. */
    public static final String runCommandType        = "run/control/*";
    /** String used as type in subscription that receives session commands. */
    public static final String sessionCommandType    = "session/control/*";
    /** String used as type in subscription that receives session, set option commands. */
    public static final String setOptionType         = "session/setoption/*";
    /** String used as type in subscription that receives info commands. */
    public static final String infoCommandType       = "coda/info/*";

    // Types in messages sent

    /** String used as type of status message periodically sent to rcServer. */
    public static final String reportStatus     = "rc/report/status";

    // Types in messages received in session/control/* callback

    /** String used as type in message for the start-reporting session command. */
    public static final String startReporting   = "session/control/startReporting";
    /** String used as type in message for the stop-reporting session command. */
    public static final String stopReporting    = "session/control/stopReporting";
    /** String used as type in message for the get-run-type session command. */
    public static final String getRunType       = "session/control/getRunType";
    /** String used as type in message for the set-run-type session command. */
    public static final String setRunType       = "session/control/setRunType";
    /** String used as type in message for the get-run-number session command. */
    public static final String getRunNumber     = "session/control/getRunNumber";
    /** String used as type in message for the set-run-number session command. */
    public static final String setRunNumber     = "session/control/setRunNumber";

    // Types in messages received in run/control/* callback

    /** String used as type in message for the set state run command. */
    public static final String setState         = "run/control/setState";
    /** String used as type in message for the get state run command. */
    public static final String getState         = "run/control/getState";
    /** String used as type in message for the set session run command. */
    public static final String setSession       = "run/control/setSession";
    /** String used as type in message for the get session run command. */
    public static final String getSession       = "run/control/getSession";
    /** String used as type in message for the release session run command. */
    public static final String releaseSession   = "run/control/releaseSession";
    /** String used as type in message for the set interval run command. */
    public static final String setInterval      = "run/control/setInterval";
    /** String used as type in message for the start run command. */
    public static final String start            = "run/control/start";
    /** String used as type in message for the stop run command. */
    public static final String stop             = "run/control/stop";
    /** String used as type in message for the reset run command. */
    public static final String reset            = "run/control/reset";
    /** String used as type in message for the exit run command. */
    public static final String exit             = "run/control/exit";

    // Types in messages received in coda/info/* callback

    /** String used as type in message for the get state info command. */
    public static final String rcGetState       = "coda/info/getState";
    /** String used as type in message for the get status info command. */
    public static final String rcGetStatus      = "coda/info/getStatus";
    /** String used as type in message for the get object type info command. */
    public static final String rcGetObjectType  = "coda/info/getObjectType";
    /** String used as type in message for the get coda class info command. */
    public static final String rcGetCodaClass   = "coda/info/getCodaClass";

    // Payload names in reportStatus (above) message
    
    public static final String    name                 = "codaName";
    public static final String    codaClass            = "codaClass";
    public static final String    state                = "state";
    public static final String    eventNumber          = "eventCount";
    public static final String    eventRate            = "eventRate";
    public static final String    dataRate             = "dataRate";
    public static final String    numberOfLongs        = "dataCount";
    public static final String    liveTime             = "liveTime";
    public static final String    filename             = "fileName";
    public static final String    objectType           = "objectType";
    public static final String    runStartTime         = "runStartTime";
    public static final String    runEndTime           = "runEndTime";
  }
