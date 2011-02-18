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

    // Useful prefixes

    /** String used as type in transition commands. */
    private static final String transitionCommand = "run/transition/";
    /** String used as type in run commands. */
    private static final String runCommand        = "run/control/";
    /** String used as type in session commands. */
    private static final String sessionCommand    = "session/control/";
    /** String used as type in set option commands. */
    private static final String setOption         = "session/setoption/";
    /** String used as type in info commands. */
    private static final String infoCommand       = "coda/info/";
    /** String used as type responding to info commands. */
    private static final String rcResponseCommand = "rc/response/";

    // Subscriptions

    /** String used as type in subscription that receives transition commands. */
    public static final String transitionCommandType = transitionCommand + "*";
    /** String used as type in subscription that receives run commands. */
    public static final String runCommandType        = runCommand + "*";
    /** String used as type in subscription that receives session commands. */
    public static final String sessionCommandType    = sessionCommand + "*";
    /** String used as type in subscription that receives session, set option commands. */
    public static final String setOptionType         = setOption + "*";
    /** String used as type in subscription that receives info commands. */
    public static final String infoCommandType       = infoCommand + "*";

    // Types in messages sent

    /** String used as type of status message periodically sent to rcServer. */
    public static final String reportStatus     = "rc/report/status";

    // Types in messages received in session/control/* callback

    /** String used as type in message for the start-reporting session command. */
    public static final String startReporting   = sessionCommand + "startReporting";
    /** String used as type in message for the stop-reporting session command. */
    public static final String stopReporting    = sessionCommand + "stopReporting";
    /** String used as type in message for the get-run-type session command. */
    public static final String getRunType       = sessionCommand + "getRunType";
    /** String used as type in message for the set-run-type session command. */
    public static final String setRunType       = sessionCommand + "setRunType";
    /** String used as type in message for the get-run-number session command. */
    public static final String getRunNumber     = sessionCommand + "getRunNumber";
    /** String used as type in message for the set-run-number session command. */
    public static final String setRunNumber     = sessionCommand + "setRunNumber";

    // Types in messages received in run/control/* callback

    /** String used as type in message for the set state run command. */
    public static final String setState         = runCommand + "setState";
    /** String used as type in message for the get state run command. */
    public static final String getState         = runCommand + "getState";
    /** String used as type in message for the set session run command. */
    public static final String setSession       = runCommand + "setSession";
    /** String used as type in message for the get session run command. */
    public static final String getSession       = runCommand + "getSession";
    /** String used as type in message for the release session run command. */
    public static final String releaseSession   = runCommand + "releaseSession";
    /** String used as type in message for the set interval run command. */
    public static final String setInterval      = runCommand + "setInterval";
    /** String used as type in message for the start run command. */
    public static final String start            = runCommand + "start";
    /** String used as type in message for the stop run command. */
    public static final String stop             = runCommand + "stop";
    /** String used as type in message for the reset run command. */
    public static final String reset            = runCommand + "reset";
    /** String used as type in message for the exit run command. */
    public static final String exit             = runCommand + "exit";

    // Types in messages received in coda/info/* callback

    /** String used as type in message for the get-state info command. */
    public static final String rcGetState       = infoCommand + "getState";
    /** String used as type in message for the get-status info command. */
    public static final String rcGetStatus      = infoCommand + "getStatus";
    /** String used as type in message for the get-object-type info command. */
    public static final String rcGetObjectType  = infoCommand + "getObjectType";
    /** String used as type in message for the get-coda-class info command. */
    public static final String rcGetCodaClass   = infoCommand + "getCodaClass";
    /** String used as type in message for the get-session info command. */
    public static final String rcGetSession     = infoCommand + "getSession";
    /** String used as type in message for the get-run-type info command. */
    public static final String rcGetRunType     = infoCommand + "getRunType";
    /** String used as type in message for the get-run-number info command. */
    public static final String rcGetRunNumber   = infoCommand + "getRunNumber";

    /** String used as type in response message for the get-state info command. */
    public static final String rcGetStateResponse       = rcResponseCommand + "getState";
    /** String used as type in response message for the get-status info command. */
    public static final String rcGetStatusResponse      = rcResponseCommand + "getStatus";
    /** String used as type in response message for the get-object-type info command. */
    public static final String rcGetObjectTypeResponse  = rcResponseCommand + "getObjectType";
    /** String used as type in response message for the get-coda-class info command. */
    public static final String rcGetCodaClassResponse   = rcResponseCommand + "getCodaClass";
    /** String used as type in response message for the get-session info command. */
    public static final String rcGetSessionResponse     = rcResponseCommand + "getSession";
    /** String used as type in response message for the get-run-type info command. */
    public static final String rcGetRunTypeResponse     = rcResponseCommand + "getRunType";
    /** String used as type in response message for the get-run-number info command. */
    public static final String rcGetRunNumberResponse   = rcResponseCommand + "getRunNumber";

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
