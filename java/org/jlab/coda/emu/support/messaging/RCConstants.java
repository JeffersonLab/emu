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
    public static final String reportStatus = "rc/report/status";
    /** String used as type of dalog message periodically sent to rcServer. */
    public static final String dalogMsg = "rc/report/dalog";

    // Types in messages received in session/control/* callback

    /** String used as type in message for the set-state command. */
    public static final String setState         = sessionCommand + "setState";
    /** String used as type in message for the set-session command. */
    public static final String setSession       = sessionCommand + "setSession";
    /** String used as type in message for the get-session command. */
    public static final String getSession       = sessionCommand + "getSession";
    /** String used as type in message for the release-session command. */
    public static final String releaseSession   = sessionCommand + "releaseSession";
    /** String used as type in message for the start-reporting command. */
    public static final String startReporting   = sessionCommand + "startReporting";
    /** String used as type in message for the stop-reporting command. */
    public static final String stopReporting    = sessionCommand + "stopReporting";
    /** String used as type in message for the set-interval command. */
    public static final String setInterval      = sessionCommand + "setInterval";
    /** String used as type in message for the start command. */
    public static final String start            = sessionCommand + "start";
    /** String used as type in message for the stop command. */
    public static final String stop             = sessionCommand + "stop";
    /** String used as type in message for the exit command. */
    public static final String exit             = sessionCommand + "exit";

    // Types in messages received in run/transition/* callback.
    // The 2 command immediately above (configure & reset) should
    // really be below in the transition section. But they ain't.

    /** String used as type in message for the configure command. */
    public static final String configure = transitionCommand + "configure";
    /** String used as type in message for the get-run-number command. */
    public static final String download = transitionCommand + "download";
    /** String used as type in message for the set-run-number command. */
    public static final String prestart = transitionCommand + "prestart";
    /** String used as type in message for the get-run-type command. */
    public static final String go       = transitionCommand + "go";
    /** String used as type in message for the set-run-type command. */
    public static final String end       = transitionCommand + "end";
    /** String used as type in message for the set-run-type command. */
    public static final String pause     = transitionCommand + "pause";
    /** String used as type in message for the set-run-type command. */
    public static final String resume    = transitionCommand + "resume";
    /** String used as type in message for the reset command. */
    public static final String reset     = transitionCommand + "reset";

    /** String used as payload name for item holding config file name. */
    public static final String configPayloadFileName = "fileName";
    /** String used as payload name for item holding config file content. */
    public static final String configPayloadFileContent = "fileContent";
    /** String used as payload name for item holding int (1 if file content changed, else 0). */
    public static final String configPayloadFileChanged = "fileChanged";

    /** String used as payload name for item holding run type int. */
    public static final String prestartPayloadRunType = "configId";
    /** String used as payload name for item holding run number. */
    public static final String prestartPayloadRunNumber = "runNumber";


    // Types in messages received in run/control/* callback

    /** String used as type in message for the get-run-number command. */
    public static final String getRunNumber     = runCommand + "getRunNumber";
    /** String used as type in message for the set-run-number command. */
    public static final String setRunNumber     = runCommand + "setRunNumber";
    /** String used as type in message for the get-run-type command. */
    public static final String getRunType       = runCommand + "getRunType";
    /** String used as type in message for the set-run-type command. */
    public static final String setRunType       = runCommand + "setRunType";

    // Types in messages received in coda/info/* callback

    /** String used as type in message for the get-state command. */
    public static final String getState      = infoCommand + "getState";
    /** String used as type in message for the get-status command. */
    public static final String getStatus     = infoCommand + "getStatus";
    /** String used as type in message for the get-object-type command. */
    public static final String getObjectType = infoCommand + "getObjectType";
    /** String used as type in message for the get-coda-class command. */
    public static final String getCodaClass  = infoCommand + "getCodaClass";

    // Types in messages received in session/setOption/* callback

    /** String used as type in message for the set-file-path command. */
    public static final String setFilePath   = setOption + "filePath";
    /** String used as type in message for the set-file-prefix command. */
    public static final String setFilePrefix = setOption + "filePrefix";
    /** String used as type in message for the set-config-file command. */
    public static final String setConfigFile    = setOption + "configFile";

    // response strings for coda/info/* commands

    /** String used as type in response message for the get-state info command. */
    public static final String rcGetStateResponse       = rcResponseCommand + "getState";
    /** String used as type in response message for the get-status info command. */
    public static final String rcGetStatusResponse      = rcResponseCommand + "getStatus";
    /** String used as type in response message for the get-object-type info command. */
    public static final String rcGetObjectTypeResponse  = rcResponseCommand + "getObjectType";
    /** String used as type in response message for the get-coda-class info command. */
    public static final String rcGetCodaClassResponse   = rcResponseCommand + "getCodaClass";


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
