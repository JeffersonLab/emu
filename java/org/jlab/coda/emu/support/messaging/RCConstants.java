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
    /** String used as type responding to get info commands. */
    private static final String rcResponseCommand = "rc/response/";
    /** String used as type responding to info commands. */
    private static final String rcReportCommand   = "rc/report/";
    /** String used as type responding to coda info commands. */
    private static final String codaInfoCommand   = "coda/info/";

    // Subscriptions

    /** String used as type in subscription that receives transition commands. */
    public static final String transitionCommandType = transitionCommand + "*";
    /** String used as type in subscription that receives run commands. */
    public static final String runCommandType        = runCommand + "*";
    /** String used as type in subscription that receives session commands. */
    public static final String sessionCommandType    = sessionCommand + "*";
    /** String used as type in subscription that receives rc response commands. */
    public static final String rcResponseCommandType = rcResponseCommand + "*";
    /** String used as type in subscription that receives coda info commands. */
    public static final String codaInfoCommandType = codaInfoCommand + "*";

    // Types in messages sent to run control

    /** String used as type of status message periodically sent to rcServer. */
    public static final String reportStatus = rcReportCommand + "status";
    /** String used as type of dalog message periodically sent to rcServer. */
    public static final String dalogMsg = rcReportCommand + "dalog";

    // Types in messages received in session/control/* callback

    /** String used as type in message for the set-session command. */
    public static final String setSession     = sessionCommand + "setSession";
    /** String used as type in message for the get-session command. */
    public static final String getSession     = sessionCommand + "getSession";

    /** String used as type in message for the start-reporting command. */
    public static final String startReporting = sessionCommand + "startReporting";
    /** String used as type in message for the stop-reporting command. */
    public static final String stopReporting  = sessionCommand + "stopReporting";

    /** String used as type in message for the set-interval command. */
    public static final String setInterval    = sessionCommand + "setInterval";
    /** String used as type in message for the exit command. */
    public static final String exit           = sessionCommand + "exit";
    /** String used as type in message for the get-config-id command. */
    public static final String getConfigId    = sessionCommand + "configId";

    /** String used as type in message for the set-run-type command. */
    public static final String setRunType     = sessionCommand + "setRunType";
    /** String used as type in message for the get-run-type command. */
    public static final String getRunType     = sessionCommand + "getRunType";

    /** String used as type in message for the set-run-number command. */
    public static final String setRunNumber   = sessionCommand + "setRunNumber";
    /** String used as type in message for the get-run-number command. */
    public static final String getRunNumber   = sessionCommand + "getRunNumber";

    // Types in messages received in run/transition/* callback.

    /** String used as type in message for the configure command. */
    public static final String configure = transitionCommand + "configure";
    /** String used as type in message for the get-run-number command. */
    public static final String download  = transitionCommand + "download";
    /** String used as type in message for the set-run-number command. */
    public static final String prestart  = transitionCommand + "prestart";
    /** String used as type in message for the get-run-type command. */
    public static final String go        = transitionCommand + "go";
    /** String used as type in message for the set-run-type command. */
    public static final String end       = transitionCommand + "end";
    /** String used as type in message for the set-run-type command. */
    public static final String pause     = transitionCommand + "pause";
    /** String used as type in message for the set-run-type command. */
    public static final String resume    = transitionCommand + "resume";
    /** String used as type in message for the reset command. */
    public static final String reset     = transitionCommand + "reset";

    // Types in messages received in run/control/* callback

    /** String used as type in message for the set-buffer-level command. */
    public static final String setBufferLevel = runCommand + "setRocBufferLevel";
    /** String used as type in message for the get-buffer-level command. */
    public static final String getBufferLevel = runCommand + "getRocBufferLevel";
    /** String used as type in message for the enable-output-channels command. */
    public static final String enableOutput   = runCommand + "enableOutput";
    /** String used as type in message for the disable-output-channels command. */
    public static final String disableOutput  = runCommand + "disableOutput";

    // Types in messages received in coda/info/* callback

    /** String used as type in message for get-state command. */
    public static final String getState      = codaInfoCommand + "getState";
    /** String used as type in message for get-status command. */
    public static final String getStatus     = codaInfoCommand + "getStatus";
    /** String used as type in message for get-object-type command. */
    public static final String getObjectType = codaInfoCommand + "getObjectType";
    /** String used as type in message for get-coda-class command. */
    public static final String getCodaClass  = codaInfoCommand + "getCodaClass";


    // Types in messages published in response to session/control & coda/info commands

    /** String used as type in message for response to get-state command. */
    public static final String getStateResponse          = rcResponseCommand + "getState";
    /** String used as type in message for response to get-status command. */
    public static final String getStatusResponse         = rcResponseCommand + "getStatus";
    /** String used as type in message for response to get-object-type command. */
    public static final String getObjectTypeResponse     = rcResponseCommand + "getObjectType";
    /** String used as type in message for response to get-coda-class command. */
    public static final String getCodaClassResponse      = rcResponseCommand + "getCodaClass";
    /** String used as type in message for response to get-session command. */
    public static final String getSessionResponse        = rcResponseCommand + "getSession";
    /** String used as type in message for response to get-run-number command. */
    public static final String getRunNumberResponse      = rcResponseCommand + "getRunNumber";
    /** String used as type in message for response to get-run-type command. */
    public static final String getRunTypeResponse        = rcResponseCommand + "getRunType";
    /** String used as type in message for response to get-config-id command. */
    public static final String getConfigIdResponse       = rcResponseCommand + "getConfigId";
    /** String used as type in message for response to get-roc-buffer-level command. */
    public static final String getRocBufferLevelResponse = rcResponseCommand + "getRocBufferLevel";

    // cMsg message payload item names

    /** String used as payload name for item holding config file name. */
    public static final String configPayloadFileName = "fileName";
    /** String used as payload name for item holding config file content. */
    public static final String configPayloadFileContent = "fileContent";
    /** String used as payload name for item holding config file content for simulated ROC. */
    public static final String configPayloadFileContentRoc = "emuRocConfig";
    /** String used as payload name for item holding int (1 if file content changed, else 0). */
    public static final String configPayloadFileChanged = "fileChanged";
    /** String used as payload name for item holding all IP addresses of platform's host. */
    public static final String configPayloadPlatformHosts = "platformHost";
    /** String used as payload name for item holding platform's cMsg domain server's TCP port. */
    public static final String configPayloadPlatformPort = "platformPort";
    /** String used as payload name for item holding configuration's count of data streams -
     *  the number of final event builders and event recorders. */
    public static final String configPayloadStreamCount = "nStreams";
    /** String used as payload name for item holding component's data streams id. */
    public static final String configPayloadStreamId = "streamID";

    /** String used as payload name for item holding run type int. */
    public static final String prestartPayloadRunType = "configId";

    /** String used as payload name for item holding run number. */
    public static final String runNumberPayload = "runNumber";
    /** String used as payload name for item holding session name. */
    public static final String sessionPayload   = "session";
    /** String used as payload name for item holding run type name. */
    public static final String runTypePayload   = "config";


    // cMsg message payload item names in report-status message

    public static final String    name                 = "codaName";
    public static final String    codaClass            = "codaClass";
    public static final String    state                = "state";
    public static final String    eventCount           = "eventCount";
    public static final String    eventCount64         = "eventCount64";
    public static final String    eventRate            = "eventRate";
    public static final String    dataRate             = "dataRate";
    public static final String    numberOfLongs        = "dataCount";
    public static final String    liveTime             = "liveTime";
    public static final String    filename             = "fileName";
    public static final String    objectType           = "objectType";
    public static final String    runStartTime         = "runStartTime";
    public static final String    runEndTime           = "runEndTime";
    public static final String    maxEventSize         = "maxEventSize";
    public static final String    minEventSize         = "minEventSize";
    public static final String    avgEventSize         = "avgEventSize";
    public static final String    chunk_X_EtBuf        = "chunk_X_EtBuf";
    public static final String    timeToBuild          = "timeToBuild";

    public static final String    inputChanLevels      = "inputChanLevels";
    public static final String    outputChanLevels     = "outputChanLevels";
    public static final String    inputChanNames       = "inputChanNames";
    public static final String    outputChanNames      = "outputChanNames";
  }
