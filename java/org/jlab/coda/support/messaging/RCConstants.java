package org.jlab.coda.support.messaging;

/**
 * 
 */
public class RCConstants {

    /** String used as type of status message periodically sent to rcServer. */
    static public String reportStatusType      = "rc/report/status";

    /** String used as type in subscription that receives transition commands. */
    static public String transitionCommandType = "run/transition/*";

    /** String used as type in subscription that receives run commands. */
    static public String runCommandType        = "run/control/*";

    /** String used as type in subscription that receives session commands. */
    static public String sessionCommandType    = "session/control/*";

}
