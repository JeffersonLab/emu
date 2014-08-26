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

package org.jlab.coda.emu.support.messaging;

import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuException;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.logger.LoggerAppender;
import org.jlab.coda.emu.support.logger.LoggingEvent;


/**
 * This class creates a connection to a cMsg server for the
 * purposes of receiving run control commands and logging dalog messages.
 */
public class CMSGPortal implements LoggerAppender {

    /** Store a reference to the EMU here (which is the only object that uses this CMSGPortal object). */
    final Emu emu;

    /** Connection object to rc multicast server of run control platform. */
    private cMsg rcServer;

    /** UDL for connection to rc multicast server of run control platform. */
    private String rcUDL;

    /** Connection object to cMsg name server of run control platform. */
    private cMsg server;

    /** UDL for connection to cMsg name server of run control platform to send
     *  messages internal to this emu. */
    private String UDL;

    /** Connection object to cMsg name server of run control platform for ROC's namespace. */
    private cMsg rocServer;

    /** UDL for connection to cMsg name server of run control platform to send messages to
     *  and receive messages on a ROC. */
    private String rocUDL;

    /** IP address of host emu's agent (and presumably the platform) is running on. */
    private String platformHost;

    /** TCP port of platform's cMsg domain server. */
    private int platformPort;

    private Logger logger;



    /**
     * Constructor.
     * @param emu reference to EMU object
     */
    public CMSGPortal(Emu emu) throws EmuException {
        this.emu = emu;

        // UDL for connection to cMsg server was originally specified
        // with -DcmsgUDL=xxx flag to interpreter when running EMU.
        rcUDL = System.getProperty("cmsgUDL");

        // Construct default UDL if necessary
        if (rcUDL == null) {
            if (emu.getExpid() == null) {
                throw new EmuException("EXPID not defined");
            }

            rcUDL = "cMsg:rc://multicast/" + emu.getExpid() + "?connectTO=25";
        }

System.out.println("\n CMSGPortal using rc UDL = " + rcUDL + "\n");
        logger = emu.getLogger();
        logger.addAppender(this);

        try {
            //--------------------------------------------
            // Create a connection to the RC server
            //--------------------------------------------
            rcServer = new cMsg(rcUDL, emu.name(), "EMU called " + this.emu.name());
            rcServer.connect();
            // allow receipt of messages
            rcServer.start();
            // only need one callback
            RcCommandHandler handler = new RcCommandHandler(CMSGPortal.this);
            // install callback for download, prestart, go, etc
            rcServer.subscribe("*", RCConstants.transitionCommandType, handler, null);
            // install callback for reset, configure, start, stop, getsession, setsession, etc
            rcServer.subscribe("*", RCConstants.runCommandType, handler, null);
            // install callback for set/get run number, set/get run type
            rcServer.subscribe("*", RCConstants.sessionCommandType, handler, null);
            // install callback for getting state, status, codaClass, & objectType
            rcServer.subscribe("*", RCConstants.infoCommandType, handler, null);
            // for future use
            rcServer.subscribe("*", RCConstants.setOptionType, handler, null);

        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Exit due to rc/cMsg connect error: " + e.getMessage());
            System.exit(-1);
        }
    }


    /**
     * Create 2 connections to the run control platform's cMsg domain server.
     * Called during the configure transition since that is when the rc
     * server sends us info about the platform host and cMsg domain server's
     * TCP port.
     * @throws EmuException if platform did not send its IP addresses and cMsg TCP port;
     *                      if cannot connect to cMsg domain server in platform.
     */
    synchronized public void cMsgServerConnect() throws  EmuException {

        // Only need one callback
        MvalReportingHandler mHandler = new MvalReportingHandler(CMSGPortal.this);

        String[] addrs = emu.getPlatformIpAddresses();
        if (addrs == null) {
            throw new EmuException("Did not receive platform's IP addresses");
        }

        // Only set these if not set before
        if (platformPort == 0)    platformPort = emu.getPlatformTcpPort();
        if (platformHost == null) platformHost = addrs[0];

        // Install callback for reporting the smallest number of evio events per ET event
        if (emu.getCodaClass() == CODAClass.SEB ||
            emu.getCodaClass() == CODAClass.PEB)  {

            boolean foundServer = false;
//System.out.println("cMsgPortal: got platform cMsg domain server port = " + platformPort);

            // Use this connection for internal communications on this emu to set M value.
            // But only need to do this once - at the first configure.
            if (server == null) {
                // To make a connection, try the IP addresses one-by-one
                for (String ip : addrs) {
                    UDL = "cMsg://" + ip + ":" + platformPort + "/cMsg/M";
                    try {
                        server = new cMsg(UDL, emu.name()+"_emu", "EmuInternal");
                        server.connect();
                    }
                    catch (cMsgException e) {
                        continue;
                    }
                    foundServer = true;
//System.out.println("cMsgPortal: got IP = " + ip + "\n try connecting with udl = " + UDL);
                    platformHost = ip;
                    break;
                }

                if (!foundServer) {
                    throw new EmuException("Cannot connect to platform's cMsg domain server");
                }

                try {
                    server.start();
                    server.subscribe(this.emu.name(), "*", mHandler, null);

                    // cMsg subdomain with namespace = expid on platform of cMsg server at default port
                    // Use this connection to send messages to the connected ROCs (through platform/agent)
                    // That happens while data is flowing.
                    rocUDL = "cMsg://" + platformHost + ":" + platformPort + "/cMsg/" + emu.getExpid();
                    rocServer = new cMsg(rocUDL, emu.name()+"_toRoc", "EmuToRoc");
                    rocServer.connect();
                }
                catch (cMsgException e) {
                    try {server.disconnect();}
                    catch (cMsgException e1) {}
                    server = null;
                    throw new EmuException("Cannot connect to platform's cMsg domain server", e);
                }
            }
        }
    }


    /**
     * Disconnect from the runcontrol platform.
     * Called from the EMU when quitting.
     * @throws cMsgException
     */
    synchronized public void shutdown() throws cMsgException {

        logger.removeAppender(this);

        if (rcServer != null) {
            try { rcServer.disconnect(); }
            catch (cMsgException e) {}
        }
        rcServer = null;

        if (server != null) {
            try { server.disconnect(); }
            catch (cMsgException e) {}
        }
        server = null;

        if (rocServer != null) {
            try { rocServer.disconnect(); }
            catch (cMsgException e) {}
        }
        rocServer = null;
    }


    /**
     * Get the IP address the platform is running on.
     * Valid only after first config transition.
     * @return IP address the platform is running on.
     */
    public String getPlatformHost() {return platformHost;}


    /**
     * Get the TCP port the platform's cMsg domain server is listening on.
     * Valid only after first config transition.
     * @return TCP port the platform's cMsg domain server is listening on.
     */
    public int getPlatformPort() {return platformPort;}


    /**
     * Get the connection object to the rc multicast server of runcontrol platform.
     * @return connection object to the rc multicast server of runcontrol platform.
     */
    public cMsg getRcServer() {return rcServer;}


    /**
     * Get the UDL used to make the connection to the rc server.
     * @return UDL used to make the connection to the rc server.
     */
    public String getRcUDL() {return rcUDL;}


    /**
     * Send messages to a callback in this emu so that it can send all rocs
     * the lowest number of evio events received by this event builder in a
     * single ET buffer. Using cMsg name server in platform.
     *
     * @param val lowest # of evio events found in a single ET buffer or reset
     *            (depending on type value)

     * @param type  "M" if val is lowest # of evio events in a single ET buffer as
     *              reported by ET input channel or "reset" if clearing M values at prestart
     */
    synchronized public void sendMHandlerMessage(int val, String type) {

        if ((server != null) && server.isConnected()) {

            try {
                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(emu.name());
                msg.setType(type);
                msg.setUserInt(val);
                if (server != null) {
                    server.send(msg);
                }

            } catch (cMsgException e) {
                try {
                    if (server.isConnected()) server.disconnect();
                } catch (cMsgException e1) {}
                server = null;
            }
        }
    }


    /**
     * Send a message to the connected rocs specifying the lowest & highest
     * number of evio events per single ET buffer that was received by the
     * input channels to this emu. It also includes the value of the highest
     * safe number of evio events for each ROC to place in an ET buffer.
     * Using cMsg domain server in platform.
     *
     * @param lowM      lowest number of ROC records per single ET buffer
     *                  received by the input channels to this emu
     * @param highM     highest number of ROC records per single ET buffer
     *                  received by the input channels to this emu
     * @param highSafeM highest number of ROC records per single ET buffer
     *                  that each ROC should be sending
     */
    synchronized public void sendRocMessage(int lowM, int highM, int highSafeM) {

        if ((rocServer != null) && rocServer.isConnected()) {

            try {
                cMsgMessage msg = new cMsgMessage();
                msg.setHistoryLengthMax(0);
                msg.setSubject(emu.name());
                msg.setType("eventsPerBuffer");
                msg.setUserInt(highSafeM);
                cMsgPayloadItem lowItem  = new cMsgPayloadItem("lowM",  lowM);
                cMsgPayloadItem highItem = new cMsgPayloadItem("highM", highM);
                msg.addPayloadItem(lowItem);
                msg.addPayloadItem(highItem);
                if (rocServer != null) {
                    rocServer.send(msg);
                }

            } catch (cMsgException e) {
                try {
                    if (rocServer.isConnected()) rocServer.disconnect();
                } catch (cMsgException e1) {}
                rocServer = null;
            }
        }
    }


    /**
     * Send an error message that ends up on the run control gui.
     * @param text text of message
     */
    synchronized public void rcGuiErrorMessage(String text) {

        if ((rcServer != null) && rcServer.isConnected()) {

            try {
                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(emu.name());
                msg.setType(RCConstants.dalogMsg);
                msg.setText(text);
                // 0-3=info, 4-7=warning, 8-11=error, 12-15=severe; < 9 is ignored by rc gui
                msg.setUserInt(9);
                if (rcServer != null) {
                    rcServer.send(msg);
                }

            } catch (cMsgException e) {
                try {
                    if (rcServer.isConnected()) rcServer.disconnect();
                } catch (cMsgException e1) {}
                rcServer = null;
            }
        }
    }

    /**
     * Send a dalog message using the cMsg system.
     * @param event event to be logged
     */
    synchronized public void append(LoggingEvent event) {

        if ((rcServer != null) && rcServer.isConnected() && event != null) {
            try {
                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(emu.name());
                msg.setType(RCConstants.dalogMsg);
                msg.setText(event.getMessage());
                // 0-3=info, 4-7=warning, 8-11=error, 12-15=severe;  < 9 is ignored by rc gui
                // Default to info message
                msg.setUserInt(0);

                if (event.hasData()) {
                    String errorLevel = event.getFormatedLevel();
                    if (errorLevel.equalsIgnoreCase("WARN")) {
                        msg.setUserInt(5);
                    }
                    else if (errorLevel.equalsIgnoreCase("ERROR")) {
                        msg.setUserInt(9);
                    }
                    else if (errorLevel.equalsIgnoreCase("BUG")) {
                        msg.setUserInt(13);
                    }
                }

                rcServer.send(msg);

            } catch (cMsgException e) {
                try {
                    if (rcServer.isConnected()) rcServer.disconnect();
                } catch (cMsgException e1) {}
                rcServer = null;
            }
        }
    }
}

