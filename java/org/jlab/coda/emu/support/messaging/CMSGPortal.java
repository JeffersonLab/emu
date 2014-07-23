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
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODAComponent;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.logger.LoggerAppender;
import org.jlab.coda.emu.support.logger.LoggingEvent;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class creates a connection to a cMsg server for the
 * purposes of receiving run control commands and logging dalog messages.
 */
public class CMSGPortal implements LoggerAppender {

    /** Store a reference to the EMU here (which is the only object that uses this CMSGPortal object). */
    final CODAComponent emu;

    /** Connection object to rc multicast server of runcontrol platform. */
    private cMsg rcServer;

    /** UDL for connection to rc multicast server of runcontrol platform. */
    private final String rcUDL;

    /** Connection object to cMsg name server of runcontrol platform. */
    private cMsg server;

    /** UDL for connection to cMsg name server of runcontrol platform to send
     *  messages internal to this emu. */
    private String UDL;

    /** Connection object to cMsg name server of runcontrol platform for ROC's namespace. */
    private cMsg rocServer;

    /** UDL for connection to cMsg name server of runcontrol platform to send messages to
     *  and receive messages on a ROC. */
    private String rocUDL;

    /** IP address of host emu's agent (and presumably the platform) is running on. */
    private String platformHost;

    private Logger logger;



    /**
     * Constructor.
     * @param emu reference to EMU object
     * @param expid experiment id
     */
    public CMSGPortal(Emu emu, String expid) {
        this.emu = emu;
        // UDL for connection to cMsg server was originally specified
        // with -DcmsgUDL=xxx flag to interpreter when running EMU.
        String udl = emu.getCmsgUDL();

        // construct default UDL if necessary
        if (udl == null) {
            if (expid == null) {
                expid = System.getenv("EXPID");
            }

            if (expid != null)  {
                udl = "cMsg:rc://multicast/" + expid;
            }
            else {
                udl = "cMsg:cMsg://localhost/cMsg/test";
            }
        }

        rcUDL = udl;

//System.out.println("\n CMSGPortal using UDL = " + UDL + "\n");
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

            //--------------------------------------------
            // Create a connection to the cMsg name server
            //--------------------------------------------

            // Find the host the platform (actually agent) is running on
            // so we can do an cMsg domain connection.
            cMsgMessage msg = rcServer.monitor("3000");
            if (msg == null) {
                System.out.println("\n\n PROBLEM: null msg in monitor\n\n");
                platformHost = "localhost";
            }
            else {
                platformHost = msg.getSenderHost();
            }

            // Only need one callback
            MvalReportingHandler mHandler = new MvalReportingHandler(CMSGPortal.this);

            // Install callback for reporting the smallest number of evio events per ET event

            if (emu.getCodaClass() == CODAClass.SEB ||
                emu.getCodaClass() == CODAClass.PEB)  {

                // Use this connection for internal communications on this emu to set M value.
                UDL = "cMsg://" + platformHost + "/cMsg/M";
                server = new cMsg(UDL, emu.name()+"_emu", "EmuInternal");
                server.connect();
                server.start();
                server.subscribe(this.emu.name(), "*", mHandler, null);

                // cMsg subdomain with namespace expid on platform of cMsg server at default port
                // Use this connection to send messages to the connected ROCs (through platform/agent)
                rocUDL = "cMsg://" + platformHost + "/cMsg/" + expid;
                rocServer = new cMsg(rocUDL, emu.name()+"_toRoc", "EmuToRoc");
                rocServer.connect();
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Exit due to rc/cMsg connect error: " + e.getMessage());
            System.exit(-1);
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
     * Get the connection object to the rc multicast server of runcontrol platform.
     * @return connection object to the rc multicast server of runcontrol platform.
     */
    public cMsg getRcServer() {
        return rcServer;
    }


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
     * Send a message to the connected rocs specifying the lowest number of evio events
     * per single ET buffer that was received by the input channels to this emu.
     * Using cMsg domain server in platform.
     *
     * @param eventsPerBuf lowest number of evio events per single ET buffer
     *                     received by the input channels to this emu
     */
    synchronized public void sendRocMessage(int eventsPerBuf) {

        if ((rocServer != null) && rocServer.isConnected()) {

            try {
                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(emu.name());
                msg.setType("eventsPerBuffer");
                msg.setUserInt(eventsPerBuf);
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
                msg.setUserInt(2);  // 0=info, 1=warning, 2=error, 3=severe; < 2 is ignored by rc gui
                msg.addPayloadItem(new cMsgPayloadItem("severity", "error"));
                DateFormat format = new SimpleDateFormat("HH:mm:ss.SSS ");
                msg.addPayloadItem(new cMsgPayloadItem("tod", format.format(new Date())));
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

                // 0=info, 1=warning, 2=error, 3=severe; < 2 is ignored by rc gui
                // Default to info message
                msg.setUserInt(0);
                msg.addPayloadItem(new cMsgPayloadItem("severity", "info"));

                if (event.hasData()) {
                    // currently none of the payload items, except severity & tod, are used
                    msg.addPayloadItem(new cMsgPayloadItem("hostName",  emu.getHostName()));
                    msg.addPayloadItem(new cMsgPayloadItem("userName",  emu.getUserName()));
                    msg.addPayloadItem(new cMsgPayloadItem("runNumber", emu.getRunNumber()));
                    msg.addPayloadItem(new cMsgPayloadItem("runType",   emu.getRunTypeId()));
                    msg.addPayloadItem(new cMsgPayloadItem("codaClass", emu.getCodaClass().name()));

                    String errorLevel = event.getFormatedLevel();
                    if (errorLevel.equalsIgnoreCase("WARN")) {
                        msg.setUserInt(1);
                        msg.addPayloadItem(new cMsgPayloadItem("severity", "warning"));
                    }
                    else if (errorLevel.equalsIgnoreCase("ERROR")) {
                        msg.setUserInt(2);
                        msg.addPayloadItem(new cMsgPayloadItem("severity", "error"));
                    }
                    else if (errorLevel.equalsIgnoreCase("BUG")) {
                        msg.setUserInt(3);
                        msg.addPayloadItem(new cMsgPayloadItem("severity", "severe"));
                    }

                    if (emu.state() != null) msg.addPayloadItem(new cMsgPayloadItem("state", emu.state().toString()));
                    msg.addPayloadItem(new cMsgPayloadItem("dalogData", event.getFormatedData()));

                    DateFormat format = new SimpleDateFormat("HH:mm:ss.SSS ");
                    format.format(new Date(event.getEventTime()));
                    msg.addPayloadItem(new cMsgPayloadItem("tod", format.format(event.getEventTime())));
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

