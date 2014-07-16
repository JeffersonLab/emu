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

    /** Connection object to rc multicast server of runcontrol platform. */
    private cMsg rcServer;

    /** UDL for connection to rc multicast server of runcontrol platform. */
    private final String rcUDL;

    /** Connection object to cMsg name server of runcontrol platform. */
    private cMsg server;

    /** UDL for connection to cMsg name server of runcontrol platform. */
    private final String UDL;

    /** Store a reference to the EMU here (which is the only object that uses this CMSGPortal object). */
    final CODAComponent emu;

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


        // TODO: a mistake of AFECS to run cMsgNameServer at default port!!!!!
        // cMsg subdomain with namespace "M" on local cMsg server at default ports
        UDL = "cMsg://localhost/cMsg/M";

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

            // only need one callback
            MvalReportingHandler mHandler = new MvalReportingHandler(CMSGPortal.this);
            server = new cMsg(UDL, emu.name(), "EMU called " + this.emu.name());
            server.connect();
            server.start();

            // install callback for reporting the smallest number of evio events per ET event
            if (emu.getCodaClass() == CODAClass.ROC) {
System.out.println("             cMsg domain, ROC subscribes to subject = ROC");
                server.subscribe("ROC", "*", mHandler, null);
            }
            else if (emu.getCodaClass() == CODAClass.SEB ||
                     emu.getCodaClass() == CODAClass.PEB)  {
System.out.println("             cMsg domain, " + emu.getCodaClass() +
                           " subscribes to subject = " + this.emu.name());
                                server.subscribe(this.emu.name(), "*", mHandler, null);
            }

        } catch (cMsgException e) {
            e.printStackTrace();
            logger.warn("Exit due to RC connect error: " + e.getMessage());
            System.exit(-1);
        }
    }


    /**
     * Disconnect from the rc multicast server / runcontrol platform.
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
    }


    /**
     * Get the connection object to the rc multicast server of runcontrol platform.
     * @return connection object to the rc multicast server of runcontrol platform.
     */
    public cMsg getRcServer() {
        return rcServer;
    }


    /**
     * Send a message to a callback in this emu so that it can calculate & send all rocs
     * the number of evio events to send in a single ET buffer.  Using cMsg domain server
     * in platform.
     *
     * @param val # of evio events found in a single ET buffer, or
     *            (total number of ET events)/(total number of Et groups) - depending
     *            on type value
     * @param type msg type:
     *             1) "M" if val is # of evio events in a single ET buffer as
     *                 reported bt ET input channel, or
     *             2) "eventsPerRoc" if val is (total number of ET events)/(total number of Et groups)
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
                rcServer = null;
            }
        }
    }


    /**
     * Send a message to the rocs specifying the number of evio events to send
     * in a single ET buffer. Using cMsg domain server in platform.
     *
     * @param eventsInBuf number of evio events for rocs to send in a single ET buffer
     */
    synchronized public void sendRocMessage(int eventsInBuf) {

        if ((server != null) && server.isConnected()) {

            try {
                cMsgMessage msg = new cMsgMessage();
                msg.setSubject("ROC");                      // to all Rocs
                msg.setType("eventsPerGroup"); // from DC, PEB, etc.
                msg.setUserInt(eventsInBuf); // # of evio events / et buffer
                if (server != null) {
                    server.send(msg);
                }

            } catch (cMsgException e) {
                try {
                    if (server.isConnected()) server.disconnect();
                } catch (cMsgException e1) {}
                rcServer = null;
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

