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

import org.jlab.coda.cMsg.cMsg;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.cMsg.cMsgPayloadItem;
import org.jlab.coda.emu.Emu;
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

    /** cMsg connection object. */
    private cMsg server;

    /** UDL to connection to cMsg system. */
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

        UDL = udl;

//System.out.println("\n CMSGPortal using UDL = " + UDL + "\n");
        logger = emu.getLogger();
        logger.addAppender(this);

        // create a connection to the RC server
        try {
            server = new cMsg(UDL, emu.name(), "EMU called " + this.emu.name());
            server.connect();
            // allow receipt of messages
            server.start();
            // only need one callback
            RcCommandHandler handler = new RcCommandHandler(CMSGPortal.this);
            // install callback for download, prestart, go, etc
            server.subscribe("*", RCConstants.transitionCommandType, handler, null);
            // install callback for reset, configure, start, stop, getsession, setsession, etc
            server.subscribe("*", RCConstants.runCommandType, handler, null);
            // install callback for set/get run number, set/get run type
            server.subscribe("*", RCConstants.sessionCommandType, handler, null);
            // install callback for getting state, status, codaClass, & objectType
            server.subscribe("*", RCConstants.infoCommandType, handler, null);
            // for future use
            server.subscribe("*", RCConstants.setOptionType, handler, null);

        } catch (cMsgException e) {
            e.printStackTrace();
            logger.warn("Exit due to RC connect error: " + e.getMessage());
            System.exit(-1);
        }
    }


    /**
     * Disconnect from the cMsg server.
     * Called from the EMU when quitting.
     * @throws cMsgException
     */
    synchronized public void shutdown() throws cMsgException {

        logger.removeAppender(this);

        if (server != null) {
            try { server.disconnect(); }
            catch (cMsgException e) {}
        }
        server = null;
    }


    /**
     * Get a reference to the cMsg connection object.
     * @return cMsg connection object
     */
    public cMsg getServer() {
        return server;
    }


    /**
     * Send a message to the rocs specifying the number of evio events to send
     * in a single ET buffer.
     *
     * @param eventsInBuf number of evio events for rocs to send in a single ET buffer
     */
    synchronized public void sendRocMessage(int eventsInBuf) {

        if ((server != null) && server.isConnected()) {

            try {
                cMsgMessage msg = new cMsgMessage();
                msg.setSubject("ROC");                      // to all Rocs
                msg.setType(emu.getCodaClass().toString()); // from DC, PEB, etc.
                msg.setUserInt(eventsInBuf); // # of evio events / et buffer
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
     * Send an error message that ends up on the run control gui.
     * @param text text of message
     */
    synchronized public void rcGuiErrorMessage(String text) {

        if ((server != null) && server.isConnected()) {

            try {
                cMsgMessage msg = new cMsgMessage();
                msg.setSubject(emu.name());
                msg.setType(RCConstants.dalogMsg);
                msg.setText(text);
                msg.setUserInt(2);  // 0=info, 1=warning, 2=error, 3=severe; < 2 is ignored by rc gui
                msg.addPayloadItem(new cMsgPayloadItem("severity", "error"));
                DateFormat format = new SimpleDateFormat("HH:mm:ss.SSS ");
                msg.addPayloadItem(new cMsgPayloadItem("tod", format.format(new Date())));
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
     * Send a dalog message using the cMsg system.
     * @param event event to be logged
     */
    synchronized public void append(LoggingEvent event) {

        if ((server != null) && server.isConnected() && event != null) {
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

                server.send(msg);

            } catch (cMsgException e) {
                try {
                    if (server.isConnected()) server.disconnect();
                } catch (cMsgException e1) {}
                server = null;
            }
        }
    }
}

