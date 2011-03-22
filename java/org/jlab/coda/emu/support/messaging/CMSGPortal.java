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
 * purposes of receiving run control commands and logging cmlog messages.
 */
public class CMSGPortal implements LoggerAppender {

    /** cMsg connection object. */
    private cMsg server;

    /** UDL to connection to cMsg system. */
    private final String UDL;

    /** Store a reference to the EMU here (which is the only object that uses this CMSGPortal object). */
    final CODAComponent comp;
    
    /** Thread which creates and maintains a healthy cMsg connection. */
    private final Thread monitorThread;

    private Logger logger;


    /**
     * Constructor.
     * @param emu reference to EMU object
     */
    public CMSGPortal(Emu emu) {
        comp = emu;
        // UDL for connection to cMsg server was originally specified
        // with -DcmsgUDL=xxx flag to interpreter when running EMU.
        String udl = emu.getCmsgUDL();

        // construct default UDL if necessary
        if (udl == null) {
            String expid = System.getenv("EXPID");
            if (expid != null)  {
                udl = "cMsg:rc://multicast/" + expid;
            }
            else {
                udl = "cMsg:cMsg://localhost/cMsg/test";
            }
        }

        UDL = udl;

System.out.println("\n CMSGPortal using UDL = " + UDL + "\n");
        logger = emu.getLogger();
        logger.addAppender(this);

        // start a thread to maintain a connection to the cMsg server
        monitorThread = new Thread(emu.getThreadGroup(), new ServerMonitor(), "cMSg Server Monitor");
        monitorThread.start();
    }


    /**
     * This class is run as a thread to ensure a viable cMsg connection.
     */
    private class ServerMonitor implements Runnable {

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p/>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        public void run() {

            // While we have not been ordered to shutdown,
            // make sure we have a viable connection to the cMsg server.
            while (!monitorThread.isInterrupted()) {

                if (server == null || !server.isConnected()) {
                    try {
                        // create connection to cMsg server
    //System.out.println("CMSGPortal creating cMsg object using UDL = " + UDL + "\n");
                        server = new cMsg(UDL, comp.name(), "EMU called " + comp.name());
                        System.out.println("CMSGPortal created cMsg object");
                        server.connect();
                        System.out.println("CMSGPortal CONNECTED");
                        // allow receipt of messages
                        server.start();
                        // only need one callback
                        RcCommandHandler handler = new RcCommandHandler(CMSGPortal.this);
                        // install callback for download, prestart, go, etc
    //System.out.println("CMSGPortal subscribe to sub = *, type = " + RCConstants.transitionCommandType);
                        server.subscribe("*", RCConstants.transitionCommandType, handler, null);
                        // install callback for reset, configure, start, stop, getsession, setsession, etc
    //System.out.println("CMSGPortal subscribe to sub = *, type = " + RCConstants.runCommandType);
                        server.subscribe("*", RCConstants.runCommandType, handler, null);
                        // install callback for set/get run number, set/get run type
    //System.out.println("CMSGPortal subscribe to sub = *, type = " + RCConstants.sessionCommandType);
                        server.subscribe("*", RCConstants.sessionCommandType, handler, null);
                        // install callback for getting state, status, codaClass, & objectType
    //System.out.println("CMSGPortal subscribe to sub = *, type = " + RCConstants.infoCommandType);
                        server.subscribe("*", RCConstants.infoCommandType, handler, null);
                        // for future use
    //System.out.println("CMSGPortal subscribe to sub = *, type = " + RCConstants.setOptionType);
                        server.subscribe("*", RCConstants.setOptionType, handler, null);

                        logger.info("cMSg server connected");

                    } catch (cMsgException e) {
                        logger.warn("cMSg server down, retry in 5 seconds");
                        if (server != null) {
                            try {
                                if (server.isConnected()) server.disconnect();
                            } catch (cMsgException e1) {
                                // ignore
                            }
                            server = null;
                        }
                    }
                }

                // wait for 5 seconds
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }

            // We're here because we've been ordered to shutdown the connection
            if (server != null) {
                try { server.disconnect(); }
                catch (cMsgException e) {
                    // ignore
                }
            }

            server = null;
            logger.warn("cMSg server monitor thread exit");
        }
    }


    /**
     * Disconnect from the cMsg server.
     * Called from the EMU when quitting.
     * @throws cMsgException
     */
    public void shutdown() throws cMsgException {
        logger.removeAppender(this);

        monitorThread.interrupt();
    }


    /**
     * Get a reference to the cMsg connection object.
     * @return cMsg connection object
     */
    public cMsg getServer() {
        return server;
    }


    /**
     * Send a cmlog message using the cMsg system.
     * @param event event to be logged
     */
    public void append(LoggingEvent event) {

        if ((server != null) && server.isConnected()) {
            cMsgMessage msg = new cMsgMessage();
            msg.setSubject(comp.name());
            msg.setType("cmlog");
            msg.setText(event.getMessage());
            msg.setUserInt(event.getLevel());

            try {
                if (event.hasData()) {
                    msg.addPayloadItem(new cMsgPayloadItem("hostName",  comp.getHostName()));
                    msg.addPayloadItem(new cMsgPayloadItem("userName",  comp.getUserName()));
                    msg.addPayloadItem(new cMsgPayloadItem("runNumber", comp.getRunNumber()));
                    msg.addPayloadItem(new cMsgPayloadItem("runType",   comp.getRunType()));
                    msg.addPayloadItem(new cMsgPayloadItem("codaClass", comp.getCodaClass()));
                    // config is not implemented
                    msg.addPayloadItem(new cMsgPayloadItem("config", "unknown"));
                    msg.addPayloadItem(new cMsgPayloadItem("severity", event.getFormatedLevel()));
                    if (comp.state() != null) msg.addPayloadItem(new cMsgPayloadItem("state", comp.state().toString()));
                    msg.addPayloadItem(new cMsgPayloadItem("cmlogData", event.getFormatedData()));
                    DateFormat format = new SimpleDateFormat("HH:mm:ss.SSS ");
                    format.format(new Date(event.getEventTime()));
                    msg.addPayloadItem(new cMsgPayloadItem("tod", format.format(event.getEventTime())));
                }

                //System.out.println("cMsg Server : " + server.toString() + " " +server.isConnected());
                if (server != null) server.send(msg);

            } catch (cMsgException e) {
                try {
                    if (server.isConnected()) server.disconnect();
                } catch (cMsgException e1) {
                    // ignore
                }
                server = null;
            }
        }
    }

}
