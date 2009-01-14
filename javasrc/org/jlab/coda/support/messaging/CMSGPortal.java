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

package org.jlab.coda.support.messaging;

import org.jlab.coda.cMsg.cMsg;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.cMsg.cMsgPayloadItem;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.support.codaComponent.CODAComponent;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.support.logger.LoggerAppender;
import org.jlab.coda.support.logger.LoggingEvent;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Sep 22, 2008
 * Time: 8:37:33 AM
 */
public class CMSGPortal implements LoggerAppender {
    private String TEST_UDL = "cMsg:cMsg://albanac.jlab.org:7030/cMsg/test";

    private static cMsg server = null;
    private String UDL;
    protected CODAComponent comp;
    protected Thread monitorThread;

    protected static CMSGPortal self = null;

    public static CMSGPortal getCMSGPortal(CODAComponent c) {
        if (self == null) self = new CMSGPortal(c);
        return self;
    }

    protected class ServerMonitor implements Runnable {

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
            while (!monitorThread.isInterrupted()) {
                if (server == null || !server.isConnected()) try {

                    server = new cMsg(UDL, comp.name(), "EMU called " + comp.name());

                    server.connect();
                    server.start();
                    server.subscribe("*", "run/transition/*", new RCTransitionHandler(self), null);
                    server.subscribe("*", "run/control/*", new RCControlHandler(self), null);
                    server.subscribe("*", "session/control/*", new RCSessionHandler(self), null);
                    Logger.info("cMSg server connected");
                } catch (cMsgException e) {
                    Logger.warn("cMSg server down, retry in 5 seconds");
                    if (server != null) {
                        try {
                            if (server.isConnected()) server.disconnect();
                        } catch (cMsgException e1) {
                            // ignore
                        }
                        server = null;
                    }
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {

                    break;
                }
            }

            if (server != null) try {

                server.disconnect();
            } catch (cMsgException e1) {
                // ignore
            }
            server = null;
            Logger.warn("cMSg server monitor thread exit");
        }
    }

    private CMSGPortal(CODAComponent c) {

        comp = c;
        String udl = c.getCmsgUDL();
        if (udl != null) UDL = udl;
        else UDL = TEST_UDL;

        Logger.addAppender(this);

        monitorThread = new Thread(Emu.THREAD_GROUP, new ServerMonitor(), "cMSg Server Monitor");
        monitorThread.start();
    }

    public void shutdown() throws cMsgException {
        Logger.removeAppender(this);

        monitorThread.interrupt();
    }

    public void append(LoggingEvent event) {

        if ((server != null) && server.isConnected()) {
            cMsgMessage msg = new cMsgMessage();
            msg.setSubject(comp.name());
            msg.setType("cmlog");
            msg.setText(event.getMessage());
            msg.setUserInt(event.getLevel());

            try {
                if (event.hasData()) {
                    msg.addPayloadItem(new cMsgPayloadItem("hostName", comp.getHostName()));
                    msg.addPayloadItem(new cMsgPayloadItem("userName", comp.getUserName()));
                    msg.addPayloadItem(new cMsgPayloadItem("runNumber", comp.getRunNumber()));
                    msg.addPayloadItem(new cMsgPayloadItem("runType", comp.getRunType()));
                    msg.addPayloadItem(new cMsgPayloadItem("config", comp.getConfig()));
                    msg.addPayloadItem(new cMsgPayloadItem("severity", event.getFormatedLevel()));
                    if (comp.state() != null) msg.addPayloadItem(new cMsgPayloadItem("state", comp.state().toString()));
                    msg.addPayloadItem(new cMsgPayloadItem("codaClass", comp.getCodaClass()));
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

    public static cMsg getServer() {
        return server;
    }
}
