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

package org.jlab.coda.support.cmsg;

import org.jlab.coda.cMsg.cMsg;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.cMsg.cMsgPayloadItem;
import org.jlab.coda.support.component.CODAComponent;
import org.jlab.coda.support.log.Logger;
import org.jlab.coda.support.log.LoggerAppender;
import org.jlab.coda.support.log.LoggingEvent;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Sep 22, 2008
 * Time: 8:37:33 AM
 */
public class CMSGPortal implements  LoggerAppender {
    private String TEST_UDL = "cMsg:cMsg://albanac.jlab.org:7030/cMsg/test";

    private static cMsg server = null;
    private String UDL;
    protected CODAComponent comp;

    public CMSGPortal(CODAComponent c) {

        comp = c;
        String udl = c.getCmsgUDL();
        if (udl != null) UDL = udl;
        else UDL = TEST_UDL;

        Logger.addAppender(this);
        try {

            server = new cMsg(UDL, comp.name(), "EMU called "+ comp.name());

            server.connect();
            server.start();
            server.subscribe("*", "run/transition/*", new RCTransitionHandler(this), null);
            server.subscribe("*", "run/control/*", new RCControlHandler(this), null);
            server.subscribe("*", "session/control/*", new RCSessionHandler(this), null);
        } catch (cMsgException e) {
            e.printStackTrace();
        }

    }

    public void shutdown() {
        try {
           
            server.disconnect();
        } catch (cMsgException e) {
            e.printStackTrace();
        }
        server = null;
    }

    public void send(String type, String subject, String text) {
        cMsgMessage msg = new cMsgMessage();
        msg.setSubject(subject);
        msg.setType(type);
        msg.setText(text);
    }




    public void append(LoggingEvent event) {

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

            System.out.println("cMsg Server : " + server.toString() + " " +server.isConnected());
            if (server != null) server.send(msg);

        } catch (cMsgException e) {
            e.printStackTrace();
        }
    }

    public static cMsg getServer() {
        return server;
    }
}
