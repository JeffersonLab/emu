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

/*
 * Created by JFormDesigner on Mon Sep 22 13:29:36 EDT 2008
 */

package org.jlab.coda.support.ui;

import org.jdesktop.layout.GroupLayout;
import org.jdesktop.layout.LayoutStyle;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.support.component.CODAState;
import org.jlab.coda.support.component.CODATransition;
import org.jlab.coda.support.component.RunControl;
import org.jlab.coda.support.component.SessionControl;
import org.jlab.coda.support.config.Configurer;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.CommandAcceptor;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.log.Logger;
import org.jlab.coda.support.log.LoggingEvent;
import org.jlab.coda.support.log.QueueAppender;
import org.jlab.coda.support.ui.log.SwingLogConsoleDialog;
import org.w3c.dom.Document;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.ResourceBundle;

/** @author Graham Heyes */
public class EMUCommander extends JFrame {

    String TEST_UDL = "cMsg://localhost:3456/cMsg/test";
    Thread monitor;
    cMsg server;
    String UDL;
    boolean verbose = true;

    private static String normalFormat = "%18s  %24s    %9d    %-18s  %-18s    %s";
    private static String normalHeader = "%18s  %24s    %9s    %-18s  %-18s    %s";

    private static String wideFormat = "%18s  %24s    %9d    %-30s  %-30s    %s";
    private static String wideHeader = "%18s  %24s    %9s    %-30s  %-30s    %s";

    public static void main(String[] args) {
        new EMUCommander();
    }

    protected class CommandHandler implements CommandAcceptor {
        private String subject;

        private State state = CODAState.UNCONFIGURED;

        CommandHandler(String subject) {

            this.subject = subject;
        }

        public void postCommand(Command cmd) throws InterruptedException {
            cMsgMessage msg = new cMsgMessage();
            msg.setSubject(cmd.toString());
            msg.setType(subject + cmd.toString());
            msg.setText(cmd.toString());
            if (cmd.equals(RunControl.configure)) {
                String emuName = System.getProperty("name");
                if (emuName == null) {
                    System.out.println("usage emu -Dname=\"my name\"");
                    System.exit(-1);
                }
                String installDir = System.getenv("INSTALL_DIR");
                if (installDir == null) {
                    System.out.println("Check that INSTALL_DIR is set");
                    System.exit(-1);
                }
                String configF = installDir + File.separator + "conf" + File.separator + emuName + ".xml";
                try {

                    System.out.println("Parse : " + configF);
                    Document d = Configurer.parseFile(configF);

                    Configurer.removeEmptyTextNodes(d.getDocumentElement());
                    System.out.println("Document : " + Configurer.serialize(d));
                    String content = Configurer.serialize(d);

                    msg.addPayloadItem(new cMsgPayloadItem("configuration", content));
                    System.out.println("\"" + content + "\"");
                } catch (Exception e) {
                    System.err.println("Exception " + e);
                }
            }
            try {
                System.out.println("CMSGPortal.append server=" + server);
                if (server != null) server.send(msg);
                State tmp = cmd.success();
                if (tmp != null) {
                    state = tmp;

                    System.out.println("Allowed transitions are " + state.allowed());
                    CODATransition.resume.allow(state.allowed());

                    System.out.println("State of " + this + " is now " + state());
                } else {
                    System.out.println("State not changed by command");
                }
                smartToolbar.update();
                smartToolbar1.update();
                smartToolbar2.update();
            } catch (cMsgException e) {
                System.out.println("CMSGPortal.append error " + e.getMessage());
            }
        }

        public State state() {
            return state;  //To change body of implemented methods use File | Settings | File Templates.
        }

    }

    public EMUCommander() {
        initComponents();
        pack();
        setVisible(true);
        QueueAppender logQueueAppender = new QueueAppender(1024);
        Logger.addAppender(logQueueAppender);
        logPanel.monitor(logQueueAppender);

        smartToolbar.configure(new CommandHandler("run/transition/"), CODATransition.class);
        smartToolbar1.configure(new CommandHandler("run/control/"), RunControl.class);
        smartToolbar2.configure(new CommandHandler("session/control/"), SessionControl.class);

        try {
            UDL = System.getProperty("cmsgUDL");
            verbose = (System.getProperty("verbose") != null);
            if (UDL == null) UDL = TEST_UDL;
            server = new cMsg(UDL, "EMUCommander", "Tool to send commands to EMUs and log responses");
            server.connect();
        } catch (cMsgException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        // subscribe and provide callback
        try {
            server.subscribe("*", "*", new CallbackAdapter(), null);
        } catch (cMsgException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        //Logger.addAppender(this);
        // enable receipt of messages and delivery to callback
        server.start();

        // wait for messages
        try {
            while (server.isConnected()) {
                Thread.sleep(1);
            }
        } catch (Exception e) {
            System.err.println(e);
        }

        // disable message delivery to callbacks
        server.stop();

        // done
        try {
            server.disconnect();
        } catch (Exception e) {
            System.exit(-1);
        }
        System.exit(0);
    }

    private void quitMenuItemActionPerformed(ActionEvent e) {
        System.exit(0);
    }

    private void prefsItemActionPerformed(ActionEvent e) {
        PrefPane theBox = new PrefPane();
        theBox.setVisible(true);
    }

    private void helpActionPerformed(ActionEvent e) {
        HelpBox theBox = new HelpBox(this);
        theBox.setVisible(true);
    }

    private void aboutMenuItemActionPerformed(ActionEvent e) {
        AboutBox theBox = new AboutBox();
        theBox.setVisible(true);
    }

    private void initComponents() {
        // JFormDesigner - CODAComponent initialization - DO NOT MODIFY  //GEN-BEGIN:initComponents
        // Generated using JFormDesigner non-commercial license
        ResourceBundle bundle = ResourceBundle.getBundle("org.jlab.coda.support.ui.rsrc.strings");
        menuBar = new JMenuBar();
        fileMenu = new JMenu();
        quitMenuItem = new JMenuItem();
        editMenu = new JMenu();
        cutMenuItem = new JMenuItem();
        copyMenuItem = new JMenuItem();
        pasteMenuItem = new JMenuItem();
        undoMenuItem = new JMenuItem();
        prefsItem = new JMenuItem();
        logMenu = new JMenu();
        clearMenuItem = new JMenuItem();
        helpMenu = new JMenu();
        help = new JMenuItem();
        aboutMenuItem = new JMenuItem();
        smartToolbar = new SmartToolbar();
        smartToolbar1 = new SmartToolbar();
        smartToolbar2 = new SmartToolbar();
        logScrollPane = new JScrollPane();
        logPanel = new SwingLogConsoleDialog();

        //======== this ========
        setTitle(bundle.getString("this.title"));
        Container contentPane = getContentPane();

        //======== menuBar ========
        {

            //======== fileMenu ========
            {
                fileMenu.setText(bundle.getString("fileMenu.text"));

                //---- quitMenuItem ----
                quitMenuItem.setText(bundle.getString("quitMenuItem.text"));
                quitMenuItem.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        quitMenuItemActionPerformed(e);
                    }
                });
                fileMenu.add(quitMenuItem);
            }
            menuBar.add(fileMenu);

            //======== editMenu ========
            {
                editMenu.setText(bundle.getString("editMenu.text"));

                //---- cutMenuItem ----
                cutMenuItem.setText(bundle.getString("cutMenuItem.text"));
                editMenu.add(cutMenuItem);

                //---- copyMenuItem ----
                copyMenuItem.setText(bundle.getString("copyMenuItem.text"));
                editMenu.add(copyMenuItem);

                //---- pasteMenuItem ----
                pasteMenuItem.setText(bundle.getString("pasteMenuItem.text"));
                editMenu.add(pasteMenuItem);

                //---- undoMenuItem ----
                undoMenuItem.setText(bundle.getString("undoMenuItem.text"));
                editMenu.add(undoMenuItem);
                editMenu.addSeparator();

                //---- prefsItem ----
                prefsItem.setText(bundle.getString("prefsItem.text"));
                prefsItem.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        prefsItemActionPerformed(e);
                    }
                });
                editMenu.add(prefsItem);
            }
            menuBar.add(editMenu);

            //======== logMenu ========
            {
                logMenu.setText(bundle.getString("logMenu.text"));

                //---- clearMenuItem ----
                clearMenuItem.setText(bundle.getString("clearMenuItem.text"));
                logMenu.add(clearMenuItem);
            }
            menuBar.add(logMenu);

            //======== helpMenu ========
            {
                helpMenu.setText(bundle.getString("helpMenu.text"));

                //---- help ----
                help.setText(bundle.getString("help.text"));
                help.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        helpActionPerformed(e);
                    }
                });
                helpMenu.add(help);

                //---- aboutMenuItem ----
                aboutMenuItem.setText(bundle.getString("aboutMenuItem.text"));
                aboutMenuItem.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        aboutMenuItemActionPerformed(e);
                    }
                });
                helpMenu.add(aboutMenuItem);
            }
            menuBar.add(helpMenu);
        }
        setJMenuBar(menuBar);

        //======== smartToolbar ========
        {
            smartToolbar.setFloatable(false);
        }

        //======== smartToolbar1 ========
        {
            smartToolbar1.setFloatable(false);
        }

        //======== smartToolbar2 ========
        {
            smartToolbar2.setFloatable(false);
        }

        //======== logScrollPane ========
        {
            logScrollPane.setViewportView(logPanel);
        }

        GroupLayout contentPaneLayout = new GroupLayout(contentPane);
        contentPane.setLayout(contentPaneLayout);
        contentPaneLayout.setHorizontalGroup(contentPaneLayout.createParallelGroup()
                .add(GroupLayout.TRAILING, contentPaneLayout.createSequentialGroup()
                .addContainerGap()
                .add(contentPaneLayout.createParallelGroup(GroupLayout.TRAILING)
                        .add(GroupLayout.LEADING, smartToolbar2, GroupLayout.DEFAULT_SIZE, 704, Short.MAX_VALUE)
                        .add(GroupLayout.LEADING, smartToolbar1, GroupLayout.DEFAULT_SIZE, 704, Short.MAX_VALUE)
                        .add(GroupLayout.LEADING, smartToolbar, GroupLayout.DEFAULT_SIZE, 704, Short.MAX_VALUE)
                        .add(GroupLayout.LEADING, logScrollPane, GroupLayout.DEFAULT_SIZE, 704, Short.MAX_VALUE))
                .addContainerGap()));
        contentPaneLayout.setVerticalGroup(contentPaneLayout.createParallelGroup()
                .add(contentPaneLayout.createSequentialGroup()
                .add(smartToolbar, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(LayoutStyle.UNRELATED)
                .add(smartToolbar1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(LayoutStyle.UNRELATED)
                .add(smartToolbar2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(LayoutStyle.UNRELATED)
                .add(logScrollPane, GroupLayout.DEFAULT_SIZE, 703, Short.MAX_VALUE)
                .addContainerGap()));
        pack();
        setLocationRelativeTo(getOwner());
        // JFormDesigner - End of component initialization  //GEN-END:initComponents
    }

    // JFormDesigner - Variables declaration - DO NOT MODIFY  //GEN-BEGIN:variables
    // Generated using JFormDesigner non-commercial license
    private JMenuBar menuBar;
    private JMenu fileMenu;
    private JMenuItem quitMenuItem;
    private JMenu editMenu;
    private JMenuItem cutMenuItem;
    private JMenuItem copyMenuItem;
    private JMenuItem pasteMenuItem;
    private JMenuItem undoMenuItem;
    private JMenuItem prefsItem;
    private JMenu logMenu;
    private JMenuItem clearMenuItem;
    private JMenu helpMenu;
    private JMenuItem help;
    private JMenuItem aboutMenuItem;
    private SmartToolbar smartToolbar;
    private SmartToolbar smartToolbar1;
    private SmartToolbar smartToolbar2;
    private JScrollPane logScrollPane;
    private SwingLogConsoleDialog logPanel;
    // JFormDesigner - End of variables declaration  //GEN-END:variables

    protected class CallbackAdapter extends cMsgCallbackAdapter {
        public void callback(cMsgMessage msg, Object userObject) {
            if (verbose) {
                System.out.println(String.format(normalFormat, msg.getSenderHost(), new java.sql.Timestamp(msg.getSenderTime().getTime()), msg.getUserInt(), msg.getSubject(), msg.getType(), msg.getText()));
            }
            switch (msg.getUserInt()) {
                case LoggingEvent.DEBUG:
                    Logger.debug(msg.getText());
                    break;
                case LoggingEvent.ERROR:
                    Logger.error(msg.getText());
                    break;
                case LoggingEvent.INFO:
                    Logger.info(msg.getText());
                    break;
                case LoggingEvent.WARN:
                    Logger.warn(msg.getText());
                    break;
            }

        }
    }
}
