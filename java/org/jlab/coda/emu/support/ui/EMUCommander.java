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

package org.jlab.coda.emu.support.ui;

import org.jdesktop.layout.GroupLayout;
import org.jdesktop.layout.LayoutStyle;
import org.jlab.coda.cMsg.*;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.CommandAcceptor;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.logger.LoggingEvent;
import org.jlab.coda.emu.support.logger.QueueAppender;
import org.jlab.coda.emu.support.ui.log.SwingLogConsoleDialog;
import org.w3c.dom.Document;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.ResourceBundle;

/**
 * EMUCommander - a tool to send commands to EMUs and log responses.
 * @author Graham Heyes
 */
public class EMUCommander extends JFrame {

    String TEST_UDL = "cMsg://localhost:7030/cMsg/test";
    Thread monitor;
    /** cMsg connection object. */
    cMsg server;
    /** UDL for cMsg connection. */
    String UDL;
    boolean verbose = true;

    private Logger logger;

    private static String normalFormat = "%18s  %24s    %9d    %-18s  %-18s    %s";
    private static String normalHeader = "%18s  %24s    %9s    %-18s  %-18s    %s";

    private static String wideFormat = "%18s  %24s    %9d    %-30s  %-30s    %s";
    private static String wideHeader = "%18s  %24s    %9s    %-30s  %-30s    %s";


    /** Run as executable. */
    public static void main(String[] args) {
        new EMUCommander();
    }

    /**
     * Defines code to execute depending on command used.
     * In this case send out a cMsg message and update GUI.
     */
    protected class CommandHandler implements CommandAcceptor {
        private String subject;
        private State state = CODAState.BOOTED;

        CommandHandler(String subject) {
            this.subject = subject;
        }

        public void postCommand(Command cmd) throws InterruptedException {
            cMsgMessage msg = new cMsgMessage();
            msg.setSubject(cmd.toString());
            msg.setType(subject + cmd.toString());
            msg.setText(cmd.toString());

            // If the command is to configure, read the config file
            // and send it to all listeners by a cMsg message.
            // Otherwise, just pass the command on by setting subject, type, & text.
            if (cmd.equals(CODATransition.CONFIGURE)) {

                // Must set the name of this object
                String emuName = System.getProperty("name");
                if (emuName == null) {
System.out.println("usage: java EMUCommander -Dname=\"my name\"");
                    System.exit(-1);
                }

                // Check to see if config file given on command line
                String configFile = System.getProperty("config");
                if (configFile == null) {
                    // Must define the INSTALL_DIR env var in order to find config files
                    String installDir = System.getenv("INSTALL_DIR");
                    if (installDir == null) {
System.out.println("Check that INSTALL_DIR is set or give -Dconfig=xxx option on cmd line");
                        System.exit(-1);
                    }
                    configFile = installDir + File.separator + "conf" + File.separator + emuName + ".xml";
                }

                try {
System.out.println("Parse : " + configFile);
                    Document d = Configurer.parseFile(configFile);

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
System.out.println("CMSGPortal.append server = " + server);
                // send cMsg message
                if (server != null) server.send(msg);

                // resulting state if command succeeded
                State tmp = cmd.success();

                if (tmp != null) {
                    state = tmp;
System.out.println("Allowed transitions are " + state.allowed());
                    // Enable/disable transition GUI buttons depending on
                    // which transitions are allowed out of our current state.
                    smartToolbar.updateButtons(state);
System.out.println("State of " + this + " is now " + state());
                } else {
System.out.println("State not changed by command");
                }

                // enable/disable GUI buttons based on having commands enabled/disabled
//                smartToolbar.update();
//                smartToolbar1.update();
//                smartToolbar2.update();
                
            } catch (cMsgException e) {
System.out.println("CMSGPortal.append error " + e.getMessage());
            }
        }

        public State state() {
            return state;
        }

    }

    /** No-arg constructor. */
    public EMUCommander() {
        initComponents();
        pack();
        setVisible(true);
        QueueAppender logQueueAppender = new QueueAppender(1024);
      //  logger = new Logger();
        logger.addAppender(logQueueAppender);
        logPanel.monitor(logQueueAppender);

        smartToolbar.configure(new CommandHandler("run/transition/"), 0);
        smartToolbar1.configure(new CommandHandler("run/control/"), 1);
        smartToolbar2.configure(new CommandHandler("session/control/"), 2);

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
            // subscribe to ALL cMsg messages
            server.subscribe("*", "*", new CallbackAdapter(), null);
        } catch (cMsgException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        //logger.addAppender(this);
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

    private void helpActionPerformed(ActionEvent e) {
        HelpBox theBox = new HelpBox(this);
        theBox.setVisible(true);
    }

    private void aboutMenuItemActionPerformed(ActionEvent e) {
        AboutBox theBox = new AboutBox();
        theBox.setVisible(true);
    }

    private void errorLoggingCheckBoxActionPerformed(ActionEvent e) {
        boolean selected = ((JCheckBox)e.getSource()).getModel().isSelected();
        if (selected) {
            if (!logger.isErrorEnabled()) {
                logger.toggleError();
                logger.info("Enable error logging");
            }
        }
        else {
            if (logger.isErrorEnabled()) {
                logger.info("Disable error logging");
                logger.toggleError();
            }
        }
    }

    private void debugLoggingCheckBoxActionPerformed(ActionEvent e) {
        boolean selected = ((JCheckBox)e.getSource()).getModel().isSelected();
        if (selected) {
            if (!logger.isDebugEnabled()) {
                logger.toggleDebug();
                logger.info("Enable debug logging");
            }
        }
        else {
            if (logger.isDebugEnabled()) {
                logger.info("Disable debug logging");
                logger.toggleDebug();
            }
        }
    }

    private void initComponents() {
        // JFormDesigner - CODAComponent initialization - DO NOT MODIFY  //GEN-BEGIN:initComponents
        // Generated using JFormDesigner non-commercial license
        ResourceBundle bundle = ResourceBundle.getBundle("org.jlab.coda.emu.support.ui.rsrc.strings");
        menuBar = new JMenuBar();
        fileMenu = new JMenu();
        quitMenuItem = new JMenuItem();
        editMenu = new JMenu();
        cutMenuItem = new JMenuItem();
        copyMenuItem = new JMenuItem();
        pasteMenuItem = new JMenuItem();
        undoMenuItem = new JMenuItem();
        logMenu = new JMenu();
        clearMenuItem = new JMenuItem();
        errorLoggingCheckBox = new JCheckBox();
        debugLoggingCheckBox = new JCheckBox();
        helpMenu = new JMenu();
        help = new JMenuItem();
        aboutMenuItem = new JMenuItem();
        smartToolbar = new SmartToolbar();
        smartToolbar1 = new SmartToolbar();
        smartToolbar2 = new SmartToolbar();
        logScrollPane = new JScrollPane();
        logPanel = new SwingLogConsoleDialog();
        logPanel.setLogger(logger);

        //======== this ========
        setTitle(bundle.getString("emuCommander.title"));
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
            }
            menuBar.add(editMenu);

            //======== logMenu ========
            {
                logMenu.setText(bundle.getString("logMenu.text"));

                //---- clearMenuItem ----
                clearMenuItem.setText(bundle.getString("clearMenuItem.text"));
                logMenu.add(clearMenuItem);

                //---- errorLoggingCheckBox ----
                errorLoggingCheckBox.setText(bundle.getString("errorLoggingCheckBox.text"));
                errorLoggingCheckBox.setPreferredSize(new Dimension(57, 19));
                errorLoggingCheckBox.setMaximumSize(new Dimension(32767, 32767));
                errorLoggingCheckBox.setMinimumSize(new Dimension(1, 1));
                errorLoggingCheckBox.setSelected(true);
                errorLoggingCheckBox.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        errorLoggingCheckBoxActionPerformed(e);
                    }
                });
                logMenu.add(errorLoggingCheckBox);

                //---- debugLoggingCheckBox ----
                debugLoggingCheckBox.setText(bundle.getString("debugLoggingCheckBox.text"));
                debugLoggingCheckBox.setMaximumSize(new Dimension(32767, 32767));
                debugLoggingCheckBox.setMinimumSize(new Dimension(1, 1));
                debugLoggingCheckBox.setPreferredSize(new Dimension(57, 19));
                debugLoggingCheckBox.setSelected(true);
                debugLoggingCheckBox.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        debugLoggingCheckBoxActionPerformed(e);
                    }
                });
                logMenu.add(debugLoggingCheckBox);
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
        contentPaneLayout.setHorizontalGroup(
            contentPaneLayout.createParallelGroup()
                .add(GroupLayout.TRAILING, contentPaneLayout.createSequentialGroup()
                    .addContainerGap()
                    .add(contentPaneLayout.createParallelGroup(GroupLayout.TRAILING)
                        .add(GroupLayout.LEADING, smartToolbar2, GroupLayout.DEFAULT_SIZE, 694, Short.MAX_VALUE)
                        .add(GroupLayout.LEADING, smartToolbar1, GroupLayout.DEFAULT_SIZE, 694, Short.MAX_VALUE)
                        .add(GroupLayout.LEADING, smartToolbar, GroupLayout.DEFAULT_SIZE, 694, Short.MAX_VALUE)
                        .add(GroupLayout.LEADING, logScrollPane, GroupLayout.DEFAULT_SIZE, 694, Short.MAX_VALUE))
                    .addContainerGap())
        );
        contentPaneLayout.setVerticalGroup(
            contentPaneLayout.createParallelGroup()
                .add(contentPaneLayout.createSequentialGroup()
                    .add(smartToolbar, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(LayoutStyle.UNRELATED)
                    .add(smartToolbar1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(LayoutStyle.UNRELATED)
                    .add(smartToolbar2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(LayoutStyle.UNRELATED)
                    .add(logScrollPane, GroupLayout.DEFAULT_SIZE, 697, Short.MAX_VALUE)
                    .addContainerGap())
        );
        pack();
        setLocationRelativeTo(getOwner());
        // JFormDesigner - End of codaComponent initialization  //GEN-END:initComponents
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
    private JMenu logMenu;
    private JMenuItem clearMenuItem;
    private JCheckBox errorLoggingCheckBox;
    private JCheckBox debugLoggingCheckBox;
    private JMenu helpMenu;
    private JMenuItem help;
    private JMenuItem aboutMenuItem;
    private SmartToolbar smartToolbar;
    private SmartToolbar smartToolbar1;
    private SmartToolbar smartToolbar2;
    private JScrollPane logScrollPane;
    private SwingLogConsoleDialog logPanel;
    // JFormDesigner - End of variables declaration  //GEN-END:variables


    // This subscription is for all cMsg messages (*,*)
    protected class CallbackAdapter extends cMsgCallbackAdapter {
        public void callback(cMsgMessage msg, Object userObject) {
            if (verbose) {
                System.out.println(String.format(normalFormat, msg.getSenderHost(), new java.sql.Timestamp(msg.getSenderTime().getTime()), msg.getUserInt(), msg.getSubject(), msg.getType(), msg.getText()));
            }
            switch (msg.getUserInt()) {
                case LoggingEvent.DEBUG:
                    logger.debug(msg.getText());
                    break;
                case LoggingEvent.ERROR:
                    logger.error(msg.getText());
                    break;
                case LoggingEvent.INFO:
                    logger.info(msg.getText());
                    break;
                case LoggingEvent.WARN:
                    logger.warn(msg.getText());
                    break;
            }

        }
    }
}
