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
 * Created by JFormDesigner on Fri Sep 19 08:31:02 EDT 2008
 */

package org.jlab.coda.support.ui;

import org.jdesktop.layout.GroupLayout;
import org.jdesktop.layout.LayoutStyle;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.support.codaComponent.CODATransition;
import org.jlab.coda.support.codaComponent.RunControl;
import org.jlab.coda.support.codaComponent.SessionControl;
import org.jlab.coda.support.configurer.Configurer;
import org.jlab.coda.support.configurer.DataNode;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.support.logger.QueueAppender;
import org.jlab.coda.support.ui.log.SwingLogConsoleDialog;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ResourceBundle;

/** @author unknown */
public class DebugFrame extends JFrame {
    private int documentCount = 0;

    public DebugFrame() {
        initComponents();
        setTitle(Emu.INSTANCE.name());
        QueueAppender logQueueAppender = new QueueAppender(1024);
        Logger.addAppender(logQueueAppender);
        logPanel.monitor(logQueueAppender);
        smartToolbar.configure(Emu.INSTANCE, CODATransition.class);
        smartToolbar1.configure(Emu.INSTANCE, RunControl.class);
        smartToolbar2.configure(Emu.INSTANCE, SessionControl.class);
        splitPane1.setDividerLocation(.75);

        setVisible(true);
    }

    /**
     * Method addDocument ...
     *
     * @param doc of type Document
     */
    public void addDocument(Document doc) {
        try {
            Node node = doc.getDocumentElement();
            DataNode dn = Configurer.treeToPanel(node,0);
            JInternalFrame f = new JInternalFrame(node.getNodeName(), true, true, true, true);

            f.setTitle(dn.getValue());
            f.getContentPane().add(dn.getContainer());
            f.setMinimumSize(new Dimension(200, 200));
            f.setLocation(300 * documentCount, 0);
            f.pack();
            f.setVisible(true);
            f.setSelected(true);
            desktopPane.add(f);
            desktopPane.validate();

            doc.setUserData("DisplayPanel", f, null);
            pack();
        } catch (Exception e) {
            System.err.println("ERROR " + e.getMessage());
            e.printStackTrace();
        }

        documentCount++;
    }

    /**
     * Method removeDocument ...
     *
     * @param doc of type Document
     */
    public void removeDocument(Document doc) {
        documentCount--;
        JInternalFrame p = (JInternalFrame) doc.getUserData("DisplayPanel");
        desktopPane.remove(p);
        desktopPane.validate();
        desktopPane.repaint();
    }

    /**
     * Method getToolBar returns the toolBar of this Framework object.
     *
     * @return the toolBar (type SmartToolbar) of this Framework object.
     */
    public SmartToolbar getToolBar() {
        return smartToolbar;
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

    private void quitMenuItemActionPerformed(ActionEvent e) {
        System.exit(0);
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
        splitPane1 = new JSplitPane();
        scrollPane1 = new JScrollPane();
        desktopPane = new MDIDesktopPane();
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

        //======== splitPane1 ========
        {
            splitPane1.setOrientation(JSplitPane.VERTICAL_SPLIT);

            //======== scrollPane1 ========
            {
                scrollPane1.setViewportView(desktopPane);
            }
            splitPane1.setTopComponent(scrollPane1);

            //======== logScrollPane ========
            {
                logScrollPane.setViewportView(logPanel);
            }
            splitPane1.setBottomComponent(logScrollPane);
        }

        GroupLayout contentPaneLayout = new GroupLayout(contentPane);
        contentPane.setLayout(contentPaneLayout);
        contentPaneLayout.setHorizontalGroup(
            contentPaneLayout.createParallelGroup()
                .add(GroupLayout.TRAILING, contentPaneLayout.createSequentialGroup()
                    .addContainerGap()
                    .add(contentPaneLayout.createParallelGroup(GroupLayout.TRAILING)
                        .add(GroupLayout.LEADING, splitPane1, GroupLayout.DEFAULT_SIZE, 706, Short.MAX_VALUE)
                        .add(GroupLayout.LEADING, smartToolbar1, GroupLayout.DEFAULT_SIZE, 706, Short.MAX_VALUE)
                        .add(GroupLayout.LEADING, smartToolbar, GroupLayout.DEFAULT_SIZE, 706, Short.MAX_VALUE)
                        .add(GroupLayout.LEADING, smartToolbar2, GroupLayout.DEFAULT_SIZE, 706, Short.MAX_VALUE))
                    .addContainerGap())
        );
        contentPaneLayout.setVerticalGroup(
            contentPaneLayout.createParallelGroup()
                .add(contentPaneLayout.createSequentialGroup()
                    .addContainerGap()
                    .add(smartToolbar, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(LayoutStyle.RELATED)
                    .add(smartToolbar1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(LayoutStyle.RELATED)
                    .add(smartToolbar2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(LayoutStyle.RELATED)
                    .add(splitPane1, GroupLayout.DEFAULT_SIZE, 719, Short.MAX_VALUE)
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
    private JMenuItem prefsItem;
    private JMenu logMenu;
    private JMenuItem clearMenuItem;
    private JMenu helpMenu;
    private JMenuItem help;
    private JMenuItem aboutMenuItem;
    private SmartToolbar smartToolbar;
    private SmartToolbar smartToolbar1;
    private SmartToolbar smartToolbar2;
    private JSplitPane splitPane1;
    private JScrollPane scrollPane1;
    private MDIDesktopPane desktopPane;
    private JScrollPane logScrollPane;
    private SwingLogConsoleDialog logPanel;
    // JFormDesigner - End of variables declaration  //GEN-END:variables
}
