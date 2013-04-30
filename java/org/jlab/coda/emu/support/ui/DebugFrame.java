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

package org.jlab.coda.emu.support.ui;

import org.jdesktop.layout.GroupLayout;
import org.jdesktop.layout.LayoutStyle;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNode;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.logger.QueueAppender;
import org.jlab.coda.emu.support.ui.log.SwingLogConsoleDialog;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.swing.*;
import javax.swing.border.LineBorder;
import javax.swing.border.TitledBorder;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.*;

/**
 * @author heyes
 * @author timmer
 */
public class DebugFrame extends JFrame {
    private int documentCount = 0;
    private Logger logger;
    private Emu emu;

    public DebugFrame(Emu emu) {
        initComponents();
        setTitle(emu.name());
        QueueAppender logQueueAppender = new QueueAppender(1024);
        this.emu = emu;
        logger = emu.getLogger();
        logger.addAppender(logQueueAppender);
        logPanel.monitor(logQueueAppender);
        smartToolbar.configure(emu, 0);
        smartToolbar1.configure(emu, 1);
        // not used
        smartToolbar2.configure(emu, 2);
        // not used
        smartToolbar3.configure(emu, 3);
        splitPane1.setDividerLocation(.75);

        setVisible(true);
    }

    /** This method adds an internal frame used to grab input for emu commands that need input. */
    public void generateInputPanel() {
        //-------------------------------------------
        // Get set of all commands that require input
        //-------------------------------------------
        // Put all commands in the set
        EnumSet<CODACommand> enumSet = EnumSet.allOf(CODACommand.class);

        // Subtract out items that do NOT have input either because they
        // 1) do not require it, or 2) are not displayed.
        for (CODACommand item : CODACommand.values()) {
            if (item.getInputType() == null || item.getGuiGroup() < 0) {
                enumSet.remove(item);
            }
        }


        //--------------------------------------------------------------------
        // 1) Look through each smartToolbar's buttons
        // 2) See if they are linked to commands expecting input (found above)
        // 3) If so, reset their action listener to grab the input from gui
        //--------------------------------------------------------------------
        // place to store input-expecting buttons & their associated toolbar
        LinkedHashMap<JButton, SmartToolbar> buttons = new LinkedHashMap<JButton, SmartToolbar>();

        SmartToolbar[] toolbars = new SmartToolbar[] {smartToolbar,  smartToolbar1,
                                                      smartToolbar2, smartToolbar3};

        // 1) & 2)
        for (SmartToolbar toolbar : toolbars) {
            Component[] comps = toolbar.getComponents();
            for (Component comp : comps) {
                for (CODACommand cmd : enumSet) {
                    if (cmd.name().equals(comp.getName())) {
                        buttons.put((JButton) comp, toolbar);
                    }
                }
            }
        }

        // now start creating panel to be displayed by this gui
        JPanel panel = new JPanel();

        // border
        Color color = new Color(0,0,255);   // blue
        LineBorder border = new LineBorder(color,1);
        TitledBorder titledBorder = BorderFactory.createTitledBorder(border, "Input", TitledBorder.LEFT,
                                                                     TitledBorder.TOP, null, color);
        panel.setBorder(titledBorder);

        // Create layout manager
        GroupLayout layout = new GroupLayout(panel);
        panel.setLayout(layout);
        // Turn off automatically adding gaps between components
        layout.setAutocreateGaps(false);
        // Turn off automatically creating gaps between components that touch
        // the edge of the container and the container.
        layout.setAutocreateContainerGaps(false);

        // Create a sequential group for the horizontal axis.
        GroupLayout.ParallelGroup hGroup = layout.createParallelGroup();
        layout.setHorizontalGroup(hGroup);
        GroupLayout.SequentialGroup rows = layout.createSequentialGroup();
        layout.setVerticalGroup(rows);

        // 3)
        // For each button create label and input widget AND set new action listener
        JButton button;
        SmartToolbar toolbar;
        Map.Entry<JButton,SmartToolbar> entry;

        Iterator<Map.Entry<JButton,SmartToolbar>> it = buttons.entrySet().iterator();
        for (;it.hasNext();) {
            entry   = it.next();
            button  = entry.getKey();
            toolbar = entry.getValue();

            GroupLayout.ParallelGroup   row = layout.createParallelGroup(GroupLayout.BASELINE);
            GroupLayout.SequentialGroup col = layout.createSequentialGroup();
            JLabel label    = new JLabel(button.getName());
            JTextField text = new JTextField();
            row.add(label);
            col.add(label);
            row.add(text);
            col.add(text);
            rows.add(row);
            hGroup.add(col);
            // remove old listener & then add new one
            toolbar.addButtonListener(emu, button, text);
        }

        // Add this panel to debug GUI
        addPanel(panel, "Input");
    }

    /**
     * Method addDocument ...
     *
     * @param doc of type Document
     */
    public void addDocument(Document doc) {
        if (doc == null) return;
        try {
            Node node = doc.getDocumentElement();
            DataNode dn = Configurer.treeToPanel(node,0);
            JInternalFrame f = new JInternalFrame(dn.getValue(), true, false, true, true);
            f.getContentPane().add(dn.getContainer());
            f.setMinimumSize(new Dimension(200, 200));
            f.setLocation(220 * documentCount, 0);
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
     * Method to add Panel to gui.
     * @param panel JPanel to add
     */
    public void addPanel(JPanel panel, String title) {
        try {
            JInternalFrame f = new JInternalFrame(title, true, false, true, true);
            f.getContentPane().add(panel);
            f.setPreferredSize(new Dimension(200, 200));
            f.setMinimumSize(new Dimension(200, 200));
            f.setLocation(220 * documentCount, 0);
            f.pack();
            f.setVisible(true);
            f.setSelected(true);
            desktopPane.add(f);
            desktopPane.validate();
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
        JInternalFrame p = (JInternalFrame) doc.getUserData("DisplayPanel");
        if (p == null) return;
        documentCount--;
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

    private void clearMenuItemActionPerformed(ActionEvent e) {
        // TODO add your code here
        logPanel.clear();
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
        splitPane1 = new JSplitPane();
        scrollPane1 = new JScrollPane();
        desktopPane = new MDIDesktopPane();
        logScrollPane = new JScrollPane();
        logPanel = new SwingLogConsoleDialog();
        logPanel.setLogger(logger);
        smartToolbar3 = new SmartToolbar();

        //======== this ========
        setTitle(bundle.getString("debugFrame.title"));
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
                editMenu.setPreferredSize(new Dimension(37, 19));

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
                clearMenuItem.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        clearMenuItemActionPerformed(e);
                    }
                });
                logMenu.add(clearMenuItem);

                //---- errorLoggingCheckBox ----
                errorLoggingCheckBox.setText(bundle.getString("errorLoggingCheckBox.text"));
                errorLoggingCheckBox.setPreferredSize(new Dimension(57, 23));
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
                debugLoggingCheckBox.setPreferredSize(new Dimension(57, 23));
                debugLoggingCheckBox.setMaximumSize(new Dimension(32767, 32767));
                debugLoggingCheckBox.setMinimumSize(new Dimension(1, 1));
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

        //======== smartToolbar3 ========
        {
            smartToolbar3.setFloatable(false);
        }

        GroupLayout contentPaneLayout = new GroupLayout(contentPane);
        contentPane.setLayout(contentPaneLayout);
        contentPaneLayout.setHorizontalGroup(
            contentPaneLayout.createParallelGroup()
                .add(contentPaneLayout.createSequentialGroup()
                             .addContainerGap()
                             .add(contentPaneLayout.createParallelGroup()
                                          .add(splitPane1, GroupLayout.DEFAULT_SIZE, 694, Short.MAX_VALUE)
                                          .add(smartToolbar1, GroupLayout.DEFAULT_SIZE, 694, Short.MAX_VALUE)
                                          .add(smartToolbar, GroupLayout.DEFAULT_SIZE, 694, Short.MAX_VALUE)
                                          .add(smartToolbar2, GroupLayout.DEFAULT_SIZE, 694, Short.MAX_VALUE)
                                          .add(smartToolbar3, GroupLayout.DEFAULT_SIZE, 694, Short.MAX_VALUE))
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
                    .add(smartToolbar3, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                    .addPreferredGap(LayoutStyle.RELATED)
                    .add(splitPane1, GroupLayout.DEFAULT_SIZE, 666, Short.MAX_VALUE)
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
    private JSplitPane splitPane1;
    private JScrollPane scrollPane1;
    private MDIDesktopPane desktopPane;
    private JScrollPane logScrollPane;
    private SwingLogConsoleDialog logPanel;
    private SmartToolbar smartToolbar3;
    // JFormDesigner - End of variables declaration  //GEN-END:variables
}
