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

package org.jlab.coda.support.ui;

import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.CommandAcceptor;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;

/** @author heyes */
public class SmartToolbar extends JToolBar {

    /**
     *
     */
    private static final long serialVersionUID = 7241838854981192095L;

    /** Field buttonHandlers */
    private final HashMap<String, Command> buttonHandlers = new HashMap<String, Command>();

    private Object[] oarray;

    /**
     *
     */
    public SmartToolbar() {
        //
        this.setFloatable(true);
    }

    /** @param orientation  */
    public SmartToolbar(int orientation) {
        super(orientation);
        //
    }

    /** @param name  */
    public SmartToolbar(String name) {
        super(name);
        //
    }

    /**
     * @param name
     * @param orientation
     */
    public SmartToolbar(String name, int orientation) {
        super(name, orientation);
        //
    }

    CommandAcceptor target = null;

    /**
     * Method configure ...
     *
     * @param o of type Object
     * @param c of type Class
     */
    public void configure(Object o, Class c) {
        oarray = c.getEnumConstants();

        target = (CommandAcceptor) o;

        try {

            for (Object anOarray : oarray) {
                Command cmd = (Command) anOarray;
                String name = cmd.name();

                buttonHandlers.put(name, cmd);
                JButton tbb = new JButton(name);

                addButtonListener(this, tbb);
                // boolean enabled =
                tbb.setEnabled(cmd.isEnabled());
                tbb.setName(name);
                this.add(tbb);

            }
        } catch (SecurityException e) {

            e.printStackTrace();
        } catch (IllegalArgumentException e) {

            e.printStackTrace();
        }
    }

    /** Method update ... */
    public void update() {
        Component[] buttons = this.getComponents();

        for (Component button : buttons) {
            String name = button.getName();
            Command cmd = buttonHandlers.get(name);
            button.setEnabled(cmd.isEnabled());

        }
    }

    /**
     * Method addButtonListener ...
     *
     * @param tb  of type JToolBar
     * @param tbb of type JButton
     */
    private void addButtonListener(final JToolBar tb, final JButton tbb) {
        tbb.addActionListener(new ActionListener() {
            /**
             * Method actionPerformed ...
             *
             * @param e of type ActionEvent
             */
            public void actionPerformed(ActionEvent e) {

                Command cmd = buttonHandlers.get(e.getActionCommand());
                // We pressed a button. There can be no args
                cmd.clearArgs();

                try {
                    target.postCommand(cmd);
                    update();
                } catch (SecurityException e1) {

                    e1.printStackTrace();
                } catch (IllegalArgumentException e1) {

                    e1.printStackTrace();
                } catch (InterruptedException e3) {

                    e3.printStackTrace();
                }
            }
        });
    }

    /** Method reset ... */
    public void reset() {
        this.removeAll();
    }

}
