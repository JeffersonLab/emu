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

    /** No-arg constructor. */
    public SmartToolbar() {
        this.setFloatable(true);
    }

    /** @param orientation {@inheritDoc} */
    public SmartToolbar(int orientation) {
        super(orientation);
    }

    /** @param name {@inheritDoc} */
    public SmartToolbar(String name) {
        super(name);
    }

    /**
     * @param name {@inheritDoc}
     * @param orientation {@inheritDoc}
     */
    public SmartToolbar(String name, int orientation) {
        super(name, orientation);
    }

    /**
     * Method configure ...
     *
     * @param target object that allows commands to be sent (eg Emu is a CodaComponent which is a CommandAcceptor)
     * @param c Class of the enum type
     */
    public void configure(CommandAcceptor target, Class c) {
        // get array of enum elements or null if not enum type
        Object[] oarray = c.getEnumConstants();

        try {
            // for each enum item ...
            for (Object anOarray : oarray) {
                Command cmd = (Command) anOarray;
                String name = cmd.name();

                // put into a hashmap(key,val)
                buttonHandlers.put(name, cmd);

                // create a button (arg is text to be displayed)
                JButton tbb = new JButton(name);

                // bug bug: I think we need to add this for it to work (Carl)
                tbb.setActionCommand(name);

                // add listener to button
                addButtonListener(target, tbb);

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

    
    /**
     * Set each button's enable status to be the
     * same as the enable status of its command.
     */
    public void update() {
        Component[] buttons = this.getComponents();

        for (Component button : buttons) {
            String name = button.getName();
            Command cmd = buttonHandlers.get(name);
            button.setEnabled(cmd.isEnabled());
        }
    }


    /**
     * Each time the given button is pressed, the target object posts or
     * executes the command associated with the button.
     *
     * @param target object that allows commands to be sent (eg Emu is a CodaComponent which is a CommandAcceptor)
     * @param tbb of type JButton
     */
    private void addButtonListener(final CommandAcceptor target, final JButton tbb) {

        tbb.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                // The name of the button must be the ActionEvent's command
                // (which is also the key of the map).
                Command cmd = buttonHandlers.get(e.getActionCommand());
                // We pressed a button. There can be no args.
                cmd.clearArgs();

                try {
                    // Execute the command associated with the given button.
                    // (Emu puts cmd into Q which another thread pulls off).
                    target.postCommand(cmd);
                    // set enable status of all buttons
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
    

    /** Remove all components from this toolbar. */
    public void reset() {
        this.removeAll();
    }

}
