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

package org.jlab.coda.emu.support.ui;

import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.codaComponent.EmuCommand;
import org.jlab.coda.emu.support.control.CommandAcceptor;
import org.jlab.coda.emu.support.control.RcCommand;
import org.jlab.coda.emu.support.control.State;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.EnumSet;
import java.util.HashMap;

/** @author heyes */
public class SmartToolbar extends JToolBar {

    /**
     *
     */
    private static final long serialVersionUID = 7241838854981192095L;

    /** Field buttonHandlers */
    private final HashMap<String, RcCommand> buttonHandlers = new HashMap<String, RcCommand>();

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
     * Enable or disable buttons depending on the emu state.
     * @param state state used to decide which buttons to enable/disable.
     */
    public void updateButtons(State state) {
        // Transitions which are allowed out of our state.
        EnumSet<CODATransition> eSet = state.allowed();

        // Names of allowed transitions
        int i=0;
        String[] names = new String[eSet.size()];
        for (CODATransition tran : eSet) {
            names[i++] = tran.name();
        }

        // Enable/disable transition GUI buttons.
        Component[] comps = getComponents();
        for (Component comp : comps) {
            JButton button = (JButton) comp;
            button.setEnabled(false);
//System.out.println("SmartToolbar: disable " + button.getName());
            for (String name : names) {
                if (button.getName().equals(name)) {
                    button.setEnabled(true);
//System.out.println("SmartToolbar: enable " + name);
                    break;
                }
            }
        }

    }


    /**
     * Method configure ...
     *
     * @param target object that allows commands to be sent (eg Emu is a CodaComponent which is a CommandAcceptor)
     * @param guiGroup gui group number of EmuCommands to configure
     */
    public void configure(CommandAcceptor target, int guiGroup) {
        // get array of enum elements of Emu commands
        //Object[] oarray = EmuCommand.class.getEnumConstants();
        EnumSet<EmuCommand> emuCmdSet = EmuCommand.getGuiGroup(guiGroup);

        try {
            // for each enum item ...
            //for (Object anOarray : oarray) {
            for (EmuCommand emuCmd : emuCmdSet) {
                RcCommand cmd = new RcCommand(emuCmd);
                String name = cmd.name();

                // put into a hashmap(key,val)
                buttonHandlers.put(name, cmd);

                // create a button (arg is text to be displayed)
                JButton tbb = new JButton(name);

                // bug bug: I think we need to add this for it to work (Carl)
                tbb.setActionCommand(name);

                // add listener to button
                addButtonListener(target, tbb);

//                tbb.setEnabled(true);
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
                RcCommand cmd = buttonHandlers.get(e.getActionCommand());

                try {
                    // Execute the command associated with the given button.
                    // (Emu puts cmd into Q which another thread pulls off).
                    target.postCommand(cmd);

                    // Enabling or disabling of buttons was done when cmd was executed.

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
