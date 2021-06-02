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

import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgMessage;
import org.jlab.coda.cMsg.cMsgPayloadItem;
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.CommandAcceptor;
import org.jlab.coda.emu.support.codaComponent.CODAStateIF;

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
    private final HashMap<String, Command> buttonHandlers = new HashMap<>(8);

    /** Store JTextField objects for certain commands. */
    private final HashMap<String, JTextField> textFields = new HashMap<>(8);

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
    public void updateButtons(CODAStateIF state) {
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
     * @param guiGroup gui group number of CODACommands to configure
     */
    public void configure(CommandAcceptor target, int guiGroup) {
        // get array of enum elements of Emu commands
        //Object[] oarray = CODACommand.class.getEnumConstants();
        EnumSet<CODACommand> emuCmdSet = CODACommand.getGuiGroup(guiGroup);

        try {
            // for each enum item ...
            //for (Object anOarray : oarray) {
            for (CODACommand codaCmd : emuCmdSet) {
                Command cmd = new Command(codaCmd);
                // mark message so emu knows it is from debug gui (clever huh?)
                cmd.fromDebugGui(true);
                String name = cmd.name();

                // put into a hashmap(key,val)
                buttonHandlers.put(name, cmd);

                // create a button (arg is text to be displayed)
                JButton tbb = new JButton(name);

                // bug bug: I think we need to add this for it to work (Carl)
                tbb.setActionCommand(name);

                // add listener to button
                addButtonListener(target, tbb);

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

        // remove existing listeners first
        ActionListener[] listeners = tbb.getActionListeners();
        for (ActionListener l : listeners) {
            tbb.removeActionListener(l);
        }

        tbb.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                // The actionCommand was set to the name of the button
                // (which is also the key of the map).
                Command cmd = buttonHandlers.get(e.getActionCommand());

                try {
                    // Execute the command associated with the given button.
                    // (Emu puts cmd into Q which another thread pulls off).
                    target.postCommand(cmd);

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
    
    /**
     * Each time the given button is pressed, the target object posts or
     * executes the command associated with the button. In this case, the
     * button has an associated textField it must read, place into a cMsg
     * message and pass it on with the command (just like run control does).
     *
     * @param target object that allows commands to be sent (eg Emu is a CodaComponent which is a CommandAcceptor)
     * @param tbb of type JButton
     * @param textField text to be placed into a cMsg
     */
    void addButtonListener(final CommandAcceptor target, final JButton tbb, final JTextField textField) {

        // remove existing listeners first
        ActionListener[] listeners = tbb.getActionListeners();
        for (ActionListener l : listeners) {
            tbb.removeActionListener(l);
        }

        tbb.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                // The actionCommand was set to the name of the button
                // (which is also the key of the map).
                Command cmd = buttonHandlers.get(e.getActionCommand());
                CODACommand codaCommand = cmd.getCodaCommand();
                CODACommand.InputType type = codaCommand.getInputType();

                // get input value
                String txt = textField.getText();
System.out.println("READ TXT AS -> " + txt);

                // blank entry means sending no msg
                if (txt == null || txt.isEmpty()) {
                    cmd.setMessage(null);
                }
                else {
                    String payloadName;
                    cMsgPayloadItem item;
                    cMsgMessage msg = new cMsgMessage();

                    switch (type) {
                        case TEXT:
                            msg.setText(txt);
                            break;

                        case USER_INT:
                            try {
                                int i = Integer.parseInt(txt);
                                msg.setUserInt(i);
                            }
                            catch (NumberFormatException ex) {
                               JOptionPane.showMessageDialog(SmartToolbar.this,
                                                              "Not integer in textfield",
                                                              "Error",
                                                              JOptionPane.ERROR_MESSAGE);
                                return;
                            }
                            break;

                        case PAYLOAD_INT:
                            try {
                                int i = Integer.parseInt(txt);
                                payloadName = codaCommand.getPayloadName();
                                System.out.println("Creating payload int with name = " + payloadName);
                                item = new cMsgPayloadItem(payloadName, i);
                                cmd.setArg(payloadName, item); // do here what callback normally does
                                msg.addPayloadItem(item);
                            }
                            catch (NumberFormatException ex) {
                                JOptionPane.showMessageDialog(SmartToolbar.this,
                                                              "Not integer in textfield",
                                                              "Error",
                                                              JOptionPane.ERROR_MESSAGE);
                                return;
                            }
                            catch (cMsgException ex) {/* never happen */}
                            break;

                        case PAYLOAD_TEXT:
                            try {
                                payloadName = codaCommand.getPayloadName();
                                item = new cMsgPayloadItem(payloadName, txt);
                                cmd.setArg(payloadName, item); // do here what callback normally does
                                msg.addPayloadItem(item);
                            }
                            catch (cMsgException e1) {/* never happen */}
                            break;

                        default:
                            return;
                    }

                    cmd.setMessage(msg);
                }


                try {
                    // Execute the command associated with the given button.
                    // (Emu puts cmd into Q which another thread pulls off).
                    target.postCommand(cmd);

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
