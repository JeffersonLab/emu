/*
 * Copyright (c) 2013, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000 Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.codaComponent;

import org.jlab.coda.emu.EmuEventNotify;
import org.jlab.coda.emu.support.control.CmdExecException;

/**
 * This class provides empty implementations of the CODAStateMachine interface.
 * Extending this class implements the CODAStateMachine interface and frees
 * any subclass from having to implement methods that aren't used.
 *
 * @author timmer
 * Date: 4/18/13
 */
public class CODAStateMachineAdapter implements CODAStateMachine {

    /** {@inheritDoc} */
    public void go()       throws CmdExecException {}
    /** {@inheritDoc} */
    public void end()      throws CmdExecException {}
    /** {@inheritDoc} */
    public void pause()    throws CmdExecException {}
    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {}
    /** {@inheritDoc} */
    public void download() throws CmdExecException {}


    /** {@inheritDoc} */
    public void reset() {}


    /** {@inheritDoc} */
    public void registerEndCallback(EmuEventNotify callback) {}
    /** {@inheritDoc} */
    public EmuEventNotify getEndCallback() {return null;}
}
