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
 * This interface is used when defining data transports, data channels,
 * and modules. It encompasses all state machine functions and the
 * ability to notify others of an END event's arrival.
 *
 * @see CODATransition
 * @author timmer
 * Date: 4/18/13
 */
public interface CODAStateMachine {

    /**
     * This method implements the GO transition of the CODA run control state machine.
     * @throws CmdExecException if error during command execution.
     */
    public void go() throws CmdExecException;

    /**
     * This method implements the END transition of the CODA run control state machine.
     * @throws CmdExecException if error during command execution.
     */
    public void end() throws CmdExecException;

    /**
     * This method implements the PAUSE transition of the CODA run control state machine.
     * @throws CmdExecException if error during command execution.
     */
    public void pause()throws CmdExecException;

    /**
     * This method implements the PRESTART transition of the CODA run control state machine.
     * @throws CmdExecException if error during command execution.
     */
    public void prestart() throws CmdExecException;

    /**
     * This method implements the DOWNLOAD transition of the CODA run control state machine.
     * @throws CmdExecException if error during command execution.
     */
    public void download() throws CmdExecException;

    /**
     * This method implements the RESET transition of the CODA run control state machine.
     */
    public void reset();

    /**
     * This method allows for setting a object used to notify the caller when an END event
     * has arrived (or any other occurrence for that matter).
     * @param callback object used for notifying caller.
     */
    public void registerEndCallback(EmuEventNotify callback);

    /**
     * This method gets the callback object previously registered by the caller
     * used to notify upon the arrival of an END event.
     * @return object used for notifying caller.
     */
    public EmuEventNotify getEndCallback();

    /**
     * This method allows for setting a object used to notify the caller when a PRESTART event
     * has arrived (or any other occurrence for that matter).
     * @param callback object used for notifying caller.
     */
    public void registerPrestartCallback(EmuEventNotify callback);

    /**
     * This method gets the callback object previously registered by the caller
     * used to notify upon the arrival of a PRESTART event.
     * @return object used for notifying caller.
     */
    public EmuEventNotify getPrestartCallback();


}
