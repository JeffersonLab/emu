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

package org.jlab.coda.emu.support.control;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Sep 24, 2008
 * Time: 11:06:37 AM
 * To change this template use File | Settings | File Templates.
 */
public interface CommandAcceptor {
    /**
     * This method puts a command object into a mailbox of a CODA
     * component that is periodically checked by that component.
     *
     * @param cmd of type Command (eg. start transition, control run variables, control session)
     * @throws InterruptedException
     */
    public void postCommand(RcCommand cmd) throws InterruptedException;

    /**
     * Get the state of the CODA component.
     * @return the state of the CODA component
     */
    public State state();
}
