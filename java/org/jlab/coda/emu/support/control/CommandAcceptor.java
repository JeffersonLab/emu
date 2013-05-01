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

import org.jlab.coda.emu.support.codaComponent.StatedObject;

/**
 * @author heyes
 * @author timmer
 * Date: Sep 24, 2008
 */
public interface CommandAcceptor extends StatedObject {
    /**
     * This method puts a command object into a mailbox of a CODA
     * component that is periodically checked by that component.
     *
     * @param cmd of type Command (eg. start transition, control run variables, control session)
     * @throws InterruptedException
     */
    public void postCommand(Command cmd) throws InterruptedException;
}
