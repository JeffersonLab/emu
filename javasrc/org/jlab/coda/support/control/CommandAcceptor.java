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

package org.jlab.coda.support.control;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Sep 24, 2008
 * Time: 11:06:37 AM
 * To change this template use File | Settings | File Templates.
 */
public interface CommandAcceptor {
    /**
     * Method postCommand ...
     *
     * @param cmd of type Command
     * @throws InterruptedException when
     */
    public void postCommand(Command cmd) throws InterruptedException;

    /** @return the state */
    public State state();
}
