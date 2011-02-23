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

/**
 * Interface for handler attached to keystorke.
 */
package org.jlab.coda.emu.support.keyboardControl;

import java.io.PrintWriter;

/** @author heyes */
public interface KbdHandler {

    /**
     * Method keyHandler ...
     *
     * @param s        of type String
     * @param out      of type PrintWriter
     * @param argument of type Object
     * @return boolean
     */
    public boolean keyHandler(String s, PrintWriter out, Object argument);

    /**
     * Method printHelp ...
     *
     * @param out of type PrintWriter
     */
    public void printHelp(PrintWriter out);

}
