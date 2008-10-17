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

package org.jlab.coda.support.component;

import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.support.control.CommandAcceptor;
import org.w3c.dom.Document;

/** @author heyes */
public interface CODAComponent extends EmuModule, Runnable, CommandAcceptor {

    /** @return the loadedConfig */
    public Document configuration();

    /** @return the localConfig */
    public Document parameters();

    /**
     * Method getCodaid returns the codaid of this CODAComponent object.
     *
     * @return the codaid (type int) of this CODAComponent object.
     */
    public int getCodaid();

    /**
     * Method getSession returns the session of this CODAComponent object.
     *
     * @return the session (type String) of this CODAComponent object.
     */
    public String getSession();

    /**
     * Method getExpid returns the expid of this CODAComponent object.
     *
     * @return the expid (type String) of this CODAComponent object.
     */
    public String getExpid();

    /**
     * Method getHostName returns the hostName of this CODAComponent object.
     *
     * @return the hostName (type String) of this CODAComponent object.
     */
    public String getHostName();

    /**
     * Method getUserName returns the userName of this CODAComponent object.
     *
     * @return the userName (type String) of this CODAComponent object.
     */
    public String getUserName();

    /**
     * Method getConfig returns the config of this CODAComponent object.
     *
     * @return the config (type String) of this CODAComponent object.
     */
    public String getConfig();

    /**
     * Method getCodaClass returns the codaClass of this CODAComponent object.
     *
     * @return the codaClass (type String) of this CODAComponent object.
     */
    public String getCodaClass();

    /**
     * Method getRunNumber returns the runNumber of this CODAComponent object.
     *
     * @return the runNumber (type int) of this CODAComponent object.
     */
    public int getRunNumber();

    /**
     * Method getRunType returns the runType of this CODAComponent object.
     *
     * @return the runType (type int) of this CODAComponent object.
     */
    public int getRunType();

    /**
     * Method getCmsgUDL returns the cmsgUDL of this CODAComponent object.
     *
     * @return the cmsgUDL (type String) of this CODAComponent object.
     */
    public String getCmsgUDL();

    /**
     * Method setConfig sets the config of this CODAComponent object.
     *
     * @param conf the config of this CODAComponent object.
     */
    public void setConfig(String conf);

    /**
     * Method setRunNumber sets the runNumber of this CODAComponent object.
     *
     * @param runNumber the runNumber of this CODAComponent object.
     */
    public void setRunNumber(int runNumber);

    /**
     * Method setRunType sets the runType of this CODAComponent object.
     *
     * @param runType the runType of this CODAComponent object.
     */
    public void setRunType(int runType);

    /**
     * Method setCodaid sets the codaid of this CODAComponent object.
     *
     * @param codaid the codaid of this CODAComponent object.
     */
    public void setCodaid(int codaid);

}