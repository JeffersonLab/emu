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

package org.jlab.coda.emu.support.codaComponent;

import org.jlab.coda.emu.support.control.CommandAcceptor;
import org.w3c.dom.Document;

/**
 * This interface describes a component in the CODA run control system.
 * To be a part of run control, a connection to the correct cMsg server
 * is necessary.
 * @author heyes
 */
public interface CODAComponent extends CommandAcceptor {

    /**
     * Get the configuration of this CODA component in the form of
     * a parsed XML document object that is loaded from a file when
     * the configure command is executed. It may change from run to run
     * and tells the Emu which modules to load, which data transports to
     * start and what data channels to open.
     *
     * @return the CODA component configuration object as a parsed XML document
     */
    public Document configuration();

    /**
     * Get the configuration of this CODA component in the form of
     * a parsed XML document object that is loaded when the configure
     * command is executed. It contains all of the status variables
     * that change from run to run.
     * 
     * @return the CODA component configuration object as a parsed XML document
     *         that contains all of the status variables that change from run to run
     */
    public Document parameters();

    /**
     * Get the name of this CODA component.
     * @return the name of this CODA component
     */
    public String name();

    /**
     * Get the id of this CODA component.
     * @return the id of this CODA component.
     */
    public int getCodaid();

    /**
     * Get the session of this CODA component.
     * @return the session of this CODA component
     */
    public String getSession();

    /**
     * Get the experiment id (expid) of this CODA component.
     * @return the experiment id (expid) of this CODA component
     */
    public String getExpid();

    /**
     * Get the name of the host this CODA component is running on.
     * @return the hostName of this CODAComponent object.
     */
    public String getHostName();

    /**
     * Get the class of this CODA component (e.g. "EMU", "ROC", "ER").
     * @return the class of this CODA component (e.g. "EMU", "ROC", "ER")
     */
    public CODAClass getCodaClass();

    /**
     * Get the run number of this CODA component.
     * @return the run number of this CODA component
     */
    public int getRunNumber();

    /**
     * Get the numeric code representing the run type of this CODA component.
     * @return the numeric code representing the run type of this CODA component.
     */
    public int getRunTypeId();

    /**
     * Get the runType of this CODA component.
     * @return the runType of this CODA component
     */
    public String getRunType();

    /**
     * Get the UDL used to connect to the cMsg server by this CODA component.
     * @return the UDL used to connect to the cMsg server by this CODA component
     */
    public String getCmsgUDL();

    /**
     * Set the run number of this CODA component.
     * @param runNumber the run number of this CODA component
     */
    public void setRunNumber(int runNumber);

    /**
     * Set the numeric code representing the run type of this CODA component.
     * @param runTypeId the numeric code representing the runType of this CODA component
     */
    public void setRunTypeId(int runTypeId);

    /**
     * Set the runType of this CODA component.
     * @param runType the runType of this CODA component
     */
    public void setRunType(String runType);

    /**
     * Set the id of this CODA component.
     * @param codaid the coda id of this CODA component
     */
    public void setCodaid(int codaid);

    /**
     * Reset this CODA component.
     */
    public void reset();

}