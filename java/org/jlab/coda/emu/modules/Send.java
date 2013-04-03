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

package org.jlab.coda.emu.modules;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.codaComponent.CODAState;

import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;

import java.util.ArrayList;
import java.util.Map;

/**
 * <pre>
 * Class <b>Send </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class Send implements EmuModule {

    /** Field state */
    private State state = CODAState.BOOTED;

    /** Field name */
    private String name = null;

    private Emu    emu;
    private Logger logger;

    /**
     * Constructor Send creates a new Send instance.
     *
     * @param name         name of module
     * @param attributeMap map containing attributes of module
     * @param emu          EMU object
     */
    public Send(String name, Map<String,String> attributeMap, Emu emu) {
        this.name = name;
        this.emu = emu;
        logger = emu.getLogger();
    }

    public Object[] getStatistics() {
        return null;
    }

    public boolean representsEmuStatistics() {
        return false;
    }

    public String name() {
        return name;
    }

    public State state() {
        return state;
    }

    /**
     * Set the state of this object.
     * @param s the state of this Cobject
     */
    public void setState(State s) {
        state = s;
    }

    /** {@inheritDoc} */
    public void reset() {
    }

    /** {@inheritDoc} */
    public void execute(Command cmd) {

        CODACommand emuCmd = cmd.getCodaCommand();

        if (emuCmd == END) {
            state = CODAState.DOWNLOADED;
        }

        state = cmd.success();
    }

    protected void finalize() throws Throwable {
        logger.info("Finalize " + name);
        super.finalize();
    }

    /** {@inheritDoc} */
    public void setInputChannels(ArrayList<DataChannel> input_channels) {
    }

    /** {@inheritDoc} */
    public void setOutputChannels(ArrayList<DataChannel> output_channels) {
    }

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getInputChannels() {
        return null;
    }

    /** {@inheritDoc} */
    public ArrayList<DataChannel> getOutputChannels() {
        return null;
    }
}