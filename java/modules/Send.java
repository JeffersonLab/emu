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

package modules;

import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;

import java.util.ArrayList;

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
    private State state = CODAState.UNCONFIGURED;

    /** Field name */
    private String name = null;

    /**
     * Constructor Send creates a new Send instance.
     *
     * @param pname of type String
     */
    public Send(String pname) {
        name = pname;
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

    public void execute(Command cmd) {

        if (cmd.equals(CODATransition.END)) {
            state = CODAState.DOWNLOADED;
        }

        state = cmd.success();
    }

    protected void finalize() throws Throwable {
        Logger.info("Finalize " + name);
        super.finalize();
    }

    public void setInputChannels(ArrayList<DataChannel> input_channels) {

    }

    public void setOutputChannels(ArrayList<DataChannel> output_channels) {

    }
}