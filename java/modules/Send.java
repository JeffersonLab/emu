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
import org.jlab.coda.support.codaComponent.CODAState;
import org.jlab.coda.support.codaComponent.CODATransition;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.support.transport.DataChannel;

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

    /**
     * @return the name
     *
     * @see org.jlab.coda.emu.EmuModule#name()
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.emu.EmuModule#getName()
    */
    public String name() {
        return name;
    }

    /** @return the state */
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

    /**
     * Method execute ...
     *
     * @param cmd of type Command
     *
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    public void execute(Command cmd) {

        if (cmd.equals(CODATransition.END)) {
            state = CODAState.ENDED;
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