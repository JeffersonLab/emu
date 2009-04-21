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
 * Class <b>Read </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class Read implements EmuModule {

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /** Field input_channels */
    private ArrayList<DataChannel> input_channels = new ArrayList<DataChannel>();

    /** Field output_channels */
    private ArrayList<DataChannel> output_channels = new ArrayList<DataChannel>();

    /** Field name */
    private String name = null;

    /**
     * Constructor Read creates a new Read instance.
     *
     * @param pname of type String
     */
    public Read(String pname) {
        name = pname;
    }

    /**
     * @return the name
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
     * Method execute ...
     *
     * @param cmd of type Command
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    public void execute(Command cmd) {

        if (cmd.equals(CODATransition.end)) {
            state = cmd.success();

        }

        state = cmd.success();
    }

    protected void finalize() throws Throwable {
        Logger.info("Finalize " + name);
        super.finalize();
    }

    public void setInput_channels(ArrayList<DataChannel> input_channels) {
        this.input_channels = input_channels;
    }

    public void setOutput_channels(ArrayList<DataChannel> output_channels) {
        this.output_channels = output_channels;
    }
}