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
 * Class <b>Read </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class Read implements EmuModule {

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /** Field name */
    private String name = null;

    private Emu    emu;
    private Logger logger;

    /**
     * Constructor Read creates a new Read instance.
     *
     * @param name         name of module
     * @param attributeMap map containing attributes of module
     * @param emu          EMU object
     */
    public Read(String name, Map<String,String> attributeMap, Emu emu) {
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

    public void execute(Command cmd) {

        CODACommand emuCmd = cmd.getCodaCommand();

        if (emuCmd == END) {
            state = cmd.success();
        }

        state = cmd.success();
    }

    protected void finalize() throws Throwable {
        logger.info("Finalize " + name);
        super.finalize();
    }

    public void setInputChannels(ArrayList<DataChannel> input_channels) {

    }

    public void setOutputChannels(ArrayList<DataChannel> output_channels) {

    }
}