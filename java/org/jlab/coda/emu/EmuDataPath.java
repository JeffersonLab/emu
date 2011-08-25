/*
 * Copyright (c) 2011, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu;


import java.util.LinkedList;


/**
 * This class represents data flow through connected EMU modules.
 * It's constructed in 2 stages. First, the EMU config file is
 * parsed and a linked list of Modules from module names is made.
 * Later when EmuModule objects are constructed by the EmuModuleFactory,
 * they are placed in the existing list so proper distribution of run
 * control commands can be made.
 *
 * @author timmer
 */
public class EmuDataPath {

    /** This is a doubly linked list of all module objects. */
    private final LinkedList<Module> modules = new LinkedList<Module>();

    /** This is a doubly linked list of all EMU modules. */
    private final LinkedList<EmuModule> emuModules = new LinkedList<EmuModule>();

    /** Class to store module-related data. */
    private class Module {
        /** Is this module the head of this data path? */
        boolean isHead;
        /** Does this module have a fifo input channel? */
        boolean hasInputFifo;
        /** Does this module have a fifo output channel? */
        boolean hasOutputFifo;

        /** EmuModule object (the actual module itself). */
        EmuModule module;
        /** Name of this module. */
        String moduleName;
        /** Input fifo's name, else null. */
        String inputFifoName;
        /** Output fifo's name, else null. */
        String outputFifoName;
    }


    /**
     * Constructor.
     *
     * @param name name of module to create
     * @param inputFifo  name of any input fifo, else null
     * @param outputFifo name of any output fifo, else null
     */
    public EmuDataPath(String name, String inputFifo, String outputFifo) {
        addModuleName(name, inputFifo, outputFifo);
    }


    /**
     * Add a module to the data path. This method does not add an EmuModule object,
     * but only the module name and any input & output fifo names. Any associated
     * EmuModule is attached later with a call to {@link #associateModule(EmuModule)}.
     *
     * @param name name of module to create and add
     * @param inputFifo  name of any input fifo, else null
     * @param outputFifo name of any output fifo, else null
     * @return <code>true</code> if module added, else <code>false</code>
     */
    synchronized public boolean addModuleName(String name, String inputFifo, String outputFifo) {

        Module mod = new Module();

        mod.moduleName     = name;
        mod.inputFifoName  = inputFifo;
        mod.outputFifoName = outputFifo;

        if (inputFifo  != null) mod.hasInputFifo = true;
        if (outputFifo != null) mod.hasOutputFifo = true;

        if (modules.size() < 1) {
            if (mod.hasInputFifo) {
                return false;
            }
            mod.isHead = true;
        }
        else if (modules.getLast().outputFifoName == null) {
            // If last module has no output fifo,
            // this is the last module in data path.
            return false;
        }
        else if (!modules.getLast().outputFifoName.equals(inputFifo)) {
            // The last module's output fifo must be
            // the same as this module's input fifo.
            return false;
        }

        System.out.println("Creating Module object with name = " + name +
        " input fifo = " + inputFifo + " and output fifo = " + outputFifo);

        modules.add(mod);
        return true;
    }

    /**
     * Clear all existing modules from data path.
     */
    synchronized public void clear() {
        modules.clear();
        emuModules.clear();
    }

    /**
     * Associate an EmuModule object with a module of the same name in the path.
     *
     * @param module EmuModule to associate with a module
     * @return <code>true</code> if module associated, else <code>false</code>
     */
    synchronized public boolean associateModule(EmuModule module) {
        for (Module mod : modules) {
            if (mod.moduleName.equals(module.name())) {
                mod.module = module;
                return true;
            }
        }
        return false;
    }

    /**
     * Get the linked list of the EmuModules in this path.
     * @return linked list of the EmuModules in this path.
     */
    synchronized public LinkedList<EmuModule> getModules() {
        emuModules.clear();
        for (Module mod : modules) {
            emuModules.add(mod.module);
        }
        return emuModules;
    }


    /**
     * Does this path contain a module of the given name?
     *
     * @param name name of module
     * @return <code>true</code> if module in path, else <code>false</code>
     */
    synchronized public boolean containsModuleName(String name) {
        for (Module mod : modules) {
            if (mod.moduleName.equals(name)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Does this path contain the given module?
     *
     * @param module module
     * @return <code>true</code> if module in path, else <code>false</code>
     */
    synchronized public boolean containsModule(EmuModule module) {
        if (emuModules == null) {
            getModules();
        }
        return emuModules.contains(module);
    }


    /**
     *  Create a string representation of this object.
     * @return a string representation of this object.
     */
    synchronized public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("input -> ");
        for (Module mod : modules) {
            sb.append(mod.moduleName + " -> ");
        }
        sb.append(" output");

        return sb.toString();
    }

}
