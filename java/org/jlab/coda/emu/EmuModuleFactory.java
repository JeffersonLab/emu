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
package org.jlab.coda.emu;

import static org.jlab.coda.emu.support.codaComponent.CODAState.*;

import org.jlab.coda.emu.modules.EventBuilding;
import org.jlab.coda.emu.modules.EventRecording;
import org.jlab.coda.emu.modules.RocSimulation;
import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.codaComponent.CODACommand;
import org.jlab.coda.emu.support.codaComponent.StatedObject;
import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.emu.support.transport.DataTransport;
import org.jlab.coda.emu.support.transport.DataTransportFactory;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * <pre>
 * Class <b>EmuModuleFactory</b>
 * This class is able to load and create EmuModules and monitor their state
 * (hence the name EmuModuleFactory). Only one of these objects exists in and
 * was created by an Emu object.
 * </pre>
 * Created on Sep 17, 2008
 *
 * @author heyes
 * @author timmer
 */
public class EmuModuleFactory implements StatedObject {

    /**
     * This object is a Vector and thus is synchronized for insertions and deletions.
     * This vector is only modified in the
     * {@link #execute(org.jlab.coda.emu.support.control.Command)} method and then only by
     * the main EMU thread. However, it is possible that other threads (such as the EMU's
     * statistics reporting thread) may call methods which use its iterator ({@link #check()},
     * {@link #state()}, {@link #findModule(String)}, and {@link #getStatisticsModule()})
     * and therefore need to be synchronized.
     */
    private final Vector<EmuModule> modules = new Vector<EmuModule>(10);

    private EmuDataPath dataPath;

    /** State of the emu. */
    private volatile State state = BOOTED;

    /** What was this emu's previous state? Useful when doing RESET transition. */
    State previousState = BOOTED;

    /** Object which creates and manages transport (data movement) objects. */
    private final DataTransportFactory transportFactory;

    /** Emu that created this EmuModuleFactory object. */
    private final Emu emu;

    /** Logger of errors and debugs associated with this emu. */
    private final Logger logger;


    /**
     * Constructor.
     * @param emu Emu that is creating this EmuModuleFactory object.
     */
    EmuModuleFactory(Emu emu) {
        this.emu = emu;
        logger = emu.getLogger();
        transportFactory = new DataTransportFactory(emu);
    }

    /**
     * Get the module from which we gather statistics.
     * Used to report statistics to Run Control.
     *
     * @return the module from which statistics are gathered.
     */
    EmuModule getStatisticsModule() {
        synchronized(modules) {
            if (modules.size() < 1) return null;

            // Return first module that says its statistics represents EMU statistics
            for (EmuModule module : modules) {
                if (module.representsEmuStatistics()) {
                    return module;
                }
            }

            // If no modules claim to speak for EMU, choose last module in config file
            return modules.lastElement();
        }
    }


    /**
     * This method executes a RESET command.
     * RESET must always have top priority and therefore its own means of execution.
     */
    public void reset() {
        // Pass RESET down to transport layer first, then to modules
        transportFactory.reset();

        for (EmuModule module : modules) {
            module.reset();
        }

        if (previousState == ERROR || previousState == BOOTED) {
            state = BOOTED;
        }
        else {
            state = CONFIGURED;
        }
logger.info("EmuModuleFactory.execute : RESET executed, setting state to " + state);

        emu.getCauses().clear();
        return;
    }


    /**
     * This method executes commands given to it.
     *
     * @param cmd of type Command
     * @see EmuModule#execute(org.jlab.coda.emu.support.control.Command)
     */
    @SuppressWarnings({"ConstantConditions"})
    public void execute(Command cmd) throws CmdExecException {

        CODACommand emuCmd = cmd.getCodaCommand();

logger.info("EmuModuleFactory.execute : " + emuCmd);

        // CONFIGURE command does not involve components and is handled directly by the EMU ...
        if (emuCmd == CONFIGURE) {
            if (state != ERROR) state = CONFIGURED;
            return;
        }

        // DOWNLOAD command does non-run-specific initialization that involves components/modules ...
        if (emuCmd == DOWNLOAD) {

            try {
                // Get the config info again since it may have changed
                Node modulesConfig = Configurer.getNode(emu.configuration(), "component/modules");

                // Check for config problems
                if (modulesConfig == null) {
                    // Only happens if  emu.configuration() is null
                    throw new DataNotFoundException("config never loaded");
                }

                // Need modules to create an emu
                if (!modulesConfig.hasChildNodes()) {
                    throw new DataNotFoundException("modules section present in config, but no modules");
                }

                // Remove all existing modules from collection
                modules.clear();

                // Create transport objects
                transportFactory.execute(cmd, false);

                // Create modules
                Node n = modulesConfig.getFirstChild();
                do {
                    if (n.getNodeType() == Node.ELEMENT_NODE) {
                        NamedNodeMap nm2 = n.getAttributes();

                        // Store all attributes in a hashmap to pass to module
                        Map<String, String> attributeMap = new HashMap<String, String>();
                        for (int j=0; j < nm2.getLength(); j++) {
                            Node a = nm2.item(j);
                            attributeMap.put(a.getNodeName(), a.getNodeValue());
                        }

                        // What type of module are we creating?
                        EmuModule module;
                        switch (emu.getCodaClass()) {
                            case DC:
                            case PEB:
                            case SEB:
                                module = new EventBuilding(n.getNodeName(), attributeMap, emu);
                                break;

                            case ER:
                                module = new EventRecording(n.getNodeName(), attributeMap, emu);
                                break;

                            case ROC:
                                module = new RocSimulation(n.getNodeName(), attributeMap, emu);
                                break;

                            default:
                                Node typeAttr = nm2.getNamedItem("class");
                                if (typeAttr == null) {
                                    throw new DataNotFoundException("module " + n.getNodeName() +
                                                                            " has no class attribute");
                                }
                                String moduleClassName = typeAttr.getNodeValue();

                                logger.info("EmuModuleFactory loads module - " + moduleClassName +
                                                    " - to create a module of name " + n.getNodeName() +
                                                    "\n  in classpath = " +
                                                    System.getProperty("java.class.path"));

                                // Load the class using the JVM's standard class loader
                                Class c = Class.forName(moduleClassName);

                                // Constructor required to have a string, a map, and an emu as args
                                Class[] parameterTypes = {String.class, Map.class, Emu.class};
                                Constructor co = c.getConstructor(parameterTypes);

                                // Create an instance
                                Object[] args = {n.getNodeName(), attributeMap, emu};
                                module = (EmuModule) co.newInstance(args);
//logger.info("EmuModuleFactory.execute DOWN : load module " + moduleClassName);
                                break;

                        }

                        dataPath.associateModule(module);
                        modules.add(module);
                    }

                } while ((n = n.getNextSibling()) != null);

                // Pass DOWNLOAD to all the modules. "modules" is only
                // changed in this method so no synchronization is necessary.
                for (EmuModule module : modules) {
                    module.execute(cmd);
                }

            // This includes ClassNotFoundException
            } catch (Exception e) {
                e.printStackTrace();
                emu.getCauses().add(e);
                state = ERROR;
                throw new CmdExecException();
            }
        }

        // PRESTART command does run-specific initialization ...
        else if (emuCmd == PRESTART) {

            // Pass prestart to transport objects first
            transportFactory.execute(cmd, false);

            // Create transportation channels for all modules
            try {
                Node modulesConfig = Configurer.getNode(emu.configuration(), "component/modules");
                Node moduleNode = modulesConfig.getFirstChild();
                // For each module in the list of modules ...
                do {
                    // Modules section present in config (no modules if no children)
                    if ((moduleNode.getNodeType() == Node.ELEMENT_NODE) && moduleNode.hasChildNodes()) {
                        // Find module object associated with this config node
                        EmuModule module = findModule(moduleNode.getNodeName());
                        if (module != null) {
                            ArrayList<DataChannel> in  = new ArrayList<DataChannel>();
                            ArrayList<DataChannel> out = new ArrayList<DataChannel>();
                            
                            // For each channel in (children of) the module ...
                            NodeList childList = moduleNode.getChildNodes();
                            for (int i=0; i < childList.getLength(); i++) {
                                Node channelNode = childList.item(i);
                                if (channelNode.getNodeType() != Node.ELEMENT_NODE) continue;

//System.out.println("EmuModuleFactory.execute PRE : looking at channel node = " + channelNode.getNodeName());
                                // Get attributes of channel node
                                NamedNodeMap nnm = channelNode.getAttributes();
                                if (nnm == null) {
//System.out.println("EmuModuleFactory.execute PRE : junk in config file (no attributes), skip " + channelNode.getNodeName());
                                    continue;
                                }

                                // Get "name" attribute node from map
                                Node channelNameNode = nnm.getNamedItem("name");

                                // If none (junk in config file) go to next channel
                                if (channelNameNode == null) {
//System.out.println("EmuModuleFactory.execute PRE : junk in config file (no name attr), skip " + channelNode.getNodeName());
                                    continue;
                                }
//System.out.println("EmuModuleFactory.execute PRE : channel node of attribute \"name\" = " + channelNameNode.getNodeName());
                                // Get name of this channel
                                String channelName = channelNameNode.getNodeValue();
//System.out.println("EmuModuleFactory.execute PRE : found channel of name " + channelName);
                                // Get "transp" attribute node from map
                                Node channelTranspNode = nnm.getNamedItem("transp");
                                if (channelTranspNode == null) {
//System.out.println("EmuModuleFactory.execute PRE : junk in config file (no transp attr), skip " + channelNode.getNodeName());
                                    continue;
                                }
                                // Get name of transport
                                String channelTransName = channelTranspNode.getNodeValue();
//System.out.println("EmuModuleFactory.execute PRE : module = " + module.name() + ", channel = " + channelName + ", transp = " + channelTransName);
                                // Look up transport object from name
                                DataTransport trans = transportFactory.findNamedTransport(channelTransName);

                                // Store all attributes in a hashmap to pass to channel
                                Map<String, String> attributeMap = new HashMap<String, String>();
                                for (int j=0; j < nnm.getLength(); j++) {
                                    Node a = nnm.item(j);
//System.out.println("Put (" + a.getNodeName() + "," + a.getNodeValue() + ") into attribute map for channel " + channelName);
                                    attributeMap.put(a.getNodeName(), a.getNodeValue());
                                }

                                // If it's an input channel ...
                                if (channelNode.getNodeName().equalsIgnoreCase("inchannel")) {
                                    // create channel
                                    DataChannel channel = trans.createChannel(channelName, attributeMap, true, emu);
                                    // add to list
                                    in.add(channel);
                                }
                                // If it's an output channel ...
                                else if (channelNode.getNodeName().equalsIgnoreCase("outchannel")) {
                                    DataChannel channel = trans.createChannel(channelName, attributeMap, false, emu);
                                    out.add(channel);
                                }
                                else {
//System.out.println("EmuModuleFactory.execute PRE : channel type \"" + channelNode.getNodeName() + "\" is unknown");
                                }
                            }
                            
                            // Add input and output channel lists to module
                            module.setInputChannels(in);
                            module.setOutputChannels(out);
                        }
                    }
                } while ((moduleNode = moduleNode.getNextSibling()) != null);  // while another module exists ...

                // Pass PRESTART to all the modules.
                for (EmuModule module : modules) {
                    module.execute(cmd);
                }

            } catch (Exception e) {
logger.error("EmuModuleFactory.execute() : threw " + e.getMessage());
                e.printStackTrace();
                emu.getCauses().add(e);
                state = ERROR;
                throw new CmdExecException();
            }
        }

        // END needs to be sent to EMU sub-components in order of data flow.
        else if (emuCmd == END) {
            LinkedList<EmuModule> mods = dataPath.getModules();

            if (mods.size() < 1) {
                logger.error("EmuModuleFactory.execute() : no modules in data path");
                state = ERROR;
                throw new CmdExecException("no modules in data path");
            }


            // pass command to input transports of FIRST module
            EmuModule emuModule = mods.getFirst();
            ArrayList<DataChannel> channelList = emuModule.getInputChannels();
            if (channelList != null) {
                for (DataChannel chan : channelList) {
                    DataTransport trans = chan.getDataTransport();
logger.info("EmuModuleFactory.execute : END thru input transport " + trans.name());
                    trans.execute(cmd, true);  // true means we're doing inputs only
                }
            }

            // pass command to all modules starting with first
            for (int i=0; i < mods.size(); i++) {
logger.info("EmuModuleFactory.execute : END thru module " + mods.get(i).name());
                mods.get(i).execute(cmd);
            }

            // pass command to output transports of LAST module
            emuModule = mods.getLast();
            channelList = emuModule.getOutputChannels();
            if (channelList != null) {
                for (DataChannel chan : channelList) {
                    DataTransport trans = chan.getDataTransport();
logger.info("EmuModuleFactory.execute : END thru output transport " + trans.name());
                    trans.execute(cmd, false);  // false means we're doing outputs only
                }
            }

            // close all transport objects including Fifos
//            transportFactory.execute(cmd, false);
        }

        // GO needs to be sent to EMU sub-components in reverse order of data flow.
        else if (emuCmd == GO) {
            LinkedList<EmuModule> mods = dataPath.getModules();

            if (mods.size() < 1) {
                logger.error("EmuModuleFactory.execute() : no modules in data path");
                state = ERROR;
                throw new CmdExecException("no modules in data path");
            }


            // pass command to output transports of LAST module
            EmuModule emuModule = mods.getLast();
            ArrayList<DataChannel> channelList = emuModule.getOutputChannels();
            if (channelList != null) {
                for (DataChannel chan : channelList) {
                    DataTransport trans = chan.getDataTransport();
//logger.info("EmuModuleFactory.execute : GO thru transport " + trans.name());
                    trans.execute(cmd, false);  // false means we're doing outputs only
                }
            }

            // pass command to all modules starting with last
            for (int i=mods.size()-1; i >= 0; i--) {
//logger.info("EmuModuleFactory.execute : GO thru module " + mods.get(i).name());
                mods.get(i).execute(cmd);
            }

            // pass command to input transports of FIRST module
            emuModule = mods.getFirst();
            channelList = emuModule.getInputChannels();
            if (channelList != null) {
                for (DataChannel chan : channelList) {
                    DataTransport trans = chan.getDataTransport();
//logger.info("EmuModuleFactory.execute : GO thru transport " + trans.name());
                    trans.execute(cmd, false);  // true means we're doing inputs only
                }
            }

            // Currently TransportFactory does nothing with GO
        }

        // Other commands (PAUSE) are passed down to transport
        // layer first, then to modules
        else {
            transportFactory.execute(cmd, false);

            for (EmuModule module : modules) {
                module.execute(cmd);
            }
        }


        if (cmd.success() != null && state != ERROR) {
            state = cmd.success();
logger.info("EmuModuleFactory.execute : transition success, setting state to " + state);
        }
        else {
logger.info("EmuModuleFactory.execute : transition NOT successful, state = " + state);
        }

    }


    /**
     * {@inheritDoc}
     * @return {@inheritDoc}
     */
    public String name() {
        return "ModuleFactory";
    }


    /**
     * Get the data path object that directs how the run control
     * commands are distributed among the EMU parts.
     *
     * @return the data path object
     */
    EmuDataPath getDataPath() {
        return dataPath;
    }


    /**
     * Set the data path object that directs how the run control
     * commands are distributed among the EMU parts.
     *
     * @param dataPath the data path object
     */
    void setDataPath(EmuDataPath dataPath) {
        this.dataPath = dataPath;
    }


    /**
     * This method locates a module given it's name.
     *
     * @param name of module
     * @return EmuModule object
     */
    public EmuModule findModule(String name) {
        synchronized(modules) {
            for (EmuModule module : modules) {
                if (module.name().equals(name)) {
                    return module;
                }
            }
        }
        return null;
    }


    /**
     * This method returns the previous state of the modules in this EMU.
     * If the EMU has not undergone any transitions yet, it returns null.
     *
     * @return state before last transition
     * @return null if no transitions undergone yet
     */
    public State previousState() {
        return previousState;
    }

    /**
     * This method checks that all of the modules created by this
     * factory "agree" on the state and returns that state or the
     * ERROR state.
     *
     * @return the state of the emu
     * @see EmuModule#state()
     */
    public State state() {

        if (state == ERROR) {
            return state;
        }

        if (modules.size() == 0) return state;

        synchronized(modules) {
            for (StatedObject module : modules) {
                if (module.state() == ERROR) {
                    state = ERROR;
                }
            }
        }

        if (transportFactory.state() == ERROR) {
            state = ERROR;
        }

        return state;
    }


    /**
     * This method returns the state of each module in a vector.
     *
     * @return vector of State objects - each one the state of a module
     */
    public Vector<State> check() {
        Vector<State> sv = new Vector<State>();

        synchronized(modules) {
            for (EmuModule module : modules) {
                sv.add(module.state());
            }
        }

        return sv;
    }

    /** This method sets the emu state to ERROR. */
    public void ERROR() {
        state = ERROR;
    }

}


