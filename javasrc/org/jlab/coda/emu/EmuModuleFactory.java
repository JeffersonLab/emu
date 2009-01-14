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

import org.jlab.coda.support.codaComponent.CODAState;
import org.jlab.coda.support.codaComponent.CODATransition;
import org.jlab.coda.support.codaComponent.RunControl;
import org.jlab.coda.support.configurer.Configurer;
import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.logger.Logger;
import org.jlab.coda.support.transport.DataChannel;
import org.jlab.coda.support.transport.DataTransport;
import org.jlab.coda.support.transport.DataTransportFactory;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Vector;

/**
 * <pre>
 * Class <b>EmuModuleFactory </b>
 * This class is the implementation of an Emu Module that is able to
 * load and create other modules and monitor their state.
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class EmuModuleFactory {

    /** Field modules */
    private static final Collection<EmuModule> modules = new Vector<EmuModule>();

    /** Field classloader */
    private ClassLoader classloader = null;

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /** Field TRANSPORT_FACTORY */
    private final static DataTransportFactory TRANSPORT_FACTORY = new DataTransportFactory();

    /**
     * Method execute ...
     *
     * @param cmd of type Command
     * @see EmuModule#execute(Command)
     */
    @SuppressWarnings({"ConstantConditions"})
    public void execute(Command cmd) throws CmdExecException {
        Logger.info("EmuModuleFactory.execute : " + cmd);
        System.out.println("EmuModuleFactory.execute : " + cmd);
        if (state != CODAState.ERROR && cmd.equals(RunControl.configure)) {
            // If we get this far configure succeeded.
            state = CODAState.CONFIGURED;
            return;
        }

        if (cmd.equals(CODATransition.download)) {
            try {

                // There are no modules loaded so we need to load some!
                URL[] locations;

                Node modulesConfig = Configurer.getNode(Emu.INSTANCE.configuration(), "codaComponent/modules");
                if (modulesConfig == null) throw new DataNotFoundException("modules section missing from config");

                if (!modulesConfig.hasChildNodes()) throw new DataNotFoundException("modules section present in config but no modules");

                NamedNodeMap nm = modulesConfig.getAttributes();

                Node srcAttr = nm.getNamedItem("src");
                Node usrSrcAttr = nm.getNamedItem("usr_src");

                String src = "modules.jar";

                if (srcAttr != null) src = srcAttr.getNodeValue();

                src = System.getenv("INSTALL_DIR") + "/lib/" + src;

                if (usrSrcAttr == null) {

                    Logger.info("Loading modules from" + src);

                    locations = new URL[] {(new File(src)).toURL()};
                } else {
                    String usrSrc = usrSrcAttr.getNodeValue();

                    Logger.info("Load system modules from " + src);
                    Logger.info("Load user modules from " + usrSrc);

                    locations = new URL[] {(new File(src)).toURL(), (new File(usrSrc)).toURL()};
                }

                classloader = new URLClassLoader(locations);

                modules.clear();

                System.gc();
                System.gc();

                System.runFinalization();

                TRANSPORT_FACTORY.execute(cmd);

                Node n = modulesConfig.getFirstChild();
                do {
                    if (n.getNodeType() == Node.ELEMENT_NODE) {
                        NamedNodeMap nm2 = n.getAttributes();

                        Node typeAttr = nm2.getNamedItem("class");
                        if (typeAttr == null) throw new DataNotFoundException("module " + n.getNodeName() + " has no class attribute");

                        String moduleClassName = "modules." + typeAttr.getNodeValue();
                        Logger.info("Require module" + moduleClassName);

                        EmuModule module = LoadModule(n.getNodeName(), moduleClassName);

                    }
                } while ((n = n.getNextSibling()) != null);

                classloader = null;

            } catch (Exception e) {
                Logger.error("EmuModuleFactory.execute() threw " + e.getMessage());
                e.printStackTrace();
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                throw new CmdExecException();
            }
        } else if (cmd.equals(CODATransition.prestart)) {
            TRANSPORT_FACTORY.execute(cmd);
            try {
                Node modulesConfig = Configurer.getNode(Emu.INSTANCE.configuration(), "codaComponent/modules");
                Node moduleNode = modulesConfig.getFirstChild();
                do {
                    if ((moduleNode.getNodeType() == Node.ELEMENT_NODE) && moduleNode.hasChildNodes()) {
                        EmuModule module = findModule(moduleNode.getNodeName());
                        if (module != null) {
                            HashMap<String, DataChannel> in = new HashMap<String, DataChannel>();
                            HashMap<String, DataChannel> out = new HashMap<String, DataChannel>();
                            NodeList l = moduleNode.getChildNodes();
                            for (int ix = 0; ix < l.getLength(); ix++) {
                                Node channelNode = l.item(ix);
                                if (channelNode.getNodeType() != Node.ELEMENT_NODE) continue;

                                NamedNodeMap nnm = channelNode.getAttributes();

                                Node channelNameNode = nnm.getNamedItem("name");

                                String channelName = channelNameNode.getNodeValue();

                                Node channelTranspNode = nnm.getNamedItem("transp");

                                String channelTransName = channelTranspNode.getNodeValue();
                                System.out.println("module " + module.name() + " channel " + channelName + " transp " + channelTransName);
                                DataTransport trans = DataTransportFactory.findNamedTransport(channelTransName);

                                if (channelNode.getNodeName().matches("inchannel")) {
                                    DataChannel channel = trans.createChannel(channelName, true);
                                    in.put(channelTransName + ":" + channelName, channel);
                                }
                                if (channelNode.getNodeName().matches("outchannel")) {
                                    DataChannel channel = trans.createChannel(channelName, false);
                                    out.put(channelTransName + ":" + channelName, channel);

                                }
                            }
                            module.setInput_channels(in);
                            module.setOutput_channels(out);
                        }
                    }
                } while ((moduleNode = moduleNode.getNextSibling()) != null);

            } catch (Exception e) {
                Logger.error("EmuModuleFactory.execute() threw " + e.getMessage());
                e.printStackTrace();
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                throw new CmdExecException();
            }
        } else TRANSPORT_FACTORY.execute(cmd);

        // pass the command to the module
        for (EmuModule module : modules)
            module.execute(cmd);

        if (cmd.equals(RunControl.reset)) {
            Logger.info("EmuModuleFactory : RESET");
            System.out.println("EmuModuleFactory : RESET");
            modules.clear();
            classloader = null;

            System.gc();
            System.gc();

            System.runFinalization();
            state = CODAState.UNCONFIGURED;
            CODAState.ERROR.getCauses().clear();
        }

        if (cmd.success() != null) state = cmd.success();

    }

    /**
     * Method LoadModule load the class for a module "moduleName" and create and
     * create an instance with name "name".
     *
     * @param name       of type String
     * @param moduleName of type String
     * @return EmuModule
     * @throws InstantiationException    when
     * @throws IllegalAccessException    when
     * @throws ClassNotFoundException    when
     * @throws SecurityException         when
     * @throws NoSuchMethodException     when
     * @throws IllegalArgumentException  when
     * @throws InvocationTargetException when
     */
    private EmuModule LoadModule(String name, String moduleName) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
        Logger.info("EmuModuleFactory loads module - " + moduleName);
        // Load the first class
        Class c = classloader.loadClass(moduleName);
        Class[] parameterTypes = {String.class};
        Constructor co = c.getConstructor(parameterTypes);
        // create an instance
        Object[] args = {name};
        EmuModule thing = (EmuModule) co.newInstance(args);
        modules.add(thing);
        return thing;
    }

    /** @return @see EmuModule#name() */
    public String name() {

        return "ModuleFactory";
    }

    /**
     * Method findModule given it's name locate a module
     *
     * @param name of type String
     * @return EmuModule
     */
    public static EmuModule findModule(String name) {

        for (EmuModule module : modules) {
            if (module.name().matches(name)) {
                return module;
            }
        }
        return null;
    }

    /**
     * Method state checks that all of the modules created by the
     * factory "agree" on the state and returns that state or the
     * ERROR state.
     *
     * @return the state
     * @see EmuModule#state()
     */
    public State state() {

        if (state == CODAState.ERROR) {
            //Logger.error("EmuModuleFactory : state() returning CODAState.ERROR");
            return state;
        }

        if (modules.size() == 0) return state;

        State module_state = null;
        State test_state = null;
        // Logger.info("check state against state of " + modules.get(0).name() + " who reports " + state);
        for (EmuModule module : modules) {
            module_state = module.state();

            if ((test_state != null) && (test_state != module_state)) {
                //noinspection ThrowableInstanceNeverThrown
                CODAState.ERROR.getCauses().add(

                        new Exception(new StringBuilder().append("ModuleFactory: module ").append(module.name()).append(" is in state ").append(module_state).append(", expected ").append(test_state).toString()));
                state = CODAState.ERROR;
            }
            test_state = module_state;

        }
        state = module_state;
        return state;

    }

    /**
     * Method check returns the state of each module in a vector.
     *
     * @return Vector<State>
     */
    public Vector<State> check() {
        Vector<State> sv = new Vector<State>();

        for (EmuModule module : modules) {
            sv.add(module.state());
        }
        return sv;
    }

    public void ERROR() {
        state = CODAState.ERROR;

    }

}
