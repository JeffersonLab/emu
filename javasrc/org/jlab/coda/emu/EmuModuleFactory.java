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

import org.jlab.coda.support.component.CODAState;
import org.jlab.coda.support.component.CODATransition;
import org.jlab.coda.support.component.RunControl;
import org.jlab.coda.support.config.Configurer;
import org.jlab.coda.support.config.DataNotFoundException;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.log.Logger;
import org.jlab.coda.support.transport.DataChannel;
import org.jlab.coda.support.transport.EmuTransportFactory;
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
public class EmuModuleFactory implements EmuModule {

    /** Field modules */
    private static final Collection<EmuModule> modules = new Vector<EmuModule>();

    /** Field classloader */
    private URLClassLoader classloader = null;

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /** Field TRANSPORT_FACTORY */
    private final static EmuTransportFactory TRANSPORT_FACTORY = new EmuTransportFactory();

    /**
     * Method execute ...
     *
     * @param cmd of type Command
     * @see EmuModule#execute(Command)
     */
    @SuppressWarnings({"ConstantConditions"})
    public void execute(Command cmd) throws CmdExecException {
        Logger.info("EmuModuleFactory.execute : " + cmd);

        if (state != CODAState.ERROR && cmd.equals(RunControl.configure)) {
            // If we get this far configure succeeded.
            state = CODAState.CONFIGURED;
            return;
        }

        if (cmd.equals(CODATransition.download)) try {
            // There are no modules loaded so we need to load some!
            URL[] locations;
            Node m = Configurer.getNode(EMUComponentImpl.INSTANCE
                    .configuration(), "component/modules");

            if (m == null) throw new DataNotFoundException("modules section missing from config");

            NamedNodeMap nm = m.getAttributes();

            Node srcAttr = nm.getNamedItem("src");

            if (srcAttr == null) throw new DataNotFoundException("modules tag has no src attribute");

            String src = srcAttr.getNodeValue();

            srcAttr = nm.getNamedItem("usr_src");

            if (srcAttr == null) {
                Logger.warn("modules tag has no usr_src attribute");

                Logger.info("Loading modules from", src);

                File pluginFile = new File(System.getenv("INSTALL_DIR") + "/lib/" + src);

                locations = new URL[] {pluginFile.toURL()};
            } else {
                String usrSrc = srcAttr.getNodeValue();

                Logger.info("Load modules " + System.getenv("INSTALL_DIR") + "/lib/" + src + "," + usrSrc);

                File pluginFile = new File(System.getenv("INSTALL_DIR") + "/lib/" + src);
                File pluginFile2 = new File(System.getenv("INSTALL_DIR") + "/lib/" + usrSrc);

                locations = new URL[] {pluginFile.toURL(), pluginFile2.toURL()};
            }

            if (!m.hasChildNodes()) throw new DataNotFoundException("modules section present in config but no modules");

            NodeList l = m.getChildNodes();

            modules.clear();

            System.gc();
            System.gc();

            System.runFinalization();

            // Transport factory is a special "hidden" module
            modules.add(TRANSPORT_FACTORY);

            classloader = new URLClassLoader(locations);

            for (int ix = 0; ix < l.getLength(); ix++) {
                Node n = l.item(ix);
                if (n.getNodeType() == Node.ELEMENT_NODE) {
                    NamedNodeMap nm2 = n.getAttributes();

                    Node typeAttr = nm2.getNamedItem("type");
                    if (srcAttr == null) throw new DataNotFoundException("module " + n.getNodeName() + " has no type attribute");

                    String type = typeAttr.getNodeValue();
                    String moduleClassName = "modules." + type;
                    Logger.info("Require module" + moduleClassName);

                    EmuModule module = LoadModule(n.getNodeName(), moduleClassName);

                }
            }
            classloader = null;

        } catch (Exception e) {
            Logger.error("EmuModuleFactory.execute() threw " + e.getMessage());
            e.printStackTrace();
            CODAState.ERROR.getCauses().add(e);
            state = CODAState.ERROR;
            throw new CmdExecException();
        }

        // pass the command to the module
        for (EmuModule module : modules)
            module.execute(cmd);

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
     * Method channels is required by EmuModule but the Module Factory has no
     * data channels so we return null.
     *
     * @return HashMap<String, DataChannel>
     * @see EmuModule#channels()
     */
    public HashMap<String, DataChannel> channels() {

        return null;
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
