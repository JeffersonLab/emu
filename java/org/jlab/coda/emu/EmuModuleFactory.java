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

import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODATransition;
import org.jlab.coda.emu.support.codaComponent.RunControl;
import org.jlab.coda.emu.support.codaComponent.StatedObject;
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

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.*;

/**
 * <pre>
 * Class <b>EmuModuleFactory </b>
 * This class is able to load and create EmuModules and monitor their state
 * (hence the name EmuModuleFactory). Only one of these objects exists and
 * is created by the Emu class.
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class EmuModuleFactory implements StatedObject {

    /**
     * This object is a Vector and thus is synchronized for insertions and deletions.
     * This vector is only modified in the {@link #execute} method and then only by
     * the main EMU thread. However, it is possible that other threads (such as the EMU's
     * statistics reporting thread) may call methods which use its iterator ({@link #check()},
     * {@link #state()}, {@link #findModule(String)}, and {@link #getStatisticsModule()})
     * and therefore need to be synchronized.
     */
    private static final Vector<EmuModule> modules = new Vector<EmuModule>(10);

    /** Field classloader */
    private EmuClassLoader classloader;

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /** Field TRANSPORT_FACTORY - singleton */
    private final static DataTransportFactory TRANSPORT_FACTORY = new DataTransportFactory();

    
    /**
     * Get the module from which we gather statistics. Used to report statistics to
     * run control.
     *
     * @return the module from which statistics are gathered.
     */
    EmuModule getStatisticsModule() {
        synchronized(modules) {
            if (modules.size() < 1) return null;

            // return first module that says its statistics represents EMU statistics
            for (EmuModule module : modules) {
                if (module.representsEmuStatistics()) {
                    return module;
                }
            }

            // if no modules claim to speak for EMU, choose last module in config file
            return modules.lastElement();
        }
    }

    /**
     * This method executes commands given to it.
     *
     * @param cmd of type Command
     * @see EmuModule#execute(Command)
     */
    @SuppressWarnings({"ConstantConditions"})
    public void execute(Command cmd) throws CmdExecException {

        Logger.info("EmuModuleFactory.execute : " + cmd);

        // CONFIGURE command does not involve components and is handled directly by the EMU ...
        if (state != CODAState.ERROR && cmd.equals(CODATransition.CONFIGURE)) {
            // If we got this far configure succeeded.
            state = CODAState.CONFIGURED;
            return;
        }

        // DOWNLOAD command does non-run-specific initialization that involves components/modules ...
        if (cmd.equals(CODATransition.DOWNLOAD)) {

            try {
                // there are no modules loaded so we need to load some
                URL[] locations;

                // get the config info again since it may have changed
                Node modulesConfig = Configurer.getNode(Emu.INSTANCE.configuration(), "component/modules");

                // check for config problems
                if (modulesConfig == null) {
                    // only happens if  Emu.INSTANCE.configuration() is null
                    throw new DataNotFoundException("config never loaded");
                }

                if (!modulesConfig.hasChildNodes()) {
                    throw new DataNotFoundException("modules section present in config but no modules");
                }

                // get attributes of the top ("component/modules") node -> names of needed jar files
                NamedNodeMap nm = modulesConfig.getAttributes();
                // get name of jar file containing source for standard, CODA-supplied modules
                Node srcAttr    = nm.getNamedItem("src");
                // get name of jar file (including full path) containing user's modules' source
                Node usrSrcAttr = nm.getNamedItem("usr_src");

                // Set name of file containing standard modules, default = modules.jar
                String src = "modules.jar";
                if (srcAttr != null) src = srcAttr.getNodeValue();

                // change file name into full path by looking in dir $INSTALL_DIR/lib/
                src = System.getenv("INSTALL_DIR") + "/lib/" + src;

                // if NO user source, look only in standard location for standard modules
                if (usrSrcAttr == null) {
                    Logger.info("Loading modules from " + src);
                    locations = new URL[] {(new File(src)).toURI().toURL()};
                }
                // if user has source, look for that file as well as for standard modules
                else {
                    String usrSrc = usrSrcAttr.getNodeValue();

                    Logger.info("Load system modules from " + src);
                    Logger.info("Load user modules from " + usrSrc);

                    locations = new URL[] {(new File(src)).toURI().toURL(),
                                           (new File(usrSrc)).toURI().toURL()};
                }

                // Load "untrusted" java code from URLs - each of which
                // represents a directory or JAR file to search.
                classloader = new EmuClassLoader(locations);

                // remove all existing modules from collection
                modules.clear();

                //------------------------------------------------------------------------------
                // NOTE:
                //
                // To unload previously used modules (classes) there are 3 necessary conditions:
                //    1) all references to the classes must be gone,
                //    2) all references to their classLoader must be gone, and finally
                //    3) the garbage collector must collect them all.
                //
                // That is irrelevant since we now use a custom classLoader (see ModuleClassLoader)
                // and use a new one each time.
                //
                // I, timmer, have discovered that the following (commented out) method
                // to reload a class never worked. It worked by accident because the of
                // the way the Emu was run:
                //       java -jar emu.jar
                // It turns out, this sets the internal classpath to ONLY the jar file.
                // Thus any new classloader would ask its parent to load the file, but the
                // parent (system classloader) would never be able to see the modules.jar
                // in the classpath (set by -cp option or in CLASSPATH env variable)
                // so it would always delegate the loading back to the new classloader.
                // And things worked fine. However, if the emu was run like:
                //       java -cp ...... org.jlab.coda.emu.Emu
                // then the system class loader would find the modules in the classpath and
                // not reload them. Hope that makes sense.
                //
                //------------------------------------------------------------------------------
//                System.gc();
//                System.gc();
//                System.runFinalization();

                // create the transport objects & channels before the modules  // TODO: WHY?
                TRANSPORT_FACTORY.execute(cmd);

                Node n = modulesConfig.getFirstChild();
                do {
                    if (n.getNodeType() == Node.ELEMENT_NODE) {
                        NamedNodeMap nm2 = n.getAttributes();

                        Node typeAttr = nm2.getNamedItem("class");
                        if (typeAttr == null) throw new DataNotFoundException("module " + n.getNodeName() + " has no class attribute");

                        // store all attributes in a hashmap to pass to channel
                        Map<String, String> attributeMap = new HashMap<String, String>();
                        for (int jx=0; jx < nm2.getLength(); jx++) {
                            Node a = nm2.item(jx);
System.out.println("Put (" + a.getNodeName() + "," + a.getNodeValue() + ") into attribute map for module " + n.getNodeName());
                            attributeMap.put(a.getNodeName(), a.getNodeValue());
                        }

                        String moduleClassName = "modules." + typeAttr.getNodeValue();
                        Logger.info("EmuModuleFactory.execute DOWN : load module " + moduleClassName);

                        // the name of the module is the first arg (node name)
                        loadModule(n.getNodeName(), moduleClassName, attributeMap);
                    }

                } while ((n = n.getNextSibling()) != null);

            } catch (Exception e) {
                e.printStackTrace();
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                throw new CmdExecException();
            }
        }

        // PRESTART command does run-specific initialization ...
        else if (cmd.equals(CODATransition.PRESTART)) {

            // pass prestart to transport objects first
            TRANSPORT_FACTORY.execute(cmd);

            // now pass it on to the modules
            try {
                Node modulesConfig = Configurer.getNode(Emu.INSTANCE.configuration(), "component/modules");
                Node moduleNode = modulesConfig.getFirstChild();
                // for each module in the list of modules ...
                do {
                    // modules section present in config (no modules if no children)
                    if ((moduleNode.getNodeType() == Node.ELEMENT_NODE) && moduleNode.hasChildNodes()) {
                        // find module object associated with this config node
                        EmuModule module = findModule(moduleNode.getNodeName());
                        if (module != null) {
                            ArrayList<DataChannel> in  = new ArrayList<DataChannel>();
                            ArrayList<DataChannel> out = new ArrayList<DataChannel>();
                            
                            // for each channel in (children of) the module ...
                            NodeList childList = moduleNode.getChildNodes();
                            for (int ix = 0; ix < childList.getLength(); ix++) {
                                Node channelNode = childList.item(ix);
                                if (channelNode.getNodeType() != Node.ELEMENT_NODE) continue;

//System.out.println("EmuModuleFactory.execute PRE : looking at channel node = " + channelNode.getNodeName());
                                // get attributes of channel node
                                NamedNodeMap nnm = channelNode.getAttributes();
                                if (nnm == null) {
//System.out.println("EmuModuleFactory.execute PRE : junk in config file (no attributes), skip " + channelNode.getNodeName());
                                    continue;
                                }

                                // get "name" attribute node from map
                                Node channelNameNode = nnm.getNamedItem("name");

                                // if none (junk in config file) go to next channel
                                if (channelNameNode == null) {
//System.out.println("EmuModuleFactory.execute PRE : junk in config file (no name attr), skip " + channelNode.getNodeName());
                                    continue;
                                }
//System.out.println("EmuModuleFactory.execute PRE : channel node of attribute \"name\" = " + channelNameNode.getNodeName());
                                // get name of this channel
                                String channelName = channelNameNode.getNodeValue();
//System.out.println("EmuModuleFactory.execute PRE : found channel of name " + channelName);
                                // get "transp" attribute node from map
                                Node channelTranspNode = nnm.getNamedItem("transp");
                                if (channelTranspNode == null) {
//System.out.println("EmuModuleFactory.execute PRE : junk in config file (no transp attr), skip " + channelNode.getNodeName());
                                    continue;
                                }
                                // get name of transport
                                String channelTransName = channelTranspNode.getNodeValue();
//System.out.println("EmuModuleFactory.execute PRE : module " + module.name() + " channel " + channelName + " transp " + channelTransName);
                                // look up transport object from name
                                DataTransport trans = DataTransportFactory.findNamedTransport(channelTransName);

                                // store all attributes in a hashmap to pass to channel
                                Map<String, String> attributeMap = new HashMap<String, String>();
                                for (int jx = 0; jx < nnm.getLength(); jx++) {
                                    Node a = nnm.item(jx);
//System.out.println("Put (" + a.getNodeName() + "," + a.getNodeValue() + ") into attribute map for channel " + channelName);
                                    attributeMap.put(a.getNodeName(), a.getNodeValue());
                                }

                                // if it's an input channel ...
                                if (channelNode.getNodeName().equalsIgnoreCase("inchannel")) {
                                    // create channel
                                    DataChannel channel = trans.createChannel(channelName, attributeMap, true);
                                    // add to list
                                    in.add(channel);
                                }
                                // if it's an output channel ...
                                else if (channelNode.getNodeName().equalsIgnoreCase("outchannel")) {
                                    DataChannel channel = trans.createChannel(channelName, attributeMap, false);
                                    out.add(channel);
                                }
                                else {
//System.out.println("EmuModuleFactory.execute PRE : channel type \"" + channelNode.getNodeName() + "\" is unknown");
                                }
                            }
                            
                            // add input and output channel lists to module
                            module.setInputChannels(in);
                            module.setOutputChannels(out);
                        }
                    }
                } while ((moduleNode = moduleNode.getNextSibling()) != null);  // while another module exists ...

            } catch (Exception e) {
                Logger.error("EmuModuleFactory.execute() : threw " + e.getMessage());
                e.printStackTrace();
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                throw new CmdExecException();
            }
        }

        // Others commands are passed down to transport layer
        // since the GO, END, PAUSE, RESUME commands are concerned
        // only with the flow of data.
        else {
            TRANSPORT_FACTORY.execute(cmd);
        }

        // Pass all commands down to all the modules. "modules" is only
        // changed in this method so no synchronization is necessary.
        for (EmuModule module : modules) {
            module.execute(cmd);
        }
        
        // RESET command
        if (cmd.equals(CODATransition.RESET)) {
            Logger.info("EmuModuleFactory.execute : RESET");
            state = CODAState.CONFIGURED;
            CODAState.ERROR.getCauses().clear();
            return;
//            modules.clear();
//            state = CODAState.UNCONFIGURED;
        }

        if (cmd.success() != null && state != CODAState.ERROR) state = cmd.success();

    }

    /**
     * Method LoadModule load the class for a module "moduleName" and create and
     * create an instance with name "name".
     *
     * @param name            name of module
     * @param moduleClassName name of java class defining module
     * @param attributeMap    map containing attributes of module
     *
     * @return EmuModule
     * 
     * @throws InstantiationException    when
     * @throws IllegalAccessException    when
     * @throws ClassNotFoundException    when
     * @throws SecurityException         when
     * @throws NoSuchMethodException     when
     * @throws IllegalArgumentException  when
     * @throws InvocationTargetException when
     */
    private EmuModule loadModule(String name, String moduleClassName,
                                 Map<String,String> attributeMap) throws InstantiationException,
                                                                         IllegalAccessException,
                                                                         ClassNotFoundException,
                                                                         SecurityException,
                                                                         NoSuchMethodException,
                                                                         IllegalArgumentException,
                                                                         InvocationTargetException {
        Logger.info("EmuModuleFactory loads module - " + moduleClassName +
                     " to create a module of name " + name);
//System.out.println("classpath = " + System.getProperty("java.class.path"));

        // Tell the custom classloader to ONLY load the named class
        // and relegate all other loading to the system classloader.
        classloader.setClassesToLoad(new String[] {moduleClassName});

        // Load the class
        Class c = classloader.loadClass(moduleClassName);

        // constructor required to have a single string as arg
        Class[] parameterTypes = {String.class, Map.class};
        Constructor co = c.getConstructor(parameterTypes);

        // create an instance
        Object[] args = {name, attributeMap};
        EmuModule thing = (EmuModule) co.newInstance(args);

        modules.add(thing);
        return thing;
    }


    /**
     * {@inheritDoc}
     * @return {@inheritDoc}
     */
    public String name() {
        return "ModuleFactory";
    }


    /**
     * Method findModule given it's name locate a module.
     *
     * @param name of type String
     * @return EmuModule
     */
    public static EmuModule findModule(String name) {
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

        synchronized(modules) {
            for (StatedObject module : modules) {
                if (module.state() == CODAState.ERROR) {
                    state = CODAState.ERROR;
                }
            }
        }

        if (TRANSPORT_FACTORY.state() == CODAState.ERROR) {
            state = CODAState.ERROR;
        }

        return state;
    }


    /**
     * Method check returns the state of each module in a vector.
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


    public void ERROR() {
        state = CODAState.ERROR;
    }

}


