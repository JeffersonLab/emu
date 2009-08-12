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
import org.jlab.coda.support.codaComponent.StatedObject;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Vector;

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

    /** Field modules */
    private static final Collection<org.jlab.coda.emu.EmuModule> modules = new Vector<org.jlab.coda.emu.EmuModule>();

    /** Field classloader */
    private ModuleClassLoader classloader;

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /** Field TRANSPORT_FACTORY */
    private final static DataTransportFactory TRANSPORT_FACTORY = new DataTransportFactory();

    /**
     * This method executes commands given to it.
     *
     * @param cmd of type Command
     * @see EmuModule#execute(Command)
     */
    @SuppressWarnings({"ConstantConditions"})
    public void execute(Command cmd) throws CmdExecException {

        Logger.info("EmuModuleFactory.execute : " + cmd);
//System.out.println("EmuModuleFactory.execute : " + cmd);

        // CONFIGURE command does not involve components and is handled directly by the EMU ...
        if (state != CODAState.ERROR && cmd.equals(RunControl.CONFIGURE)) {
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
                classloader = new ModuleClassLoader(locations);

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
                // so it would always delegate the loading to the new classloader.
                // And things worked fine. However, if the emu was run like:
                //       java -cp ...... org.jlab.coda.emu.Emu
                // then the system class loader would find the modules in the classpath and
                // not reload them. Hope that makes sense.
                //
                //------------------------------------------------------------------------------
//                System.gc();
//                System.gc();
//                System.runFinalization();

                TRANSPORT_FACTORY.execute(cmd);

                Node n = modulesConfig.getFirstChild();
                do {
                    if (n.getNodeType() == Node.ELEMENT_NODE) {
                        NamedNodeMap nm2 = n.getAttributes();

                        Node typeAttr = nm2.getNamedItem("class");
                        if (typeAttr == null) throw new DataNotFoundException("module " + n.getNodeName() + " has no class attribute");

                        String moduleClassName = "modules." + typeAttr.getNodeValue();
                        Logger.info("Require module " + moduleClassName);

                        // Tell the custom classloader to ONLY load the named class
                        // and relegate all other loading to the system classloader.
                        classloader.setClassToLoad(moduleClassName);
                        
                        // the name of the module is the first arg (node name)
                        EmuModule module = LoadModule(n.getNodeName(), moduleClassName);
                    }

                } while ((n = n.getNextSibling()) != null);

                classloader = null;

            } catch (Exception e) {
                e.printStackTrace();
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                throw new CmdExecException();
            }
        }

        // PRESTART command does run-specific initialization ...
        else if (cmd.equals(CODATransition.PRESTART)) {
            TRANSPORT_FACTORY.execute(cmd);
            try {
                Node modulesConfig = Configurer.getNode(Emu.INSTANCE.configuration(), "component/modules");
                Node moduleNode = modulesConfig.getFirstChild();
                do {
                    if ((moduleNode.getNodeType() == Node.ELEMENT_NODE) && moduleNode.hasChildNodes()) {
                        EmuModule module = findModule(moduleNode.getNodeName());
                        if (module != null) {
                            ArrayList<DataChannel> in = new ArrayList<DataChannel>();
                            ArrayList<DataChannel> out = new ArrayList<DataChannel>();
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
                                    in.add(channel);
                                }
                                if (channelNode.getNodeName().matches("outchannel")) {
                                    DataChannel channel = trans.createChannel(channelName, false);
                                    out.add(channel);

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
        }

        // Others commands are passed down to transport layer
        // since the GO, END, PAUSE, RESUME commands are concerned
        // only with the flow of data.
        else {
            TRANSPORT_FACTORY.execute(cmd);
        }

        // pass the command to the module
        for (EmuModule module : modules)
            module.execute(cmd);

        if (cmd.equals(RunControl.RESET)) {
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

        if (cmd.success() != null && state != CODAState.ERROR) state = cmd.success();

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
    private EmuModule LoadModule(String name, String moduleName) throws InstantiationException,
                                                                        IllegalAccessException,
                                                                        ClassNotFoundException,
                                                                        SecurityException,
                                                                        NoSuchMethodException,
                                                                        IllegalArgumentException,
                                                                        InvocationTargetException {
        Logger.info("EmuModuleFactory loads module - " + moduleName +
                     " to create a module of name " + name);
        System.out.println("classpath = " + System.getProperty("java.class.path"));
        // Load the first class
        Class c = classloader.loadClass(moduleName);

        // constructor required to have a single string as arg
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

        for (StatedObject module : modules) {

            if (module.state() == CODAState.ERROR) {
                state = CODAState.ERROR;
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

/**
 * This class is a custom classLoader which delegates all loading to its parent (in this case, system)
 * classLoader except for a single named class which is "refound" and reloaded using standard URLClassLoader
 * means. The reloading of that single class in ensured if and only if a new object of this type is
 * used each time a class needs reloading.<p>
 *
 * <b>Dynamic Class Reloading</b><p>
 * (http://tutorials.jenkov.com/java-reflection/dynamic-class-loading-reloading.html)<p>
 *
 * Dynamic class reloading is a bit more challenging. Java's builtin Class loaders always check if a class is already
 * loaded before loading it. Reloading the class is therefore not possible using Java's builtin class loaders.
 * To reload a class you will have to implement your own ClassLoader subclass.<p>
 *
 * Even with a custom subclass of ClassLoader you have a challenge. Every loaded class needs to be linked.
 * This is done using the ClassLoader.resolve() method. This method is final, and thus cannot be overridden in your
 * ClassLoader subclass. The resolve() method will not allow any given ClassLoader instance to link the same class
 * twice. Therefore, everytime you want to reload a class you must use a new instance of your ClassLoader subclass.
 * This is not impossible, but necessary to know when designing for class reloading.<p>
 *
 * The trick is to:
 * <ul>
 * <li>  Use an interface as the variable type, and just reload the implementing class, or
 * <li>  Use a superclass as the variable type, and just reload a subclass.
 * </ul><p>
 * Either of these two methods will work if the type of the variable, the interface or superclass,
 * is not reloaded when the implementing class or subclass is reloaded.<p>
 *
 * To make this work you will of course need to implement your class loader to let the interface or superclass be
 * loaded by its parent. When your class loader is asked to load the MyObject class, it will also be asked to load
 * the MyObjectInterface class, or the MyObjectSuperclass class, since these are referenced from within the MyObject
 * class. Your class loader must delegate the loading of those classes to the same class loader that loaded the class
 * containing the interface or superclass typed variables.<p>
 *
 */
class ModuleClassLoader extends URLClassLoader {

    /** Name of the single class we want loaded. */
    String classToLoad;

    /**
     * Constructor. The system class loader is assumed to be the parent.
     * @param urls URLs to search for the classes of interest.
     */
    public ModuleClassLoader(URL[] urls) {
        super(urls);
    }

    /**
     * Constructor.
     * @param urls URLs to search for the classes of interest.
     * @param parent parent classloader
     */
    public ModuleClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }
    
    /**
     * Set the name of the single class we want to load.
     * @param className name of the single class we want to load
     */
    public void setClassToLoad(String className) {
        classToLoad = className;
    }

    /**
     * Method to load the class of interest. This is called many
     * times by the system - once for each class that the class
     * we are trying to load references.
     *
     * @param name name of class to load
     * @return Class object
     * @throws ClassNotFoundException if code for class cannot be found
     */
    public Class loadClass(String name) throws ClassNotFoundException {

        // Default to standard URLClassLoader behavior
        if (classToLoad == null) super.loadClass(name);

        // We only want to load 1 specific class. Everything else needs to
        // be loaded by the system classloader (the parent in this case).
        // We especially do not want to load "org.jlab.coda.emu.EmuModule"
        // since that will not be the same class as the identically named
        // one loaded by the system loader. Because all modules implement
        // the EmuModule interface, this loader will be asked to load it.
        // Simply pass the request back to the parent and we're OK. Now
        // there's only one version of EmuModule that everyone agrees on.
        if(!classToLoad.equals(name)) {
//System.out.println("ModuleClassLoader: have parent load " + name);
            return getParent().loadClass(name);
        }

        // Cannot call super.loadClass(name) since that calls ClassLoader.loadClass(name)
        // (since URLClassLoader never overwrites it), which in turn delegates the finding
        // to the parent. We want to use the URLClassLoader to find it from scratch so call
        // the findClass() method directly.
        
//System.out.println("ModuleClassLoader: I will load/find " + name);
        return findClass(name);
    }

}

