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

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuClassLoader;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODACommand;

import static org.jlab.coda.emu.support.codaComponent.CODAState.*;

import org.jlab.coda.emu.support.codaComponent.StatedObject;
import static org.jlab.coda.emu.support.codaComponent.CODACommand.*;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.control.State;
import org.jlab.coda.emu.support.logger.Logger;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.net.URL;
import java.net.MalformedURLException;
import java.io.File;

/**
 * Only one of these objects is created in the EmuModuleFactory object
 * which is, in turn has only one existing object created by the Emu class.
 * Just like EmuModuleFactory can create, store and control Modules, this object
 * can create, store and control DataTransport objects.
 * 
 * @author heyes
 * @author timmer
 */
public class DataTransportFactory implements StatedObject {

    /** Vector containing all DataTransport objects. */
    private final Vector<DataTransport> transports = new Vector<DataTransport>();

    /** Name of this object. */
    private final String name = "Transport factory";

    /** Field state */
    private State state = BOOTED;

    private Logger logger;

    private Emu emu;


    /**
     * The Fifo transport object is special and is handled separately
     * from the other transports.
     * It is static so all Emus in this JVM can see it. That, in turn,
     * allows all Emus in one JVM to talk to each other through fifos.
     */
    private static DataTransport fifoTransport;

    // Create 1 fifo data transport to allow communication between Emus seamlessly.
    static {
        try {
            HashMap<String, String> attrs = new HashMap<String, String>();
            attrs.put("class", "Fifo");
            attrs.put("server", "false");
            fifoTransport = new DataTransportImplFifo("Fifo", attrs, null);
        }
        catch (DataNotFoundException e) {/* never happen */ }
    }


    /**
     * Constructor.
     * @param emu Emu object which created this data transport factory.
     */
    public DataTransportFactory(Emu emu) {
        this.emu = emu;
        logger = emu.getLogger();
    }

    /**
     * This method finds the DataTransport object corresponding to the given name.
     *
     * @param name name of transport object
     * @return DataTransport object corresponding to given name
     * @throws DataNotFoundException when no transport object of that name can be found
     */
    public DataTransport findNamedTransport(String name) throws DataNotFoundException {
        DataTransport t;

        // first look in non-fifo transports
        if (!transports.isEmpty()) {
            for (DataTransport transport : transports) {
                t = transport;
                if (t.name().equals(name)) return t;
            }
        }

        // now look at fifo transport
        if (fifoTransport.name().equals(name)) {
            return fifoTransport;
        }

        throw new DataNotFoundException("Data Transport not found");
    }

    /** {@inheritDoc} */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    public State state() {
        if (state == ERROR) return state;

        if (transports.size() == 0) return state;

        State s;
        state = transports.get(0).state();

        for (DataTransport transport : transports) {
//logger.debug("  DataTransportFactory.state() : transport " + transport.name() + " is in state " +
//                transport.state());
            s = transport.state();

            if (s == ERROR) {
                state = ERROR;
            }
        }

        return state;
    }

    
    /**
     * This method executes a RESET command.
     * RESET must always have top priority and therefore its own means of execution.
     */
    public void reset() {
        // reset (hard close) transport objects for RESET command
        for (DataTransport t : transports) {
logger.debug("  DataTransportFactory reset(): " + t.name() + " for " + emu.name());
            t.reset();
        }

        // reset Fifos
        fifoTransport.reset();
        transports.clear();

        // set the transport factory state
        state = CONFIGURED;
    }


    /**
     * This method is only called by the EmuModuleFactory's (a singleton used by Emu)
     * execute method.
     *
     * @param cmd of type Command
     * @param forInput <code>true</code> if the command applicable only to the
     *                 input channels of a transport, else <code>false</code>
     *                 if applicable to output channels. Used only for commands
     *                 GO & END in which data flow order in EMU subcomponents
     *                 is important.
     * @throws CmdExecException if command is invalid or fails
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableInstanceNeverThrown"})
    public void execute(Command cmd, boolean forInput) throws CmdExecException {
        // DOWNLOAD loads transport implementation classes, creates objects from them and
        // stores them along with a transport fifo implementation object.
        CODACommand emuCmd = cmd.getCodaCommand();
        logger.info("  DataTransportFactory.execute : " + emuCmd);

        if (emuCmd == DOWNLOAD) {

            try {
                // If doing a download from the downloaded state,
                // close the existing transport objects first
                // (this step is normally done from RESET).
                for (DataTransport t : transports) {
                    logger.debug("  DataTransportFactory.execute DOWNLOAD : " + t.name() + " close");
                    t.close();
                }

                // remove all current data transport objects
                transports.removeAllElements();

                Node m = Configurer.getNode(emu.configuration(), "component/transports");
                if (!m.hasChildNodes()) {
                    logger.warn("transport section present in config but no transports");
                    return;
                }

                NodeList l = m.getChildNodes();

                //****************************************************
                // TODO: only create transports if used by a channel!!
                //****************************************************

                // for each child node (under component/transports) ...
                for (int ix = 0; ix < l.getLength(); ix++) {
                    Node n = l.item(ix);

                    if (n.getNodeType() == Node.ELEMENT_NODE) {
                        // type is "server" (send data to) or "client" (get data from)
                        String transportType = n.getNodeName();

                        // store all attributes in a hashmap
                        Map<String, String> attrib = new HashMap<String, String>();
                        if (n.hasAttributes()) {
                            NamedNodeMap attr = n.getAttributes();

                            for (int jx = 0; jx < attr.getLength(); jx++) {
                                Node a = attr.item(jx);
                                attrib.put(a.getNodeName(), a.getNodeValue());
                            }
                        }

                        if (transportType.equalsIgnoreCase("server")) attrib.put("server", "true");
                        else attrib.put("server", "false");

                        // get the name used to access transport
                        String transportName = attrib.get("name");
                        if (transportName == null) throw new DataNotFoundException("transport name attribute missing in config");
//logger.info("  DataTransportFactory.execute DOWN : creating " + transportName);

                        // Generate a name for the implementation of this transport
                        // from the name passed from the configuration.
                        String transportClass = attrib.get("class");
                        if (transportClass == null) throw new DataNotFoundException("transport class attribute missing in config");
                        String implName = "org.jlab.coda.emu.support.transport.DataTransportImpl" + transportClass;

                        // Fifos are created internally, not by an Emu
                        if (transportClass.equals("Fifo")) {
//logger.warn("  DataTransportFactory.execute DOWN : Emu does not need to specify FIFOs in transport section of config");
                            state = cmd.success();
                            continue;
                        }

                        try {
                            Class c = DataTransportFactory.class.getClassLoader().loadClass(implName);
//logger.info("  DataTransportFactory.execute DOWN : loaded class = " + c);

                            // 2 constructor args
                            Class[] parameterTypes = {String.class, Map.class, Emu.class};
                            Constructor co = c.getConstructor(parameterTypes);

                            // create an instance & store reference
                            Object[] args = {transportName, attrib, emu};
                            transports.add((DataTransport) co.newInstance(args));

//logger.info("  DataTransportFactory.execute DOWN : created " + transportName + " of protocol " + transportClass);

                        } catch (Exception e) {
                            state = ERROR;
                            emu.getCauses().add(e);
                            throw new CmdExecException("cannot load transport class", e);
                        }
                    } // if node is element
                    state = cmd.success();
//System.out.println("  DataTransportFactory.execute: final state = " + state);
                } // for each child node
            }
            catch (DataNotFoundException e) {
                // If we're here, the transport section is missing from the config file.
                // This is permissible if and only if Fifo is the only transport used.

                // state = ERROR;
                // ERROR.getCauses().add(e);
                // throw new CmdExecException("transport section missing/incomplete from config", e);
logger.warn("  DataTransportFactory.execute DOWN : transport section missing/incomplete from config");
            }

        }  // end of DOWNLOAD

        // Pass commands down to all transport objects: DOWNLOAD, PRESTART, PAUSE.
        if (emuCmd != END && emuCmd != GO) {
            for (DataTransport transport : transports) {
logger.debug("  DataTransportFactory.execute : pass " + emuCmd + " down to " + transport.name());
                // forInput is ignored here
                transport.execute(cmd, forInput);
            }
        }
        //  GO is handled in the EmuModuleFactory, not here.
        // END is handled in the EmuModuleFactory and we close all transport objects here.
        else if (emuCmd == END) {
            for (DataTransport t : transports) {
logger.debug("  DataTransportFactory.execute : close " + t.name());
                t.close();
            }

            // clear Fifos (not included in "transports" vector)
//System.out.println("CLOSE FIFOs");
            fifoTransport.close();
        }

    }


    /**
     * This method is only called by the EmuModuleFactory's execute method.
     * This is not used since I (timmer) could never get it to work
     * quite right. It attempts to reload classes dynamically but somehow fails in
     * loading internal classes correctly.
     *
     * WARNING: THIS CODE IS OUTDATED & UNUSED.
     *
     *
     * @param cmd of type Command
     * @param forInput <code>true</code> if the command applicable only to the
     *                 input channels of a transport, else <code>false</code>
     *                 if applicable to output channels. Used only for commands
     *                 GO & END in which data flow order in EMU subcomponents
     *                 is important.
     * @throws CmdExecException if command is invalid or fails
     * @see org.jlab.coda.emu.EmuModule#execute(org.jlab.coda.emu.support.control.Command)
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableInstanceNeverThrown"})
    public void execute_EmuLoader(Command cmd, boolean forInput) throws CmdExecException {
        logger.info("DataTransportFactory.execute : " + cmd);

        CODACommand emuCmd = cmd.getCodaCommand();

        // DOWNLOAD loads transport implementation classes, creates objects from them and
        // stores them along with a transport fifo implementation object.
        if (emuCmd == DOWNLOAD) {

            try {
                Node m = Configurer.getNode(emu.configuration(), "component/transports");
//System.out.println("component/transports node = " + m);
                if (!m.hasChildNodes()) throw new DataNotFoundException("transport section present in config but no transports");

                NodeList l = m.getChildNodes();

                // remove all current data transport objects
                transports.removeAllElements();

                // We don't really need to be loading the transport classes explicitly since they are
                // part of the emu package. However, this gives us the future option of users creating
                // their own transport classes and having them dynamically loaded. Just add another
                // agreed-upon path to look in, but for now, just look in the classpath.
                String classpath = System.getProperty("java.class.path");
//System.out.println("CLASSPATH = \n" + classpath);
                URL[] urls;
                try {
                    // if no class path, try current directory and emu.jar in current directory
                    if (classpath == null) {
                        urls    = new URL[2];
                        urls[0] = new File(".").toURI().toURL();
                        urls[1] = new File("emu.jar").toURI().toURL();
                    }
                    else {
                        String[] paths = classpath.split(":");
                        urls = new URL[paths.length];
                        for (int i = 0; i < urls.length; i++) {
                            urls[i] = new File(paths[i]).toURI().toURL();
                        }
                    }
                }
                // should never happen
                catch (MalformedURLException e) {
                    urls = null;
                }

                // classloader that can reload classes
                EmuClassLoader ecl = new EmuClassLoader(urls);

                // for each child node (under component/transports) ...
                for (int ix = 0; ix < l.getLength(); ix++) {
                    Node n = l.item(ix);

                    if (n.getNodeType() == Node.ELEMENT_NODE) {
                        // type is "server" (send data to) or "client" (send data from)
                        String transportType = n.getNodeName();

                        // store all attributes in a hashmap
                        Map<String, String> attrib = new HashMap<String, String>();
                        if (n.hasAttributes()) {
                            NamedNodeMap attr = n.getAttributes();

                            for (int jx = 0; jx < attr.getLength(); jx++) {
                                Node a = attr.item(jx);
                                attrib.put(a.getNodeName(), a.getNodeValue());
                            }
                        }

                        if (transportType.equalsIgnoreCase("server")) attrib.put("server", "true");
                        else attrib.put("server", "false");

                        // get the name used to access transport
                        String transportName = attrib.get("name");
                        if (transportName == null) throw new DataNotFoundException("transport name attribute missing in config");
                        logger.info("DataTransportFactory creating : " + transportName);

                        // Generate a name for the implementation of this transport
                        // from the name passed from the configuration.
                        String transportClass = attrib.get("class");
                        if (transportClass == null) throw new DataNotFoundException("transport class attribute missing in config");
                        String transportImplName = "org.jlab.coda.emu.support.transport.DataTransportImpl" + transportClass;
                        String channelImplName = "org.jlab.coda.emu.support.transport.DataChannelImpl" + transportClass;

                        try {
                            ecl.setClassesToLoad(new String[] {transportImplName,channelImplName});
                            // Loading the transport class will automatically
                            // load the corresponding channel class.
                            Class c = ecl.loadClass(transportImplName);
                            ecl.loadClass(channelImplName);
logger.info("&*&*&* Transport loaded class = " + c);

                            // 2 constructor args
                            Class[] parameterTypes = {String.class, Map.class};
                            Constructor co = c.getConstructor(parameterTypes);

                            // create an instance & store reference
                            Object[] args = {transportName, attrib};
                            transports.add((DataTransport) co.newInstance(args));
                            ecl.setClassesToLoad(null);

                            logger.info("DataTransportFactory created : " + transportName + " class " + transportClass);

                        } catch (Exception e) {
                            state = ERROR;
                            emu.getCauses().add(e);
                            throw new CmdExecException("cannot load transport class", e);
                        }
                    } // if node is element
                    state = cmd.success();
                } // for each child node
            }
            catch (DataNotFoundException e) {
                state = ERROR;
                emu.getCauses().add(e);
                throw new CmdExecException("transport section missing/incomplete from config", e);
            }

            // create a fifo data transport // bug bug: WHY?
            try {
                HashMap<String, String> attrs = new HashMap<String, String>();
                attrs.put("class", "Fifo");
                attrs.put("server", "false");
                transports.add(new DataTransportImplFifo("Fifo", attrs, emu));
            } catch (DataNotFoundException e) {
                state = ERROR;
                emu.getCauses().add(e);
                throw new CmdExecException(e);
            }

        }  // end of DOWNLOAD


        // Pass all commands down to all transport objects
        for (DataTransport transport : transports) {
            logger.debug("Transport : " + transport.name() + " execute " + cmd);
            transport.execute(cmd, forInput);
        }


        // RESET & END transitions
        if (emuCmd == RESET || emuCmd == END) {
            for (DataTransport t : transports) {
                t.close();
            }
            transports.clear();
            state = cmd.success();
        }

    }



}
