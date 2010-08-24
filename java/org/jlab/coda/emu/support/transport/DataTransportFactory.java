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
 */
public class DataTransportFactory implements StatedObject {

    /** Vector containing all DataTransport objects. */
    private static final Vector<DataTransport> transports = new Vector<DataTransport>();

    /** Name of this object. */
    private final String name = "Transport factory";

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /**
     * This method finds the DataTransport object corresponding to the given name.
     *
     * @param name name of transport object
     * @return DataTransport object corresponding to given name
     * @throws DataNotFoundException when no transport object of that name can be found
     */
    public static DataTransport findNamedTransport(String name) throws DataNotFoundException {
        DataTransport t;

        if (transports.isEmpty()) throw new DataNotFoundException("Data Transport not found, transports vector is empty");

        for (DataTransport transport : transports) {
            t = transport;
            if (t.name().equals(name)) return t;
        }
        throw new DataNotFoundException("Data Transport not found");
    }

    /** {@inheritDoc} */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    public State state() {
        if (state == CODAState.ERROR) return state;

        if (transports.size() == 0) return state;

        State s;
        state = transports.get(0).state();

        for (DataTransport transport : transports) {
            //System.out.println("check state : " + transport.name() + " " + transport.state());
            s = transport.state();

            if (s == CODAState.ERROR) {
                state = CODAState.ERROR;
            }
        }

        return state;
    }

    
    /**
     * This method is only called by the EmuModuleFactory's (a singleton used by Emu)
     * execute method.
     *
     * @param cmd of type Command
     *
     * @throws org.jlab.coda.emu.support.control.CmdExecException
     *          if command is invalid or fails
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableInstanceNeverThrown"})
    public void execute(Command cmd) throws CmdExecException {
        Logger.info("  DataTransportFactory.execute : " + cmd);

        // DOWNLOAD loads transport implementation classes, creates objects from them and
        // stores them along with a transport fifo implementation object.
        if (cmd.equals(CODATransition.DOWNLOAD)) {

            try {
                Node m = Configurer.getNode(Emu.INSTANCE.configuration(), "component/transports");
//System.out.println("component/transports node = " + m);
                if (!m.hasChildNodes()) throw new DataNotFoundException("transport section present in config but no transports");

                NodeList l = m.getChildNodes();

                // If doing a download from the downloaded state,
                // close the existing transport objects first
                // (this step is normally done from RESET).
                for (DataTransport t : transports) {
                    Logger.debug("  DataTransportFactory.execute DOWNLOAD : " + t.name() + " close");
                    t.close();
                }

                // remove all current data transport objects
                transports.removeAllElements();

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
                        Logger.info("  DataTransportFactory.execute DOWN : creating " + transportName);

                        // Generate a name for the implementation of this transport
                        // from the name passed from the configuration.
                        String transportClass = attrib.get("class");
                        if (transportClass == null) throw new DataNotFoundException("transport class attribute missing in config");
                        String implName = "org.jlab.coda.emu.support.transport.DataTransportImpl" + transportClass;

                        try {
                            Class c = DataTransportFactory.class.getClassLoader().loadClass(implName);
Logger.info("  DataTransportFactory.execute DOWN : loaded class = " + c);

                            // 2 constructor args
                            Class[] parameterTypes = {String.class, Map.class};
                            Constructor co = c.getConstructor(parameterTypes);

                            // create an instance & store reference
                            Object[] args = {transportName, attrib};
                            transports.add((DataTransport) co.newInstance(args));

                            Logger.info("  DataTransportFactory.execute DOWN : created " + transportName + " of protocol " + transportClass);

                        } catch (Exception e) {
                            state = CODAState.ERROR;
                            CODAState.ERROR.getCauses().add(e);
                            throw new CmdExecException("cannot load transport class", e);
                        }
                    } // if node is element
                    state = cmd.success();
                } // for each child node
            }
            catch (DataNotFoundException e) {
                state = CODAState.ERROR;
                CODAState.ERROR.getCauses().add(e);
                throw new CmdExecException("transport section missing/incomplete from config", e);
            }

            // create a fifo data transport to allow communication between modules seemlessly
            try {
                HashMap<String, String> attrs = new HashMap<String, String>();
                attrs.put("class", "Fifo");
                attrs.put("server", "false");
                transports.add(new DataTransportImplFifo("Fifo", attrs));
            } catch (DataNotFoundException e) {
                state = CODAState.ERROR;
                CODAState.ERROR.getCauses().add(e);
                throw new CmdExecException(e);
            }

        }  // end of DOWNLOAD

        // Pass all commands down to all transport objects: DOWNLOAD, PRESTART, GO, PAUSE, RESET, END
        for (DataTransport transport : transports) {
            Logger.debug("  DataTransportFactory.execute : pass " + cmd + " down to " + transport.name());
            transport.execute(cmd);
        }

        // close channels for END transition
        if (cmd.equals(CODATransition.END)) {
            for (DataTransport t : transports) {
                Logger.debug("  DataTransportFactory.execute END : " + t.name() + " close");
                // close only the channels as transport object survives
                t.closeChannels();
            }
            state = cmd.success();
        }

        // close transport objects for RESET transition
        if (cmd.equals(CODATransition.RESET)) {
            for (DataTransport t : transports) {
                Logger.debug("  DataTransportFactory.execute RESET : " + t.name() + " close");
                // close the whole transport object as it will disappear
                t.close();
            }
            transports.clear();
            state = cmd.success();
        }

    }


    /**
     * This method is only called by the EmuModuleFactory's (a singleton used by Emu)
     * execute method. This is not used since I (timmer) could never get it to work
     * quite right. It attempts to reload classes dynamically but somehow fails in
     * loading internal classes correctly.
     *
     * @param cmd of type Command
     *
     * @throws org.jlab.coda.emu.support.control.CmdExecException
     *          if command is invalid or fails
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableInstanceNeverThrown"})
    public void execute_EmuLoader(Command cmd) throws CmdExecException {
        Logger.info("DataTransportFactory.execute : " + cmd);

        // DOWNLOAD loads transport implementation classes, creates objects from them and
        // stores them along with a transport fifo implementation object.
        if (cmd.equals(CODATransition.DOWNLOAD)) {

            try {
                Node m = Configurer.getNode(Emu.INSTANCE.configuration(), "component/transports");
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
                        Logger.info("DataTransportFactory creating : " + transportName);

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
Logger.info("&*&*&* Transport loaded class = " + c);

                            // 2 constructor args
                            Class[] parameterTypes = {String.class, Map.class};
                            Constructor co = c.getConstructor(parameterTypes);

                            // create an instance & store reference
                            Object[] args = {transportName, attrib};
                            transports.add((DataTransport) co.newInstance(args));
                            ecl.setClassesToLoad(null);

                            Logger.info("DataTransportFactory created : " + transportName + " class " + transportClass);

                        } catch (Exception e) {
                            state = CODAState.ERROR;
                            CODAState.ERROR.getCauses().add(e);
                            throw new CmdExecException("cannot load transport class", e);
                        }
                    } // if node is element
                    state = cmd.success();
                } // for each child node
            }
            catch (DataNotFoundException e) {
                state = CODAState.ERROR;
                CODAState.ERROR.getCauses().add(e);
                throw new CmdExecException("transport section missing/incomplete from config", e);
            }

            // create a fifo data transport // bug bug: WHY?
            try {
                HashMap<String, String> attrs = new HashMap<String, String>();
                attrs.put("class", "Fifo");
                attrs.put("server", "false");
                transports.add(new DataTransportImplFifo("Fifo", attrs));
            } catch (DataNotFoundException e) {
                state = CODAState.ERROR;
                CODAState.ERROR.getCauses().add(e);
                throw new CmdExecException(e);
            }

        }  // end of DOWNLOAD


        // Pass all commands down to all transport objects
        for (DataTransport transport : transports) {
            Logger.debug("Transport : " + transport.name() + " execute " + cmd);
            transport.execute(cmd);
        }


        // RESET & END transitions
        if (cmd.equals(CODATransition.RESET) || cmd.equals(CODATransition.END)) {
            for (DataTransport t : transports) {
                t.close();
            }
            transports.clear();
            state = cmd.success();
        }

    }



}
