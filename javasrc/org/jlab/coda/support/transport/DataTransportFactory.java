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

package org.jlab.coda.support.transport;

import org.jlab.coda.emu.Emu;
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
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

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
            if (t.name().matches(name)) return t;
        }
        throw new DataNotFoundException("Data Transport not found");
    }

    /**
     * @return the name
     *
     * @see org.jlab.coda.emu.EmuModule#name()
     */
    public String name() {
        return name;
    }

    /** @return the state */
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
     * this method is only called by the EmuModuleFactory's (a singleton used by Emu)
     * execute method.
     *
     * @param cmd of type Command
     *
     * @throws org.jlab.coda.support.control.CmdExecException
     *          if command is invalid or fails
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableInstanceNeverThrown"})
    public void execute(Command cmd) throws CmdExecException {
        Logger.info("DataTransportFactory.execute : " + cmd);

        if (cmd.equals(CODATransition.DOWNLOAD)) {
            try {
                Node m = Configurer.getNode(Emu.INSTANCE.configuration(), "component/transports");
                System.out.println("component/transports node = " + m);
                if (!m.hasChildNodes()) throw new DataNotFoundException("transport section present in config but no transports");

                NodeList l = m.getChildNodes();

                transports.removeAllElements();

                System.gc();
                System.gc();

                System.runFinalization();

                for (int ix = 0; ix < l.getLength(); ix++) {
                    Node n = l.item(ix);
                    if (n.getNodeType() == Node.ELEMENT_NODE) {
                        String transportType = n.getNodeName();

                        Map<String, String> attrib = new HashMap<String, String>();
                        if (n.hasAttributes()) {
                            NamedNodeMap attr = n.getAttributes();

                            for (int jx = 0; jx < attr.getLength(); jx++) {
                                Node a = attr.item(jx);

                                attrib.put(a.getNodeName(), a.getNodeValue());
                            }
                        }
                        if (transportType.matches("server")) attrib.put("server", "true");
                        else attrib.put("server", "false");
                        // get the name used to access transport
                        String serverName = attrib.get("name");
                        if (serverName == null) throw new DataNotFoundException("transport server name attribute missing in config");
                        Logger.info("DataTransportFactory creating : " + serverName);
                        // Generate a name for the implementation of this
                        // transport
                        // from
                        // the name passed from the configuration.
                        String transportClass = attrib.get("class");
                        if (transportClass == null) throw new DataNotFoundException("transport server class attribute missing in config");
                        String implName = "org.jlab.coda.support.transport.DataTransportImpl" + transportClass;

                        try {
                            ClassLoader cl = getClass().getClassLoader();
                            Class c = cl.loadClass(implName);
                            Class[] parameterTypes = {java.lang.String.class, Map.class};
                            Constructor co = c.getConstructor(parameterTypes);
                            // create an instance
                            Object[] args = {serverName, attrib};
                            transports.add((DataTransport) co.newInstance(args));
                            Logger.info("DataTransportFactory created : " + serverName + " class " + transportClass);
                        } catch (Exception e) {
                            CODAState.ERROR.getCauses().add(e);
                            throw new CmdExecException();
                        }

                    }
                    state = cmd.success();
                }
            } catch (Exception e) {
                state = CODAState.ERROR;
                throw new CmdExecException("transport section missing from config");
            }
            try {
                HashMap<String, String> attrs = new HashMap<String, String>();
                attrs.put("class", "Fifo");
                attrs.put("server", "false");
                transports.add(new DataTransportImplFifo("Fifo", attrs));
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                throw new CmdExecException();
            }

        }

        for (DataTransport transport : transports) {
            Logger.debug("Transport : " + transport.name() + " execute " + cmd);
            transport.execute(cmd);
        }

        if (cmd.equals(RunControl.RESET) || cmd.equals(CODATransition.END)) {
            for (DataTransport t : transports) {
                t.close();
            }
            transports.clear();
            state = cmd.success();
        }

    }

}
