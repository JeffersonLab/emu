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

/** @author heyes */
public class DataTransportFactory {

    /** Field transports */
    private static final Vector<DataTransport> transports = new Vector<DataTransport>();

    /** Field name */
    private final String name = "Transport factory";

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /**
     * Method findNamedTransport ...
     *
     * @param name of type String
     * @return DataTransport
     * @throws org.jlab.coda.support.configurer.DataNotFoundException
     *          when
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
            s = transport.state();

            if (s != state) {

                //noinspection ThrowableInstanceNeverThrown
                CODAState.ERROR.getCauses().add(new Exception(new StringBuilder().append("TransportFactory: transport ").append(transport.name()).append(" is in state ").append(s).append(", expected ").append(state).toString()));
                state = CODAState.ERROR;
            }

        }

        return state;
    }

    /**
     * Method execute ...
     *
     * @param cmd of type Command
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableInstanceNeverThrown"})
    public void execute(Command cmd) throws CmdExecException {
        Logger.info("DataTransportFactory.execute : " + cmd);

        if (cmd.equals(CODATransition.download)) {
            try {
                Node m = Configurer.getNode(Emu.INSTANCE.configuration(), "codaComponent/transports");
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
                            System.err.println("Exception " + e.getMessage());
                            e.printStackTrace();
                            CODAState.ERROR.getCauses().add(e);
                            DataNotFoundException dnf = new DataNotFoundException("Unable to create data transport of transportClass " + transportClass);
                            CODAState.ERROR.getCauses().add(dnf);
                            Logger.error(dnf);
                            state = CODAState.ERROR;
                            return;
                        }

                    }
                    state = cmd.success();
                }
            } catch (Exception e) {
                CODAState.ERROR.getCauses().add(new DataNotFoundException("transport section missing from config"));
                state = CODAState.ERROR;
                return;
            }
            try {
                transports.add(new DataTransportImplFifo("Fifo", new HashMap<String, String>()));
            } catch (DataNotFoundException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

        }

        for (DataTransport transport : transports) {
            Logger.debug("Transport : " + transport.name() + " execute " + cmd);
            transport.execute(cmd);
        }

        if (cmd.equals(RunControl.reset) || cmd.equals(CODATransition.end)) {
            for (DataTransport t : transports) {
                t.close();
            }
            transports.clear();
            state = cmd.success();
            return;
        }

    }

    /**
     * Method setInput_channels sets the input_channels of this EmuModule object.
     *
     * @param input_channels the input_channels of this EmuModule object.
     */
    public void setInput_channels(HashMap<String, DataChannel> input_channels) {
        // dummy, does nothing
    }

    /**
     * Method setOutput_channels sets the output_channels of this EmuModule object.
     *
     * @param output_channels the output_channels of this EmuModule object.
     */
    public void setOutput_channels(HashMap<String, DataChannel> output_channels) {
        // dummy, does nothing
    }

}
