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

import org.jlab.coda.emu.EMUComponentImpl;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.support.component.CODAState;
import org.jlab.coda.support.component.CODATransition;
import org.jlab.coda.support.component.RunControl;
import org.jlab.coda.support.config.Configurer;
import org.jlab.coda.support.config.DataNotFoundException;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.log.Logger;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/** @author heyes */
public class EmuTransportFactory implements EmuModule {

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
     * @throws org.jlab.coda.support.config.DataNotFoundException
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

        for (EmuModule transport : transports) {
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
        Logger.info("EmuTransportFactory.execute : " + cmd);

        if (cmd.equals(CODATransition.download)) {
            try {
                Node m = Configurer.getNode(EMUComponentImpl.INSTANCE
                        .configuration(), "component/transports");
                if (!m.hasChildNodes()) throw new DataNotFoundException("transport section present in config but no transports");

                NodeList l = m.getChildNodes();

                transports.removeAllElements();

                System.gc();
                System.gc();

                System.runFinalization();

                for (int ix = 0; ix < l.getLength(); ix++) {
                    Node n = l.item(ix);
                    if (n.getNodeType() == Node.ELEMENT_NODE) {
                        String name = n.getNodeName();
                        Logger.info("EmuTransportFactory creating : " + name);
                        Map<String, String> attrib = new HashMap<String, String>();
                        if (n.hasAttributes()) {
                            NamedNodeMap attr = n.getAttributes();

                            for (int jx = 0; jx < attr.getLength(); jx++) {
                                Node a = attr.item(jx);

                                attrib.put(a.getNodeName(), a.getNodeValue());
                            }
                        }

                        // Generate a name for the implementation of this
                        // transport
                        // from
                        // the name passed from the configuration.
                        String type = attrib.get("type");
                        String implName = "org.jlab.coda.support.transport.TransportImpl" + type;

                        try {
                            ClassLoader cl = getClass().getClassLoader();
                            Class c = cl.loadClass(implName);
                            Class[] parameterTypes = {java.lang.String.class, Map.class};
                            Constructor co = c.getConstructor(parameterTypes);
                            // create an instance
                            Object[] args = {name, attrib};
                            transports.add((DataTransport) co.newInstance(args));

                        } catch (Exception e) {
                            CODAState.ERROR.getCauses().add(e);
                            DataNotFoundException dnf = new DataNotFoundException("Unable to create data transport of type " + type);
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

        }

        for (DataTransport transport : transports)
            transport.execute(cmd);

        if (cmd.equals(RunControl.reset) || cmd.equals(CODATransition.end)) {
            transports.clear();
            state = cmd.success();
            return;
        }

    }

    /**
     * Method channels ...
     *
     * @return HashMap<String, DataChannel>
     * @see org.jlab.coda.emu.EmuModule#channels()
     */
    public HashMap<String, DataChannel> channels() {
        // TODO Auto-generated method stub
        return null;
    }
}
