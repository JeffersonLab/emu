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

package modules;

import org.jlab.coda.emu.EMUComponentImpl;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.support.component.CODAState;
import org.jlab.coda.support.component.CODATransition;
import org.jlab.coda.support.config.Configurer;
import org.jlab.coda.support.config.DataNotFoundException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.transport.DataChannel;
import org.jlab.coda.support.transport.DataTransport;
import org.jlab.coda.support.transport.EmuTransportFactory;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;

/**
 * <pre>
 * Class <b>Send </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class Send implements EmuModule {

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /** Field channels */
    private final HashMap<String, DataChannel> channels = new HashMap<String, DataChannel>();

    /** Field name */
    private String name = null;

    /**
     * Constructor Send creates a new Send instance.
     *
     * @param pname of type String
     */
    public Send(String pname) {
        name = pname;
    }

    /**
     * @return the name
     * @see org.jlab.coda.emu.EmuModule#name()
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.emu.EmuModule#getName()
    */
    public String name() {
        return name;
    }

    /**
     * Method channels ...
     *
     * @return HashMap<String, DataChannel>
     * @see org.jlab.coda.emu.EmuModule#channels()
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.emu.EmuModule#getChannels()
    */
    public HashMap<String, DataChannel> channels() {

        return channels;
    }

    /** @return the state */
    public State state() {
        return state;
    }

    /**
     * Method execute ...
     *
     * @param cmd of type Command
     * @see org.jlab.coda.emu.EmuModule#execute(Command)
     */
    public void execute(Command cmd) {

        if (cmd.equals(CODATransition.end)) {
            state = CODAState.ENDED;

            for (DataChannel c : channels.values()) {
                c.close();
            }
            channels.clear();
        }

        if (cmd.equals(CODATransition.prestart)) {
            try {
                Node top = Configurer.getNode(EMUComponentImpl.INSTANCE
                        .configuration(), "component/modules/" + name());

                NodeList l = top.getChildNodes();

                for (int ix = 0; ix < l.getLength(); ix++) {
                    Node n = l.item(ix);
                    if (n.getNodeType() != Node.ELEMENT_NODE) continue;

                    if (!n.hasAttributes()) {
                        continue;
                    }
                    NamedNodeMap nnm = n.getAttributes();

                    String uses = nnm.getNamedItem("uses").getNodeValue();

                    DataTransport trans = EmuTransportFactory
                            .findNamedTransport(uses);

                    DataChannel c = trans.createChannel(n.getNodeName());
                    channels.put(c.getName(), c);

                }

            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        state = cmd.success();
    }
}