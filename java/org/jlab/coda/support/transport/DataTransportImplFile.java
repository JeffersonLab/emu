package org.jlab.coda.support.transport;

import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.Command;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 10, 2008
 * Time: 1:18:47 PM
 * Implement a DataTransport that uses DataCHannel based on EVIO format files.
 */
public class DataTransportImplFile extends DataTransportCore implements DataTransport {

    public DataTransportImplFile(String pname, Map<String, String> attrib) throws DataNotFoundException {
        super(pname, attrib);
    }

    /**
     * Method execute When passed a Command object executes the command
     * in the context of the receiving module.
     *
     * @param cmd something to chew on.
     */
    public void execute(Command cmd) {
        // Dummy - nothing to see here folks, move along.
    }

    /**
     * Method createChannel ...
     *
     * @param name    the name of this channel
     * @param isInput true if not false
     *
     * @return DataChannel
     */
    public DataChannel createChannel(String name, boolean isInput) throws DataTransportException {
        System.out.println("create channel " + name);
        DataChannel c = new DataChannelImplFile(name() + ":" + name, this, isInput);
        channels().put(c.getName(), c);
        System.out.println("put channel " + c.getName());
        System.out.println("channels " + channels() + " " + this);
        return c;
    }

}