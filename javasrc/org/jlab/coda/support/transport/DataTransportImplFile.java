package org.jlab.coda.support.transport;

import org.jlab.coda.support.configurer.DataNotFoundException;
import org.jlab.coda.support.control.CmdExecException;
import org.jlab.coda.support.control.Command;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 10, 2008
 * Time: 1:18:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class DataTransportImplFile extends DataTransportCore implements DataTransport {

    public DataTransportImplFile(String pname, Map<String, String> attrib) throws DataNotFoundException {
        super(pname, attrib);
    }

    /**
     * Method execute When passed a Command object executes the command
     * in the context of the receiving module.
     *
     * @param cmd of type Command
     * @throws org.jlab.coda.support.control.CmdExecException
     *
     */
    public void execute(Command cmd) throws CmdExecException {

    }

    /**
     * Method createChannel ...
     *
     * @param name    of type String
     * @param isInput
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