package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.Command;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Nov 10, 2008
 * Time: 1:18:47 PM
 * Implement a DataTransport that uses DataCHannel based on EVIO format files.
 */
public class DataTransportImplFile extends DataTransportCore implements DataTransport {

    public DataTransportImplFile(String pname, Map<String, String> attrib, Emu emu) throws DataNotFoundException {
        super(pname, attrib, emu);
    }

    /** {@inheritDoc} */
    public void execute(Command cmd, boolean forInput) {
        // Dummy - nothing to see here folks, move along.
    }

    // TODO: should this be here???
    /**
     * Close this DataTransport object.
     * This method does nothing as closing files is handled by the channel when an
     * "END" command comes along.
     */
    public void close() {
//        logger.debug("    DataTransport File: close() called, do noting");
    }


    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                     boolean isInput, Emu emu)
            throws DataTransportException {

        DataChannel c = new DataChannelImplFile(name() + ":" + name, this, attributeMap, isInput, emu);
        if (isInput) {
            inChannels().put(c.getName(), c);
        }
        else {
            outChannels().put(c.getName(), c);
        }
        allChannels().put(c.getName(), c);
        return c;
    }

}