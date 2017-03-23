package org.jlab.coda.emu.support.transport;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;

import java.util.Map;

/**
 * Implement a DataTransport that uses DataChannels based on EVIO format files.
 * This class does nothing besides provide a way to create file channels which
 * do all the work.
 *
 * @author heyes
 * Date: Nov 10, 2008
 */
public class DataTransportImplFile extends DataTransportAdapter {

    /**
     * Constructor.
     *
     * @param name    name of this transport object
     * @param attrib  Map of attributes of this transport
     * @param emu     emu that created this transport
     *
     * @throws DataNotFoundException when "server" and "class" attributes
     *                               are missing from attrib map.
     */
    public DataTransportImplFile(String name, Map<String, String> attrib, Emu emu) throws DataNotFoundException {
        super(name, attrib, emu);
    }

    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                     boolean isInput, Emu emu, EmuModule module,
                                     int outputIndex)
            throws DataTransportException {

        return new DataChannelImplFile(name, this, attributeMap,
                                       isInput, emu, module, outputIndex);
    }
}