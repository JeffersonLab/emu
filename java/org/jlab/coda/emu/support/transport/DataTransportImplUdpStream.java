/*
 * Copyright (c) 2022, Jefferson Science Associates
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
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.logger.Logger;

import java.net.*;
import java.util.HashMap;
import java.util.Map;

/**
 * This class specifies a single UDP client which
 * connects to a hardware front-end server.
 *
 * @author timmer
 * (3/10/2022)
 */
public class DataTransportImplUdpStream extends DataTransportAdapter {

    private final Logger logger;

    final HashMap<Integer, DataChannelImplUdpStream> inputChannelTable = new HashMap<>(16);


    /**
     * Constructor called during "download".
     *
     * @param pname  name of transport
     * @param attrib transport's attribute map from config file
     * @param emu    emu object this transport belongs to
     *
     * @throws DataNotFoundException  when udl not given or cannot connect to cmsg server
     * @throws SocketException  if cannot create UDP socket
     */
    public DataTransportImplUdpStream(String pname, Map<String, String> attrib, Emu emu)
            throws DataNotFoundException, SocketException, DataTransportException {

        // pname is the "name" entry in the attrib map
        super(pname, attrib, emu);
        this.logger = emu.getLogger();
    }


    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                     boolean isInput, Emu emu, EmuModule module,
                                     int outputIndex)
                throws DataTransportException {

        DataChannelImplUdpStream newChannel = new DataChannelImplUdpStream(name, this, attributeMap,
                                                                           isInput, emu, module, outputIndex);

        if (isInput) {
            // Store this channel so it can be looked up, once the emu domain
            // client attaches to our server, and connected to that client.
            inputChannelTable.put(newChannel.getID(), newChannel);
        }

        return newChannel;
    }


    /** {@inheritDoc}. Disconnect from cMsg server. */
    public void reset() {
        inputChannelTable.clear();
    }

 }
