/*
 * Copyright (c) 2014, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.transport;

import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgNetworkConstants;
import org.jlab.coda.cMsg.cMsgUtilities;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.EmuUtilities;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.logger.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * This class specifies a single cMsg server.
 * Connects to and disconnects from the server.
 *
 * @author timmer
 * (4/18/2014)
 */
public class DataTransportImplEmu extends DataTransportAdapter {

    private final Logger logger;

    /** If we start up a server, this is the UDP & TCP listening port. */
    private int port;

    private EmuDomainServer emuServer;

//    /** Subnet client uses to connect to server if possible. */
//    String preferredSubnet;
//
    final HashMap<Integer, DataChannelImplEmu> inputChannelTable =
            new HashMap<Integer, DataChannelImplEmu>(16);



    /**
     * Constructor called during "download".
     *
     * @param pname  name of transport
     * @param attrib transport's attribute map from config file
     * @param emu    emu object this transport belongs to
     *
     * @throws  DataNotFoundException
     *          when udl not given or cannot connect to cmsg server
     */
    public DataTransportImplEmu(String pname, Map<String, String> attrib, Emu emu)
            throws DataNotFoundException, cMsgException {

        // pname is the "name" entry in the attrib map
        super(pname, attrib, emu);
        this.logger = emu.getLogger();
//
//        // Emu domain preferred subnet in dot-decimal format
//        preferredSubnet = null;
//        String str = attrib.get("subnet");
//        if (str != null && cMsgUtilities.isDottedDecimal(str) == null) {
//            preferredSubnet = null;
//        }

        // Is this transport a server (sends data to) or client (gets data from)?
        // Yeah, it's seems backwards ...
        if (!isServer) {
            // This emu domain transport gets data from some source.
            // I know this seems backwards, but if the config says we're a client
            // (receives data), then we start up a server which accepts a connection
            // that sends data to this object and so we are a server in the traditional,
            // TCP sense.

            // TCP listening port
            String portStr = attrib.get("port");
            if (portStr != null) {
                try {
                    port = Integer.parseInt(portStr);
                    if (port < 1024 || port > 65535) {
                        throw new DataNotFoundException("Bad port value (" + port + ")");
                    }
                }
                catch (NumberFormatException e) {
                    throw new DataNotFoundException("Bad port value (" + portStr + ")");
                }
            }
            else {
                port = cMsgNetworkConstants.emuTcpPort;
System.out.println("Port should be specified in config file, using default " + port);
               // throw new DataNotFoundException("Port MUST be specified in config file");
            }

            // Start up Emu domain server (receiver of data)
System.out.println("STARTING UP EMU SERVER in " + name + " with port " + port);
            emuServer = new EmuDomainServer(port, emu.getExpid(), name, this);
            emuServer.start();
        }
        else {
            // We'll have a TCP client (but config server) connect to the downstream Emu's
            // emuServer which will make the connection in "prestart". This constructor is
            // called in "download".
        }

    }


    /** {@inheritDoc} */
    public DataChannel createChannel(String name, Map<String,String> attributeMap,
                                     boolean isInput, Emu emu, EmuModule module,
                                     int outputIndex)
                throws DataTransportException {

        DataChannelImplEmu newChannel = new DataChannelImplEmu(name, this, attributeMap,
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
        try {
            logger.debug("    DataTransportImplEmu.reset(): cmsg disconnect : " + name());
            emuServer.stopServer();
        } catch (Exception e) {}
    }

 }
