/*
 * Copyright (c) 2016, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.transport;


import java.util.HashMap;

/**
 * This class is an enum which lists all the possible communication types
 * used in input and output data channels.
 *
 * @author timmer
 */
public enum TransportType {

    /** ET system. */
    ET,

    /** Emu domain of the cmsg package which uses TCP sockets. */
    EMU,

    /** Cmsg domain of cmsg package, publish-subscribe using sockets. */
    CMSG,

    /** File. */
    FILE,

    /** Fifo used between modules in one emu. */
    FIFO;


    /** Map containing mapping of string of CODA class name to an enum/command. */
    private static HashMap<String, TransportType> codaClassToEnumMap = new HashMap<String, TransportType>();

    // Fill static hashmap after all enum objects created.
    static {
        for (TransportType item : TransportType.values()) {
            codaClassToEnumMap.put(item.name(), item);
        }
    }


    /**
     * Map from type of incoming message from CODA class name to a particular enum.
     * @param s CODA class name.
     * @return associated enum, else null.
     */
    public static TransportType get(String s) {
        return codaClassToEnumMap.get(s);
    }

}