package org.jlab.coda.support.data;

/**
 * Code values for different types of CODA data.
 * @author timmer
 */
public enum DataType {
    UNKNOWN(0x0),
    INT32(0x1),
    FLOAT_IEEE(0x2),
    CSTRING(0x3),
    INT16(0x4),
    UINT16(0x5),
    INT8(0x6),
    UINT8(0x7),
    DOUBLE_IEEE(0x8),
    FLOAT_VAX(0x9),
    DOUBLE_VAX(0xA),
    REPEAT(0xF),
    BANK(0x10),
    SEGMENT(0x20),
    PACKET_0(0x30),
    PACKET_3(0x33),
    PACKET_4(0x34),
    PACKET_5(0x35),
    PACKET_6(0x36),
    PACKET_7(0x37);

    int value;

    // Constructor
    DataType(int val) {
        value = val;
    }   
}
