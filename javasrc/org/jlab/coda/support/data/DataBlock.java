package org.jlab.coda.support.data;

import java.nio.IntBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: heyes
 * Date: Feb 17, 2009
 * Time: 7:49:47 AM
 * To change this template use File | Settings | File Templates.
 */
public interface DataBlock {
    int PAYLOAD_START = 2;

    int getLength();

    IntBuffer getBuffer();

    int getData(int index);

    int getDataType();

    void setDataType(int dataType);

    void dumpHeader();

    IntBuffer getPayload();

    void decode() throws BankFormatException;

}
