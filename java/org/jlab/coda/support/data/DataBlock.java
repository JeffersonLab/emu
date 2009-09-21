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

    /** Default value of index into the buffer where the data starts.
     * This works for a bank but not segment which overrides this value. */
    int PAYLOAD_START = 2;

    /**
     * Get the length of the block including the header
     * (but NOT including the first int in the case of a bank
     * which is the length being returned here).
     * @return length of the block
     */
    int getLength();

    /**
     * Get the buffer as a buffer of 32 bit ints.
     * The buffer includes all initial lengths and headers
     * as well as the actual data which follows that.
     *
     * @return the buffer as a buffer of 32 bit ints
     */
    IntBuffer getBuffer();

    /**
     * Get the 32 bit integer data at the given index.
     * @param index index of desired data
     * @return the 32 bits of data at the given index as an int
     */
    int getData(int index);

    /**
     * Get the data type (8 bits) from the header.
     * @return the data type (8 bits) from the header as an int
     */
    int getDataType();

    /**
     * Set the data type (8 bits) in the header.
     * @param dataType data type (lowest 8 bits)
     */
    void setDataType(int dataType);

    /**
     * Print the first 32 bit int which is or contains the header.
     */
    void dumpHeader();

    /**
     * Get the buffer NOT including the length and header.
     * @return the buffer NOT including the length and header
     */
    IntBuffer getPayload();

    /**
     * Takes this buffer and if it's a data bank or data segment,
     * breaks it down into a list of sub banks/segments (each of
     * which may contain sub banks/segments etc).
     *
     * @throws BankFormatException
     */
    void decode() throws BankFormatException;

}
