package org.jlab.coda.emu.support.data;

import org.jlab.coda.jevio.ByteParser;
import org.jlab.coda.jevio.EvioEvent;
import org.jlab.coda.jevio.EvioException;

import java.nio.ByteOrder;
import java.nio.ByteBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: timmer
 * Date: Feb 2, 2010
 * Time: 3:17:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class EvioByteParser extends ByteParser {

    /**
     * This is simply a wrapper on a method in the jevio class,
     * org.jlab.coda.jevio.ByteParser.parseEvent(byte[],ByteOrder,int),
     * that is not public due to the degree of expertise needed to use it properly.<br>
     * {@inheritdoc}
     *
     * @param bytes     {@inheritdoc}
     * @param byteOrder {@inheritdoc}
     * @param depth     {@inheritdoc}
     * @return          {@inheritdoc}
     * @throws EvioException  {@inheritdoc}
     */
    public EvioEvent parseEvent(byte[] bytes, ByteOrder byteOrder, int depth) throws EvioException {
        return super.parseEvent(bytes, byteOrder, depth);
    }


    /**
     * This is simply a wrapper on a method in the jevio class,
     * org.jlab.coda.jevio.ByteParser.parseEvent(ByteBuffer, int),
     * that is not public due to the degree of expertise needed to use it properly.<br>
     * class that are not public due to the degree of expertise needed to use them properly.<br>
     * {@inheritdoc}
     *
     * @param buf   {@inheritdoc}
     * @param depth {@inheritdoc}
     * @return      {@inheritdoc}
     * @throws EvioException {@inheritdoc}
     */
    public EvioEvent parseEvent(ByteBuffer buf, int depth) throws EvioException {
        return super.parseEvent(buf, depth);
    }


}
