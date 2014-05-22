package org.jlab.coda.emu.support.data;

import com.lmax.disruptor.EventFactory;

import java.nio.ByteBuffer;

/**
 * Class used by the Disruptor's RingBuffer to populate itself with PayloadBanks.
 * @author timmer (4/8/14)
 */
public class PayloadBankFactory implements EventFactory<PayloadBuffer> {

    /** Size in bytes of the ByteBuffer to be created
     *  when constructing a PayloadBuffer object. */
    private final int bufferSize;


    public PayloadBankFactory (int bufferSize) {
        this.bufferSize = bufferSize;
    }


    public PayloadBuffer newInstance() {
        return new PayloadBuffer(ByteBuffer.allocate(bufferSize));
    }
}
