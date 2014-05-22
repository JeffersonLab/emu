package org.jlab.coda.emu.support.data;

import com.lmax.disruptor.EventFactory;
import org.jlab.coda.jevio.EvioEvent;


/**
 * Class used by the Disruptor's RingBuffer to populate itself with PayloadBuffers.
 * @author timmer (4/7/14)
 */
public class RingItemFactory implements EventFactory<RingItem> {

    /** There are multiple classes that extend RingItem, which do we construct? */
    QueueItemType itemType;

    public RingItemFactory(QueueItemType itemType) {
        this.itemType = itemType;
    }


    public RingItem newInstance() {
        if (itemType == QueueItemType.PayloadBuffer) {
            return new PayloadBuffer();
        }
        return new PayloadBank((EvioEvent)null);
    }
}
