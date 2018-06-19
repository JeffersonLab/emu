/*
 * Copyright (c) 2008, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.support.transport;


import com.lmax.disruptor.RingBuffer;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.support.codaComponent.CODAStateMachine;
import org.jlab.coda.emu.support.codaComponent.StatedObject;
import org.jlab.coda.emu.support.data.RingItem;


/**
 * This interface defines an object that can send and
 * receive data. It refers to a single, particular connection
 * (eg. a single socket or cMsg connection object).
 *
 * @author heyes
 * @author timmer
 * Created on Sep 12, 2008
 */
public interface DataChannel extends CODAStateMachine, StatedObject {

    /**
     * Get the type of transport this channel implements.
     * @return type of transport this channel implements.
     */
    public TransportType getTransportType();

    /**
     * Get the CODA ID number of the CODA component connected to this
     * data channel. In event building, this is used, for example, to check
     * the ROC id of incoming event which allows consistency checking.
     * @return the CODA ID number the CODA component connected to this
     *         data channel.
     */
    public int getID();

    /**
     * Get the module which created this channel.
     * @return module which created this channel.
     */
    public EmuModule getModule();

    /**
     * Get the record ID number of the latest event through this channel.
     * In CODA event building this is used, for example, to track the
     * record ids for input events which allows consistency
     * checks of incoming data.
     * @return the record ID number of the latest event through channel.
     */
    public int getRecordId();

    /**
     * Set the record ID number of the latest event through this channel.
     * @param recordId record ID number of the latest event through channel.
     */
    public void setRecordId(int recordId);

    /**
     * Get the name of this data channel.
     * @return the name of this data channel.
     */
    public String name();

    /**
     * Get whether this channel is an input channel (true),
     * or it is an output channel (false).
     * @return <code>true</code> if input channel, else <code>false</code>
     */
    public boolean isInput();

    /**
     * Get the DataTransport object used to create this data channel.
     * @return the DataTransport object used to create this data channel.
     */
    public DataTransport getDataTransport();

    /**
     * Get the one and only input ring buffer of this data channel.
     * @return the input ring buffer.
     */
    public RingBuffer<RingItem> getRingBufferIn();

    /**
     * Get the total number of data-holding output ring buffers.
     * @return total number of data-holding output ring buffers.
     */
    public int getOutputRingCount();

    /**
     * Get the array of output ring buffers.
     * @return array of output ring buffers.
     */
    public RingBuffer<RingItem>[] getRingBuffersOut();

    /**
     * Get the relative fill level (0-100) of the ring of this input channel.
     * @return relative fill level (0-100) of all the ring.
     */
    public int getInputLevel();

    /**
     * Get the relative fill level (0-100) of all the rings of this output channel.
     * Module calls this to report its output channels' levels.
     * (Input levels are associated directly with the module).
     * @return relative fill level (0-100) of all the rings together.
     */
    public int getOutputLevel();

    /**
     * Set the output channel's list of possible destination IP addresses of emu TCP server
     * when using emu sockets to communicate.
     * @param ipList  list of possible destination IP addresses if connecting to server.
     */
    public void setDestinationIpList(String[] ipList);

    /**
     * Set the output channel's list of destination broadcast addresses of emu TCP server
     * when using emu sockets to communicate - each corresponding to the same element of the IpList.
     * @param baList   list of destination broadcast addresses each corresponding
     *                 to the same element of the IpList.
     */
    public void setDestinationBaList(String[] baList);

    /**
     * Get the prescale value associated with this channel.
     * Defaults to 1. Used only for ET output of ER.
     * @return prescale value associated with this channel.
     */
    public int getPrescale();

    /**
     * Turn on regulation of the flow of data through an output channel.
     * Currently this is implemented only for the emu socket channel - for testing purposes.
     * It sets the rate of output buffers (typically 4MB each) and specifies
     * the number of ROC (entangled) events per buffer.
     *
     * @param eventsPerBuffer   events per output 4 MB buffer.
     * @param buffersPerSec     desired number of buffers to be sent per second.
     */
    public void regulateOutputBufferRate(int eventsPerBuffer, double buffersPerSec);

}
