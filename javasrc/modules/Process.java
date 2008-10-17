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

package modules;

import org.jlab.coda.emu.EMUComponentImpl;
import org.jlab.coda.emu.EmuModule;
import org.jlab.coda.emu.EmuModuleFactory;
import org.jlab.coda.support.component.CODAState;
import org.jlab.coda.support.component.CODATransition;
import org.jlab.coda.support.config.Configurer;
import org.jlab.coda.support.config.DataNotFoundException;
import org.jlab.coda.support.control.Command;
import org.jlab.coda.support.control.State;
import org.jlab.coda.support.evio.DataRecord;
import org.jlab.coda.support.evio.EVIORecordException;
import org.jlab.coda.support.log.Logger;
import org.jlab.coda.support.transport.DataChannel;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * <pre>
 * Class <b>Process </b>
 * </pre>
 *
 * @author heyes
 *         Created on Sep 17, 2008
 */
public class Process implements EmuModule, Runnable {

    /** Field name */
    private final String name;

    /** Field state */
    private State state = CODAState.UNCONFIGURED;

    /** Field input_channels */
    private final HashMap<String, DataChannel> input_channels = new HashMap<String, DataChannel>();

    /** Field output_channels */
    private final HashMap<String, DataChannel> output_channels = new HashMap<String, DataChannel>();

    /** Field actionThread */
    private Thread actionThread;

    /** Field last_error */
    private final Throwable last_error = null;

    /** Field count */
    private long count = 0;

    /** Field data_count */
    private long data_count = 0;

    /**
     * <pre>
     * Class <b>Watcher </b>
     * </pre>
     *
     * @author heyes
     *         Created on Sep 17, 2008
     */
    private class Watcher extends Thread {
        /** Method run ... */
        public void run() {
            while ((state == CODAState.ACTIVE) || (state == CODAState.PAUSED)) {
                try {
                    sleep(2000);

                    synchronized (this) {
                        while (state == CODAState.ACTIVE) {
                            sleep(200);
                            Configurer.setValue(EMUComponentImpl.INSTANCE
                                    .parameters(), "status/events", Long
                                    .toString(count));
                            Configurer.setValue(EMUComponentImpl.INSTANCE
                                    .parameters(), "status/data_count", Long
                                    .toString(data_count));
                        }
                    }

                } catch (InterruptedException e) {
                    Logger.info("Process thread " + name() + " interrupted");

                } catch (DataNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /** Field watcher */
    private Watcher watcher = null;

    /**
     * Constructor Process creates a new Process instance.
     *
     * @param pname of type String
     */
    public Process(String pname) {
        name = pname;
    }

    /** @see org.jlab.coda.emu.EmuModule#name() */
    public String name() {
        return name;
    }

    /**
     * Method channels ...
     *
     * @return HashMap<String, DataChannel>
     * @see org.jlab.coda.emu.EmuModule#channels()
     */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.emu.EmuModule#getChannels()
    */
    public HashMap<String, DataChannel> channels() {
        return input_channels;
    }

    /** Method run ... */
    @SuppressWarnings({"ConstantConditions"})
    public void run() {
        Collection<DataChannel> inValues = input_channels.values();
        Iterator<DataChannel> it = inValues.iterator();
        ArrayList<BlockingQueue<DataRecord>> full_in_queues = new ArrayList<BlockingQueue<DataRecord>>();
        ArrayList<BlockingQueue<DataRecord>> empty_in_queues = new ArrayList<BlockingQueue<DataRecord>>();

        Collection<DataChannel> outValues = output_channels.values();
        Iterator<DataChannel> it2 = outValues.iterator();
        ArrayList<BlockingQueue<DataRecord>> full_out_queues = new ArrayList<BlockingQueue<DataRecord>>();
        ArrayList<BlockingQueue<DataRecord>> empty_out_queues = new ArrayList<BlockingQueue<DataRecord>>();

        while (it.hasNext()) {
            DataChannel c = it.next();
            full_in_queues.add(c.getFull());

            empty_in_queues.add(c.getEmpty());
        }

        while (it2.hasNext()) {
            DataChannel c = it2.next();
            full_out_queues.add(c.getFull());

            empty_out_queues.add(c.getEmpty());
        }

        while ((state == CODAState.ACTIVE) || (state == CODAState.PAUSED)) {
            try {

                while (state == CODAState.ACTIVE) {
                    for (int out_ix = 0; out_ix < full_out_queues.size(); out_ix++) {

                        DataRecord[] inDr = new DataRecord[full_in_queues
                                .size()];
                        int outLen = 0;
                        for (int in_ix = 0; in_ix < full_in_queues.size(); in_ix++) {
                            DataRecord dr = full_in_queues.get(in_ix)
                                    .take();
                            dr.check();
                            inDr[in_ix] = dr;
                            outLen += (inDr[in_ix].getRecordLength() + 1);
                        }

                        DataRecord outDr = empty_out_queues.get(out_ix)
                                .take();
                        // outLen is the count of long words to go in in the
                        // outbound buffer. The buffer needs to be
                        // (outLen+1)*4 bytes
                        // long the extra 4 bytes hold the length
                        if (outDr.getBufferLength() < (outLen + 1) * 4) {
                            // new data buffer 10% bigger than needed
                            outDr = new DataRecord((outLen + 1 + outLen / 10) * 4);
                        }

                        int out_offset = 4;
                        for (int in_ix = 0; in_ix < full_in_queues.size(); in_ix++) {

                            int length = (inDr[in_ix].getRecordLength() + 1) * 4;
                            System.arraycopy(inDr[in_ix].getData(), 0, outDr.getData(), out_offset, length);
                            out_offset += length;

                            empty_in_queues.get(in_ix).put(inDr[in_ix]);
                        }
                        int dataLen = out_offset / 4 - 1;
                        outDr.setRecordLength(dataLen);
                        data_count += dataLen;
                        count++;

                        full_out_queues.get(out_ix).put(outDr);
                    }
                    // System.out.format("Process.run : loop to next
                    // queue");
                }

                Thread.sleep(2000);
            } catch (InterruptedException e) {
                if (state == CODAState.ENDED) return;
            } catch (EVIORecordException e) {
                // End the run...
                for (int ix = 0; ix < e.getCauses().size(); ix++) {
                    CODAState.ERROR.getCauses().add(e.getCauses().get(ix));
                }
                state = CODAState.ERROR;
            }
        }
    }

    /** @return the state */
    public State state() {
        return state;
    }

    /**
     * Method getError returns the error of this Process object.
     *
     * @return the error (type Throwable) of this Process object.
     */
    public Throwable getError() {
        return last_error;
    }

    /** @see org.jlab.coda.emu.EmuModule#execute(Command) */
    public void execute(Command cmd) {
        Date theDate = new Date();
        if (cmd.equals(CODATransition.end)) {
            state = CODAState.ENDED;
            actionThread.interrupt();

            if (watcher != null) watcher.interrupt();
            watcher = null;

            try {
                Configurer.setValue(EMUComponentImpl.INSTANCE.parameters(), "status/run_end_time", theDate.toString());
            } catch (DataNotFoundException e) {
                e.printStackTrace();
            }
            input_channels.clear();
        }

        if (cmd.equals(CODATransition.prestart)) {
            count = 0;
            data_count = 0;
            try {
                Node top = Configurer.getNode(EMUComponentImpl.INSTANCE
                        .configuration(), "component/modules/" + name());
                NamedNodeMap nnm;

                NodeList l = top.getChildNodes();
                for (int ix = 0; ix < l.getLength(); ix++) {
                    Node n = l.item(ix);
                    if (n.getNodeType() != Node.ELEMENT_NODE) continue;

                    EmuModule module = EmuModuleFactory.findModule(n.getNodeName());

                    if (module != null) {
                        nnm = n.getAttributes();

                        Object out = nnm.getNamedItem("output");

                        HashMap<String, DataChannel> cs = module.channels();
                        Collection<DataChannel> cv = cs.values();
                        for (DataChannel c : cv) {
                            if (out != null) {
                                output_channels.put(c.getName(), c);
                            } else {
                                input_channels.put(c.getName(), c);
                            }
                        }

                    }
                }

                actionThread = new Thread(EMUComponentImpl.THREAD_GROUP, this, name);

            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }

            try {
                Configurer.setValue(EMUComponentImpl.INSTANCE.parameters(), "status/run_start_time", "--prestart--");
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
        }

        if (cmd.equals(CODATransition.pause)) {
            state = CODAState.PAUSED;
            actionThread.interrupt();
            watcher.interrupt();
        }

        if (cmd.equals(CODATransition.go)) {
            State old_state = state;
            state = CODAState.ACTIVE;
            if (old_state != CODAState.PAUSED) {
                watcher = new Watcher();
                watcher.start();

            }

            try {
                Configurer.setValue(EMUComponentImpl.INSTANCE.parameters(), "status/run_start_time", theDate.toString());
            } catch (DataNotFoundException e) {
                CODAState.ERROR.getCauses().add(e);
                state = CODAState.ERROR;
                return;
            }
            if (old_state != CODAState.PAUSED) {

                actionThread.start();
            }
        }

        state = cmd.success();
    }
}
