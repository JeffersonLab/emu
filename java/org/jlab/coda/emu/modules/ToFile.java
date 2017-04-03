/*
 * Copyright (c) 2012, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu.modules;

import com.lmax.disruptor.*;
import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.codaComponent.CODAStateIF;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.CmdExecException;
import org.jlab.coda.emu.support.data.ControlType;
import org.jlab.coda.emu.support.data.RingItem;
import org.jlab.coda.emu.support.transport.DataChannel;
import org.jlab.coda.jevio.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

/**
 * This class is a simplified event recording module. It has only a single
 * thread which writes incoming data into a file. No output channels are
 * used.<p>
 *
 * Used this module to test the ordering of events coming from the EB.
 * The output channels were modified to place user events, arriving prior
 * to the prestart event, after the prestart instead. This module simply
 * wrote things directly to file so this behavior could be tested.
 *
 * @author timmer
 * (2016)
 */
public class ToFile extends ModuleAdapter {

    /** Main thread of this module. */
    RecordingThread thread;

    /** There should only be one input DataChannel. */
    private DataChannel inputChannel;

    /** END event detected by one of the recording threads. */
    private volatile boolean haveEndEvent;

    /** Maximum time in milliseconds to wait when commanded to END but no END event received. */
    private long endingTimeLimit = 30000;

    // ---------------------------------------------------

    /** If {@code true}, get debug print out. */
    private boolean debug = false;

    //-------------------------------------------
    // Disruptor (RingBuffer)  stuff
    //-------------------------------------------

    /** One RingBuffer. */
    private RingBuffer<RingItem> ringBufferIn;

    /** One sequence per recording thread. */
    public Sequence sequence;

    /** All recording threads share one barrier. */
    public SequenceBarrier barrierIn;

    //-------------------------------------------
    // Output file stuff
    //-------------------------------------------

    private String directory;
    private String fileName;
    private String dictionaryXML;
    private long   split;

    private EventWriterUnsync evioFileWriter;



    /**
     * Constructor creates a new EventRecording instance.
     *
     * @param name name of module
     * @param attributeMap map containing attributes of module
     */
    public ToFile(String name, Map<String, String> attributeMap, Emu emu)
                        throws EvioException {

        super(name, attributeMap, emu);

        int runNumber  = emu.getRunNumber();
        String runType = emu.getRunType();

        // Directory given in config file?
        try {
            directory = attributeMap.get("dir");
//logger.info("      DataChannel File: config file directory = " + directory);
        }
        catch (Exception e) {}

        // Filename given in config file?
        try {
            fileName = attributeMap.get("fileName");
//logger.info("      DataChannel File: config file name = " + fileName);
        }
        catch (Exception e) {}

        // Dictionary given in config file?
        try {
            String dictionaryFile = attributeMap.get("dictionary");
            if (dictionaryFile != null) {
                // Load the contents of the file into a String
                File dFile = new File(dictionaryFile);
                if (dFile.exists() && dFile.isFile()) {
                    FileInputStream fileInputStream = new FileInputStream(dFile);
                    int fileSize = (int)fileInputStream.getChannel().size();
                    byte[] buf = new byte[fileSize];
                    DataInputStream dataStream = new DataInputStream(fileInputStream);
                    dataStream.read(buf);
                    dictionaryXML = new String(buf, 0, fileSize, "US-ASCII");
//
// This works but does a lot of XML parsing - good way to check format
//                EvioXMLDictionary dictionary = new EvioXMLDictionary(dFile);
//                dictionaryXML = dictionary.toXML();
                }
                else {
logger.info("      DataChannel File: dictionary file cannot be read");
                }

//logger.info("      DataChannel File: config dictionary = " + dictionaryFile);
            }
        }
        catch (Exception e) {}

        // Split parameter given in config file?
        try {
            String splitStr = attributeMap.get("split");
            if (splitStr != null) {
                try {
                    split = Long.parseLong(splitStr);
                    // Ignore negative values
                    if (split < 0L) split = 0L;
                }
                catch (NumberFormatException e) {
                    split = 0L;
                }
//logger.info("      DataChannel File: split = " + split);
            }
        }
        catch (Exception e) {}

        if (fileName == null) {
            if (split > 0L) {
                // First specifier   (%d)  replaced with run #,
                // second specifier (%05d) replaced with split #
                fileName = "codaOutputFile_%d.dat%05d";
            }
            else {
                fileName = "codaOutputFile_%d.dat";
            }
        }

        try {
            // Make overwriting the file OK if there is NO splitting of the file.
            // If there is no file splitting, overwriting the file will occur when
            // the file name is static or if the run # is repeated.
            boolean overWriteOK = true;
            if (split > 0L) overWriteOK = false;

            evioFileWriter = new EventWriterUnsync(fileName, directory, runType,
                                                   runNumber, split, outputOrder,
                                                   dictionaryXML, overWriteOK);
            logger.info("      DataChannel File: file = " + evioFileWriter.getCurrentFilePath());

            // Tell emu what that output name is for stat reporting.
            // Get the name from the file writer object so that the
            // final filename is used with all string substitutions made.
            // This must be done each time the file is split.
            emu.setOutputDestination(evioFileWriter.getCurrentFilePath());
        }
        catch (EvioException e) {
            e.printStackTrace();
            throw e;
        }
    }


    /** {@inheritDoc} */
    public void addInputChannels(ArrayList<DataChannel> input_channels) {
        super.addInputChannels(input_channels);
        if (inputChannels.size() > 0) {
            inputChannel = inputChannels.get(0);
        }
    }


    /**
     * Get the one input channel in use.
     * @return  the one input channel in use.
     */
    public DataChannel getInputChannel() {return inputChannel;}

    /** {@inheritDoc} */
    public void clearChannels() {
        inputChannels.clear();
        inputChannel = null;
    }


    //---------------------------------------
    // Start and end threads
    //---------------------------------------


    /**
     * Method to start threads for stats, filling Qs, and recording events.
     * It creates these threads if they don't exist yet.
     */
    private void startThreads() {
        if (RateCalculator != null) {
            RateCalculator.interrupt();
        }
        RateCalculator = new Thread(emu.getThreadGroup(), new RateCalculatorThread(), name+":watcher");

        if (RateCalculator.getState() == Thread.State.NEW) {
            RateCalculator.start();
        }

        thread = new RecordingThread(emu.getThreadGroup(), name+":recorder");
        thread.start();
    }


    /**
     * End all record threads because an END cmd or event came through.
     * The record thread calling this method is not interrupted.
     *
     * @param thisThread the record thread calling this method; if null,
     *                   all record threads are interrupted
     * @param wait if <code>true</code> check if END event has arrived and
     *             if all the Qs are empty, if not, wait up to 1/5 second.
     */
    private void endRecordThreads(RecordingThread thisThread, boolean wait) {

        if (wait) {
            // Look to see if anything still on the input channel Q
            long startTime = System.currentTimeMillis();
// TODO: fix this
//            boolean haveUnprocessedEvents = channelQ.size() > 0;
            boolean haveUnprocessedEvents = true;

            // Wait up to endingTimeLimit millisec for events to
            // be processed & END event to arrive, then proceed
            while ((haveUnprocessedEvents || !haveEndEvent) &&
                   (System.currentTimeMillis() - startTime < endingTimeLimit)) {
                try {Thread.sleep(200);}
                catch (InterruptedException e) {}
// TODO: fix this
//                haveUnprocessedEvents = channelQ.size() > 0;
                haveUnprocessedEvents = false;
            }

            if (haveUnprocessedEvents || !haveEndEvent) {
if (debug) System.out.println("  ToFile mod: will end threads but no END event or ring not empty!");
                moduleState = CODAState.ERROR;
                emu.setErrorState("ToFile will end threads but no END event or ring not empty");
            }
        }

        // NOTE: the EMU calls this ToFile module's end() and reset()
        // methods which, in turn, call this method. In this case,
        // all recording threads will be interrupted in the following code.

        // Interrupt all recording threads except the one calling this method
        if (thread != thisThread) {
            thread.interrupt();
            try {
                thread.join(250);
                if (thread.isAlive()) {
                    thread.stop();
                }
            }
            catch (InterruptedException e) {
            }
        }
    }


    //---------------------------------------
    // Threads
    //---------------------------------------


    /**
     * This thread is started by the GO transition and runs while the state of the module is ACTIVE.
     * When the state is ACTIVE, this thread pulls one bank off the input DataChannel.
     * The bank is written to an output file.
     * The count of outgoing banks and the count of data words are incremented.
     */
    private class RecordingThread extends Thread {

        // RingBuffer Stuff
        /** Available sequence (largest index of items desired). */
        private long availableSequence;
        /** Next sequence (index of next item desired). */
        private long nextSequence;



        RecordingThread(ThreadGroup group, String name) {
            super(group, name);
        }


        @Override
        public void run() {

            int totalNumberEvents, wordCount;
            RingItem    ringItem;
            ControlType controlType;

            // Ring Buffer stuff
            availableSequence = -2L;
            nextSequence = sequence.get() + 1L;


            while (moduleState == CODAState.ACTIVE || paused) {

                try {
                    // Will BLOCK here waiting for item if none available
                    // Only wait or read-volatile-memory if necessary ...
                        // Available sequence may be larger than what we desired
//System.out.println("  ToFile mod: " + order + ", wait for seq " + nextSequence);
                    availableSequence = barrierIn.waitFor(nextSequence);
//System.out.println("  ToFile mod: " + order + ", got seq " + availableSequence);

                    while (nextSequence <= availableSequence) {
                        ringItem = ringBufferIn.get(nextSequence);
                        wordCount = ringItem.getNode().getLength() + 1;
                        controlType = ringItem.getControlType();
                        totalNumberEvents = ringItem.getEventCount();

                        // In this case, ringItems all contain EvioNode objects since
                        // they come directly from an ET input channel.
                        if (ringItem.getBuffer() != null) {
                            evioFileWriter.writeEvent(ringItem.getBuffer());
                        }
                        else {
                            if (ringItem.isFirstEvent()) {
                                evioFileWriter.setFirstEvent(ringItem.getNode());
                            }
                            else {
                                // Last boolean arg means do (not) force to hard disk.
                                evioFileWriter.writeEvent(ringItem.getNode(), false);
                            }
                        }

                        ringItem.releaseByteBuffer();

                        eventCountTotal += totalNumberEvents;
                        wordCountTotal += wordCount;

                        // If END event, interrupt other record threads then quit this one.
                        if (controlType == ControlType.END) {
System.out.println("  ToFile mod: found END event");
                            try {
                                evioFileWriter.close();
                            }
                            catch (Exception e) {
                                errorMsg.compareAndSet(null, "Cannot write to file");
                                throw e;
                            }

                            haveEndEvent = true;
                            endRecordThreads(this, false);
                            if (endCallback != null) endCallback.endWait();
                            return;
                        }

                        // Release the events back to the ring buffer for re-use
                        sequence.set(nextSequence++);
                    }
                }
                catch (InterruptedException e) {
                    if (debug) System.out.println("  ToFile mod: INTERRUPTED recording thread " + Thread.currentThread().getName());
                    return;
                }
                catch (AlertException e) {
                    if (debug) System.out.println("  ToFile mod: ring buf alert");
                    // If we haven't yet set the cause of error, do so now & inform run control
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ToFile ring buf alert");
                    return;
                }
                catch (TimeoutException e) {
                    if (debug) System.out.println("  ToFile mod: ring buf timeout");
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ToFile ring buf timeout");
                    return;
                }
                catch (Exception e) {
                    if (debug) System.out.println("  ToFile mod: MAJOR ERROR recording event: " + e.getMessage());
                    moduleState = CODAState.ERROR;
                    emu.setErrorState("ToFile MAJOR ERROR recording event: " + e.getMessage());
                    return;
                }
            }
            System.out.println("  ToFile mod: recording thread ending");
        }

    }


    //---------------------------------------
    // State machine
    //---------------------------------------


    /** {@inheritDoc} */
    public void reset() {
        Date theDate = new Date();
        CODAStateIF previousState = moduleState;
        moduleState = CODAState.CONFIGURED;

        if (RateCalculator != null) RateCalculator.interrupt();

        // Recording threads must be immediately ended
        endRecordThreads(null, false);

        RateCalculator = null;

        paused = false;

        if (previousState.equals(CODAState.ACTIVE)) {
            try {
                // Set end-of-run time in local XML config / debug GUI
                Configurer.setValue(emu.parameters(), "status/run_end_time", theDate.toString());
            }
            catch (DataNotFoundException e) {}
        }
    }


    /** {@inheritDoc} */
    public void go() {
        moduleState = CODAState.ACTIVE;
        paused = false;

        try {
            // set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }


    /** {@inheritDoc} */
    public void end() {
        moduleState = CODAState.DOWNLOADED;

        // The order in which these thread are shutdown does(should) not matter.
        // Rocs should already have been shutdown, followed by the input transports,
        // followed by this module (followed by the output transports).
        if (RateCalculator != null) RateCalculator.interrupt();

        // Recording threads should already be ended by END event
        endRecordThreads(null, true);

        RateCalculator = null;

        paused = false;

        try {
            // Set end-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_end_time", (new Date()).toString());
        }
        catch (DataNotFoundException e) {}
    }


    /** {@inheritDoc} */
    public void prestart() throws CmdExecException {

        moduleState = CODAState.PAUSED;
        paused = true;

        // Make sure we have only one input channel
        if (inputChannels.size() != 1) {
            moduleState = CODAState.ERROR;
            emu.setErrorState("ToFile module does not have exactly 1 input channel");
            return;
        }

        //------------------------------------------------
        // Disruptor (RingBuffer) stuff for input channels
        //------------------------------------------------

        // Get input channel's ring buffer
        ringBufferIn = inputChannels.get(0).getRingBufferIn();

        sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

        // This sequence is the last consumer before producer comes along
        ringBufferIn.addGatingSequences(sequence);

        // We have 1 barrier (shared by recording threads)
        barrierIn = ringBufferIn.newBarrier();

        // Reset some variables
        eventRate = wordRate = 0F;
        eventCountTotal = wordCountTotal = 0L;

        // Create & start threads
        startThreads();

        try {
            // Set start-of-run time in local XML config / debug GUI
            Configurer.setValue(emu.parameters(), "status/run_start_time", "--prestart--");
        }
        catch (DataNotFoundException e) {}
    }

}