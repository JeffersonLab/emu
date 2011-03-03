/*
 * Copyright (c) 2011, Jefferson Science Associates
 *
 * Thomas Jefferson National Accelerator Facility
 * Data Acquisition Group
 *
 * 12000, Jefferson Ave, Newport News, VA 23606
 * Phone : (757)-269-7100
 *
 */

package org.jlab.coda.emu;

import org.jlab.coda.cMsg.cMsgConstants;
import org.jlab.coda.cMsg.cMsgException;
import org.jlab.coda.cMsg.cMsgPayloadItem;
import org.jlab.coda.emu.support.codaComponent.CODAState;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.jlab.coda.emu.support.control.Command;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.messaging.CMSGPortal;
import org.jlab.coda.emu.support.ui.DebugFrame;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.io.File;
import java.net.InetAddress;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Since there can be multiple EMUs defined in a config file or running in a JVM,
 * this class handles their creation and configuration.
 *
 * @author timmer
 */
public class EmuFactory {

    /** Singleton pattern. */
    private static final EmuFactory INSTANCE = new EmuFactory();
    private static boolean debug;

    private LinkedList<String> names = new LinkedList<String>();
    private LinkedList<String> configFileNames = new LinkedList<String>();
    private LinkedList<Document> loadedConfigs = new LinkedList<Document>();

    private EmuFactory() {}

    /**
     * Get singleton instance of this class.
     * @return singleton instance of this class.
     */
    public EmuFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private static void decodeCommandLine(String[] args) {

        // loop over all args
        for (String arg : args) {
            if (arg.equalsIgnoreCase("-h")) {
                usage();
                System.exit(-1);
            } else if (arg.equalsIgnoreCase("-debug")) {
                debug = true;
            } else {
                usage();
                System.exit(-1);
            }
        }
    }


    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage:\n\n" +
                "   java Emu\n" +
                "        [-h]                 print this help\n" +
                "        [-debug]             turn on printout\n" +
                "        [-Dname=xxx]         set name of EMU\n"+
                "        [-Dconfig=xxx]       set config file name to be loaded at configuration\n"+
                "        [-Dlconfig=xxx]      set local config file name for loading static info\n"+
                "        [-Dexpid=xxx]        set experiment ID\n"+
                "        [-Dsession=xxx]      set experimental session name\n"+
                "        [-user.name=xxx]     set user's name (defaults to expid, then session)\n"+
                "        [-DcmsgUDL=xxx]      set UDL to connect to cMsg server\n"+
                "        [-DDebugUI]          display a control GUI\n");
    }

    /**
     * Method main, entry point for this program simply creates an object of class Emu.
     * The Emu gets arguments from the environment via environment variables and Java properties.
     *
     * @param args of type String[]
     */
    @SuppressWarnings({"UnusedParameters"})
    public static void main(String[] args) {
        decodeCommandLine(args);
        try {
            INSTANCE.createEmus();
        }
        catch (EmuException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    /**
     * Constructor.
     * A thread is started to monitor the state field.
     * Java system properties are read and if required a debug GUI is started.
     * <p/>
     * The emu is named from the "name" property.
     * <p/>
     * The emu loads local.xml which contains a specification of status parameters.
     * <p/>
     * The emu starts up a connecton to the cMsg server.
     * <p/>
     * By the end of the constructor several threads have been started and the static
     * method main will not exit while they are running.
     */
    public void createEmus() throws EmuException {

        // Start up a GUI to control the EMU
        boolean debugUI = false;
        if (System.getProperty("DebugUI") != null) {
            debugUI = true;
        }

        String cmsgUDL = System.getProperty("cmsgUDL");

        // Must set the names of the EMUs to create. They may
        // be given on the command line or in config files.
        String cmdLineNames = System.getProperty("name");

        // Multiple names may be separated by commas, colons, or semicolons
        // (which don't work so well on the command line),
        // but not white space, and are looked for in that order.
        if (cmdLineNames != null) {
            String[] strs;

            if (cmdLineNames.contains(",")) {
                strs = cmdLineNames.split(",");
            }
            else if (cmdLineNames.contains(":")) {
                strs = cmdLineNames.split(":");
            }
            else if (cmdLineNames.contains(";")) {
                strs = cmdLineNames.split(";");
            }
            else {
                strs = new String[1];
                strs[0] = cmdLineNames;
            }

            System.out.println("Found names:");
            for (String s : strs) {
                if (names.contains(s)) {
                    Logger.error("EMUFactory exit - all component names must be unique, \"" + s + "\" is not");
                    System.exit(-1);
                }
                names.add(s);
                System.out.println("- " + s);
            }
        }

        //-------------------------------------------------------------
        // If no names are defined, then we must get them from a config
        // file given by a command line argument. We can't wait for a
        // CONFIGURE command to arrive because we need to open a cmsg
        // connection and need a name to do this (before we can even
        // receive such a command).
        //-------------------------------------------------------------

        // First find our config file(s) (not local config) defined
        // on the command line, using a single default if none given.
        String configF = System.getProperty("config");
        if (configF != null) {
            String[] strs;

            if (configF.contains(",")) {
                strs = configF.split(",");
            }
            else if (configF.contains(":")) {
                strs = configF.split(":");
            }
            else if (configF.contains(";")) {
                strs = configF.split(";");
            }
            else {
                strs = new String[1];
                strs[0] = configF;
            }

            System.out.println("Look for config files:");
            for (String s : strs) {
                // TODO: file comparison can be more complicated
                if (configFileNames.contains(s)) {
                    Logger.error("EMUFactory exit - all files names must be unique, \"" + s + "\" is not");
                    System.exit(-1);
                }
                configFileNames.add(s);
                System.out.println("- " + s);
            }
        }
        else {
            // If no component names & no config file names are given, give up.
            if (cmdLineNames == null) {
                Logger.error("EMUFactory exit - no names or config files given");
                System.exit(-1);
            }

            // If we're here, component names exist, but no config file was given.
            // Since config files are not explicitly given, the INSTALL_DIR
            // environmental variable must be defined in order to find them.
            String installDir = System.getenv("INSTALL_DIR");
            if (installDir == null) {
                Logger.error("EMUFactory exit - INSTALL_DIR is not set");
                System.exit(-1);
            }

System.out.println("Look for a config file for each component:");
            String fileName;
            for (String name : names) {
                fileName = installDir + File.separator + "emu/conf" +
                                        File.separator + name + ".xml";
                configFileNames.add(fileName);

System.out.println("- " + fileName);
            }
        }

        // If we've identified some config files, try to load them
        if (configFileNames.size() > 0) {
            Document document;

            for (String fileName : configFileNames) {
                try {
                    // Parse XML config file and turn it into Document object.
                    document = Configurer.parseFile(fileName);
                    loadedConfigs.add(document);
                    Configurer.removeEmptyTextNodes(document.getDocumentElement());
                } catch (DataNotFoundException e) {
                    // parsing XML error
                    Logger.error("EMUFactory exit - parsing config file FAILED", e.getMessage());
                    System.exit(-1);
                }
            }

            // Pull component(s) name(s) out of the config file(s).
            // Each file may contain only 1 component.
            Node modulesConfig;
            LinkedHashSet<String> componentNames = new LinkedHashSet<String>();
            //for (int i=0; i < loadedConfigs.length; i++) {
            for (Document doc : loadedConfigs) {
                try {
                    // get the config info again since it may have changed
                    modulesConfig = Configurer.getNode(doc, "component");
                    // get attribute of the top ("component") node
                    NamedNodeMap nm = modulesConfig.getAttributes();
                    // get name of component from node
                    Node nameAttr = nm.getNamedItem("name");
System.out.println("Found component " + nameAttr.getNodeValue());
                    componentNames.add(nameAttr.getNodeValue());
                } catch (DataNotFoundException e) {
                    // parsing XML error
                    Logger.error("EMUFactory exit - parsing config file FAILED", e.getMessage());
                    System.exit(-1);
                }
            }

            // Compare with names from command line if any. They must agree exactly.
            if (names.size() > 0) {
                if (componentNames.size() != names.size() ||
                   !componentNames.containsAll(names)) {
                    Logger.error("EMUFactory exit - component name must match names in config files exactly");
                    System.exit(-1);
                }
            }
            else {
                names.addAll(componentNames);
            }
        }

        // Create EMU objects here. By this time we should have
        // all the names and possibly configurations too.
        for (int i=0; i < names.size(); i++) {
            if (configFileNames.size() > 0) {
                new Emu(names.get(i), configFileNames.get(i),
                        loadedConfigs.get(i), cmsgUDL, debugUI);
            }
            else {
                new Emu(names.get(i), null, null, cmsgUDL, debugUI);
            }
        }
    }



}
