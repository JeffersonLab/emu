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

import org.jlab.coda.emu.support.codaComponent.CODAClass;
import org.jlab.coda.emu.support.configurer.Configurer;
import org.jlab.coda.emu.support.configurer.DataNotFoundException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.LinkedHashSet;
import java.util.LinkedList;

/**
 * Since there can be multiple EMUs defined in a config file or running in a JVM,
 * this class handles their creation and configuration.
 *
 * @author timmer
 */
public class EmuFactory {


    /** List of CODA component names. All names must be defined. */
    private LinkedList<String> names = new LinkedList<String>();

    /** List of CODA component types. Type default to EMU if not given. */
    private LinkedList<String> types = new LinkedList<String>();

    /** List of config file names if any. LinkedList allows null items. */
    private LinkedList<String> configFileNames = new LinkedList<String>();

    /**
     * List of parsed config file objects if any. Must
     * have 1-to-1 correspondence to config file names.
     */
    private LinkedList<Document> loadedConfigs = new LinkedList<Document>();



    /**
     * Method to decode the command line used to start this application.
     * @param args command line arguments
     */
    private static void decodeCommandLine(String[] args) {
        // loop over all args
        for (String arg : args) {
            if (arg.equalsIgnoreCase("-h") ||
                arg.equalsIgnoreCase("-help") ) {
                usage();
                System.exit(-1);
            }
            // ignore all other args (not -D which is special)
        }
    }


    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage:\n\n" +
                "   java Emu\n" +
                "        [-h]                 print this help\n" +
                "        [-help]              print this help\n" +
                "        [-Dname=xxx]         set name of EMU\n"+
                "        [-Dtype=xxx]         set CODA component type (eg. CDEB, ER)\n"+
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
            EmuFactory emuFactory = new EmuFactory();
            emuFactory.createEmus();
        }
        catch (EmuException e) {
            e.printStackTrace();
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
                    System.out.println("EMUFactory exit - all component names must be unique, \"" + s + "\" is not");
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
                    System.out.println("EMUFactory exit - all files names must be unique, \"" + s + "\" is not");
                    System.exit(-1);
                }
                configFileNames.add(s);
                System.out.println("- " + s);
            }
        }
        else {
            // If no component names & no config file names are given, give up.
            if (cmdLineNames == null) {
                System.out.println("EMUFactory exit - no names and no config files given");
                System.exit(-1);
            }

            // If we're here, component names exist, but no config files were given.
            // Run Control will pass config files to the emus during the "configure"
            // transition.


//            // If we're here, component names exist, but no config file was given.
//            // Since config files are not explicitly given, the INSTALL_DIR
//            // environmental variable must be defined in order to find them.
//            String coolHome = System.getenv("COOL_HOME");
//            if (coolHome == null) {
//                System.out.println("EMUFactory exit - COOL_HOME env. variable is not set");
//                System.exit(-1);
//            }
//
//System.out.println("Look for a config file for each component:");
//            String fileName;
//            for (String name : names) {
//                fileName = coolHome + File.separator + "emu/conf" +
//                                      File.separator + name + ".xml";
//                configFileNames.add(fileName);
//
//System.out.println("- " + fileName);
//            }
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
                    System.out.println("EMUFactory exit - parsing config file " + fileName +
                                       " FAILED: " + e.getMessage());
                    System.exit(-1);
                }
            }

            // Pull component(s) name(s) (and CODA component type(s) if any)
            // out of the config file(s). Each file may contain only 1 component.

            Node modulesConfig = null;
            LinkedHashSet<String> componentNames = new LinkedHashSet<String>();

            for (Document doc : loadedConfigs) {
                try {
                    // get the config info
                    modulesConfig = Configurer.getNode(doc, "component");
                } catch (DataNotFoundException e) {
                    // parsing XML error
                    System.out.println("EMUFactory exit - cannot find \"component\" element in config file: " +
                                       e.getMessage());
                    System.exit(-1);
                }

                // get attributes of the top ("component") node
                NamedNodeMap nm = modulesConfig.getAttributes();

                // get name of component from node
                Node attr = nm.getNamedItem("name");
                if (attr == null) {
                    System.out.println("No \"name\" attr in component element of config file");
                    System.exit(-1);
                }
System.out.println("Found component " + attr.getNodeValue());
                componentNames.add(attr.getNodeValue());

                // get type of component, if any
                attr = nm.getNamedItem("type");
                if (attr != null) {
                    types.add(attr.getNodeValue());
                }
                else {
                    types.add(null); //  type = EMU by default
                }
            }

            // Compare with names from command line if any. They must agree exactly.
            // In other words, if names are specified on the command line, then ALL
            // names must be specified there. If names are specified, and config
            // files are specified, ALL config files must be given on the command
            // line.
            if (names.size() > 0) {
                if (componentNames.size() != names.size() ||
                   !componentNames.containsAll(names)) {
                    System.out.println("EMUFactory exit - component name must match names in config files exactly");
                    System.exit(-1);
                }
            }
            else {
                names.addAll(componentNames);
            }
        }

        // See if any CODA component types were given on the command line.
        // If any are given, ALL must be given. Things must match what's in
        // the config files (unless nothing given there).
        String codaTypes = System.getProperty("type");
        if (codaTypes != null) {
            String[] strs;

            if (codaTypes.contains(",")) {
                strs = codaTypes.split(",");
            }
            else if (codaTypes.contains(":")) {
                strs = codaTypes.split(":");
            }
            else if (codaTypes.contains(";")) {
                strs = codaTypes.split(";");
            }
            else {
                strs = new String[1];
                strs[0] = codaTypes;
            }

            if (strs.length != names.size()) {
                System.out.println("EMUFactory exit - need 1 type for each component if specified on cmd line");
                System.exit(-1);
            }

            if (types.size() > 0) {
                String typ;
                for (int i=0; i<strs.length; i++) {
                    typ = types.get(i);
                    // If no type in config file, it's assumed to be EMU & changed later.
                    // Don't compare now.
                    if (typ != null && !strs[i].equals(typ)) {
                        System.out.println("EMUFactory exit - type on cmd line (" + strs[i] +
                                           ") must match type in config file ("+ types.get(i) + ")");
                        System.exit(-1);
                    }
                }
            }

            for (int i=0; i<strs.length; i++) {
                if (CODAClass.get(strs[i]) == null) {
                    System.out.println("EMUFactory exit - all types must be valid, \"" + strs[i] + "\" is not");
                    System.exit(-1);
                }
                types.add(i, strs[i]);
                System.out.println("- " + strs[i]);
            }
        }


        // Create EMU objects here. By this time we should have
        // all the names and possibly configurations too.
        for (int i=0; i < names.size(); i++) {
            if (configFileNames.size() > 0) {
                new Emu(names.get(i), types.get(i), configFileNames.get(i),
                        loadedConfigs.get(i), cmsgUDL, debugUI);
            }
            else {
                new Emu(names.get(i), null, null, null, cmsgUDL, debugUI);
            }
        }
    }



}
