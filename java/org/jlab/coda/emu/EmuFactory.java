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
import java.util.LinkedList;

/**
 * This class handles the creation of one or more EMUs
 * that are to be run in a single Java JVM.
 *
 * @author timmer
 */
public class EmuFactory {


    /** List of CODA component names. All names must be defined. */
    private LinkedList<String> names = new LinkedList<String>();

    /** List of CODA component types. Type default to EMU if not given. */
    private LinkedList<String> types = new LinkedList<String>();



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
                "   java EmuFactory\n" +
                "        [-h]                 print this help\n" +
                "        [-help]              print this help\n" +
                "        [-Dname=xxx]         set name of EMU\n"+
                "        [-Dtype=xxx]         set CODA component type (eg. PEB, ER)\n"+
                "        [-Dexpid=xxx]        set experiment ID\n"+
                "        [-Dsession=xxx]      set experimental session name\n"+
                "        [-Duser.name=xxx]    set user's name (defaults to expid, then session)\n"+
                "        [-DcmsgUDL=xxx]      set UDL to connect to cMsg server\n"+
                "        [-DrcAddr=xxx]       set IP address to use with RC server\n"+
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
     * Method to construct EMUs.
     * A thread is started to monitor the state field.
     * Java system properties are read and, if required, a debug GUI is started.
     * <p/>
     * The emu is named from the "name" property.
     * <p/>
     * The emu loads local.xml which contains a specification of status parameters.
     * <p/>
     * The emu starts up a connection to the cMsg server.
     * <p/>
     * By the end of this method, several threads have been started and the static
     * method main will not exit while they are running.
     */
    public void createEmus() throws EmuException {

        // Start up a GUI to control the EMU
        boolean debugUI = false;
        if (System.getProperty("DebugUI") != null) {
            debugUI = true;
        }

        // Must set the names of the EMUs to create.
        String cmdLineNames = System.getProperty("name");

        // Multiple names may be separated by commas, colons, or semicolons
        // (which don't work so well on the command line),
        // but not white space, and are looked for in that order.
        if (cmdLineNames == null) {
            System.out.println("EMUFactory exit - must provide component name(s)");
            System.exit(-1);
        }
        else {
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

        // See if any CODA component types were given on the command line.
        // If any are given, ALL must be given.
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

System.out.println("Found types:");
            for (int i=0; i<strs.length; i++) {
                if (CODAClass.get(strs[i]) == null) {
                    System.out.println("EMUFactory exit - all types must be valid, \"" + strs[i] + "\" is not");
                    System.exit(-1);
                }
                types.add(i, strs[i]);
                System.out.println("- " + strs[i]);
            }
        }


        // Create EMU objects here. By this time we should have all the names.
        for (int i=0; i < names.size(); i++) {
            if (types.size() > 0) {
                new Emu(names.get(i), types.get(i), debugUI);
            }
            else {
                new Emu(names.get(i), null, debugUI);
            }
        }
    }



}
