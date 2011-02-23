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

/**
 * Control the program from the keyboard.
 */
package org.jlab.coda.emu.support.keyboardControl;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.emu.support.logger.Logger;
import org.jlab.coda.emu.support.logger.PrintAppender;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Vector;

/** @author heyes */
public class ApplicationConsole implements KbdHandler, Runnable {

    /** Field handlers */
    private static final Vector<KbdHandler> handlers = new Vector<KbdHandler>();
    /** Field consoles */
    private static final Vector<ApplicationConsole> consoles = new Vector<ApplicationConsole>();

    static {
        new ApplicationConsole();
    }

    /** Field in */
    private BufferedReader in;
    /** Field out */
    private PrintWriter out;
    /** Field appender */
    private PrintAppender appender;
    /** Field monitorThread */
    private Thread monitorThread = null;

    /** Constructor ApplicationConsole creates a new ApplicationConsole instance. */
    private ApplicationConsole() {
        add(this);
    }

    /**
     * Constructor ApplicationConsole creates a new ApplicationConsole instance.
     *
     * @param inp  of type BufferedReader
     * @param outp of type PrintWriter
     */
    private ApplicationConsole(BufferedReader inp, PrintWriter outp) {
        in = inp;
        out = outp;
        consoles.add(this);
        appender = new PrintAppender(out);
        Logger.addAppender(appender);
        monitorThread = new Thread(Emu.THREAD_GROUP, this, "Console Monitor");
    }

    /**
     * Method monitor ...
     *
     * @param inp  of type BufferedReader
     * @param outp of type PrintWriter
     * @return ApplicationConsole
     */
    public static ApplicationConsole monitor(BufferedReader inp, PrintWriter outp) {
        ApplicationConsole m = new ApplicationConsole(inp, outp);

        m.monitorThread.start();
        return m;
    }

    /** Method close ... */
    void close() {
        try {
            Logger.removeAppender(appender);
            consoles.remove(this);
            in.close();
            monitorThread.interrupt();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Method closeAll ... */
    public static void closeAll() {
        Iterator<ApplicationConsole> iter = consoles.iterator();

        for (; iter.hasNext();) {
            ApplicationConsole c = iter.next();
            try {
                Logger.removeAppender(c.appender);
                c.getIn().close();
                c.monitorThread.interrupt();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        consoles.removeAllElements();
    }

    /**
     * Method monitorSysin ...
     *
     * @return ApplicationConsole
     */
    public static ApplicationConsole monitorSysin() {
        InputStreamReader unbuffered = new InputStreamReader(System.in);
        BufferedReader keyboard = new BufferedReader(unbuffered);
        PrintWriter console = new PrintWriter(System.out, true);

        return monitor(keyboard, console);
    }

    /** @param handler  */
    @SuppressWarnings({"JavaDoc"})
    public static void add(Object handler) {
        handlers.add((KbdHandler) handler);
    }

    /**
     * Method get ...
     *
     * @param key of type Character
     * @return Object
     */
    public static Object get(Character key) {
        return handlers.get(key);
    }

    /**
     * Method list ...
     *
     * @param out of type PrintWriter
     */
    private static void list(PrintWriter out) {
        out.println("Keyboard menu");

        for (KbdHandler h : handlers) {
            h.printHelp(out);
        }
    }

    /** Method run ... */
    /*
    * (non-Javadoc)
    *
    * @see java.lang.Thread#run()
    */
    public void run() {

        do try {
            out.print(Emu.INSTANCE.name() + "> ");
            out.flush();

            while (!in.ready()) {
                Thread.sleep(200);
            }
            String line = in.readLine();

            if ((line != null) && (line.length() != 0)) {
                // ... process "inputLine" ...

                if (line.trim().equals("BYE")) {
                    Logger.info("remote command line closing");
                    close();
                    return;
                }

                boolean handled = false;
                // ... iterate over all handlers.
                Iterator<KbdHandler> iter = handlers.iterator();

                for (; iter.hasNext();) {
                    KbdHandler h = iter.next();
                    handled |= h.keyHandler(line.trim(), out, null);
                }

                if (!handled) out.println("Unrecognized kbd input - " + line);

            }
        } catch (IOException error) {
            Logger.error("Error reading keyboard: ", error);
            return;
        } catch (InterruptedException e) {

            Logger.info("application console thread exiting.", e.getMessage());
            return;
        } while (true);
    }

    /** @see KbdHandler#keyHandler(String, PrintWriter, Object) */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.emu.support.kbd.KbdHandler#keyHandler(char,
    *      java.lang.Object)
    */
    public boolean keyHandler(String s, PrintWriter out, Object argument) {
        if (s.startsWith("h")) {
            list(out);
            return true;
        }
        return false;
    }

    /** @see KbdHandler#printHelp(PrintWriter) */
    /*
    * (non-Javadoc)
    *
    * @see org.jlab.coda.emu.support.kbd.KbdHandler#printHelp(char)
    */
    public void printHelp(PrintWriter out) {
        out.println("KeyboardControl");
        out.println("\th - help");
        out.println("\tquit - quit ");
    }

    /** @return the in */
    BufferedReader getIn() {
        return in;
    }

}
