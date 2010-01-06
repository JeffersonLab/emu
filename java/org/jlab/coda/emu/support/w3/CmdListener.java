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

package org.jlab.coda.emu.support.w3;

import org.jlab.coda.emu.Emu;
import org.jlab.coda.support.keyboardControl.ApplicationConsole;
import org.jlab.coda.support.logger.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/** @author heyes */
public class CmdListener implements Runnable {

    /** Field server */
    private ServerSocket server;

    /** Constructor CmdListener creates a new CmdListener instance. */
    public CmdListener() {
        Thread serverLoop = new Thread(Emu.THREAD_GROUP, this, "Telnet port 8084 monitor");
        serverLoop.start();
    }

    /** Method close ... */
    public void close() {
        try {
            server.close();
        } catch (IOException e) {

            // ignore
        }
    }

    /** Method run ... */
    @SuppressWarnings({"InfiniteLoopStatement"})
    public void run() {
        try {
            server = new ServerSocket(8084);

            do {
                Socket incoming = server.accept();

                Logger.info("new remote command line connection");

                BufferedReader in = new BufferedReader(new InputStreamReader(incoming.getInputStream()));

                PrintWriter out = new PrintWriter(incoming.getOutputStream(), true /* autoFlush */);

                ApplicationConsole.monitor(in, out);

            } while (true);
        } catch (IOException e) {
            // ignore
        }

    }
}
