package org.jlab.coda.emu.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;

import org.jlab.coda.et.*;
import org.jlab.coda.et.enums.Mode;
import org.jlab.coda.jevio.ByteDataTransformer;

/**
 * This class is an example of a program which injects an evio event
 * into the ER's ET system to be used by the ER as a "first event".
 *
 * @author Carl Timmer
 */
public class InjectFirstEvent {

    public InjectFirstEvent() {
    }


    private static void usage() {
        System.out.println("\nUsage: java InjectFirstEvent -f <et name>\n" +
                "                      [-h] [-v] [-r] [-m]\n" +
                "                      [-host <ET host>] [-w <big endian? 0/1>]\n" +
                "                      [-g <group>] [-d <delay>] [-p <ET port>]\n" +
                "                      [-i <interface address>] [-a <mcast addr>]\n" +
                "                      \n\n" +

                "       -f     ET system's (memory-mapped file) name\n" +
                "       -host  ET system's host if direct connection (default to local)\n" +
                "       -h     help\n" +
                "       -v     verbose output\n\n" +

                "       -g     group from which to get new events (1,2,...)\n" +
                "       -d     delay in millisec between each round of getting and putting events\n\n" +

                "       -p     ET port (TCP for direct, UDP for broad/multicast)\n" +
                "       -r     act as remote (TCP) client even if ET system is local\n" +

                "       -i     outgoing network interface address (dot-decimal)\n" +
                "       -a     multicast address(es) (dot-decimal), may use multiple times\n" +
                "       -m     multicast to find ET (use default address if -a unused)\n" +
                "       -b     broadcast to find ET\n\n" +

                "        This program works by making a direct connection to the\n" +
                "        ET system's server port and host unless at least one multicast address\n" +
                "        is specified with -a, the -m option is used, or the -b option is used\n" +
                "        in which case multi/broadcasting used to find the ET system.\n" +
                "        If multi/broadcasting fails, look locally to find the ET system.\n" +
                "        This program injects a \"first event\" into the ER's ET system\n\n");
    }


    public static void main(String[] args) {

        int group=1, delay=0, port=0;
        boolean verbose=false, remote=false;
        boolean multicast=false;
        HashSet<String> multicastAddrs = new HashSet<String>();
        String outgoingInterface=null, etName=null, host=null;


        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-f")) {
                etName = args[++i];
            }
            else if (args[i].equalsIgnoreCase("-host")) {
                host = args[++i];
            }
            else if (args[i].equalsIgnoreCase("-a")) {
                try {
                    String addr = args[++i];
                    if (InetAddress.getByName(addr).isMulticastAddress()) {
                        multicastAddrs.add(addr);
                        multicast = true;
                    }
                    else {
                        System.out.println("\nignoring improper multicast address\n");
                    }
                }
                catch (UnknownHostException e) {}
            }
            else if (args[i].equalsIgnoreCase("-i")) {
                outgoingInterface = args[++i];
            }
            else if (args[i].equalsIgnoreCase("-v")) {
                verbose = true;
            }
            else if (args[i].equalsIgnoreCase("-r")) {
                remote = true;
            }
            else if (args[i].equalsIgnoreCase("-m")) {
                multicast = true;
            }
            else if (args[i].equalsIgnoreCase("-p")) {
                try {
                    port = Integer.parseInt(args[++i]);
                    if ((port < 1024) || (port > 65535)) {
                        System.out.println("Port number must be between 1024 and 65535.");
                        usage();
                        return;
                    }
                }
                catch (NumberFormatException ex) {
                    System.out.println("Did not specify a proper port number.");
                    usage();
                    return;
                }
            }
            else if (args[i].equalsIgnoreCase("-g")) {
                try {
                    group = Integer.parseInt(args[++i]);
                    if ((group < 1) || (group > 10)) {
                        System.out.println("Group number must be between 0 and 10.");
                        usage();
                        return;
                    }
                }
                catch (NumberFormatException ex) {
                    System.out.println("Did not specify a proper group number.");
                    usage();
                    return;
                }
            }
            else if (args[i].equalsIgnoreCase("-d")) {
                try {
                    delay = Integer.parseInt(args[++i]);
                    if (delay < 1) {
                        System.out.println("delay must be > 0.");
                        usage();
                        return;
                    }
                }
                catch (NumberFormatException ex) {
                    System.out.println("Did not specify a proper delay.");
                    usage();
                    return;
                }
            }
            else {
                usage();
                return;
            }
        }

        if (etName == null) {
            usage();
            return;
        }


        try {
            EtSystemOpenConfig config = new EtSystemOpenConfig();

            // if multicasting to find ET
            if (multicast) {
                if (multicastAddrs.size() < 1) {
                    // Use default mcast address if not given on command line
                    config.addMulticastAddr(EtConstants.multicastAddr);
                }
                else {
                    // Add multicast addresses to use
                    for (String mcastAddr : multicastAddrs) {
                        config.addMulticastAddr(mcastAddr);
                    }
                }
            }

            if (multicast) {
                System.out.println("Multicasting");
                if (port == 0) {
                    port = EtConstants.udpPort;
                }
                config.setUdpPort(port);
                config.setNetworkContactMethod(EtConstants.multicast);
                config.setHost(EtConstants.hostAnywhere);
            }
            else {
                if (port == 0) {
                    port = EtConstants.serverPort;
                }
                config.setTcpPort(port);
                config.setNetworkContactMethod(EtConstants.direct);
                if (host == null) {
                    host = EtConstants.hostLocal;
                }
                config.setHost(host);
                System.out.println("Direct connection to " + host);
            }

            // Defaults are to use operating system default buffer sizes and turn off TCP_NODELAY
            config.setNetworkInterface(outgoingInterface);
            config.setWaitTime(0);
            config.setEtName(etName);
            config.setResponsePolicy(EtConstants.policyError);

            if (remote) {
                System.out.println("Set as remote");
                config.setConnectRemotely(remote);
            }

            // create ET system object with verbose debugging output
            EtSystem sys = new EtSystem(config);
            if (verbose) {
                sys.setDebug(EtConstants.debugInfo);
            }
            sys.open();


            // get GRAND_CENTRAL station object
            EtStation gc = sys.stationNameToObject("GRAND_CENTRAL");

            // attach to GRAND_CENTRAL
            EtAttachment att = sys.attach(gc);


            // create control array of correct size
            int[] con = new int[EtConstants.stationSelectInts];
            for (int i=0; i < EtConstants.stationSelectInts; i++) {
                con[i] = i+1;
            }


            /* Full evio file format of bank of unsigned int (8). */
            int data[] = {
                    0x0000000b,
                    0x00000001,
                    0x00000008,
                    0x00000001,
                    0x00000000,
                    0x00005204,  // first event, user event, last block, version 4
                    0x00000000,
                    0xc0da0100,

                    0x00000002,  // bank of unsigned ints
                    0x00010102,
                    0x00000008,
            };

            ByteBuffer dataBuf = ByteBuffer.wrap(ByteDataTransformer.toBytes(data, ByteOrder.BIG_ENDIAN));


            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Hit any key to inject first event");
            br.readLine(); // We read from user's input

            // Get 1 new event
            EtEvent[] mevs = sys.newEvents(att, Mode.SLEEP, false, 0, 1, 4*11, 1);

            // Write buffer into it
            mevs[0].getDataBuffer().put(dataBuf);
            mevs[0].setByteOrder(ByteOrder.BIG_ENDIAN);

            // Set data length to be full buf size even though we only wrote 1 int
            mevs[0].setLength(4*11);

            // Set event's control array
            mevs[0].setControl(con);

            // Put event back into ET system
            sys.putEvents(att, mevs);
        }
        catch (Exception ex) {
            System.out.println("Error using ET system as producer");
            ex.printStackTrace();
        }
    }

}
