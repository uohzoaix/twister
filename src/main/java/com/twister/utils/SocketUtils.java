package com.twister.utils;
import java.io.IOException;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

/**
 *
 */
public class SocketUtils {

    private SocketUtils() {
    }


    /**
     * Util methods to write a defined number of random alphanumeric string (50 characters) on the socket.
     * <p/>
     *
     * @param socket          the socket to write
     * @param numberOfPackets the number of packet to write on the socket
     * @param port            the local port number.
     * @return the list of packets written on the socket
     * @throws java.io.IOException if an I/O error occurs.
     */
    public static List<String> writeRandomPackets(DatagramSocket socket, int numberOfPackets, int port) throws IOException {
        return writeRandomPackets(socket, numberOfPackets, port, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Util methods to write a defined number of random alphanumeric string (50 characters) on the socket.
     * <p/>
     * A delay may be set to write a new packet after a time period.
     *
     * @param socket          the socket to write
     * @param numberOfPackets the number of packet to write on the socket
     * @param port            the local port where to send the packet
     * @param wait            delay to write a new packet on the socket
     * @param timeUnit        the time unit
     * @return the list of packets written on the socket
     * @throws IOException if an I/O error occurs.
     */
    public static List<String> writeRandomPackets(DatagramSocket socket, int numberOfPackets, int port, long wait, TimeUnit timeUnit) throws IOException {
        int packetLength = 50;
        List<String> packets = new ArrayList<String>(numberOfPackets);
        try {
            for (int i = 0; i < numberOfPackets; i++) {
                String packet = randomAlphanumeric(packetLength);
                packets.add(packet);
                DatagramPacket datagramPacket = new DatagramPacket(packet.getBytes(), packetLength, InetAddress.getLocalHost(), port);
                socket.send(datagramPacket);
                TimeUtils.sleep(wait, timeUnit);
            }
        } finally {
            socket.close();
        }
        return packets;
    }


    /**
     * Util methods to write a defined number of random alphanumeric string (50 characters) on the socket.
     * <p/>
     *
     * @param socket          the socket to write
     * @param numberOfPackets the number of packet to write on the socket
     * @return the list of packets written on the socket
     * @throws java.io.IOException if an I/O error occurs.
     */
    public static List<String> writeRandomPackets(Socket socket, int numberOfPackets) throws IOException {
        return writeRandomPackets(socket, numberOfPackets, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Util methods to write a defined number of random alphanumeric string (50 characters) on the socket.
     * <p/>
     * A delay may be set to write a new packet after a time period.
     *
     * @param socket          the socket to write
     * @param numberOfPackets the number of packet to write on the socket
     * @param wait            delay to write a new packet on the socket
     * @param timeUnit        the time unit
     * @return the list of packets written on the socket
     * @throws IOException if an I/O error occurs.
     */
    public static List<String> writeRandomPackets(Socket socket, int numberOfPackets, long wait, TimeUnit timeUnit) throws IOException {
        int packetLength = 50;
        List<String> packets = new ArrayList<String>(numberOfPackets);
        try {
            PrintStream output = new PrintStream(socket.getOutputStream());
            for (int i = 0; i < numberOfPackets; i++) {
                String packet = randomAlphanumeric(packetLength);
                packets.add(packet);
                output.println(packet);
                output.flush();
                TimeUtils.sleep(wait, timeUnit);
            }
        } finally {
            socket.close();
        }
        return packets;
    }

 


}