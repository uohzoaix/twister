package com.twister.simple;

import java.util.Queue;

import com.google.common.collect.Queues;
import com.twister.nio.server.NioUdpServer;

/**
 * is udp server
 * 
 * @author guoqing
 * 
 */
public class TestNioUdpServer {

	public static void main(String[] args) throws Exception {
		Queue<String> udpQueue = Queues.newConcurrentLinkedQueue();
		int port;
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		} else {
			port = 10237;
		}
		boolean debug = true;
		NioUdpServer uss = new NioUdpServer(udpQueue, port, debug);
		System.out.println("port:" + port + " isdebug " + debug);
		uss.run();
	}

}