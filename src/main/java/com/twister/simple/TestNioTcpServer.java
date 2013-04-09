package com.twister.simple;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Queues;
import com.twister.nio.server.NioTcpServer;

/**
 * is udp server
 * 
 * @author guoqing
 * 
 */
public class TestNioTcpServer {
	private static final Logger logger = LoggerFactory.getLogger(TestNioTcpServer.class);

	public static NioTcpServer debug(String[] args) {
		Queue<String> inqueue = Queues.newConcurrentLinkedQueue();
		int port;
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		} else {
			port = 10236;
		}
		logger.info("port:" + port);
		NioTcpServer nss = new NioTcpServer(inqueue, port, true);
		return nss;
	}

	public static void main(String[] args) throws Exception {
		debug(args).run();

	}
}
