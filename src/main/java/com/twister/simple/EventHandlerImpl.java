
package com.twister.simple;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.nio.NIOTCPHandler;

/**
 * http://shmilyaw-hotmail-com.iteye.com/blog/1556187
 * 
 * @author guoqing
 * 
 */
public class EventHandlerImpl {
	public static Logger logger = LoggerFactory
			.getLogger(ReceiveTCPServer.class);

	public static void main(String[] args) {
		int PORT = 10236;
		ServerSocket servSock = null;
		Socket skt = null;
		try {			 
			logger.info("tcp server is start port "+PORT);
			NIOTCPHandler server = new NIOTCPHandler(null);
			server.listen(PORT);
			 
		   
		} catch (Exception e) {
			e.printStackTrace();
		} finally { }
	}
}