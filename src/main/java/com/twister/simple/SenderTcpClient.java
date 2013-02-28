package com.twister.simple;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

public class SenderTcpClient {
	public static Logger logger = LoggerFactory
			.getLogger(SenderTcpClient.class);

	private static InetAddress host;
	private static final int PORT = 10236;

	public static void main(String[] args) {
		try {
			host = InetAddress.getLocalHost();
			logger.info("tcp client start host "+host.getHostAddress()+":"+PORT);
			run();
		} catch (UnknownHostException e) {
			logger.info("Host ID not found!");
			System.exit(1);
		}
		
	}

	private static void run() {
		Socket socket = null;
		try {
			
			int numberOfPackets = 100;
			int packetLength = 20;
			List<String> packets = new ArrayList<String>(numberOfPackets);
			for (int i = 0; i < numberOfPackets; i++) {			 
				socket = new Socket(host, PORT);	
				socket.setSoTimeout(10*1000);			 
				PrintWriter out = new PrintWriter(socket.getOutputStream(),true); // 创建数据传输流	
				
				logger.info(i+" tcp send to " + host + " port " + PORT);	
				String packet = "test " + randomAlphanumeric(packetLength)
						+ "\n";
				packets.add(packet);
				logger.info(packet);							 
				out.println(packet);
				Utils.sleep(10);
				out.flush();
				
//				// read feedback
//				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//				String response =  null;
//				while((response = in.readLine()) != null){
//			      System.out.println(response);
//			    }
//				in.close();
				out.close();
				socket.close();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				socket.close();
			} catch (IOException e) {			 
				e.printStackTrace();
			}
		}
		 
	}
}
