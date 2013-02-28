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

/**
 * http://shmilyaw-hotmail-com.iteye.com/blog/1556187
 * 
 * @author guoqing
 * 
 */
public class ReceiveTCPServer {
	public static Logger logger = LoggerFactory
			.getLogger(ReceiveTCPServer.class);

	public static void main(String[] args) {
		int PORT = 10236;
		ServerSocket servSock = null;
		Socket skt = null;
		try {
			servSock = new ServerSocket(PORT);	
			logger.info("tcp server is start port "+PORT);
			
			while (true) {
				// 2.调用accept方法，建立和客户端的连接
				skt = servSock.accept();
				SocketAddress clientAddress = skt.getRemoteSocketAddress();
				DataInputStream in = new DataInputStream(skt.getInputStream()); 
				PrintWriter out = new PrintWriter(skt.getOutputStream(), true); 
				byte[] buf  = new byte[1024];
				int len = 0;
				StringBuffer line=new StringBuffer();
				while(( len = in.read(buf))!=-1){
					  String text = new String(buf,0,len);
					  line.append(text);					 				      
				}
				 logger.info(line.toString());
				// send feedback
				// out.println(clientAddress.toString()+ " receive ok");	
				skt.close();
		   }		
		   
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				skt.close();
				servSock.close();				
			} catch (IOException e) {				 
				e.printStackTrace();
			}		

		}
	}
}