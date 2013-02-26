package com.twister.simple;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.commons.io.input.TailerListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTailerListener extends TailerListenerAdapter {

	/**
	 * The log file tailer
	 */
	 
	public static Logger logger = LoggerFactory.getLogger(MyTailerListener.class);

    @Override
    public void handle(String line) {           
          logger.debug("File handle new line" +line);   
    	
       	   // 创建发送方的套接字，IP默认为本地，端口号随机			
			try {
				 // Given
		        int count = 10;
		        int port = 1234;
		     
		        // When
		        DatagramSocket sendSocket = new DatagramSocket();		       
				// 确定要发送的消息：
				String mes =line;
				String rdalp=randomAlphanumeric(count);
				// 由于数据报的数据是以字符数组传的形式存储的，所以传转数据
				byte[] buf = mes.getBytes();
 
				InetAddress ip = InetAddress.getLocalHost();
				// 创建发送类型的数据报：
				DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip,	port);
				// 通过套接字发送数据：
				sendSocket.send(sendPacket);
				// 关闭套接字
				// sendSocket.close();
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			
    }  
}