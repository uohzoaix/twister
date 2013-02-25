package com.twister.simple;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

public class Tail2 extends TailerListenerAdapter {
	/**
	 * The log file tailer
	 */
	 
	public static Logger logger = LoggerFactory.getLogger(Tail2.class);
	public DatagramSocket sendSocket;
	

    @Override
    public void handle(String line) {           
           logger.info("File handle new line");
           System.out.print(line);    
       	   // 创建发送方的套接字，IP默认为本地，端口号随机
			
			try {
				sendSocket = new DatagramSocket();
				// 确定要发送的消息：
				String mes =line;
				// 由于数据报的数据是以字符数组传的形式存储的，所以传转数据
				byte[] buf = mes.getBytes();

				// 确定发送方的IP地址及端口号，地址为本地机器地址
				int port = 1234;
				InetAddress ip = InetAddress.getLocalHost();
				// 创建发送类型的数据报：
				DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, ip,	port);
				// 通过套接字发送数据：
				sendSocket.send(sendPacket);
				// 关闭套接字
				sendSocket.close();
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

    @Override
    public void fileRotated() {
        logger.info("File was rotated or rename");
    }
    
    public Tail2(){
    	
    }

 
 
	/**
	 * Command-line launcher
	 */
	public static void main(String[] args) {	
		String[] infiles = new String[] { "/tmp/tailer.log" };
		if (args.length < 1) {
			System.out.println("Usage: Tail <filename>");
			args=infiles;
		}		    
		 
		File file=new File(args[0]);
		TailerListener listener = new Tail2();
		Tailer tailer =Tailer.create(file, listener, 100, false);
		tailer.run();		
		 	 
	}
	
	
}
