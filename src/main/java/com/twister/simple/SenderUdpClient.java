package com.twister.simple;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 普通的udp,没有用nio,模拟syslog-ng
 * 
 * @author guoqing
 * 
 */
public class SenderUdpClient {
	public static Logger logger = LoggerFactory.getLogger(SenderUdpClient.class);
	public static String logfile = "src/main/resources/accessLog.txt";
	private static Charset charSet = Charset.forName("UTF-8");
	
	public static void main(String[] args) {
		try {
			
			int port = 10237; // 客户端发送数据端口
			InetAddress ip = InetAddress.getLocalHost();
			DatagramSocket socket = new DatagramSocket();
			
			logger.info("nio udp cli " + ip.toString() + "" + port);
			int numberOfPackets = 100;
			int packetLength = 45;
			List<String> packets = new ArrayList<String>(numberOfPackets);
			String packet1 = "test " + randomAlphanumeric(packetLength);
			RandomAccessFile file = new RandomAccessFile(logfile, "r");
			long filePointer = 0;
			boolean issend = true;
			while (issend) {
				long fileLength = logfile.length();
				if (fileLength < filePointer) {
					file = new RandomAccessFile(logfile, "r");
					filePointer = 0;
				}
				if (fileLength > filePointer) {
					file.seek(filePointer);
					String line = null;
					int i = 0;
					while ((line = file.readLine()) != null) {
						// 8859_1
						String packet = new String(line.getBytes("8859_1"), charSet); // 编码转换
						if (i > 20) {
							line = null;
							break;
						}
						logger.info(i + " port " + port + " " + packet + " len " + packet.length());
						// 创建发送类型的数据报：
						DatagramPacket datagramPacket = new DatagramPacket(packet.getBytes(), packet.length(), ip, port);
						// 通过套接字发送数据：
						socket.send(datagramPacket);
						i++;
					}
					filePointer = file.getFilePointer();
					if (line == null) {
						issend = false;
						break;
					}
				}
			}
			file.close();
			
			// 关闭套接字
			socket.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
