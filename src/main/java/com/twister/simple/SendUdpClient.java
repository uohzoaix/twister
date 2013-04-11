package com.twister.simple;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * 普通的udp,没有用nio,模拟syslog-ng
 * 
 * @author guoqing
 * 
 */
public class SendUdpClient {
	public static Logger logger = LoggerFactory.getLogger(SendUdpClient.class);
	public static String logfile = "src/main/resources/accessLog.txt";	
	private static Charset charSet = Charset.forName("UTF-8");
	public static String host = "127.0.0.1";
	public static int port = 10237; // 客户端发送数据端口

	public static void main(String[] args) {
		logger.info("Usage : " + SendUdpClient.class.getName() + " <host> <port> <accessFile>");
		if (args.length > 1) {
			host = args[0];
		}
		if (args.length > 2) {
			port = Integer.valueOf(args[1]);
		}
		if (args.length > 3) {
			logfile = args[2];
		}
		try {
			File tmpfile = new File(logfile);
			Preconditions.checkArgument(tmpfile.isFile(), "TextFileSpout expects a file but '" + tmpfile
					+ "' is not exists.");			

			InetAddress ip = InetAddress.getByName(host);
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
						i++;
						StringBuffer packet = new StringBuffer(new String(line.getBytes("8859_1"), charSet)); // 编码转换
						if (packet.length() > 0 && packet.charAt(packet.length() - 1) != '\n') {
							packet.append("\n");
						}
//						if (i > 20) {
//							line = null;
//							break;
//						}
						System.out.print(i + " " + packet.toString());
						// 创建发送类型的数据报：
						DatagramPacket datagramPacket = new DatagramPacket(packet.toString().getBytes(charSet), packet.length(), ip, port);
						// 通过套接字发送数据：
						socket.send(datagramPacket);
						
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
