package com.twister.simple;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;
/**
 * 普通的udp,没有用nio,模拟syslog-ng
 * @author guoqing
 *
 */
public class SenderUdpClient {
	public static Logger logger = LoggerFactory.getLogger(SenderUdpClient.class);

	public static void main(String[] args) {
		try {

			int port = 10234;			// 客户端发送数据端口		 
			InetAddress ip = InetAddress.getLocalHost();
			DatagramSocket socket = new DatagramSocket();

			// 确定要发送的消息：
			String mes = "你好！接收方！";

			// 由于数据报的数据是以字符数组传的形式存储的，所以传转数据
			byte[] buf = mes.getBytes();
			int numberOfPackets = 100;

			int packetLength = 50;
			List<String> packets = new ArrayList<String>(numberOfPackets);			 
			for (int i = 0; i < numberOfPackets; i++) {
				String packet ="test "+randomAlphanumeric(packetLength);
				logger.info(packet);
				packets.add(packet);
				// 创建发送类型的数据报：
				DatagramPacket datagramPacket = new DatagramPacket(	packet.getBytes(), packetLength, ip, port);
				// 通过套接字发送数据：
				socket.send(datagramPacket);
				// 确定接受反馈数据的缓冲存储器，即存储数据的字节数组
				//byte[] getBuf = new byte[1024];
				// 创建接受类型的数据报
				//DatagramPacket getPacket = new DatagramPacket(getBuf,getBuf.length);
				// 通过套接字接受数据
				//socket.receive(getPacket);
				// 解析反馈的消息，并打印
				//String backMes = new String(getBuf, 0, getPacket.getLength());
				//System.out.println("接受方返回的消息：" + backMes);				
				Utils.sleep(100);
			}
			// 关闭套接字
			socket.close();			 
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
