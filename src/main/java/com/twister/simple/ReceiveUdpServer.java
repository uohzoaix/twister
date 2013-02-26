package com.twister.simple;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ReceiveUdpServer {

	public static void main(String[] args) {
		DatagramSocket receiveSocket = null;
		DatagramSocket responseSocket = null;
		try {
			int port = 10239; // receive		 
			int BUFFSIZE = 1024;
			// 确定接受方的IP和端口号，IP地址为本地机器地址
			InetAddress ip = InetAddress.getLocalHost();
			// 确定数据报接受的数据的数组大小
			byte[] buf = new byte[BUFFSIZE];

			SimpleDateFormat fmtdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			System.out.println(ip.toString() + " " + port);
			// 创建接收方的套接字,并制定端口号和IP地址
			receiveSocket = new DatagramSocket(port, ip); // 用于接收数据
			while (true) {
				// 创建接受类型的数据报，数据将存储在buf中
				DatagramPacket getPacket = new DatagramPacket(buf, buf.length);
				// 接收数据
				receiveSocket.receive(getPacket);
				// got data
				// 解析发送方传递的消息，并打印
				String receive = new String(buf, 0, getPacket.getLength());
				System.out.println("Receive data: IP = "
						+ getPacket.getAddress().getHostAddress() + ", Port = "
						+ getPacket.getPort() + ", Data = " + receive
						+ ", Time =" + fmtdate.format(new Date()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (receiveSocket != null)
				receiveSocket.close();
			if (responseSocket != null)
				responseSocket.close();
		}
	}
}
