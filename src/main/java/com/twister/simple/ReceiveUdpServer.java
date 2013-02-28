package com.twister.simple;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
/**
 * nio udpserver,模拟syslog-ng
 * @author guoqing
 *
 */
public class ReceiveUdpServer {
	public static void main(String[] args) {
		run();
	}

	public static void run() {
		DatagramChannel channel = null;
		DatagramSocket socket = null;
		Selector selector = null;

		try {

			int port = 10234; // receive
			int BUFFSIZE = 1024;
			// 确定接受方的IP和端口号，IP地址为本地机器地址
			InetAddress ip = InetAddress.getLocalHost();
			// 确定数据报接受的数据的数组大小
			byte[] buf = new byte[BUFFSIZE];
			ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFSIZE);
			
			selector = Selector.open(); 		// 打开选择器  
			channel = DatagramChannel.open();	// 打开UDP通道  		
			channel.configureBlocking(false);    // 非阻塞 
			channel.socket().setReuseAddress(true);
			socket = channel.socket();
			socket.bind(new InetSocketAddress(port));
			System.out.println("server start!");			 
			channel.register(selector, SelectionKey.OP_READ);  // 向通道注册选择器和对应事件标识,返回对应的SelectionKey
			SimpleDateFormat fmtdate = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			System.out.println(ip.toString() + " " + port);
			
			while (true) {
				//nio udp,轮询 
				int nKeys = selector.select();
				if (nKeys > 0) {					 
					Iterator<SelectionKey> iterator =  selector.selectedKeys().iterator();
					while (iterator.hasNext()) {
						SelectionKey sk = (SelectionKey)iterator.next();
						iterator.remove();
						if (sk.isReadable()) {
							// 在这里datagramChannel与channel实际是同一个对象
							DatagramChannel datagramChannel = (DatagramChannel) sk.channel();
							byteBuffer.clear();
							SocketAddress sa = datagramChannel.receive(byteBuffer);
							// 将缓冲区准备为数据传出状态
							byteBuffer.flip();
							// 测试：通过将收到的ByteBuffer首先通过缺省的编码解码成CharBuffer 再输出
							CharBuffer charBuffer = Charset.defaultCharset().decode(byteBuffer);
							System.out.println("receive message:"+ charBuffer.toString());
							
							//String echo = "server reply!";
							//ByteBuffer buffer = Charset.defaultCharset().encode(echo);
							//Thread.sleep(100);// 延时 test
							//datagramChannel.send(buffer, sa);
						}
					}
				}
				//udp
				// 创建接收方的套接字,并制定端口号和IP地址
				// socket = new DatagramSocket(port, ip); // 用于接收数据
//				// 创建接受类型的数据报，数据将存储在buf中 
//				DatagramPacket getPacket = new DatagramPacket(buf, buf.length);
//				// 接收数据
//				receiveSocket.receive(getPacket);
//				// got data
//				// 解析发送方传递的消息，并打印
//				String receive = new String(buf, 0, getPacket.getLength());
//				System.out.println("Receive data: IP = "
//						+ getPacket.getAddress().getHostAddress() + ", Port = "
//						+ getPacket.getPort() + ", Data = " + receive
//						+ ", Time =" + fmtdate.format(new Date()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				channel.close();
				selector.selectNow();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
		}
	}
}
