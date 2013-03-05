package com.twister.simple;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.input.TailerListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

public class SenderTcpClient {
	public static Logger logger = LoggerFactory
			.getLogger(SenderTcpClient.class);
	public static String logfile = "src/main/resources/accessLog.txt";
	private static InetAddress host;
	private static final int PORT = 10236;
	private static Charset charSet = Charset.forName("UTF-8");

	public static void main(String[] args) {
		try {
			host = InetAddress.getLocalHost();
			logger.info("tcp client start host " + host.getHostAddress() + ":"
					+ PORT);
			run();		 
		} catch (UnknownHostException e) {
			logger.info("Host ID not found!");
			System.exit(1);
		}

	}
 
	private static void run() {
		Socket socket = null;
		RandomAccessFile file=null;
		try {
			int numberOfPackets = 100;
			int packetLength = 20;
			List<String> packets = new ArrayList<String>(numberOfPackets);
			file = new RandomAccessFile(logfile, "r");
			long filePointer = 0;
			boolean issend=true;
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
						socket = new Socket(host, PORT);
						socket.setSoTimeout(30*1000);
						String packet = new String(line.getBytes("8859_1"),	charSet); // 编码转换
						i++;
						logger.info(i + " tcp" + " port " + PORT +" " + packet + " len "	+ packet.length());
						packets.add(packet);
						PrintWriter out = new PrintWriter(socket.getOutputStream(),true); // 创建数据传输流
						//发送数据：
						out.println(packet);
						Utils.sleep(100);
						out.flush(); 
						out.close();
						socket.close(); 
					}
					filePointer = file.getFilePointer();
					if (line == null) {
						issend=false;
						break;
					}
				}
			}
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				socket.close();
				file.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
