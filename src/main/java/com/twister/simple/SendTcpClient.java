package com.twister.simple;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import backtype.storm.utils.Utils;

public class SendTcpClient {
	public static Logger logger = LoggerFactory.getLogger(SendTcpClient.class);

	public static void main(String[] args) {
		String logfile = "/tmp/access.log";
		String host = "127.0.0.1";
		int PORT = 10236;
		Charset charSet = Charset.forName("UTF-8");
		logger.info("Usage : " + SendTcpClient.class.getName() + " <host> <port> <accessFile>");

		if (args.length > 1) {
			host = args[0];
		}
		if (args.length > 2) {
			PORT = Integer.valueOf(args[1]);
		}
		if (args.length > 3) {
			logfile = args[2];
		}
		logger.info("tcp client start host " + host + ":" + PORT);

		Socket socket = null;
		RandomAccessFile file = null;
		try {
			File tmpfile = new File(logfile);
			Preconditions.checkArgument(tmpfile.isFile(), "TextFileSpout expects a file but '" + tmpfile + "' is not exists.");
			int numberOfPackets = 100;
			int packetLength = 20;
			List<String> packets = new ArrayList<String>(numberOfPackets);
			file = new RandomAccessFile(logfile, "r");
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
						socket = new Socket(host, PORT);
						// socket.setSoTimeout(30 * 1000);
						StringBuffer packet = new StringBuffer(new String(line.getBytes("8859_1"), charSet)); // 编码转换
						if (packet.length() > 0 && packet.charAt(packet.length() - 1) != '\n') {
							packet.append("\n");
						}
						// if (i > 20) {
						// line = null;
						// break;
						// }
						System.out.print(i + " " + packet.toString());
						i++;
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // 创建数据传输流
						// 发送数据：
						if (packet.lastIndexOf("\n") > 0) {
							out.print(packet);
						} else {
							out.println(packet);
						}
						// Utils.sleep(100);
						out.flush();
						out.close();
						socket.close();
					}
					filePointer = file.getFilePointer();
					if (line == null) {
						issend = false;
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
			System.exit(0);
		}

	}

}
