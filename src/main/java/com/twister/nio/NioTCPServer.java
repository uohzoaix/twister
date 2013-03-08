package com.twister.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.nio.NIOEvent.EventHandlerAdapter;

/**
 * 测试功能 nio tcp socket use test
 * 
 */

public class NioTCPServer extends EventHandlerAdapter {
	private static final Logger logger = LoggerFactory.getLogger(NioTCPServer.class);
	private static Charset charSet = Charset.forName("UTF-8");
	private ServerSocketChannel serverSocketChannel = null;
	private Selector selector = null;
	private int maxBufferSize = 104857600;
	private int readChunckSize = 8192;
	private ByteBuffer readBuffer;
	private ByteBuffer writeBuffer;
	private CharBuffer stream;
	private CharBuffer streamRead;
	boolean writing;
	boolean closing;
	boolean closed;
	
	public NioTCPServer() {
		this.maxBufferSize = 104857600;
		this.readChunckSize = 8192;
		this.readBuffer = ByteBuffer.allocate(readChunckSize);
		this.stream = CharBuffer.allocate(readChunckSize);
		this.streamRead = stream.duplicate();
		this.writeBuffer = ByteBuffer.allocateDirect(readChunckSize);
		
	}
	
	/**
	 * Binds the socket provided by the channel to a port. The backlog for the
	 * bind is 128 connections just like in Tornado. Starts the IOLoop.
	 * 
	 * @param port
	 * @throws Exception
	 */
	public void listen(int port) throws Exception {
		// 生成一个侦听端
		serverSocketChannel = ServerSocketChannel.open();
		InetAddress localhost = InetAddress.getLocalHost();
		// 生成一个信号监视器
		selector = Selector.open();
		// 侦听端绑定到一个端口
		final ServerSocket serverSocket = serverSocketChannel.socket();
		logger.info("TCP Server Socket host:" + localhost.getHostAddress() + "port:" + port);
		serverSocket.bind(new InetSocketAddress(port), 1024);
		// 将侦听端设为异步方式
		serverSocketChannel.configureBlocking(false);
		// 设置侦听端所选的异步信号OP_ACCEPT
		serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
		while (true) {
			int n = selector.select();
			if (n == 0)
				continue;
			Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
			while (iter.hasNext()) {
				SelectionKey key = (SelectionKey) iter.next();
				if (key.isAcceptable()) {
					onAccept(key);
				}
				if (key.isReadable()) {
					onRead(key);
				}
				if (key.isWritable()) {
					onWrite(key);
				}
				iter.remove();
			}
		}
		
	}
	
	@Override
	protected void onAccept(SelectionKey selectionKey) throws Exception {
		// 获取SocketChannel来通信
		SelectableChannel channel = selectionKey.channel();
		SocketChannel clientChannel = ((ServerSocketChannel) channel).accept();
		clientChannel.configureBlocking(false);
		clientChannel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(readChunckSize));
		channel.configureBlocking(false);
		
	}
	
	@Override
	protected void onWrite(SelectionKey selectionKey) throws Exception {
		SocketChannel clientChannel = (SocketChannel) selectionKey.channel();
		String remoteip = clientChannel.getRemoteAddress().toString();
		// 将缓冲区清空以备下次写入
		this.writeBuffer.clear();
		// 返回为之创建此键的通道。
		String sendText = "receive " + remoteip + " OK!";
		// 向缓冲区中输入数据
		writeBuffer.put(sendText.getBytes());
		// 将缓冲区各标志复位,因为向里面put了数据标志被改变要想从中读取数据发向服务器,就要复位
		writeBuffer.flip();
		// 输出到通道
		clientChannel.write(writeBuffer);
		logger.info("服务器端向客户端发送数据 " + sendText);
		clientChannel.register(selector, SelectionKey.OP_READ);
	}
	
	@Override
	protected void onRead(SelectionKey selectionKey) throws Exception {
		SocketChannel clientChannel = (SocketChannel) selectionKey.channel();
		String remoteip = clientChannel.getRemoteAddress().toString();
		// 将缓冲区清空以备下次读取
		readBuffer.clear();
		// 读取服务器发送来的数据到缓冲区中
		int count = clientChannel.read(readBuffer);
		if (count > 0) {
			String receiveText = new String(readBuffer.array(), 0, count);
			logger.info("服务器端接受客户端数据 [" + remoteip + "] " + receiveText);
			clientChannel.register(selector, SelectionKey.OP_READ);
		}
	}
	
	public void close() {
		this.closing = true;
		if (!this.writing) {
			this.closed = true;
			this.stream.clear();
			this.readBuffer.clear();
			this.streamRead.clear();
			try {
				this.serverSocketChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		int port = 10236;
		NioTCPServer tcpServer = new NioTCPServer();
		try {
			tcpServer.listen(port);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
