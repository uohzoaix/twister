package com.twister.spout;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.common.base.Preconditions;
import com.twister.nio.log.AccessLog;

/**
 * Spout to feed messages into Storm from an TCP Socket.
 * <p>
 * This spout emits tuples containing only one field, named "packet" for each
 * packet receive on the TCP socket.
 * </p>
 * 
 * <pre></pre>
 * 
 * @author guoqing
 * 
 */
public class SyslogNioTcpSpout extends BaseRichSpout {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3845095415477875994L;
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	private final static int DEFAULT_SYSLOG_TCP_PORT = 514;
	private final static int readChunckSize = 1024;
	private static Charset charSet = Charset.forName("UTF-8");
	private SpoutOutputCollector collector;
	private final int port;
	private InetAddress ip = null;
	private ServerSocketChannel serverSocketChannel = null;
	private ServerSocket server = null;
	private Selector selector = null;
	private ByteBuffer readBuffer;
	
	public SyslogNioTcpSpout() {
		this.port = DEFAULT_SYSLOG_TCP_PORT;
		try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public SyslogNioTcpSpout(int port) {
		this.port = port;
		try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		}
	}
	
	public SyslogNioTcpSpout(int port, InetAddress ip) {
		this.port = port;
		try {
			this.ip = ip;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
		Preconditions.checkState(server == null, "SyslogTcpSpout already open on port " + port);
		this.collector = collector;
		this.readBuffer = ByteBuffer.allocate(readChunckSize);
		try {
			// TCPServer accept, 监听端口，准备连接客户端
			logger.info(" SyslogNioTcpSpout server start,will bind on port " + port);
			// 生成一个侦听端
			serverSocketChannel = ServerSocketChannel.open();
			// 生成一个信号监视器
			selector = Selector.open();
			// 侦听端绑定到一个server端口
			server = serverSocketChannel.socket();
			server.bind(new InetSocketAddress(port));
			// 将侦听端设为异步方式
			serverSocketChannel.configureBlocking(false);
			// 设置侦听端所选的异步信号OP_ACCEPT
			serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
			logger.info("register SyslogNioTcpSpout on port " + port + " ip:" + ip.getHostAddress());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void close() {
		try {
			if (!server.isClosed()) {
				server.close();
				selector.close();
				serverSocketChannel.close();
			}
			
		} catch (IOException e) {
			e.printStackTrace(); // To change body of catch statement use File |
									// Settings | File Templates.
		}
		logger.info("Closing SyslogTcpSpout on port " + port);
		
	}
	
	@Override
	public void nextTuple() {
		
		try {
			// 选择一组键，并且相应的通道已经打开
			int lks = selector.select();
			if (lks == 0)
				return;
			Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
			
			while (iter.hasNext()) {
				SelectionKey selectionKey = iter.next();
				iter.remove();
				if (selectionKey.isAcceptable()) {
					// 获取SocketChannel来通信
					SelectableChannel channel = selectionKey.channel();
					SocketChannel clientChannel = ((ServerSocketChannel) channel).accept();
					clientChannel.configureBlocking(false);
					clientChannel.register(selector, SelectionKey.OP_READ);
				}
				if (selectionKey.isReadable()) {
					// 处理读请求
					SocketChannel clientChannel = (SocketChannel) selectionKey.channel();
					String remoteip = clientChannel.getRemoteAddress().toString();
					// 读取服务器发送来的数据到缓冲区中
					readBuffer.clear();
					StringBuffer vec = new StringBuffer();
					int len;
					// logger.info("服务器端接受客户端数据  isReadable "+remoteip);
					while ((len = clientChannel.read(readBuffer)) > 0) {
						readBuffer.flip();
						String receiveText = new String(readBuffer.array(), 0, len, charSet);
						System.out.println("receiveText " + receiveText);
						vec.append(receiveText);
						// 复位，清空
						readBuffer.clear();
					}
					
					if (vec.length() > 0) {
						logger.info("服务器端接受客户端数据 [" + remoteip + "] " + vec.length());
						String text = vec.toString();
						String[] lines = text.split("\n");
						for (int i = 0; i < lines.length; i++) {
							String line = lines[i];
							if (line == null || line.length() < 1) {
								continue;
							}
							logger.info(line);
							AccessLog alog = new AccessLog(line);
							logger.info(alog.repr());
							collector.emit(new Values(line));
						}
					} else {
						logger.info("我的心在等待，永远在等待!");
					}
					clientChannel.register(selector, SelectionKey.OP_READ);
					Utils.sleep(100);
				}
			}
			
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			try {
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public boolean isDistributed() {
		return false;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("packet"));
	}
	
	@Override
	public void ack(Object o) {
	}
	
	@Override
	public void fail(Object o) {
		
	}
	
}