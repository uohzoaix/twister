package com.twister.spout;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
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

import com.google.common.base.Preconditions;
import com.twister.io.input.AccessLog;

/**
 * Spout to feed messages into Storm from an UDP Socket.
 * <p>
 * This spout emits tuples containing only one field, named "packet" for each
 * packet receive on the UDP socket. TODO Point to point
 * </p>
 * 
 * <pre></pre>
 * 
 * @author guoqing
 * 
 */
public class SyslogNioUdpSpout extends BaseRichSpout {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2549996244317249537L;
	
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final static int DEFAULT_SYSLOG_UDP_PORT = 1234;
	// This limit stems from the maximum supported UDP size of 65535 octets
	// specified in RFC 768
	private final static int MAX_SESSAGE_SIZE = 65535;
	private final static int readChunckSize = 1024;
	private final int port;
	private SpoutOutputCollector collector;
	private DatagramChannel channel = null;
	private DatagramSocket socket = null;
	private Selector selector = null;
	private InetAddress ip;
	private ByteBuffer byteBuffer;
	
	public SyslogNioUdpSpout() {
		this.port = DEFAULT_SYSLOG_UDP_PORT;
		try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public SyslogNioUdpSpout(int port) {
		this.port = port;
		try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public SyslogNioUdpSpout(int port, InetAddress ip) {
		this.port = port;
		try {
			this.ip = ip;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			selector = Selector.open();
			// 打开选择器
			channel = DatagramChannel.open(); // 打开UDP通道
			channel.configureBlocking(false); // 非阻塞
			channel.socket().setReuseAddress(true);
			socket = channel.socket();
			socket.bind(new InetSocketAddress(port));
			System.out.println("server start! " + port);
			channel.register(selector, SelectionKey.OP_READ); // 向通道注册选择器和对应事件标识,返回对应的SelectionKey
			Preconditions.checkState(socket.isBound(), "Socket on port " + port + " already bound.");
			logger.info("Opening SyslogNioUdpSpout on port " + port + " ip:" + ip);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void close() {
		if (!socket.isClosed()) {
			try {
				channel.close();
				socket.close();
				selector.selectNow();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			logger.info("Closing SyslogUdpSpout on port " + port);
		}
	}
	
	@Override
	public void nextTuple() {
		byteBuffer = ByteBuffer.allocate(readChunckSize);
		try {
			// 选择一组键，并且相应的通道已经打开
			int lks = selector.select();
			if (lks == 0)
				return;
			Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
			while (iter.hasNext()) {
				SelectionKey sk = iter.next();
				iter.remove();
				if (sk.isReadable()) {
					// 在这里datagramChannel与channel实际是同一个对象
					DatagramChannel clientChannel = (DatagramChannel) sk.channel();
					String remoteip = clientChannel.toString();
					StringBuffer packet = new StringBuffer();
					boolean isreader = true;
					byteBuffer.clear();
					while (isreader) {
						clientChannel.receive(byteBuffer);
						byteBuffer.flip();
						CharBuffer charBuffer = Charset.defaultCharset().decode(byteBuffer);
						if (charBuffer.length() == 0 || charBuffer.toString() == null) {
							isreader = false;
						}
						packet.append(charBuffer.toString());
						// 复位，清空
						byteBuffer.clear();
					}
					String line = packet.toString();
					if (packet.length() > 0) {
						logger.info("nio UDP服务器端接受客户端数据 " + remoteip + line + " bf " + packet.length());
						System.out.println(line);
						AccessLog alog = new AccessLog(line);
						// System.out.println(alog.repr());
						collector.emit(new Values("hello"));
					}
					clientChannel.register(selector, SelectionKey.OP_READ);
				}
			}
		} catch (IOException e) {
			// TODO
			e.printStackTrace();
			throw new RuntimeException(e);
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
		// TODO log ?
	}
}