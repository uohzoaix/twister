package com.twister.spout.trans;

import java.io.IOException;
import java.math.BigInteger;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.twister.nio.log.AccessLog;
import com.twister.nio.log.AccessLogAnalysis;

import backtype.storm.Config;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RegisteredGlobalState;
import backtype.storm.utils.Utils;

public class SyslogNioUdpTransSpout extends BaseTransactionalSpout<TransactionMetadata> {
	private static final long serialVersionUID = 2549996244317249537L;
	public static final int MAX_TRANSACTION_SIZE = 100;
	public final static Logger logger = LoggerFactory.getLogger(SyslogNioUdpTransSpout.class);
	private final static int DEFAULT_SYSLOG_UDP_PORT = 1234;
	private final static int MAX_SESSAGE_SIZE = 65535; 
	private final static int readChunckSize = 1024;
	private static InetAddress ip;
	private static int port=10237; 

	public SyslogNioUdpTransSpout() {
		this.port = DEFAULT_SYSLOG_UDP_PORT;
		try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public SyslogNioUdpTransSpout(int port) {
		this.port = port;
		try {
			this.ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	
	public static class TransSpoutCoordinator implements Coordinator<TransactionMetadata> {
		TransactionMetadata lastTransactionMetadata;
		int nextRead = 0;		
		public TransSpoutCoordinator() {
			nextRead = 1;
		}
		
		@Override
		public TransactionMetadata initializeTransaction(BigInteger txid, TransactionMetadata prevMetadata) {
			System.out.println("TransSpoutCoordinator initializeTransaction "+port);
			int cur = nextRead;
			nextRead += 1;
			TransactionMetadata ret = new TransactionMetadata(nextRead, cur);
			return ret;
		}
		
		@Override
		public boolean isReady() {
			System.out.println("TransSpoutCoordinator isReady ");
			return true;
		}
		
		@Override
		public void close() {
			System.out.println("TransSpoutCoordinator close ");
		}
	}
	
	public static class TransSpoutEmitter implements Emitter<TransactionMetadata> {
		private DatagramChannel channel = null;
		private DatagramSocket socket = null;
		private Selector selector = null;		 
		private ByteBuffer byteBuffer; 
		public TransSpoutEmitter() {
			try {
				selector = Selector.open();
				// 打开选择器
				channel = DatagramChannel.open(); // 打开UDP通道
				channel.configureBlocking(false); // 非阻塞
				channel.socket().setReuseAddress(true);
				socket = channel.socket();
				socket.bind(new InetSocketAddress(port));
				System.out.println("nio udp server start! " + port);
				channel.register(selector, SelectionKey.OP_READ); // 向通道注册选择器和对应事件标识,返回对应的SelectionKey
				Preconditions.checkState(socket.isBound(), "Socket on port " + port + " already bound.");
				logger.info("Opening SyslogNioUdpSpout on port " + port + " ip:" + ip);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		@Override
		public void emitBatch(TransactionAttempt tx, TransactionMetadata coordinatorMeta, BatchOutputCollector collector) {
			long tweetId = coordinatorMeta.index;
			logger.info("udp tweetId " + tweetId);
			int cc=0;			
			byteBuffer = ByteBuffer.allocate(readChunckSize);
			cc += 1;
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
						StringBuffer vec = new StringBuffer();
						boolean isreader = true;
						byteBuffer.clear();
						while (isreader) {
							clientChannel.receive(byteBuffer);
							byteBuffer.flip();
							CharBuffer charBuffer = Charset.forName("UTF-8").decode(byteBuffer);
							if (charBuffer.length() == 0 || charBuffer.toString() == null) {
								isreader = false;
							}
							String packet = charBuffer.toString();
							vec.append(packet);
							// 复位，清空
							byteBuffer.clear();
						}
						
						if (vec.length() > 0) {
							logger.info("nio UDP服务器端接受客户端数据 " + vec.length());
							String text = vec.toString();
							String[] lines = text.split("\n");
							for (int i = 0; i < lines.length; i++) {
								String line = lines[i];
								if (line == null || line.length() < 1) {
									continue;
								}
								logger.info(line);
								AccessLog alog = new AccessLog(line);
								logger.info("" + alog.repr());
								 
								List<Integer> taskids=collector.emit(new Values(tx,alog.outKey(),alog));								
								logger.info("SyslogNioUdpTransSpout was sent to task ids " + taskids.toString());
							}							
						} else {
							logger.info("SyslogNioUdpTransSpout " + port + " 我的心在等待，永远在等待!");
							Utils.sleep(1 * 1000);
						}
						if (!clientChannel.isRegistered()) {
							clientChannel.register(selector, SelectionKey.OP_READ);
						}						
					}
				}
				tweetId+=1;
				logger.info("udp cc " + cc);
			} catch (IOException e) {				 
				e.printStackTrace();
				logger.error(e.getMessage());
				throw new RuntimeException(e);
			}
			
		}
		
		@Override
		public void cleanupBefore(BigInteger txid) { 
			System.out.println("cleanupBefore "+txid);
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
		
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public ITransactionalSpout.Coordinator<TransactionMetadata> getCoordinator(Map conf, TopologyContext context) {
		return new TransSpoutCoordinator();
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public backtype.storm.transactional.ITransactionalSpout.Emitter<TransactionMetadata> getEmitter(Map conf,
			TopologyContext context) {
		return new TransSpoutEmitter();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("txid","ukey","AccessLog"));
	}
	
}
