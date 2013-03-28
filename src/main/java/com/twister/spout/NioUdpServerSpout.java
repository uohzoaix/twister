package com.twister.spout;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.Executor;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.nio.log.AccessLog;
import com.twister.utils.Common;

import backtype.storm.spout.SpoutOutputCollector;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * is tcp nio server
 * 
 * @author guoqing
 * 
 */
public class NioUdpServerSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 2549996244567293L;
	private String progName = NioUdpServerSpout.class.getSimpleName();
	private final Logger logger = LoggerFactory.getLogger(NioUdpServerSpout.class.getName());
	
	public SpoutOutputCollector collector;
	public Map<String, Object> conf;
	public TopologyContext context;
	public String componentId;
	public int taskid;
	public static final Pattern Ipv4 = Common.Ipv4;
	
	private ConnectionlessBootstrap bootstrap;
	private ChannelFactory channelFactory;
	private Channel serverChannel;
	private final int port;
	private final static int bufferSize = 1024;
	private volatile boolean running = false;
	private long spoutLines = 0;
	
	// SynchronousQueue or ArrayBlockingQueue
	private static Queue<String> queue = new LinkedList<String>();
	private String localip = "127.0.0.1";
	private Fields _fields = new Fields("AccessLog");
	
	public NioUdpServerSpout(int port) {
		this.port = port;
	}
	
	public boolean isRunning() {
		return running;
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.conf = conf;
		this.context = context;
		this.componentId = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		
		channelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
		bootstrap = new ConnectionlessBootstrap(channelFactory);
		try {
			// Set up the pipeline factory.
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				@Override
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					// Add the text line codec combination first,
					pipeline.addLast("framer", new LineBasedFrameDecoder(bufferSize));
					pipeline.addLast("decoder", new StringDecoder());
					pipeline.addLast("encoder", new StringEncoder());
					
					// and then business logic.
					pipeline.addLast("handler", new UdpEventHandler(queue));
					return pipeline;
				}
			});
			// bootstrap.setOption("reuseAddress", true);
			// bootstrap.setOption("tcpNoDelay", true);
			// bootstrap.setOption("broadcast", false);
			// bootstrap.setOption("sendBufferSize", bufferSize);
			// bootstrap.setOption("receiveBufferSize", bufferSize);
			// Bind and start to accept incoming connections.
			serverChannel = bootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
			localip = InetAddress.getLocalHost().getHostAddress();
			running = true;
			logger.info(progName + "udp spout started,listening on " + localip + ":" + port);
		} catch (UnknownHostException e) {
			logger.error(e.getStackTrace().toString());
		}
		
	}
	
	/**
	 * 消费者
	 */
	@Override
	public void nextTuple() {
		
		AccessLog alog = null;
		try {
			String txt = this.queue.poll();
			if (txt != null && txt.length() > 10) {
				// send obj
				String[] lines = txt.split("\n");
				for (int i = 0; i < lines.length; i++) {
					String line = lines[i];
					if (Ipv4.matcher(line).find()) {
						spoutLines++;
						alog = new AccessLog(line);
						if (alog != null) {
							logger.info(spoutLines + " " + line);
							collector.emit(new Values(alog));
							// logger.info(alog.toString());
						}
					}
					
				}
			}
			
		} catch (Exception e) {
			logger.error(e.getStackTrace().toString());
		}
	}
	
	@Override
	public void close() {
		System.out.println("stopping UDP server");
		serverChannel.close();
		channelFactory.releaseExternalResources();
		bootstrap.releaseExternalResources();
		running = false;
		System.out.println("server stopped");
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(_fields);
	}
	
	@Override
	public void ack(Object msgid) {
		logger.debug("ack msgid " + msgid.toString());
	}
	
	@Override
	public void fail(Object msgid) {
		logger.debug("fail msgid " + msgid.toString());
	}
	
	/**
	 * 产生者
	 * 
	 * @author guoqing
	 * 
	 */
	class UdpEventHandler extends SimpleChannelUpstreamHandler {
		// 产生者/消费者
		private long transLines = 0;
		private Queue<String> queue;
		
		public UdpEventHandler(final Queue<String> queue) {
			this.queue = queue;
		}
		
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			try {
				// see LineBasedFrameDecoder
				String buffer = (String) e.getMessage();
				transLines += 1;
				// SynchronousQueue put ,spout poll
				logger.info("recvd length " + buffer.length() + "/" + transLines + " bytes [" + buffer.toString() + "]");
				this.queue.offer(buffer);
			} catch (Exception e2) {
				logger.error(e2.getStackTrace().toString());
			}
			
		}
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			// Close the connection when an exception is raised.
			logger.warn("Unexpected exception from downstream.", e.getCause());
			// e.getChannel().close();
		}
		
	}
}
