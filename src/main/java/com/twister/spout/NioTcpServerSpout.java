package com.twister.spout;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
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

import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.twister.nio.log.AccessLog;
import com.twister.storage.AccessLogCacheManager;
import com.twister.utils.Common;
import com.twister.utils.JedisConnection.JedisExpireHelps;

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
public class NioTcpServerSpout extends BaseRichSpout {
	
	private String progName = getClass().getSimpleName();
	private final Logger logger = LoggerFactory.getLogger(getClass().getName());
	private static final long serialVersionUID = 25499962443175493L;
	public SpoutOutputCollector collector;
	public Map<String, Object> conf;
	public TopologyContext context;
	public String componentId;
	public int taskid;
	public static final Pattern Ipv4 = Common.Ipv4;
	
	private ServerBootstrap bootstrap;
	private ChannelFactory channelFactory;
	private Channel serverChannel;
	private final int port;
	private final static int bufferSize = 1024;
	private volatile boolean running = false;
	private static long spoutLines = 0;
	private AccessLogCacheManager alogManager; // reids
	
	// SynchronousQueue or ArrayBlockingQueue ,LinkedList;
	private Queue<String> queue = new LinkedList<String>();
	private String localip = "127.0.0.1";
	private Fields _fields = new Fields("AccessLog");
	
	public NioTcpServerSpout(int port) {
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
		alogManager = new AccessLogCacheManager();
		Jedis jedis = alogManager.getMasterJedis();
		channelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		bootstrap = new ServerBootstrap(channelFactory);
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
					pipeline.addLast("handler", new TcpEventHandler(queue));
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
			jedis.select(JedisExpireHelps.DBIndex);
			String serinfo = "TcpSpout:" + localip + ":" + port;
			jedis.set(serinfo, serinfo);
			logger.info(progName + " tcp spout started,listening on " + localip + ":" + port);
		} catch (UnknownHostException e) {
			logger.error(e.getStackTrace().toString());
		}
		
	}
	
	@Override
	public void nextTuple() {
		AccessLog alog = null;
		try {
			String txt = queue.poll();
			if (txt != null && txt.length() > 10) {
				// send obj
				String[] lines = txt.split("\n");
				for (int i = 0; i < lines.length; i++) {
					String line = lines[i];
					if (Ipv4.matcher(line).find()) {
						alog = new AccessLog(line);
						if (alog != null) {
							spoutLines++;
							logger.debug(spoutLines + " " + line);
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
	
	class TcpEventHandler extends SimpleChannelUpstreamHandler {
		private Queue<String> queue;
		private long transLines = 0;
		
		public TcpEventHandler(final Queue<String> queue) {
			this.queue = queue;
		}
		
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			try {
				// see LineBasedFrameDecoder
				String buffer = (String) e.getMessage();
				transLines += 1;
				// SynchronousQueue put ,spout poll
				logger.debug("recvd length " + buffer.length() + "/" + transLines + " bytes [" + buffer.toString()
						+ "]");
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
