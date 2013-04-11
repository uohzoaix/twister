package com.twister.nio.server;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

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

import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.google.common.collect.Queues;
import com.mongodb.BasicDBObject;
import com.twister.storage.AccessLogCacheManager;
import com.twister.storage.mongo.MongoManager;
import com.twister.storage.redis.JedisManager.JedisExpireHelps;
import com.twister.utils.Common;
import com.twister.utils.Constants;

/**
 * is udp server
 * 
 * @author guoqing
 * 
 */
public class NioUdpServer implements Runnable {
	private ConnectionlessBootstrap bootstrap;
	private ChannelFactory channelFactory;
	private Channel serverChannel;
	private final int port;
	private final static int bufferSize = 1024;
	private static final Logger logger = LoggerFactory.getLogger(NioUdpServer.class.getName());
	private final boolean isdebug;
	private volatile boolean running = false;
	private long transLines = 0l;
	private AccessLogCacheManager alogManager; // reids
	// SynchronousQueue or ArrayBlockingQueue ,LinkedList;
	private final Queue<String> queue;
	public MongoManager mgo;

	/**
	 * 
	 * @param shareQueue
	 * @param port
	 * @param isdebug,debug=true exec shareQueue queue.poll(),debug=false not exec poll
	 */
	public NioUdpServer(final Queue<String> shareQueue, int port, boolean isdebug) {
		this.queue = shareQueue;
		this.port = port;
		this.isdebug = isdebug;
	}

	public void run() {
		mgo = MongoManager.getInstance();
		channelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool(), 4);
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
					pipeline.addLast("handler", new UdpEventHandler());
					return pipeline;
				}
			});
			bootstrap.setOption("reuseAddress", true);
			// bootstrap.setOption("child.tcpNoDelay", true);
			bootstrap.setOption("child.keepAlive", true);
			serverChannel = bootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
			String localip = InetAddress.getLocalHost().getHostAddress();
			running = true;
			String dts = Common.createDataStr();
			String serinfo = "udp:" + localip + ":" + port + " " + dts;
			BasicDBObject sermap = new BasicDBObject();
			sermap.put("ip", localip);
			sermap.put("port", port);
			sermap.put("kind", "udp");
			sermap.put("desc", "spout");
			sermap.put("day", dts);
			mgo.insertOrUpdate(Constants.SpoutTable, sermap, sermap);
			logger.info("服务端已准备好 " + serinfo);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			stop();
		}

	}

	public void stop() {
		logger.info("stopping UDP server");
		channelFactory.releaseExternalResources();
		// serverChannel.close();
		bootstrap.releaseExternalResources();
		running = false;
		logger.info("server stopped");
	}

	public boolean isRunning() {
		return running;
	}

	public class UdpEventHandler extends SimpleChannelUpstreamHandler {
		public UdpEventHandler() {
		}
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			try {
				// see LineBasedFrameDecoder
				String buffer = (String) e.getMessage();
				// transLines += 1;
				// logger.info("udp recvd length " + buffer.length() + "/" + transLines + " bytes [" + buffer.toString() + "] " + isdebug);
				queue.offer(buffer);
				synchronized (this) {
					if (isdebug) {
						queue.poll();
					}
				}
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

	// public static void main(String[] args) {
	// Queue<String> udpQueue = Queues.newConcurrentLinkedQueue();
	// int port;
	// if (args.length > 0) {
	// port = Integer.parseInt(args[0]);
	// } else {
	// port = 10237;
	// }
	// boolean debug = true;
	// NioUdpServer uss = new NioUdpServer(udpQueue, port, debug);
	// logger.info("port:" + port + " isdebug " + debug);
	// uss.run();
	// }
}
