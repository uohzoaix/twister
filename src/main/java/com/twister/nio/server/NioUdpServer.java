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


import com.google.common.collect.Queues;
import com.mongodb.BasicDBObject;
import com.twister.storage.AccessLogCacheManager;
import com.twister.storage.mongo.MongoManager;

import com.twister.utils.Common;
import com.twister.utils.Constants;

/**
 * is test udp server
 * 
 * @author guoqing
 * 
 */
public class NioUdpServer {
	private ConnectionlessBootstrap bootstrap;
	private ChannelFactory channelFactory;
	private Channel serverChannel;
	private int port;
	private String host;
	private static final Logger logger = LoggerFactory.getLogger(NioUdpServer.class.getName());
	private volatile boolean run = false;
	// SynchronousQueue ;
	private final Queue<String> queue;
	private MongoManager mgo;
	private MoniterQueue moniter;

	/**
	 * 
	 * @param shareQueue
	 * @param port
	 * @param isdebug,debug=true exec shareQueue queue.poll(),debug=false not exec poll
	 */
	public NioUdpServer(final Queue<String> shareQueue, int port) {
		this.queue = shareQueue;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void run() {
		mgo = MongoManager.getInstance();
		moniter = new MoniterQueue(this.queue, "MoniterQueue");
		moniter.show();
		channelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool(), 4);
		bootstrap = new ConnectionlessBootstrap(channelFactory);
		try {
			// Set up the pipeline factory.
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				@Override
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					// Add the text line codec combination first,
					pipeline.addLast("framer", new LineBasedFrameDecoder(Constants.MaxFrameLength));
					pipeline.addLast("decoder", new StringDecoder());
					pipeline.addLast("encoder", new StringEncoder());
					// and then business logic.
					pipeline.addLast("handler", new UdpEventHandler());
					return pipeline;
				}
			});
			bootstrap.setOption("reuseAddress", true);
			bootstrap.setOption("child.udpNoDelay", true);
			bootstrap.setOption("child.keepAlive", true);
			serverChannel = bootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
			String localip = InetAddress.getLocalHost().getHostAddress();
			this.setHost(localip);
			run = true;
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
		run = false;
		logger.info("server stopped");
	}

	public boolean isRunning() {
		return run;
	}

	public class UdpEventHandler extends SimpleChannelUpstreamHandler {
		public UdpEventHandler() {
		}
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			try {
				// see LineBasedFrameDecoder
				String buffer = (String) e.getMessage();
				queue.offer(buffer);
				// logger.info("udp recvd " + " [" + buffer.toString() + "] ");
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
	//
	// NioUdpServer uss = new NioUdpServer(udpQueue, port);
	// logger.info("port:" + port);
	// uss.run();
	// }
}
