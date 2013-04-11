package com.twister.nio.server;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;

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

import java.util.concurrent.atomic.AtomicLong;
import com.google.common.collect.Queues;
import com.mongodb.BasicDBObject;
import com.twister.utils.Common;
import com.twister.utils.Constants;
import com.twister.storage.mongo.MongoManager;

/**
 * is test tcp server,connent info to mongodb twisterServer
 * 
 * @author guoqing
 * 
 */
public class NioTcpServer {
	private ServerBootstrap bootstrap;
	private ChannelFactory channelFactory;
	private Channel serverChannel;
	private final int port;
	private final int bufferSize = 1024;
	private final static boolean isdebug = true;
	private volatile boolean running = false;
	private static final Logger logger = LoggerFactory.getLogger(NioTcpServer.class.getName());
	private final AtomicLong transLines = new AtomicLong();
	// SynchronousQueue or ArrayBlockingQueue ,LinkedList;
	private final Queue<String> queue;
	public MongoManager mgo;


	/**
	 * 
	 * @param shareQueue
	 * @param port
	 * @param isdebug,debug=true exec shareQueue queue.poll(),debug=false not exec poll
	 */
	public NioTcpServer(final Queue<String> shareQueue, int port) {
		this.queue = shareQueue;
		this.port = port;

	}

	/**
	 * test
	 */

	public void run() {
		mgo = MongoManager.getInstance();
		channelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
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
					pipeline.addLast("handler", new TcpEventHandler());
					return pipeline;
				}
			});
			// 这里设置tcpNoDelay和keepAlive参数，前面的child前缀必须要加上，用来指明这个参数将被应用到接收到的Channels
			bootstrap.setOption("reuseAddress", true);
			bootstrap.setOption("child.tcpNoDelay", true);
			bootstrap.setOption("child.keepAlive", true);
			// Bind and start to accept incoming connections.
			serverChannel = bootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
			running = true;
			String localip = InetAddress.getLocalHost().getHostAddress();
			String dts = Common.createDataStr();
			String serinfo = "tcp:" + localip + ":" + port + " " + dts;
			BasicDBObject sermap = new BasicDBObject();
			sermap.put("ip", localip);
			sermap.put("port",port);
			sermap.put("kind", "tcp");
			sermap.put("desc", "spout");
			sermap.put("day", dts);
			mgo.insertOrUpdate(Constants.SpoutTable, sermap, sermap);
			logger.info("服务端已准备好 " + serinfo);

		} catch (UnknownHostException e) {
			stop();
		}
	}

	public void stop() {
		logger.info("stopping UDP server");
		channelFactory.releaseExternalResources();
		serverChannel.close();
		bootstrap.releaseExternalResources();
		running = false;
		logger.info("server stopped");
	}

	public boolean isRunning() {
		return running;
	}

	public class TcpEventHandler extends SimpleChannelUpstreamHandler {
		public TcpEventHandler() {
		}

		@Override
		public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
			if (e instanceof ChannelStateEvent) {
				ChannelStateEvent evt = (ChannelStateEvent) e;
				// System.out.println(evt.getState());
			}
			// Let SimpleChannelHandler call actual event handler methods below.
			super.handleUpstream(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			try {
				// see LineBasedFrameDecoder
				String buffer = (String) e.getMessage();
				transLines.incrementAndGet();
				// SynchronousQueue put ,spout poll
				logger.info("recvd length " + buffer.length() + "/" + transLines + " bytes [" + buffer.toString() + "]");

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

	public static void main(String[] args) {
		Queue<String> tcpQueue = Queues.newConcurrentLinkedQueue();
		int port;
		if (args.length > 0) {
			port = Integer.parseInt(args[0]);
		} else {
			port = 10236;
		}
		logger.info("port:" + port);
		NioTcpServer ser = new NioTcpServer(tcpQueue, port);
		ser.run();
	}
}