package com.twister.nio.client;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;
import com.mongodb.BasicDBObject;
import com.twister.entity.AccessLogAnalysis;
import com.twister.storage.mongo.MongoManager;
import com.twister.utils.Common;
import com.twister.utils.Constants;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * tail accesslog use tcp sent to nioserverspout
 * 
 * @author guoqing
 * 
 */
public class SendNioTcpClient {
	private static Logger logger = LoggerFactory.getLogger(SendNioTcpClient.class);
	private ChannelFactory channelFactory;
	private ClientBootstrap bootstrap;
	private DatagramChannel clientChannel;
	private ChannelFuture future;
	private Tailer tailer;
	// Queues.newConcurrentLinkedQueue
	private final Queue<String> queue = Queues.newConcurrentLinkedQueue();

	private volatile boolean running = false;
	private final boolean isdebug = false;
	private String host = "127.0.0.1";
	private int port = 10236;
	private long ct = 0;
	private long begtime;
	private long lasttime;
	/**
	 * The listener to notify of events when tailing.
	 */

	private long interval = 100;

	private File file = new File(Constants.nginxAccess);
	private boolean end = false;
    
	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public boolean isEnd() {
		return end;
	}

	public void setEnd(boolean end) {
		this.end = end;
	}

	public SendNioTcpClient() {
		MongoManager mgo = MongoManager.getInstance();
		List<Map> list = mgo.query(Constants.SpoutTable, new BasicDBObject().append("desc", "spout").append("kind", "tcp"));
		int i = Common.getRandomInt(0, list.size() - 1);
		Map<String, String> mp = list.get(i);
		this.host = String.valueOf(mp.get("ip"));
		this.port = Integer.parseInt(String.valueOf(mp.get("port")));
		dispclient();
		TailFile();
	}

	public SendNioTcpClient(File filename) {
		MongoManager mgo = MongoManager.getInstance();
		List<Map> list = mgo.query(Constants.SpoutTable, new BasicDBObject().append("desc", "spout").append("kind", "tcp"));
		int i = Common.getRandomInt(0, list.size() - 1);
		Map<String, String> mp = list.get(i);
		System.out.println(mp);
		this.host = String.valueOf(mp.get("ip"));
		this.port = Integer.parseInt(String.valueOf(mp.get("port")));
		this.file = filename;
		dispclient();
		TailFile();
	}

	public SendNioTcpClient(String host, int port) {
		this.host = host;
		this.port = port;
		dispclient();
		TailFile();
	}

	public SendNioTcpClient(String host, int port, File filename) {
		this.host = host;
		this.port = port;
		this.file = filename;
		dispclient();
		TailFile();
	}

	public void dispclient() {
		MongoManager mgo = MongoManager.getInstance();
		List<Map> list = mgo.query(Constants.SpoutTable, new BasicDBObject().append("desc", "spout").append("kind", "tcp"));
		for (Map m : list) {
			System.out.println(m);
		}
	}

	public void TailFile() {
		Preconditions.checkArgument(this.file.isFile(), "TextFileSpout expects a file but '" + this.file.toString() + "' is not exists.");
		// This listener send each file line in the queue
		TailerListener listener = new TcpQueueSender();
		try {
			tailer = new Tailer(this.file, listener, this.interval, this.end);
		} catch (Exception e) {
			tailer = new Tailer(this.file, listener, this.interval, this.end);
		}
		// Start a tailer thread
		Thread thread = new Thread(tailer);
		thread.setDaemon(true);
		thread.start();
		logger.info(file.toString() + " " + this.end);
		logger.info("" + host + " " + port);
	}
	

	public void run() {
		try {
			this.running = true;
			this.setEnd(true);
			begtime = System.currentTimeMillis();
			// Configure the client.
			channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), 6, 6);
			bootstrap = new ClientBootstrap(channelFactory);

			// Set up the pipeline factory.
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					pipeline.addLast("framer", new LineBasedFrameDecoder(Constants.MaxFrameLength));
					pipeline.addLast("decoder", new StringDecoder());
					pipeline.addLast("encoder", new StringEncoder());
					// and then business logic.
					pipeline.addLast("handler", new SendNioTcpClientHandler());
					return pipeline;
				}

			});
			// 参数名不用加上"child."前缀
			bootstrap.setOption("tcpNoDelay", true);
			bootstrap.setOption("keepAlive", true);

			// Start the connection attempt.
			future = bootstrap.connect(new InetSocketAddress(host, port));
			// Wait until the connection is closed or the connection attempt
			// fails.

			logger.info("连接服务器 " + host + ":" + port);
			ChannelFuture cf = future.getChannel().getCloseFuture().awaitUninterruptibly();
			// clientChannel = (DatagramChannel) bootstrap.bind(new InetSocketAddress(0));

		} catch (Exception e) {
			e.printStackTrace();
			stop();
		}
	}

	public void stop() {

		this.running = false;
		System.out.println("stopping UDP server");
		clientChannel.close();
		channelFactory.releaseExternalResources();
		bootstrap.releaseExternalResources();
		System.out.println("server stopped");

	}

	public boolean isRunning() {
		return running;
	}

	/**
	 * A listener for the tailer sending current file line in a blocking queue.
	 */
	private class TcpQueueSender extends TailerListenerAdapter {
		public TcpQueueSender() {
		}

		@Override
		public void handle(String line) {
			try {
				line = new String(line.getBytes("8859_1"), Charset.forName("UTF-8"));
				if (!line.endsWith("\n")) {
					line += "\n";
				}
				queue.offer(line);
				// logger.debug("add queue length=" + line.length() + "/" + ct + " line = [" + line + "]");
			} catch (Exception e) {
				logger.error("Tailing on file " + file.getAbsolutePath() + " was interrupted.");
			}
		}

		@Override
		public void fileRotated() {
			logger.info("File was rotated or rename");
		}
	}

	private class SendNioTcpClientHandler extends SimpleChannelUpstreamHandler {
		private final AtomicLong transline = new AtomicLong();

		public SendNioTcpClientHandler() {
		}

		@Override
		public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
			if (e instanceof ChannelStateEvent) {
				// ChannelStateEvent evt = (ChannelStateEvent) e;
				// System.out.println(evt.getState());
			}
			super.handleUpstream(ctx, e);
		}

		@Override
		public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
			// connected
			SendHandle(ctx, e);
		}

		@Override
		public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {
			// 长连接
			SendHandle(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			// back the received msg to the server
			// Server is supposed to send nothing. Therefore, do nothing.

			String buffer = (String) e.getMessage();
			dumperValue(buffer);
			// logger.info("back recvd " + buffer.length() + "/" + " bytes [" + buffer.toString() + "]");
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			// Close the connection when an exception is raised.
			logger.warn("Unexpected exception from downstream.", e.getCause());

		}

		public void SendHandle(ChannelHandlerContext ctx, ChannelStateEvent e) {
			Channel channel = e.getChannel();
			while (channel.isWritable()) {
				try {
					String line = queue.poll();
					lasttime = System.currentTimeMillis();
					if (line != null) {
						channel.write(line);
						if (transline.equals(Long.MAX_VALUE)) {
							transline.set(0);
						}
						transline.incrementAndGet();
						if (isdebug) {
							logger.info(transline + " line=[" + line + "],size=" + line.length());
						} else {
							if (begtime + Constants.OutPutTime < lasttime) {
								begtime = lasttime;
								logger.info(transline + " line=[" + line + "],size=" + line.length());
							}
						}
					}

				} catch (Exception e1) {
					e1.printStackTrace();
					logger.error(e1.getStackTrace().toString());
				}

			}

		}

	}

	private synchronized void dumperValue(final String line) {
		String tmp = line;
		if (tmp.endsWith("\n")) {
			tmp += "\n";
		}
		FileWriter fw;
		try {
			fw = new FileWriter("SendNioTcpClientReceived.txt", true);
			fw.write(tmp);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		SendNioTcpClient cli = null;
		try {
			// Parse options.
			String host = "127.0.0.1";
			int port = 10236;
			File logfile = new File(Constants.nginxAccess);
			logger.info("Usage 1: " + SendNioTcpClient.class.getName() + " <accessFile>");
			logger.info("Usage 2: " + SendNioTcpClient.class.getName() + " <host> <port>");
			logger.info("Usage 3: " + SendNioTcpClient.class.getName() + " <host> <port> <accessFile>");
			if (args.length == 3) {
				host = args[0];
				port = Integer.valueOf(args[1]);
				logfile = new File(args[2]);
				cli = new SendNioTcpClient(host, port, logfile);
			} else if (args.length == 2) {
				host = args[0];
				port = Integer.valueOf(args[1]);
				cli = new SendNioTcpClient(host, port);
			} else if (args.length == 1) {
				logfile = new File(args[0]);
				cli = new SendNioTcpClient(logfile);
			} else {
				cli = new SendNioTcpClient();
			}
			cli.run();
		} catch (Exception e) {
			e.printStackTrace();
			cli.stop();
			System.exit(0);
		}

	}
}
