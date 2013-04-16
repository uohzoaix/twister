package com.twister.nio.client;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
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
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;
import com.mongodb.BasicDBObject;
import com.twister.storage.mongo.MongoManager;
import com.twister.utils.Common;
import com.twister.utils.Constants;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * tail accesslog use udp sent to nioserverspout 有丢包情况
 * 
 * @author guoqing
 * 
 */
public class SendNioUdpClient {
	private static Logger logger = LoggerFactory.getLogger(SendNioUdpClient.class);
	private DatagramChannelFactory channelFactory;
	private ConnectionlessBootstrap bootstrap;
	private DatagramChannel clientChannel;
	private ChannelFuture future;
	private Tailer tailer;
	private final AtomicLong transline = new AtomicLong();
	// must use Queues.newConcurrentLinkedQueue
	private final Queue<String> queue = Queues.newConcurrentLinkedQueue();
	private volatile boolean running = false;
	private final boolean isdebug = false;
	private String host = "127.0.0.1";
	private int port = 10237;
	private long ct = 0;
	/**
	 * The listener to notify of events when tailing.
	 */

	private long interval = 100;
	private File file = new File(Constants.nginxAccess);
	// is true start begin
	private boolean end = false;
	private long begtime;
	private long lasttime;

	public SendNioUdpClient() {
		MongoManager mgo = MongoManager.getInstance();
		List<Map> list = mgo.query(Constants.SpoutTable, new BasicDBObject().append("desc", "spout").append("kind", "udp"));
		int i = Common.getRandomInt(0, list.size() - 1);
		Map<String, String> mp = list.get(i);
		this.host = String.valueOf(mp.get("ip"));
		this.port = Integer.parseInt(String.valueOf(mp.get("port")));
		dispclient();
		TailFile();
	}

	public SendNioUdpClient(File filename) {
		MongoManager mgo = MongoManager.getInstance();
		List<Map> list = mgo.query(Constants.SpoutTable, new BasicDBObject().append("desc", "spout").append("kind", "udp"));
		int i = Common.getRandomInt(0, list.size() - 1);
		Map<String, String> mp = list.get(i);
		this.host = String.valueOf(mp.get("ip"));
		this.port = Integer.parseInt(String.valueOf(mp.get("port")));
		this.file = filename;
		dispclient();
		TailFile();
	}

	public SendNioUdpClient(String host, int port) {
		this.host = host;
		this.port = port;
		dispclient();
		TailFile();
	}

	public void dispclient() {
		MongoManager mgo = MongoManager.getInstance();
		List<Map> list = mgo.query(Constants.SpoutTable, new BasicDBObject().append("desc", "spout").append("kind", "udp"));
		for (Map m : list) {
			System.out.println(m);
		}
	}

	public SendNioUdpClient(String host, int port, File filename) {
		this.host = host;
		this.port = port;
		this.file = filename;
		dispclient();
		TailFile();
		logger.info("" + host + ":" + port + "" + file.toString() + " " + end);
	}

	public void TailFile() {
		Preconditions.checkArgument(file.isFile(), "TextFileSpout expects a file but '" + file.toString() + "' is not exists.");
		// This listener send each file line in the queue
		TailerListener listener = new UdpQueueSender();
		// param end Set to true to tail from the end of the file, false to tail from the beginning of the file.
		tailer = new Tailer(this.file, listener, this.interval, this.end);
		// Start a tailer thread
		Thread thread = new Thread(tailer);
		thread.setDaemon(true);
		thread.start();

	}

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

	public void run() {
		this.running = true;
		this.setEnd(true);
		begtime = System.currentTimeMillis();
		// Configure the client.
		channelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
		bootstrap = new ConnectionlessBootstrap(channelFactory);

		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("framer", new LineBasedFrameDecoder(Constants.MaxFrameLength));
				pipeline.addLast("decoder", new StringDecoder());
				pipeline.addLast("encoder", new StringEncoder());
				// and then business logic.
				pipeline.addLast("handler", new SendNioUdpClientHandler());
				return pipeline;
			}

		});

		bootstrap.setOption("udpNoDelay", true);
		bootstrap.setOption("keepAlive", true);

		// Start the connection attempt.
		future = bootstrap.connect(new InetSocketAddress(host, port));
		// Wait until the connection is closed or the connection attempt fails.
		logger.info("连接服务器 " + host + ":" + port);
		future.getChannel().getCloseFuture().awaitUninterruptibly();
		// clientChannel = (DatagramChannel) bootstrap.bind(new
		// InetSocketAddress(0));

	}

	public void stop() {
		this.running = false;
		logger.info("stopping UDP server");
		clientChannel.close();
		channelFactory.releaseExternalResources();
		bootstrap.releaseExternalResources();
		logger.info("server stopped");

	}

	public boolean isRunning() {
		return running;
	}

	/**
	 * A listener for the tailer sending current file line in a blocking queue.
	 */
	private class UdpQueueSender extends TailerListenerAdapter {
		public UdpQueueSender() {

		}

		@Override
		public void handle(String line) {
			try {
				// ct += 1;
				line = new String(line.getBytes("8859_1"), Charset.forName("UTF-8"));
				if (!line.endsWith("\n")) {
					line += "\n";
				}
				queue.offer(line);
				// logger.info("offer queue " + ct + " line = [" + line + "],size" + line.length());
			} catch (Exception e) {
				logger.error("Tailing on file " + file.getAbsolutePath() + " was interrupted.");
			}
		}

		@Override
		public void fileRotated() {
			logger.info("File was rotated or rename");
		}
	}

	private class SendNioUdpClientHandler extends SimpleChannelUpstreamHandler {
		public SendNioUdpClientHandler() {
		}

		@Override
		public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
			// connected
			// System.out.println("channelConnected");
			SendHandle(ctx, e);

		}

		@Override
		public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {
			// 长连接
			// System.out.println("channelInterestChanged");
			SendHandle(ctx, e);
		}

		/**
		 * 不接回返回
		 */
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			// back the received msg to the server
			// Server is supposed to send nothing. Therefore, do nothing.
			// transline.incrementAndGet();
			String buffer = (String) e.getMessage();
			dumperValue(buffer);
			// logger.info("back recvd length " + buffer.length() + "/" + transLines + " bytes [" + buffer.toString() + "]");
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			// Close the connection when an exception is raised.
			logger.warn("Unexpected exception from downstream.", e.getCause());
			// e.getChannel().close();
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
			fw = new FileWriter("SendNioTdpClientReceived.txt", true);
			fw.write(tmp);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {

		SendNioUdpClient cli = null;
		try {
			// Parse options.
			String host = "127.0.0.1";
			int port = 10237;
			File logfile = new File(Constants.nginxAccess);
			logger.info("Usage 1: " + SendNioUdpClient.class.getName() + " <accessFile>");
			logger.info("Usage 2: " + SendNioUdpClient.class.getName() + " <host> <port>");
			logger.info("Usage 3: " + SendNioUdpClient.class.getName() + " <host> <port> <accessFile>");

			if (args.length == 3) {
				host = args[0];
				port = Integer.valueOf(args[1]);
				logfile = new File(args[2]);
				cli = new SendNioUdpClient(host, port, logfile);
			} else if (args.length == 2) {
				host = args[0];
				port = Integer.valueOf(args[1]);
				cli = new SendNioUdpClient(host, port);
			} else if (args.length == 1) {
				logfile = new File(args[0]);
				cli = new SendNioUdpClient(logfile);
			} else {
				cli = new SendNioUdpClient();
			}
			cli.run();
		} catch (Exception e) {
			e.printStackTrace();
			cli.stop();
			System.exit(0);
		}

	}
}
