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
public class SendNioUdpClient implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(SendNioUdpClient.class);
	public static String logfile = "src/main/resources/accessLog.txt";
	private static Charset charSet = Charset.forName("UTF-8");

	private static DatagramChannelFactory channelFactory;
	private static ConnectionlessBootstrap bootstrap;
	private DatagramChannel clientChannel;
	private ChannelFuture future;
	private Tailer tailer;
	private final AtomicLong transLines = new AtomicLong();
	// must use Queues.newConcurrentLinkedQueue
	private final Queue<String> queue = Queues.newConcurrentLinkedQueue();
	private volatile boolean running = false;
	private final boolean isdebug = false;
	private String host = "127.0.0.1";
	private final int port;
	private static int bufferSize = 1024;
	private long ct = 0;
	/**
	 * The listener to notify of events when tailing.
	 */
	
	private long interval = 100;
	private File file;
	private boolean end = true;
	
	public SendNioUdpClient() {
		MongoManager mgo = MongoManager.getInstance();
		List<Map> list = mgo.query(Constants.SpoutTable, new BasicDBObject().append("desc", "spout").append("kind", "tcp"));
		int i = Common.getRandomInt(0, list.size() - 1);
		Map<String, String> mp = list.get(i);
		this.host = String.valueOf(mp.get("ip")) == null ? "127.0.0.1" : String.valueOf(mp.get("ip"));
		this.port = Integer.valueOf((String) mp.get("port")) == null ? 10236 : Integer.valueOf((String) mp.get("port"));
		this.file = new File("src/main/resources/accessLog.txt");
		dispclient();
		TailFile();
	}

	public SendNioUdpClient(String host, int port) {
		this.host = host;
		this.port = port;
		this.file = new File("src/main/resources/accessLog.txt");
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

	public SendNioUdpClient(String host, int port, File filename, boolean end) {
		this.host = host;
		this.port = port;
		this.end = end;
		this.file = filename;
		dispclient();
		TailFile();
	}

	public void TailFile() {
		Preconditions.checkArgument(file.isFile(), "TextFileSpout expects a file but '" + file.toString()
				+ "' is not exists.");
		// This listener send each file line in the queue
		TailerListener listener = new UdpQueueSender();
		tailer = new Tailer(this.file, listener, this.interval, this.end);
		// Start a tailer thread
		Thread thread = new Thread(tailer);
		thread.setDaemon(true);
		thread.start();
	}
	
	public void run() {
		this.running = true;
		// Configure the client.
		channelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool(), 4);
		bootstrap = new ConnectionlessBootstrap(channelFactory);
		
		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("framer", new LineBasedFrameDecoder(bufferSize));
				pipeline.addLast("decoder", new StringDecoder());
				pipeline.addLast("encoder", new StringEncoder());
				// and then business logic.
				pipeline.addLast("handler", new SendNioUdpClientHandler());
				return pipeline;
			}
			
		});
		
		// bootstrap.setOption("udpNoDelay", true);
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
					if (line != null) {
						channel.write(line);
						if (isdebug) {
							transLines.incrementAndGet();
							logger.info("poll queue " + transLines + " line= [" + line + "],size=" + line.length());
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
	public static void main(String[] args) throws Exception {
		// Print usage if no argument is specified.
		String[] args1 = new String[] { "localhost", "10237", "src/main/resources/accessLog.txt" };
		
		if (args.length < 2 || args.length > 3) {
			System.err.println("Usage: " + SendNioUdpClient.class.getName() + " <host> <port> [<accessFile>]");
			args = args1;
			// System.exit(0);
		}
		SendNioUdpClient cli = null;
		try {
			
			// Parse options.
			final String host = args[0];
			final int port = Integer.parseInt(args[1]);
			File logfile = new File(args[2]);
			// param end Set to true to tail from the end of the file, false to tail from the beginning of the file.
			System.out.println("sending " + host + " " + port + " " + logfile);
			cli = new SendNioUdpClient(host, port, logfile, false);
			cli.run();
		} catch (Exception e) {
			// TODO: handle exception
			cli.stop();
			System.exit(0);
		}
	}
}
