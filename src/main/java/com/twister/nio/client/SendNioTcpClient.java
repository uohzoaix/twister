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
import com.twister.entity.AccessLogAnalysis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * tail accesslog use tcp sent to nioserverspout
 * 
 * @author guoqing
 * 
 */
public class SendNioTcpClient implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(SendNioTcpClient.class);
	private static ChannelFactory channelFactory;
	private static ClientBootstrap bootstrap;
	private DatagramChannel clientChannel;
	private ChannelFuture future;
	private Tailer tailer;
	// Queues.newConcurrentLinkedQueue
	private final Queue<String> queue = Queues.newConcurrentLinkedQueue();
	
	private volatile boolean running = false;
	private final boolean isdebug = true;
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
	
	public SendNioTcpClient(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public SendNioTcpClient(String host, int port, String filename, boolean end) {
		this.host = host;
		this.port = port;
		this.end = end;
		file = new File(filename);
		Preconditions.checkArgument(file.isFile(), "TextFileSpout expects a file but '" + file.toString()
				+ "' is not exists.");
		// This listener send each file line in the queue
		TailerListener listener = new TcpQueueSender();
		tailer = new Tailer(this.file, listener, this.interval, this.end);
		// Start a tailer thread
		Thread thread = new Thread(tailer);
		thread.setDaemon(true);
		thread.start();
	}
	
	public void run() {
		try {
			this.running = true;
			// Configure the client.
			channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
					Executors.newCachedThreadPool(), 4, 4);
			bootstrap = new ClientBootstrap(channelFactory);
			
			// Set up the pipeline factory.
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					pipeline.addLast("framer", new LineBasedFrameDecoder(bufferSize));
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
					if (line != null) {
						channel.write(line);
						if (isdebug) {
							transline.incrementAndGet();
							logger.info(transline + " line=[" + line + "],size=" + line.length());
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

	public static void main(String[] args) throws Exception {
		// Print usage if no argument is specified.
		String[] args1 = new String[] { "localhost", "10236", "src/main/resources/accessLog.txt" };
		
		if (args.length < 2 || args.length > 3) {
			System.err.println("Usage: " + SendNioTcpClient.class.getName() + " <host> <port> [<accessFile>]");
			args = args1;
			// System.exit(0);
		}
		SendNioTcpClient cli = null;
		try {
			// Parse options.
			final String host = args[0];
			final int port = Integer.parseInt(args[1]);
			String logfile = args[2];
			System.out.println("sending " + host + " " + port + " " + logfile);
			cli = new SendNioTcpClient(host, port, logfile, false);
			cli.run();
		} catch (Exception e) {
			cli.stop();
			System.exit(0);
		}
		
	}
}
