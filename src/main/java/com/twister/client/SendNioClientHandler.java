package com.twister.client;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.buffer.ChannelBuffer;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder
 * @author guoqing
 * 
 */
public class SendNioClientHandler extends SimpleChannelUpstreamHandler {
	
	private static final Logger logger = LoggerFactory.getLogger(SendNioClientHandler.class.getName());
	
	private long transferredBytes;
	private final SynchronousQueue<String> queue;
	/** Maximum length of a frame we're willing to decode. */
	private final int maxLength;
	private long transLines = 0;
	private final AtomicLong transline = new AtomicLong();
	private long pollcnt = 0;
	
	public long getTransferredBytes() {
		return transferredBytes;
	}
	
	public SendNioClientHandler(final SynchronousQueue<String> queue, final int maxLength) {
		this.queue = queue;
		this.maxLength = maxLength;
	}
	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
		if (e instanceof ChannelStateEvent) {
			if (((ChannelStateEvent) e).getState() != ChannelState.INTEREST_OPS) {
				logger.info("handleUpstream ==== " + e.toString());
			}
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
		// Keep sending messages whenever the current socket buffer has room.
		// tcp长连接
		
		SendHandle(ctx, e);
		
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		// back the received msg to the server
		// Server is supposed to send nothing. Therefore, do nothing.
		transline.incrementAndGet();
		String buffer = (String) e.getMessage();
		logger.info("back recvd length " + buffer.length() + "/" + transLines + " bytes [" + buffer.toString() + "]");
	}
	
	@Override
	public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) {
		transferredBytes += e.getWrittenAmount();
		
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		// Close the connection when an exception is raised.
		logger.warn("Unexpected exception from downstream.", e.getCause());
		e.getChannel().close();
	}
	
	public void SendHandle(ChannelHandlerContext ctx, ChannelStateEvent e) {
		Channel channel = e.getChannel();
		while (channel.isWritable()) {
			
			try {
				pollcnt++;
				String line = queue.poll(100, TimeUnit.MILLISECONDS);
				if (line != null) {
					channel.write(line);
					logger.info("from queue length=" + line.length() + "/" + pollcnt + " line= [" + line + "]");
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			
		}
		
	}
	
}