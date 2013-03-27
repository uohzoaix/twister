package com.twister.spout;

import java.nio.charset.Charset;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.DynamicChannelBuffer;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;

/**
 * @author guoqing
 * 
 */
public class SpoutEventHandler extends IAccessLogSpout {
	
	public SpoutEventHandler(int maxLength) {
		
	}
	
	private static final Logger logger = LoggerFactory.getLogger(SpoutEventHandler.class.getName());
	
	private long transLines = 0;
	
	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
		if (e instanceof ChannelStateEvent) {
			logger.info(e.toString());
		}
		// Let SimpleChannelHandler call actual event handler methods below.
		super.handleUpstream(ctx, e);
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		try {
			// Discard received data silently by doing nothing.
			String buffer = (String) e.getMessage();
			transLines += 1;
			logger.info("recvd length " + buffer.length() + "/" + transLines + " bytes [" + buffer.toString() + "]");
		} catch (Exception e2) {
			e2.printStackTrace();
		}
		
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		// Close the connection when an exception is raised.
		logger.warn("Unexpected exception from downstream.", e.getCause());
		// e.getChannel().close();
	}
	
}
