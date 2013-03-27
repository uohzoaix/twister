package com.twister.spout;

import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;

import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichSpout;

//implements IComponent, IRichSpout
public abstract class IAccessLogSpout extends SimpleChannelUpstreamHandler {
	
	public IAccessLogSpout() {
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4454125352292243843L;
	
}
